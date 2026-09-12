package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/singledo"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/common/xsync"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type AutoURLTestOption struct {
	Tolerance       uint16   `group:"tolerance,omitempty"`
	AffinityTTL     int      `group:"affinity-ttl,omitempty"`
	IncludeDirect   *bool    `group:"include-direct,omitempty"`
	AutoSelect      *bool    `group:"auto-select,omitempty"`
	AutoSelectPorts []uint16 `group:"auto-select-ports,omitempty"`
	FixedPorts      []uint16 `group:"fixed-ports,omitempty"`
}

type affinityEntry struct {
	proxy  C.Proxy
	expire time.Time
}

type AutoURLTest struct {
	*GroupBase
	selected        string
	fastNode        C.Proxy
	fastSingle      *singledo.Single[C.Proxy]
	affinity        *xsync.Map[string, affinityEntry]
	probing         *xsync.Map[string, *singledo.Single[struct{}]]
	tolerance       uint16
	disableUDP      bool
	affinityTTL     atomic.Int64
	autoSelect      atomic.Bool
	autoSelectPorts map[uint16]struct{}
	fixedPorts      map[uint16]struct{}
	testUrl         string
	expectedStatus  string
}

var _ ProxyGroup = (*AutoURLTest)(nil)
var _ SelectAble = (*AutoURLTest)(nil)

// probeSemaphore caps the number of concurrently running per-target probes
// across all auto-url-test groups, bounding the dial/goroutine fan-out on
// high-cardinality traffic (scanners, P2P, many distinct destinations).
var probeSemaphore = make(chan struct{}, 8)

func (a *AutoURLTest) Now() string {
	return a.fast(false).Name()
}

func (a *AutoURLTest) Set(name string) error {
	var p C.Proxy
	for _, proxy := range a.GetProxies(false) {
		if proxy.Name() == name {
			p = proxy
			break
		}
	}
	if p == nil {
		return errors.New("proxy not exist")
	}
	a.ForceSet(name)
	return nil
}

func (a *AutoURLTest) ForceSet(name string) {
	a.selected = name
	a.fastSingle.Reset()
	// a manual pin wins over any existing per-target affinity
	a.affinity.Clear()
}

// shouldAutoSelect reports whether the given inbound port should use
// per-target auto selection. fixed-ports wins over auto-select-ports,
// and either list wins over the runtime autoSelect toggle.
func (a *AutoURLTest) shouldAutoSelect(metadata *C.Metadata) bool {
	if metadata == nil || metadata.InPort == 0 {
		return a.autoSelect.Load()
	}
	if _, ok := a.fixedPorts[metadata.InPort]; ok {
		return false
	}
	if _, ok := a.autoSelectPorts[metadata.InPort]; ok {
		return true
	}
	return a.autoSelect.Load()
}

// portSet indexes a port list for O(1) membership tests; nil when empty.
// Built once at construction and never mutated afterwards.
func portSet(ports []uint16) map[uint16]struct{} {
	if len(ports) == 0 {
		return nil
	}
	set := make(map[uint16]struct{}, len(ports))
	for _, port := range ports {
		set[port] = struct{}{}
	}
	return set
}

// sortedPorts returns the ports of a port set in ascending order for a
// stable JSON output; an empty (non-nil) slice when the set is empty.
func sortedPorts(set map[uint16]struct{}) []uint16 {
	if len(set) == 0 {
		return []uint16{}
	}
	ports := make([]uint16, 0, len(set))
	for port := range set {
		ports = append(ports, port)
	}
	sort.Slice(ports, func(i, j int) bool { return ports[i] < ports[j] })
	return ports
}

// DialContext implements C.ProxyAdapter
func (a *AutoURLTest) DialContext(ctx context.Context, metadata *C.Metadata) (c C.Conn, err error) {
	proxy := a.fast(true)

	key := ""
	fromAffinity := false
	if a.shouldAutoSelect(metadata) {
		// per-target affinity lookup and background probing only apply
		// while auto-select is active for this inbound port (fixed-ports
		// and auto-select-ports override the runtime toggle); otherwise
		// every dial goes straight through the current fastest node like
		// a plain url-test group
		if metadata != nil && metadata.Valid() {
			key = metadata.RemoteAddress()
		}
		if key != "" {
			if entry, ok := a.getAffinity(key); ok {
				proxy = entry
				fromAffinity = true
			} else {
				go a.probeTarget(key)
			}
		}
	}

	c, err = proxy.DialContext(ctx, metadata)
	if err != nil && fromAffinity {
		a.affinity.Delete(key)
		proxy = a.fast(true)
		c, err = proxy.DialContext(ctx, metadata)
		go a.probeTarget(key)
	}

	if err == nil {
		c.AppendToChains(a)
	} else {
		a.onDialFailed(proxy.Type(), err, a.healthCheck)
	}

	if N.NeedHandshake(c) {
		c = callback.NewFirstWriteCallBackConn(c, func(err error) {
			if err == nil {
				a.onDialSuccess()
			} else {
				a.onDialFailed(proxy.Type(), err, a.healthCheck)
			}
		})
	}

	return c, err
}

// ListenPacketContext implements C.ProxyAdapter
func (a *AutoURLTest) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	proxy := a.fast(true)
	pc, err := proxy.ListenPacketContext(ctx, metadata)
	if err == nil {
		pc.AppendToChains(a)
	} else {
		a.onDialFailed(proxy.Type(), err, a.healthCheck)
	}

	return pc, err
}

// Unwrap implements C.ProxyAdapter
func (a *AutoURLTest) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	return a.fast(touch)
}

func (a *AutoURLTest) getAffinity(key string) (C.Proxy, bool) {
	entry, ok := a.affinity.Load(key)
	if !ok {
		return nil, false
	}
	if !time.Now().Before(entry.expire) || !entry.proxy.AliveForTestUrl(a.testUrl) {
		a.affinity.Delete(key)
		return nil, false
	}
	return entry.proxy, true
}

func (a *AutoURLTest) probeTarget(key string) {
	s, loaded := a.probing.LoadOrStore(key, singledo.NewSingle[struct{}](time.Second*10))
	_, _, _ = s.Do(func() (struct{}, error) {
		a.doProbe(key)
		return struct{}{}, nil
	})
	if !loaded {
		a.probing.Delete(key)
	}
}

func (a *AutoURLTest) doProbe(key string) {
	probeCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(a.testTimeout))
	defer cancel()

	// bound global probe fan-out so high-cardinality destinations cannot spawn
	// an unbounded number of concurrent per-group dials
	select {
	case probeSemaphore <- struct{}{}:
		defer func() { <-probeSemaphore }()
	case <-probeCtx.Done():
		return
	}

	mp := a.tcpProbe(probeCtx, key)

	var best C.Proxy
	var minDelay uint16 = 0xffff
	for _, proxy := range a.GetProxies(false) {
		delay, ok := mp[proxy.Name()]
		if !ok || delay == 0 {
			continue
		}
		if delay < minDelay {
			minDelay = delay
			best = proxy
		}
	}
	if best != nil {
		if !a.autoSelect.Load() && len(a.autoSelectPorts) == 0 {
			// auto-select was disabled while this probe was in flight and
			// no auto-select-ports are configured to keep per-target
			// selection alive; its result must not be recorded as
			// per-target affinity
			return
		}
		a.cleanExpiredAffinity()
		a.affinity.Store(key, affinityEntry{
			proxy:  best,
			expire: time.Now().Add(time.Duration(a.affinityTTL.Load())),
		})
	}
}

func (a *AutoURLTest) cleanExpiredAffinity() {
	now := time.Now()
	a.affinity.Range(func(key string, entry affinityEntry) bool {
		if !now.Before(entry.expire) {
			a.affinity.Delete(key)
		}
		return true
	})
}

func (a *AutoURLTest) tcpProbe(ctx context.Context, key string) map[string]uint16 {
	mp := map[string]uint16{}
	var wg sync.WaitGroup
	var lock sync.Mutex

	var m C.Metadata
	if err := m.SetRemoteAddress(key); err != nil {
		return mp
	}
	m.NetWork = C.TCP

	proxies := a.GetProxies(false)
	for _, proxy := range proxies {
		proxy := proxy
		wg.Add(1)
		go func() {
			defer wg.Done()
			meta := m
			start := time.Now()
			c, err := proxy.DialContext(ctx, &meta)
			if err == nil {
				_ = c.Close()
				lock.Lock()
				mp[proxy.Name()] = uint16(time.Since(start) / time.Millisecond)
				lock.Unlock()
			}
		}()
	}
	wg.Wait()

	return mp
}

func (a *AutoURLTest) healthCheck() {
	a.fastSingle.Reset()
	a.GroupBase.healthCheck()
	a.fastSingle.Reset()
}

func (a *AutoURLTest) fast(touch bool) C.Proxy {
	elm, _, shared := a.fastSingle.Do(func() (C.Proxy, error) {
		proxies := a.GetProxies(touch)
		if a.selected != "" {
			for _, proxy := range proxies {
				if !proxy.AliveForTestUrl(a.testUrl) {
					continue
				}
				if proxy.Name() == a.selected {
					a.fastNode = proxy
					return proxy, nil
				}
			}
		}

		fast := proxies[0]
		minDelay := fast.LastDelayForTestUrl(a.testUrl)
		fastNotExist := true

		for _, proxy := range proxies[1:] {
			if a.fastNode != nil && proxy.Name() == a.fastNode.Name() {
				fastNotExist = false
			}

			if !proxy.AliveForTestUrl(a.testUrl) {
				continue
			}

			delay := proxy.LastDelayForTestUrl(a.testUrl)
			if delay < minDelay {
				fast = proxy
				minDelay = delay
			}

		}
		// tolerance
		if a.fastNode == nil || fastNotExist || !a.fastNode.AliveForTestUrl(a.testUrl) || a.fastNode.LastDelayForTestUrl(a.testUrl) > fast.LastDelayForTestUrl(a.testUrl)+a.tolerance {
			a.fastNode = fast
		}
		return a.fastNode, nil
	})
	if shared && touch { // a shared fastSingle.Do() may cause providers untouched, so we touch them again
		a.Touch()
	}

	return elm
}

// SupportUDP implements C.ProxyAdapter
func (a *AutoURLTest) SupportUDP() bool {
	if a.disableUDP {
		return false
	}
	return a.fast(false).SupportUDP()
}

// IsL3Protocol implements C.ProxyAdapter
func (a *AutoURLTest) IsL3Protocol(metadata *C.Metadata) bool {
	return a.fast(false).IsL3Protocol(metadata)
}

// MarshalJSON implements C.ProxyAdapter
func (a *AutoURLTest) MarshalJSON() ([]byte, error) {
	all := []string{}
	for _, proxy := range a.GetProxies(false) {
		all = append(all, proxy.Name())
	}
	affinity := map[string]any{}
	now := time.Now()
	a.affinity.Range(func(key string, entry affinityEntry) bool {
		if !now.Before(entry.expire) {
			a.affinity.Delete(key)
			return true
		}
		affinity[key] = map[string]any{
			"proxy":  entry.proxy.Name(),
			"expire": entry.expire.UnixMilli(),
		}
		return true
	})
	return json.Marshal(map[string]any{
		"type":            a.Type().String(),
		"now":             a.Now(),
		"all":             all,
		"testUrl":         a.testUrl,
		"expectedStatus":  a.expectedStatus,
		"fixed":           a.selected,
		"hidden":          a.Hidden(),
		"icon":            a.Icon(),
		"emptyFallback":   a.EmptyFallback().Name(),
		"autoSelect":      a.autoSelect.Load(),
		"autoSelectPorts": sortedPorts(a.autoSelectPorts),
		"fixedPorts":      sortedPorts(a.fixedPorts),
		"affinityTTL":     time.Duration(a.affinityTTL.Load()) / time.Second,
		"affinity":        affinity,
	})
}

func (a *AutoURLTest) Providers() []P.ProxyProvider {
	return a.providers
}

func (a *AutoURLTest) Proxies() []C.Proxy {
	return a.GetProxies(false)
}

// URLTest implements ProxyGroup
func (a *AutoURLTest) URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (map[string]uint16, error) {
	return a.GroupBase.URLTest(ctx, a.testUrl, expectedStatus)
}

// Adapter implements C.Proxy
func (a *AutoURLTest) Adapter() C.ProxyAdapter {
	return a
}

// AliveForTestUrl implements C.Proxy
func (a *AutoURLTest) AliveForTestUrl(url string) bool {
	return a.fast(false).AliveForTestUrl(url)
}

// DelayHistory implements C.Proxy
func (a *AutoURLTest) DelayHistory() []C.DelayHistory {
	return a.fast(false).DelayHistory()
}

// ExtraDelayHistories implements C.Proxy
func (a *AutoURLTest) ExtraDelayHistories() map[string]C.ProxyState {
	return a.fast(false).ExtraDelayHistories()
}

// LastDelayForTestUrl implements C.Proxy
func (a *AutoURLTest) LastDelayForTestUrl(url string) uint16 {
	return a.fast(false).LastDelayForTestUrl(url)
}

func (a *AutoURLTest) SetAffinityTTL(sec int) {
	if sec <= 0 {
		sec = 600
	}
	a.affinityTTL.Store(int64(time.Duration(sec) * time.Second))
}

func (a *AutoURLTest) ClearAffinity() {
	a.affinity.Clear()
}

// SetAutoSelect toggles per-target affinity auto-selection as the default
// for inbound ports not covered by fixed-ports/auto-select-ports. When
// disabled, those ports degrade to plain url-test behavior: every dial goes
// straight through the current fastest (fixed) node and no per-target
// affinity lookup, background probing or affinity recording happens.
func (a *AutoURLTest) SetAutoSelect(b bool) {
	if a.autoSelect.Load() == b {
		return
	}
	a.autoSelect.Store(b)
	if !b {
		// dropping auto-selection also drops all recorded per-target
		// affinity and in-flight probe state, and forces the fast node to
		// be re-evaluated on the next dial
		a.affinity.Clear()
		a.probing.Range(func(key string, s *singledo.Single[struct{}]) bool {
			a.probing.Delete(key)
			return true
		})
		a.fastSingle.Reset()
	}
}

func NewAutoURLTest(option GroupCommonOption, opt AutoURLTestOption, emptyFallback C.Proxy, providers []P.ProxyProvider) (*AutoURLTest, error) {
	if emptyFallback == nil {
		return nil, errors.New("empty fallback proxy not exist")
	}
	if opt.AffinityTTL <= 0 {
		opt.AffinityTTL = 600
	}
	return &AutoURLTest{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:           option.Name,
			Type:           C.AutoURLTest,
			Hidden:         option.Hidden,
			Icon:           option.Icon,
			Filter:         option.Filter,
			ExcludeFilter:  option.ExcludeFilter,
			ExcludeType:    option.ExcludeType,
			TestTimeout:    option.TestTimeout,
			MaxFailedTimes: option.MaxFailedTimes,
			EmptyFallback:  emptyFallback,
			Providers:      providers,
		}),
		fastSingle:      singledo.NewSingle[C.Proxy](time.Second * 10),
		affinity:        &xsync.Map[string, affinityEntry]{},
		probing:         &xsync.Map[string, *singledo.Single[struct{}]]{},
		tolerance:       opt.Tolerance,
		disableUDP:      option.DisableUDP,
		affinityTTL:     atomic.NewInt64(int64(time.Duration(opt.AffinityTTL) * time.Second)),
		autoSelect:      atomic.NewBool(opt.AutoSelect == nil || *opt.AutoSelect),
		autoSelectPorts: portSet(opt.AutoSelectPorts),
		fixedPorts:      portSet(opt.FixedPorts),
		testUrl:         option.URL,
		expectedStatus:  option.ExpectedStatus,
	}, nil
}
