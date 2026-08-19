package route

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/metacubex/mihomo/common/yaml"
	"github.com/metacubex/mihomo/component/dialer"
	mihomoHttp "github.com/metacubex/mihomo/component/http"
	"github.com/metacubex/mihomo/config"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/hub/executor"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"

	"github.com/metacubex/chi"
	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

// subscriptionsMu serializes every subscription mutation, so concurrent API
// calls can never interleave file/meta/runtime updates.
var subscriptionsMu sync.Mutex

const (
	maxSubscriptionSize = 64 << 20 // 64MB safety cap for downloaded content
	subconverterWait    = 10 * time.Second
	mergeTimeout        = 30 * time.Second
)

// errRejectedSubscription marks a merged runtime that tried to smuggle a
// top-level `subscription:` section; handlers translate it to 400.
var errRejectedSubscription = errors.New("merged runtime contained a top-level 'subscription' section")

type subscriptionCtxKey struct{}

// subscriptionProfile is one entry of the clashctl-compatible profiles.yaml meta file.
type subscriptionProfile struct {
	ID   int    `yaml:"id" json:"id"`
	Path string `yaml:"path" json:"path"`
	URL  string `yaml:"url" json:"url"`
}

type subscriptionMeta struct {
	Use      int                   `yaml:"use" json:"use"`
	Profiles []subscriptionProfile `yaml:"profiles" json:"profiles"`
}

func subscriptionRouter() http.Handler {
	r := chi.NewRouter()
	r.Use(checkSubscriptionEnabled)
	r.Get("/", getSubscriptions)
	r.Post("/", addSubscription)
	r.Route("/{id}", func(r chi.Router) {
		r.Use(parseSubscriptionID)
		r.Put("/", updateSubscription)
		r.Delete("/", deleteSubscription)
		r.Post("/update", updateSubscriptionContent)
		r.Post("/use", useSubscription)
	})
	return r
}

func checkSubscriptionEnabled(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !config.GetSubscriptionCfg().Enable {
			render.Status(r, http.StatusForbidden)
			render.JSON(w, r, newError("subscription management is disabled in config"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func parseSubscriptionID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// id must be a plain positive integer: it is used to build file paths
		// under the subscription directory, which prevents directory traversal
		id, err := strconv.Atoi(chi.URLParam(r, "id"))
		if err != nil || id <= 0 {
			render.Status(r, http.StatusNotFound)
			render.JSON(w, r, ErrNotFound)
			return
		}
		ctx := context.WithValue(r.Context(), subscriptionCtxKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func subscriptionIDFromRequest(r *http.Request) int {
	id, _ := r.Context().Value(subscriptionCtxKey{}).(int)
	return id
}

// subscriptionSettings resolves the raw subscription config with defaults applied.
func subscriptionSettings() config.RawSubscription {
	cfg := config.GetSubscriptionCfg()
	if cfg.SubconverterPort == 0 {
		cfg.SubconverterPort = 25500
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 30
	}
	if cfg.UA == "" {
		cfg.UA = "mihomo"
	}
	if cfg.Directory == "" {
		cfg.Directory = C.Path.HomeDir()
	}
	if !filepath.IsAbs(cfg.Directory) {
		cfg.Directory = filepath.Join(C.Path.HomeDir(), cfg.Directory)
	}
	cfg.Directory = filepath.Clean(cfg.Directory)
	if cfg.MetaFile == "" {
		cfg.MetaFile = filepath.Join(cfg.Directory, "profiles.yaml")
	} else if !filepath.IsAbs(cfg.MetaFile) {
		cfg.MetaFile = filepath.Join(cfg.Directory, cfg.MetaFile)
	}
	if cfg.MixinFile == "" {
		cfg.MixinFile = filepath.Join(cfg.Directory, "mixin.yaml")
	} else if !filepath.IsAbs(cfg.MixinFile) {
		cfg.MixinFile = filepath.Join(cfg.Directory, cfg.MixinFile)
	}
	if cfg.RuntimeFile == "" {
		cfg.RuntimeFile = filepath.Join(cfg.Directory, "runtime.yaml")
	} else if !filepath.IsAbs(cfg.RuntimeFile) {
		cfg.RuntimeFile = filepath.Join(cfg.Directory, cfg.RuntimeFile)
	}
	return cfg
}

func isPathWithin(base, p string) bool {
	rel, err := filepath.Rel(base, p)
	if err != nil {
		return false
	}
	return filepath.IsLocal(rel)
}

// subscriptionProfilePath returns the profile file path for the given id,
// guaranteeing the result stays inside the subscription directory.
func subscriptionProfilePath(settings config.RawSubscription, id int) (string, error) {
	p := filepath.Join(settings.Directory, "profiles", strconv.Itoa(id)+".yaml")
	if !isPathWithin(settings.Directory, p) {
		return "", errors.New("profile path escaped the subscription directory")
	}
	return p, nil
}

func loadSubscriptionMeta(settings config.RawSubscription) (subscriptionMeta, error) {
	var meta subscriptionMeta
	data, err := os.ReadFile(settings.MetaFile)
	if err != nil {
		if os.IsNotExist(err) {
			return meta, nil
		}
		return meta, err
	}
	if err := yaml.Unmarshal(data, &meta); err != nil {
		return meta, err
	}
	// never trust paths stored in the meta file: recompute any path that is
	// not inside the subscription directory (e.g. profiles.yaml written by
	// clashctl with absolute paths)
	for i := range meta.Profiles {
		if !isPathWithin(settings.Directory, meta.Profiles[i].Path) {
			if p, err := subscriptionProfilePath(settings, meta.Profiles[i].ID); err == nil {
				meta.Profiles[i].Path = p
			}
		}
	}
	return meta, nil
}

func saveSubscriptionMeta(settings config.RawSubscription, meta subscriptionMeta) error {
	data, err := yaml.Marshal(meta)
	if err != nil {
		return err
	}
	return atomicWriteFile(settings.MetaFile, data, 0o600)
}

// atomicWriteFile writes data to a temp file in the same directory and
// renames it over path, so readers never observe a partially written file.
func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-subscription-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after successful rename
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func findSubscription(meta subscriptionMeta, id int) (subscriptionProfile, bool) {
	for _, p := range meta.Profiles {
		if p.ID == id {
			return p, true
		}
	}
	return subscriptionProfile{}, false
}

func nextSubscriptionID(meta subscriptionMeta) int {
	maxID := 0
	for _, p := range meta.Profiles {
		if p.ID > maxID {
			maxID = p.ID
		}
	}
	return maxID + 1
}

func validSubscriptionURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	return u.Scheme == "http" || u.Scheme == "https"
}

// downloadAndConvert downloads the subscription and, when a subconverter
// binary is configured, converts it to a clash config through it.
func downloadAndConvert(ctx context.Context, settings config.RawSubscription, subURL string) ([]byte, error) {
	data, err := downloadSubscriptionContent(ctx, settings, subURL)
	if err != nil {
		return nil, err
	}
	if settings.Subconverter == "" {
		return data, nil
	}
	return convertSubscription(ctx, settings, subURL)
}

func downloadSubscriptionContent(ctx context.Context, settings config.RawSubscription, subURL string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(settings.Timeout)*time.Second)
	defer cancel()

	header := map[string][]string{"User-Agent": {settings.UA}}
	// dial directly instead of routing through the tunnel, mirroring the
	// clashctl curl behavior: re-downloads must work even when the currently
	// active subscription's proxies are unreachable
	directDialer := dialer.NewDialer()
	resp, err := mihomoHttp.HttpRequest(ctx, subURL, http.MethodGet, header, nil, mihomoHttp.WithDialer(directDialer))
	if err != nil {
		return nil, fmt.Errorf("download subscription failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download subscription failed: unexpected status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxSubscriptionSize))
	if err != nil {
		return nil, fmt.Errorf("read subscription response failed: %w", err)
	}
	if len(data) == 0 {
		return nil, errors.New("subscription content is empty")
	}
	return data, nil
}

// subconverterEffectivePort returns the port the subconverter binary will
// actually listen on. The subconverter reads its own pref.yml (server.port)
// and ignores CLI flags, so that file is the source of truth. When it cannot
// be read (or has no server.port), the configured subconverter-port is used.
func subconverterEffectivePort(settings config.RawSubscription) int {
	if settings.Subconverter != "" {
		prefPath := filepath.Join(filepath.Dir(settings.Subconverter), "pref.yml")
		if data, err := os.ReadFile(prefPath); err == nil {
			var doc map[string]any
			if err := yaml.Unmarshal(data, &doc); err == nil {
				if server, ok := doc["server"].(map[string]any); ok {
					if port, ok := server["port"].(int); ok && port > 0 {
						return port
					}
				}
			}
		}
	}
	return settings.SubconverterPort
}

// startSubconverter launches the subconverter binary and waits until its
// HTTP port accepts requests. It returns a cleanup func that terminates the
// process (SIGTERM, then SIGKILL after a grace period).
func startSubconverter(ctx context.Context, settings config.RawSubscription) (cleanup func(), err error) {
	cmd := exec.CommandContext(ctx, settings.Subconverter)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start subconverter failed: %w", err)
	}

	cleanup = func() {
		if cmd.Process == nil {
			return
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	}

	checkURL := fmt.Sprintf("http://127.0.0.1:%d/version", subconverterEffectivePort(settings))
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(subconverterWait)
	for time.Now().Before(deadline) {
		resp, err := client.Get(checkURL)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return cleanup, nil
			}
		}
		select {
		case <-ctx.Done():
			cleanup()
			return nil, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	cleanup()
	return nil, fmt.Errorf("subconverter did not become ready within %s: %s", subconverterWait, buf.String())
}

func convertSubscription(ctx context.Context, settings config.RawSubscription, subURL string) ([]byte, error) {
	cleanup, err := startSubconverter(ctx, settings)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	q := url.Values{}
	q.Set("target", "clash")
	q.Set("url", subURL)
	convertURL := fmt.Sprintf("http://127.0.0.1:%d/sub?%s", subconverterEffectivePort(settings), q.Encode())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, convertURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", settings.UA)

	client := &http.Client{Timeout: time.Duration(settings.Timeout) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("subconverter request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("subconverter returned status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxSubscriptionSize))
	if err != nil {
		return nil, fmt.Errorf("read subconverter response failed: %w", err)
	}
	if len(data) == 0 {
		return nil, errors.New("subconverter returned empty content")
	}
	return data, nil
}

// sanitizeUntrustedConfig strips the top-level "subscription" key from bytes
// derived from subscription content (downloads or merged runtime). The global
// subscription config must only ever come from the operator's main config
// file: a low-trust subscription carrying its own `subscription:` section must
// not be able to redirect the API to attacker-controlled paths/binaries.
// It returns the sanitized bytes and whether the key was present.
func sanitizeUntrustedConfig(data []byte) ([]byte, bool, error) {
	var doc map[string]any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, false, err
	}
	_, hadKey := doc["subscription"]
	delete(doc, "subscription")
	out, err := yaml.Marshal(doc)
	if err != nil {
		return nil, false, err
	}
	return out, hadKey, nil
}

// validateSubscriptionContent makes sure the downloaded content parses as a
// mihomo config, contains at least one proxy or proxy-provider, and does not
// try to smuggle a `subscription:` section into the running process.
func validateSubscriptionContent(data []byte) error {
	sanitized, hadKey, err := sanitizeUntrustedConfig(data)
	if err != nil {
		return fmt.Errorf("subscription is not a valid mihomo config: %w", err)
	}
	if hadKey {
		return errors.New("subscription content must not contain a top-level 'subscription' section")
	}

	// the sanitized bytes never contain a top-level `subscription:` key, so
	// UnmarshalRawConfig cannot clobber the global subscription config; parse
	// without a save/restore wrapper (which would race concurrent config reloads)
	rawCfg, err := config.UnmarshalRawConfig(sanitized)
	if err != nil {
		return fmt.Errorf("subscription is not a valid mihomo config: %w", err)
	}
	if len(rawCfg.Proxy) == 0 && len(rawCfg.ProxyProvider) == 0 {
		return errors.New("subscription contains no proxies or proxy-providers")
	}
	return nil
}

// mergeWithMixin runs the clashctl-compatible yq merge expression over the
// profile and mixin files and returns the runtime config bytes.
func mergeWithMixin(ctx context.Context, settings config.RawSubscription, profilePath string) ([]byte, error) {
	if _, err := os.Stat(settings.MixinFile); err != nil {
		if os.IsNotExist(err) {
			return os.ReadFile(profilePath) // no mixin: runtime equals the profile
		}
		return nil, err
	}

	// the merge expression is long, write it to a temp script file so no
	// shell escaping or argv length concerns apply (os/exec never invokes a shell)
	exprPath, err := writeMergeExpressionFile(settings.Directory)
	if err != nil {
		return nil, err
	}
	defer os.Remove(exprPath)

	ctx, cancel := context.WithTimeout(ctx, mergeTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, settings.Yq, "eval-all", "--from-file", exprPath, profilePath, settings.MixinFile)
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("merge profile with mixin failed: %w: %s", err, errBuf.String())
	}
	return out.Bytes(), nil
}

func writeMergeExpressionFile(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-merge-*.yq")
	if err != nil {
		return "", err
	}
	name := tmp.Name()
	if _, err := tmp.WriteString(subscriptionMergeExpression); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return "", err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return "", err
	}
	return name, nil
}

// reloadRuntime validates the merged runtime config and applies it without
// killing the process, then persists it to runtime.yaml.
func reloadRuntime(ctx context.Context, settings config.RawSubscription, runtimeData []byte) error {
	// the merged runtime is derived from untrusted subscription content:
	// a top-level `subscription:` section must never reach the parser, as it
	// would redirect the API to attacker-controlled paths/binaries. Reject it.
	sanitized, hadKey, err := sanitizeUntrustedConfig(runtimeData)
	if err != nil {
		return fmt.Errorf("merged runtime config is invalid: %w", err)
	}
	if hadKey {
		return errRejectedSubscription
	}

	runtimeData, err = ensureAutoGroup(sanitized)
	if err != nil {
		return fmt.Errorf("merged runtime config is invalid: %w", err)
	}

	// runtimeData has been sanitized (no top-level `subscription:` key) and is
	// validated below; parsing cannot clobber the global subscription config,
	// and a save/restore wrapper would race concurrent config reloads
	cfg, err := executor.ParseWithBytes(runtimeData)
	if err != nil {
		return fmt.Errorf("merged runtime config is invalid: %w", err)
	}
	if err := atomicWriteFile(settings.RuntimeFile, runtimeData, 0o644); err != nil {
		return err
	}
	executor.ApplyConfig(cfg, true)
	log.Infoln("[SUBSCRIPTION] runtime config reloaded from %s", settings.RuntimeFile)
	return nil
}

// ensureAutoGroup guarantees the runtime config carries the default Auto
// (auto-url-test) group and a MATCH,Auto fallback rule, so "use this group by
// default" holds even when a subscription's own proxy-groups replace the
// operator's groups. It is a no-op when an Auto group is already present.
func ensureAutoGroup(data []byte) ([]byte, error) {
	var doc map[string]any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}

	groups, _ := doc["proxy-groups"].([]any)
	for _, g := range groups {
		group, ok := g.(map[string]any)
		if !ok {
			continue
		}
		if name, _ := group["name"].(string); name == "Auto" {
			return data, nil
		}
	}

	// collect all proxy names from the runtime to build the Auto group's list
	var names []any
	names = append(names, "DIRECT")
	if proxies, ok := doc["proxies"].([]any); ok {
		for _, p := range proxies {
			proxy, ok := p.(map[string]any)
			if !ok {
				continue
			}
			if name, ok := proxy["name"].(string); ok {
				names = append(names, name)
			}
		}
	}

	auto := map[string]any{
		"name":            "Auto",
		"type":            "auto-url-test",
		"url":             C.DefaultTestURL,
		"expected-status": "*",
		"include-direct":  true,
		"affinity-ttl":    600,
		"proxies":         names,
	}
	doc["proxy-groups"] = append(groups, auto)

	// ensure a MATCH,Auto fallback rule so the Auto group is actually used:
	// append it only when no Match rule exists (a user-defined Match wins)
	rules, _ := doc["rules"].([]any)
	hasMatch := false
	for _, rule := range rules {
		rs, ok := rule.(string)
		if !ok {
			continue
		}
		if strings.HasPrefix(strings.ToUpper(rs), "MATCH,") {
			hasMatch = true
			break
		}
	}
	if !hasMatch {
		doc["rules"] = append(rules, "MATCH,Auto")
	}

	out, err := yaml.Marshal(doc)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// waitForKernelIdle blocks until the kernel finishes the ongoing config
// application. Parsing a config (executor.ParseWithBytes) temporarily reads
// live kernel state through config.temporaryUpdateGeneral, so parsing while
// an ApplyConfig is in flight races on that state (upstream design). Waiting
// for the tunnel to reach Running plus a small grace keeps the subscription
// restore/reloads from overlapping a startup or reload apply.
func waitForKernelIdle() {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if tunnel.Status() == tunnel.Running {
			// OnRunning() is stored right before the tail of ApplyConfig
			// (updateUpdater/resolver.ResetConnection), leave a small grace
			time.Sleep(200 * time.Millisecond)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	log.Warnln("[SUBSCRIPTION] kernel did not become idle within 30s, proceeding anyway")
}

// activateSubscription makes id the active subscription: merge profile with
// mixin, write runtime.yaml, reload the kernel and persist `use` in the meta
// file. When yq is not configured it only records the selection and returns a
// hint, since no runtime merge/reload is possible.
func activateSubscription(ctx context.Context, settings config.RawSubscription, meta subscriptionMeta, id int) (string, error) {
	profile, ok := findSubscription(meta, id)
	if !ok {
		return "", fmt.Errorf("subscription %d not found", id)
	}

	if settings.Yq == "" {
		meta.Use = id
		if err := saveSubscriptionMeta(settings, meta); err != nil {
			return "", err
		}
		log.Infoln("[SUBSCRIPTION] set active subscription %d (yq not configured, kernel not reloaded)", id)
		return "yq is not configured, subscription selected without merging or reload", nil
	}

	// never parse/apply while another config application is in flight
	waitForKernelIdle()

	runtimeData, err := mergeWithMixin(ctx, settings, profile.Path)
	if err != nil {
		return "", err
	}
	if err := reloadRuntime(ctx, settings, runtimeData); err != nil {
		return "", err
	}

	meta.Use = id
	if err := saveSubscriptionMeta(settings, meta); err != nil {
		return "", err
	}
	log.Infoln("[SUBSCRIPTION] set active subscription %d", id)
	return "", nil
}

// restoreActiveSubscription re-applies the active subscription recorded in the
// meta file whenever the route server is (re)created, so the kernel state of
// the previous run is restored on process restart AND after any main config
// reload (which resets the kernel to the base config). It is serialized
// against API mutations. Errors are logged, never fatal.
func restoreActiveSubscription() {
	subscriptionsMu.Lock()
	defer subscriptionsMu.Unlock()

	settings := subscriptionSettings()
	if !settings.Enable {
		return
	}

	meta, err := loadSubscriptionMeta(settings)
	if err != nil {
		log.Warnln("[SUBSCRIPTION] restore active subscription failed to read meta: %v", err)
		return
	}
	if meta.Use <= 0 {
		return
	}
	if _, ok := findSubscription(meta, meta.Use); !ok {
		log.Warnln("[SUBSCRIPTION] active subscription %d not found in meta, skip restore", meta.Use)
		return
	}

	log.Infoln("[SUBSCRIPTION] restoring active subscription %d", meta.Use)
	message, err := activateSubscription(context.Background(), settings, meta, meta.Use)
	if err != nil {
		log.Warnln("[SUBSCRIPTION] restore active subscription %d failed: %v", meta.Use, err)
		return
	}
	if message != "" {
		log.Infoln("[SUBSCRIPTION] %s", message)
	}
}

func getSubscriptions(w http.ResponseWriter, r *http.Request) {
	settings := subscriptionSettings()
	meta, err := loadSubscriptionMeta(settings)
	if err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}

	profiles := make([]render.M, 0, len(meta.Profiles))
	for _, p := range meta.Profiles {
		updatedAt := ""
		if fi, err := os.Stat(p.Path); err == nil {
			updatedAt = fi.ModTime().Format(time.RFC3339)
		}
		profiles = append(profiles, render.M{
			"id":        p.ID,
			"url":       p.URL,
			"path":      p.Path,
			"updatedAt": updatedAt,
		})
	}

	render.JSON(w, r, render.M{
		"profiles":  profiles,
		"use":       meta.Use,
		"enable":    settings.Enable,
		"directory": settings.Directory,
	})
}

func addSubscription(w http.ResponseWriter, r *http.Request) {
	req := struct {
		URL string `json:"url"`
		Use bool   `json:"use"`
	}{}
	if err := render.DecodeJSON(r.Body, &req); err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, ErrBadRequest)
		return
	}
	req.URL = strings.TrimSpace(req.URL)
	if !validSubscriptionURL(req.URL) {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("invalid subscription url"))
		return
	}

	subscriptionsMu.Lock()
	defer subscriptionsMu.Unlock()

	settings := subscriptionSettings()
	meta, err := loadSubscriptionMeta(settings)
	if err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}

	id := nextSubscriptionID(meta)
	path, err := subscriptionProfilePath(settings, id)
	if err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError(err.Error()))
		return
	}

	data, err := downloadAndConvert(r.Context(), settings, req.URL)
	if err != nil {
		render.Status(r, http.StatusBadGateway)
		render.JSON(w, r, newError(err.Error()))
		return
	}
	if err := validateSubscriptionContent(data); err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError(err.Error()))
		return
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}
	if err := atomicWriteFile(path, data, 0o644); err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}

	meta.Profiles = append(meta.Profiles, subscriptionProfile{ID: id, Path: path, URL: req.URL})
	if err := saveSubscriptionMeta(settings, meta); err != nil {
		_ = os.Remove(path) // roll back the orphaned profile file
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}
	log.Infoln("[SUBSCRIPTION] added subscription %d: %s", id, req.URL)

	message := ""
	if req.Use {
		if message, err = activateSubscription(r.Context(), settings, meta, id); err != nil {
			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, newError(err.Error()))
			return
		}
	}

	resp := render.M{"id": id, "status": "ok"}
	if message != "" {
		resp["message"] = message
	}
	render.Status(r, http.StatusCreated)
	render.JSON(w, r, resp)
}

func updateSubscription(w http.ResponseWriter, r *http.Request) {
	id := subscriptionIDFromRequest(r)
	req := struct {
		URL string `json:"url"`
	}{}
	if err := render.DecodeJSON(r.Body, &req); err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, ErrBadRequest)
		return
	}
	req.URL = strings.TrimSpace(req.URL)
	if !validSubscriptionURL(req.URL) {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("invalid subscription url"))
		return
	}

	subscriptionsMu.Lock()
	defer subscriptionsMu.Unlock()

	settings := subscriptionSettings()
	meta, err := loadSubscriptionMeta(settings)
	if err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}
	profile, ok := findSubscription(meta, id)
	if !ok {
		render.Status(r, http.StatusNotFound)
		render.JSON(w, r, ErrNotFound)
		return
	}

	data, err := downloadAndConvert(r.Context(), settings, req.URL)
	if err != nil {
		render.Status(r, http.StatusBadGateway)
		render.JSON(w, r, newError(err.Error()))
		return
	}
	if err := validateSubscriptionContent(data); err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError(err.Error()))
		return
	}
	if err := atomicWriteFile(profile.Path, data, 0o644); err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}

	for i := range meta.Profiles {
		if meta.Profiles[i].ID == id {
			meta.Profiles[i].URL = req.URL
			break
		}
	}
	if err := saveSubscriptionMeta(settings, meta); err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}
	log.Infoln("[SUBSCRIPTION] updated subscription %d: %s", id, req.URL)

	message := ""
	if meta.Use == id {
		// keep the runtime in sync with the active subscription (clashctl behavior)
		if message, err = activateSubscription(r.Context(), settings, meta, id); err != nil {
			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, newError(err.Error()))
			return
		}
	}

	resp := render.M{"status": "ok"}
	if message != "" {
		resp["message"] = message
	}
	render.JSON(w, r, resp)
}

func updateSubscriptionContent(w http.ResponseWriter, r *http.Request) {
	id := subscriptionIDFromRequest(r)

	subscriptionsMu.Lock()
	defer subscriptionsMu.Unlock()

	settings := subscriptionSettings()
	meta, err := loadSubscriptionMeta(settings)
	if err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}
	profile, ok := findSubscription(meta, id)
	if !ok {
		render.Status(r, http.StatusNotFound)
		render.JSON(w, r, ErrNotFound)
		return
	}

	data, err := downloadAndConvert(r.Context(), settings, profile.URL)
	if err != nil {
		render.Status(r, http.StatusBadGateway)
		render.JSON(w, r, newError(err.Error()))
		return
	}
	if err := validateSubscriptionContent(data); err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError(err.Error()))
		return
	}
	if err := atomicWriteFile(profile.Path, data, 0o644); err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}
	log.Infoln("[SUBSCRIPTION] refreshed subscription %d: %s", id, profile.URL)

	message := ""
	if meta.Use == id {
		// keep the runtime in sync with the active subscription (clashctl behavior)
		if message, err = activateSubscription(r.Context(), settings, meta, id); err != nil {
			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, newError(err.Error()))
			return
		}
	}

	resp := render.M{"status": "ok"}
	if message != "" {
		resp["message"] = message
	}
	render.JSON(w, r, resp)
}

func deleteSubscription(w http.ResponseWriter, r *http.Request) {
	id := subscriptionIDFromRequest(r)

	subscriptionsMu.Lock()
	defer subscriptionsMu.Unlock()

	settings := subscriptionSettings()
	meta, err := loadSubscriptionMeta(settings)
	if err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}
	profile, ok := findSubscription(meta, id)
	if !ok {
		render.Status(r, http.StatusNotFound)
		render.JSON(w, r, ErrNotFound)
		return
	}
	if meta.Use == id {
		// align with clashctl: refuse to delete the subscription in use, the
		// operator must switch to another one first
		render.Status(r, http.StatusConflict)
		render.JSON(w, r, newError("subscription is in use, please switch to another subscription first"))
		return
	}

	_ = os.Remove(profile.Path)

	profiles := meta.Profiles[:0]
	for _, p := range meta.Profiles {
		if p.ID != id {
			profiles = append(profiles, p)
		}
	}
	meta.Profiles = profiles
	if err := saveSubscriptionMeta(settings, meta); err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}
	log.Infoln("[SUBSCRIPTION] deleted subscription %d: %s", id, profile.URL)

	render.JSON(w, r, render.M{"status": "ok"})
}

func useSubscription(w http.ResponseWriter, r *http.Request) {
	id := subscriptionIDFromRequest(r)

	subscriptionsMu.Lock()
	defer subscriptionsMu.Unlock()

	settings := subscriptionSettings()
	meta, err := loadSubscriptionMeta(settings)
	if err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}
	if _, ok := findSubscription(meta, id); !ok {
		render.Status(r, http.StatusNotFound)
		render.JSON(w, r, ErrNotFound)
		return
	}

	message, err := activateSubscription(r.Context(), settings, meta, id)
	if err != nil {
		if errors.Is(err, errRejectedSubscription) {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, newError(err.Error()))
			return
		}
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}

	resp := render.M{"status": "ok", "use": id}
	if message != "" {
		resp["message"] = message
	}
	render.JSON(w, r, resp)
}

// subscriptionMergeExpression is the clashctl-compatible profile/mixin merge,
// taken verbatim from /root/clashctl/scripts/lib/config.sh (_merge_config).
const subscriptionMergeExpression = `
########################################
#              Load Files              #
########################################
select(fileIndex==0) as $config |
select(fileIndex==1) as $mixin |

########################################
#              Deep Merge              #
########################################
$mixin |= del(._custom) |
(($config // {}) * $mixin) as $runtime |
$runtime |

########################################
#               Rules                  #
########################################
.rules = (
  ($mixin.rules.prepend // []) +
  ($config.rules // []) +
  ($mixin.rules.append // [])
) |

########################################
#                Proxies               #
########################################
.proxies = (
  ($mixin.proxies.prepend // []) +
  (
    ($config.proxies // []) as $configList |
    ($mixin.proxies.override // []) as $overrideList |
    $configList | map(
      . as $configItem |
      (
        $overrideList[] | select(.name == $configItem.name)
      ) // $configItem
    )
  ) +
  ($mixin.proxies.append // [])
) |

########################################
#             ProxyGroups              #
########################################
.proxy-groups = (
  ($mixin.proxy-groups.prepend // []) +
  (
    ($config.proxy-groups // []) as $configList |
    ($mixin.proxy-groups.override // []) as $overrideList |
    $configList | map(
      . as $configItem |
      (
        $overrideList[] | select(.name == $configItem.name)
      ) // $configItem
    )
  ) +
  ($mixin.proxy-groups.append // [])
) |

########################################
#         ProxyGroups Inject           #
########################################
($mixin.proxy-groups.inject // {}) as $inj |
.proxy-groups[] |= (
  . as $g |
  ($inj | .[$g.name] // []) as $extra |
  .proxies = (.proxies + $extra | unique)
)
`
