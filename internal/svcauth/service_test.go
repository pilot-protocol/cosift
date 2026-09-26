package svcauth

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

func getOverTCP(t *testing.T, addr, path, cred string) int {
	t.Helper()
	req, _ := http.NewRequest("GET", "http://"+addr+path, nil)
	if cred != "" {
		req.Header.Set("Authorization", "Bearer "+cred)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// §1: no file or an invalid file at startup leaves the listener down with one
// log line; a later reload with a valid file starts it.
func TestListenerLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, h *harness)
		want  string
	}{
		{"absent", func(*testing.T, *harness) {}, "svcauth: %s absent — /v1 listener disabled"},
		{"invalid", func(t *testing.T, h *harness) {
			c := baseConfig()
			c["schema_version"] = 2
			writeConfig(t, h.path, c)
		}, "svcauth: ERROR %s invalid (schema_version: must be 1) — /v1 listener disabled"},
		{"bad mode", func(t *testing.T, h *harness) {
			writeConfig(t, h.path, baseConfig())
			if err := os.Chmod(h.path, 0o644); err != nil {
				t.Fatal(err)
			}
		}, "svcauth: ERROR %s invalid (mode must not grant group write or any other access) — /v1 listener disabled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil)
			tc.setup(t, h)
			h.svc.Start(t.Context())
			if addr := h.svc.Addr(); addr != "" {
				t.Fatalf("listening on %s", addr)
			}
			want := strings.Replace(tc.want, "%s", h.path, 1)
			if lines := h.logs.Lines(); len(lines) != 1 || lines[0] != want {
				t.Fatalf("log %q, want %q", lines, want)
			}
			if w := h.do("GET", "/v1/articles", "", bearerAuth(testKeys["community-wiki"])); w.Code != http.StatusUnauthorized {
				t.Fatalf("no config: %d", w.Code)
			}

			cfg := baseConfig()
			cfg["listen"] = freeListen(t)
			writeConfig(t, h.path, cfg)
			if err := h.svc.Reload(t.Context()); err != nil {
				t.Fatal(err)
			}
			addr := h.svc.Addr()
			if addr != cfg["listen"] {
				t.Fatalf("addr %q, want %v", addr, cfg["listen"])
			}
			if code := getOverTCP(t, addr, "/v1/articles", testKeys["community-wiki"]); code != http.StatusOK {
				t.Fatalf("over TCP: %d", code)
			}
			if code := getOverTCP(t, addr, "/v1/articles", ""); code != http.StatusUnauthorized {
				t.Fatalf("over TCP without a credential: %d", code)
			}
			if !strings.Contains(h.logs.String(), "svcauth: /v1 listening on "+addr) {
				t.Fatalf("log %s", h.logs.String())
			}
		})
	}
}

// §1.2: an invalid or deleted file on reload keeps the active config; a
// changed listen address is not re-bound.
func TestReloadKeepsConfig(t *testing.T) {
	cfg := baseConfig()
	h := newHarness(t, cfg)
	h.svc.Start(t.Context())
	addr := h.svc.Addr()
	if addr == "" {
		t.Fatal("not listening")
	}
	key := testKeys["dash-prod"]
	check := func(stage string, want int) {
		t.Helper()
		if code := getOverTCP(t, addr, "/v1/articles/stats", key); code != want {
			t.Fatalf("%s: %d, want %d", stage, code, want)
		}
	}
	check("initial", http.StatusOK)

	bad := baseConfig()
	principalByID(bad, "dash-prod")["scopes"] = []string{"articles:read_all", "articles:write"}
	delete(principalByID(bad, "dash-prod"), "keys")
	for _, edit := range []func(){
		func() { writeConfig(t, h.path, bad) },
		func() { writeConfig(t, h.path, `{"schema_version":1,"principals":[]}`) },
		func() { writeConfig(t, h.path, "{") },
		func() {
			writeConfig(t, h.path, baseConfig())
			_ = os.Chmod(h.path, 0o666)
		},
		func() { _ = os.Remove(h.path) },
	} {
		edit()
		if err := h.svc.Reload(t.Context()); err == nil {
			t.Fatal("invalid reload accepted")
		}
		check("after an invalid reload", http.StatusOK)
	}
	if !strings.Contains(h.metrics(), `cosift_v1_config_reloads_total{result="error"} 5`) {
		t.Fatal(h.metrics())
	}
	if n := strings.Count(h.logs.String(), "svcauth: ERROR reload of "+h.path+" refused ("); n != 5 {
		t.Fatalf("%d reload errors logged:\n%s", n, h.logs.String())
	}

	next := baseConfig()
	next["listen"] = freeListen(t)
	principalByID(next, "dash-prod")["scopes"] = []string{"articles:read_all"}
	writeConfig(t, h.path, next)
	if err := h.svc.Reload(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.svc.Addr() != addr {
		t.Fatalf("re-bound to %s", h.svc.Addr())
	}
	if !strings.Contains(h.logs.String(), "svcauth: WARN listen changed to "+next["listen"].(string)) {
		t.Fatalf("log %s", h.logs.String())
	}
	if code := getOverTCP(t, addr, "/v1/articles/stats", key); code != http.StatusOK {
		t.Fatalf("after a valid reload: %d", code)
	}
	if !strings.Contains(h.metrics(), `cosift_v1_config_reloads_total{result="ok"} 1`) {
		t.Fatal(h.metrics())
	}
}

func TestPepperWarnings(t *testing.T) {
	h := newHarness(t, baseConfig(), withPepper("short"))
	h.svc.Start(t.Context())
	if n := strings.Count(h.logs.String(), "COSIFT_SVC_PEPPER absent or shorter than 32 bytes"); n != 1 {
		t.Fatalf("%d warnings at start:\n%s", n, h.logs.String())
	}
	for range 2 {
		if err := h.svc.Reload(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if n := strings.Count(h.logs.String(), "COSIFT_SVC_PEPPER absent or shorter than 32 bytes"); n != 3 {
		t.Fatalf("%d warnings after two reloads with key principals", n)
	}
	cfg := baseConfig()
	cfg["principals"] = principalsOf(cfg)[:5]
	writeConfig(t, h.path, cfg)
	if err := h.svc.Reload(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(h.logs.String(), "COSIFT_SVC_PEPPER absent or shorter than 32 bytes"); n != 3 {
		t.Fatalf("warned on a reload without key principals (%d)", n)
	}
}

// Shutdown waits for in-flight handlers, so the article store can close
// after it; afterwards nothing restarts the listener.
func TestShutdownDrains(t *testing.T) {
	release, entered := make(chan struct{}), make(chan struct{})
	h := newHarness(t, baseConfig(), withRoutes(func(func(string, v1.Principal, *http.Request)) []v1.Route {
		return []v1.Route{{Method: "GET", Path: "/v1/articles", Scopes: []v1.Scope{v1.ScopeArticlesRead}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(entered)
			<-release
			v1.WriteJSON(w, http.StatusOK, map[string]any{})
		})}}
	}))
	h.svc.Start(t.Context())
	addr := h.svc.Addr()
	codes := make(chan int, 1)
	go func() {
		req, _ := http.NewRequest("GET", "http://"+addr+"/v1/articles", nil)
		req.Header.Set("Authorization", "Bearer "+testKeys["community-wiki"])
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			codes <- -1
			return
		}
		resp.Body.Close()
		codes <- resp.StatusCode
	}()
	<-entered
	done := make(chan error, 1)
	go func() { done <- h.svc.Shutdown(context.Background()) }()
	select {
	case err := <-done:
		t.Fatalf("Shutdown returned with a handler running: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if code := <-codes; code != http.StatusOK {
		t.Fatalf("in-flight request: %d", code)
	}
	if err := h.svc.Reload(t.Context()); err == nil || !strings.Contains(err.Error(), "shutting down") {
		t.Fatalf("reload after shutdown: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	stuck := &tracker{}
	stuck.enter()
	if err := stuck.wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stuck handler: %v", err)
	}
}
