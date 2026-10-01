package svcauth

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/svcauth/svcauthtest"
	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

const (
	testPepper    = "test-pepper-0123456789abcdef0123456789"
	peerToken     = "peer-token-0123456789"
	adminToken    = "admin-token-0123456789"
	mcpSub        = "100000000000000000001"
	mcpEmail      = "mcp-prod@cosift-test.iam.gserviceaccount.com"
	synthSub      = "100000000000000000002"
	synthEmail    = "synth-prod@cosift-test.iam.gserviceaccount.com"
	synthStgSub   = "100000000000000000003"
	synthStgEmail = "synth-staging@cosift-test.iam.gserviceaccount.com"
	resolverSub   = "100000000000000000004"
	resolverEmail = "resolver-prod@cosift-test.iam.gserviceaccount.com"
	resolverStSub = "100000000000000000005"
	resolverStEm  = "resolver-staging@cosift-test.iam.gserviceaccount.com"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func (s *syncBuffer) Lines() []string {
	return strings.Split(strings.TrimSpace(s.String()), "\n")
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock { return &fakeClock{t: time.Now()} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// testKeys are csk_ keys for the key principals, minted once per process.
var testKeys = func() map[string]string {
	m := map[string]string{}
	for _, id := range []string{"community-wiki", "dash-prod", "dash-staging", "golive-admin", "dash-prod-2"} {
		k, _, err := NewKey()
		if err != nil {
			panic(err)
		}
		m[id] = k
	}
	return m
}()

func keyID(key string) string { return key[4:20] }

func keyEntry(key string) map[string]any {
	return map[string]any{"key_id": keyID(key), "digest": FormatDigest(Digest([]byte(testPepper), key)), "created_at": "2026-09-28"}
}

// baseConfig is a full example config with test identities and real digests.
func baseConfig() map[string]any {
	return map[string]any{
		"schema_version": 1,
		"failed_auth":    map[string]any{"per_minute": 30, "burst": 10},
		"writes_frozen":  false,
		"principals": []any{
			map[string]any{"id": "mcp-prod", "kind": "oidc", "sub": mcpSub, "email": mcpEmail, "env": "prod", "scopes": []string{"articles:read"}, "rpm": 1200, "counts_reads": true},
			map[string]any{"id": "synth-prod", "kind": "oidc", "sub": synthSub, "email": synthEmail, "env": "prod", "scopes": []string{"articles:read", "articles:write", "retrieve:read"}, "rpm": 300, "writes_per_hour": 30, "writes_burst": 10, "writes_per_day": 150},
			map[string]any{"id": "synth-staging", "kind": "oidc", "sub": synthStgSub, "email": synthStgEmail, "env": "staging", "scopes": []string{"articles:read", "articles:write", "retrieve:read"}, "rpm": 120, "writes_per_hour": 10, "writes_burst": 5, "writes_per_day": 15},
			map[string]any{"id": "resolver-prod", "kind": "oidc", "sub": resolverSub, "email": resolverEmail, "env": "prod", "scopes": []string{"articles:read", "articles:stub", "retrieve:read"}, "rpm": 300, "writes_per_hour": 300, "writes_burst": 50, "writes_per_day": 500},
			map[string]any{"id": "resolver-staging", "kind": "oidc", "sub": resolverStSub, "email": resolverStEm, "env": "staging", "scopes": []string{"articles:read", "articles:stub", "retrieve:read"}, "rpm": 120, "writes_per_hour": 60, "writes_burst": 20, "writes_per_day": 60},
			map[string]any{"id": "community-wiki", "kind": "key", "keys": []any{keyEntry(testKeys["community-wiki"])}, "env": "prod", "scopes": []string{"articles:read"}, "rpm": 1200},
			map[string]any{"id": "dash-prod", "kind": "key", "keys": []any{keyEntry(testKeys["dash-prod"])}, "env": "prod", "scopes": []string{"articles:read_all", "articles:moderate"}, "rpm": 120},
			map[string]any{"id": "dash-staging", "kind": "key", "keys": []any{keyEntry(testKeys["dash-staging"])}, "env": "staging", "scopes": []string{"articles:read_all", "articles:moderate"}, "rpm": 120},
		},
	}
}

func principalsOf(cfg map[string]any) []any { return cfg["principals"].([]any) }

func principalByID(cfg map[string]any, id string) map[string]any {
	for _, p := range principalsOf(cfg) {
		if m := p.(map[string]any); m["id"] == id {
			return m
		}
	}
	panic("no principal " + id)
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// writeConfig writes a service-auth.json owned by the test's uid, mode 0640.
func writeConfig(t *testing.T, path string, cfg any) {
	t.Helper()
	var b []byte
	switch c := cfg.(type) {
	case string:
		b = []byte(c)
	default:
		b = mustJSON(c)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(tmp, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

func testChecks() Checks {
	return Checks{Pepper: []byte(testPepper), PeerAuthToken: peerToken, AdminToken: adminToken}
}

// stubRoutes is the full article route table with recording handlers.
func stubRoutes(record func(pattern string, p v1.Principal, r *http.Request)) []v1.Route {
	h := func(w http.ResponseWriter, r *http.Request) {
		p, _ := v1.PrincipalFrom(r.Context())
		if record != nil {
			record(r.Pattern, p, r)
		}
		v1.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
	}
	read := []v1.Scope{v1.ScopeArticlesRead, v1.ScopeArticlesReadAll}
	return []v1.Route{
		{Method: "POST", Path: "/v1/articles/match", Scopes: []v1.Scope{v1.ScopeArticlesRead}, BodyLimit: 8 << 10, Handler: http.HandlerFunc(h)},
		{Method: "GET", Path: "/v1/articles/{id}", Scopes: read, Handler: http.HandlerFunc(h)},
		{Method: "GET", Path: "/v1/articles/by-slug/{slug}", Scopes: read, Handler: http.HandlerFunc(h)},
		{Method: "GET", Path: "/v1/articles", Scopes: read, Handler: http.HandlerFunc(h)},
		{Method: "PUT", Path: "/v1/articles/{id}", Scopes: []v1.Scope{v1.ScopeArticlesWrite, v1.ScopeArticlesStub}, BodyLimit: 512 << 10, Handler: http.HandlerFunc(h)},
		{Method: "DELETE", Path: "/v1/articles/{id}", Scopes: []v1.Scope{v1.ScopeArticlesStub}, Handler: http.HandlerFunc(h)},
		{Method: "POST", Path: "/v1/articles/{id}/status", Scopes: []v1.Scope{v1.ScopeArticlesModerate}, BodyLimit: 4 << 10, Handler: http.HandlerFunc(h)},
		{Method: "POST", Path: "/v1/articles/purge-prelive", Scopes: []v1.Scope{v1.ScopeArticlesAdmin}, BodyLimit: 1 << 10, Handler: http.HandlerFunc(h)},
		{Method: "GET", Path: "/v1/articles/stats", Scopes: []v1.Scope{v1.ScopeArticlesReadAll}, Handler: http.HandlerFunc(h)},
		{Method: "GET", Path: "/v1/article-versions/{id}", Scopes: []v1.Scope{v1.ScopeArticlesReadAll}, Handler: http.HandlerFunc(h)},
		{Method: "GET", Path: "/v1/article-versions/{id}/{version}", Scopes: []v1.Scope{v1.ScopeArticlesReadAll}, Handler: http.HandlerFunc(h)},
		{Method: "POST", Path: "/v1/search", Scopes: []v1.Scope{v1.ScopeRetrieveRead}, BodyLimit: 8 << 10, Ungated: true, Handler: http.HandlerFunc(h)},
		{Method: "POST", Path: "/v1/contents", Scopes: []v1.Scope{v1.ScopeRetrieveRead}, BodyLimit: 16 << 10, Ungated: true, Handler: http.HandlerFunc(h)},
	}
}

// routeRequests is one concrete request per stub route.
var routeRequests = []struct{ method, path string }{
	{"POST", "/v1/articles/match"},
	{"GET", "/v1/articles/01J8ZC2Q7W4X9M3K5N6P8R0T2V"},
	{"GET", "/v1/articles/by-slug/rust-async-runtimes"},
	{"GET", "/v1/articles"},
	{"PUT", "/v1/articles/01J8ZC2Q7W4X9M3K5N6P8R0T2V"},
	{"DELETE", "/v1/articles/01J8ZC2Q7W4X9M3K5N6P8R0T2V"},
	{"POST", "/v1/articles/01J8ZC2Q7W4X9M3K5N6P8R0T2V/status"},
	{"POST", "/v1/articles/purge-prelive"},
	{"GET", "/v1/articles/stats"},
	{"GET", "/v1/article-versions/01J8ZC2Q7W4X9M3K5N6P8R0T2V"},
	{"GET", "/v1/article-versions/01J8ZC2Q7W4X9M3K5N6P8R0T2V/2"},
	{"POST", "/v1/search"},
	{"POST", "/v1/contents"},
}

type recorded struct {
	pattern string
	p       v1.Principal
}

type harness struct {
	t      *testing.T
	svc    *Service
	src    *svcauthtest.Source
	signer *svcauthtest.Signer
	logs   *syncBuffer
	clock  *fakeClock
	ready  *v1.Readiness
	path   string

	mu    sync.Mutex
	calls []recorded
}

type harnessCfg struct {
	opts   Options
	noFill bool
	routes func(record func(string, v1.Principal, *http.Request)) []v1.Route
}

type harnessOpt func(*harnessCfg)

func withPepper(p string) harnessOpt { return func(c *harnessCfg) { c.opts.Pepper = []byte(p) } }

func withClientIPHeader(h string) harnessOpt {
	return func(c *harnessCfg) { c.opts.ClientIPHeader = h }
}

func withoutCerts() harnessOpt { return func(c *harnessCfg) { c.noFill = true } }

func withRoutes(f func(record func(string, v1.Principal, *http.Request)) []v1.Route) harnessOpt {
	return func(c *harnessCfg) { c.routes = f }
}

// lenient raises the failed-auth limit for tests that send many failures.
func lenient(cfg map[string]any) map[string]any {
	cfg["failed_auth"] = map[string]any{"per_minute": 600, "burst": 100}
	return cfg
}

// newHarness builds a Service over stubRoutes with cfg installed and the
// certificate copy filled; no socket is opened.
func newHarness(t *testing.T, cfg map[string]any, opts ...harnessOpt) *harness {
	t.Helper()
	h := &harness{t: t, logs: &syncBuffer{}, clock: newClock(), ready: &v1.Readiness{}, signer: svcauthtest.NewSigner("kid-1")}
	h.src = svcauthtest.NewSource(svcauthtest.JWKS(h.signer), 21600)
	h.path = filepath.Join(t.TempDir(), "service-auth.json")
	hc := harnessCfg{routes: stubRoutes, opts: Options{
		Path: h.path, OwnerUID: uint32(os.Getuid()), Pepper: []byte(testPepper),
		PeerAuthToken: peerToken, AdminToken: adminToken, Readiness: h.ready,
		Logger: log.New(h.logs, "", 0), Now: h.clock.Now, CertSource: h.src.Fetch,
	}}
	for _, f := range opts {
		f(&hc)
	}
	svc, err := New(hc.opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Mount(hc.routes(h.record)); err != nil {
		t.Fatal(err)
	}
	h.svc = svc
	t.Cleanup(func() { _ = svc.Shutdown(context.Background()) })
	if cfg != nil {
		if _, ok := cfg["listen"]; !ok {
			cfg["listen"] = freeListen(t)
		}
		writeConfig(t, h.path, cfg)
		c, err := svc.load()
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		svc.Policy.Install(c)
	}
	if !hc.noFill && !svc.certs.refresh(context.Background()) {
		t.Fatal("certificate fill failed")
	}
	h.ready.SetReady()
	return h
}

func (h *harness) record(pattern string, p v1.Principal, _ *http.Request) {
	h.mu.Lock()
	h.calls = append(h.calls, recorded{pattern, p})
	h.mu.Unlock()
}

func (h *harness) handlerCalls() []recorded {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]recorded(nil), h.calls...)
}

// token is a valid token for (sub, email) at the harness clock.
func (h *harness) token(sub, email string) string {
	return h.signer.Token(svcauthtest.Claims(sub, email, h.clock.Now()))
}

// freeListen is a loopback address with a port that was free a moment ago.
func freeListen(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// waitFor polls cond for up to 5 s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

type reqOpt func(*http.Request)

func bearerAuth(cred string) reqOpt {
	return func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+cred) }
}

func header(k, v string) reqOpt { return func(r *http.Request) { r.Header.Add(k, v) } }

func remote(addr string) reqOpt { return func(r *http.Request) { r.RemoteAddr = addr } }

func jsonBody(r *http.Request) { r.Header.Set("Content-Type", "application/json") }

// newRequest is a request from a direct loopback peer.
func newRequest(method, target, body string, opts ...reqOpt) *http.Request {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	r := httptest.NewRequest(method, target, rd)
	r.RemoteAddr = "127.0.0.1:40000"
	for _, f := range opts {
		f(r)
	}
	return r
}

func serve(h *harness, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.svc.ServeHTTP(w, r)
	return w
}

func (h *harness) do(method, target, body string, opts ...reqOpt) *httptest.ResponseRecorder {
	return serve(h, newRequest(method, target, body, opts...))
}

func decodeErr(t *testing.T, w *httptest.ResponseRecorder) v1.Error {
	t.Helper()
	var e v1.Error
	if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil {
		t.Fatalf("body %q: %v", w.Body.String(), err)
	}
	return e
}

// wantUnauthenticated asserts the uniform 401 on the wire.
func wantUnauthenticated(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401 (body %s)", w.Code, w.Body.String())
	}
	if got, want := w.Body.String(), `{"error":"Unauthorized","status":401,"code":"unauthenticated","detail":"missing or invalid credential"}`+"\n"; got != want {
		t.Fatalf("body %q, want %q", got, want)
	}
	if got := w.Header().Get("WWW-Authenticate"); got != `Bearer realm="cosift-v1"` {
		t.Fatalf("WWW-Authenticate %q", got)
	}
}

// lastReason is the reason on the last audit line.
func (h *harness) lastReason() string {
	lines := h.logs.Lines()
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.HasPrefix(lines[i], "v1 req=") {
			if _, r, ok := strings.Cut(lines[i], " reason="); ok {
				r, _, _ = strings.Cut(r, " ")
				return r
			}
			return ""
		}
	}
	return ""
}

func (h *harness) metrics() string {
	var b bytes.Buffer
	h.svc.Metrics.WritePrometheus(&b)
	return b.String()
}
