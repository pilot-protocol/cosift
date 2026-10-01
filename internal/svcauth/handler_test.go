package svcauth

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/svcauth/svcauthtest"
	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

func TestCredentialExtraction(t *testing.T) {
	h := newHarness(t, lenient(baseConfig()))
	tok := h.token(synthSub, synthEmail)
	cases := []struct {
		name   string
		opts   []reqOpt
		reason string
	}{
		{"none", nil, "missing_credential"},
		{"two headers", []reqOpt{header("Authorization", "Bearer "+tok), header("Authorization", "Bearer "+tok)}, "malformed_credential"},
		{"basic", []reqOpt{header("Authorization", "Basic dXNlcjpwYXNz")}, "malformed_credential"},
		{"scheme only", []reqOpt{header("Authorization", "Bearer")}, "malformed_credential"},
		{"scheme and space", []reqOpt{header("Authorization", "Bearer ")}, "malformed_credential"},
		{"two spaces", []reqOpt{header("Authorization", "Bearer  "+tok)}, "malformed_credential"},
		{"tab separator", []reqOpt{header("Authorization", "Bearer\t"+tok)}, "malformed_credential"},
		{"trailing space", []reqOpt{header("Authorization", "Bearer "+tok+" ")}, "malformed_credential"},
		{"inner tab", []reqOpt{header("Authorization", "Bearer "+tok[:10]+"\t"+tok[10:])}, "malformed_credential"},
		{"non-ASCII", []reqOpt{header("Authorization", "Bearer "+tok+"é")}, "malformed_credential"},
		{"4097 bytes", []reqOpt{header("Authorization", "Bearer "+strings.Repeat("a", 4097))}, "malformed_credential"},
		{"unknown shape", []reqOpt{header("Authorization", "Bearer opaque-token")}, "unknown_credential"},
		{"query parameter", []reqOpt{func(r *http.Request) { r.URL.RawQuery = "access_token=" + tok }}, "missing_credential"},
		{"serverless header", []reqOpt{header("X-Serverless-Authorization", "Bearer "+tok)}, "missing_credential"},
		{"cookie", []reqOpt{header("Cookie", "session="+tok)}, "missing_credential"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wantUnauthenticated(t, h.do("POST", "/v1/search", `{}`, tc.opts...))
			if got := h.lastReason(); got != tc.reason {
				t.Fatalf("reason %q, want %q", got, tc.reason)
			}
		})
	}
	for _, scheme := range []string{"bearer", "BEARER", "BeArEr"} {
		if w := h.do("POST", "/v1/search", `{}`, header("Authorization", scheme+" "+tok)); w.Code != http.StatusOK {
			t.Errorf("scheme %q: status %d", scheme, w.Code)
		}
	}
	if n := len(strings.Repeat("a", 4096)); n != maxCredential {
		t.Fatal(n)
	}
}

// No existing engine credential reaches any /v1 route.
func TestExistingTokensRefusedOnEveryRoute(t *testing.T) {
	h := newHarness(t, lenient(baseConfig()))
	tokens := map[string]string{
		"cluster.peer_auth_token":   peerToken,
		"server.admin_token":        adminToken,
		"ck_ user token":            "ck_42_ABCDEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFG",
		"community admin token":     "community-admin-0123456789abcdef",
		"JWT-shaped admin token":    "aaaa.bbbb.cccc",
		"csk_-prefixed admin token": "csk_" + adminToken,
	}
	for name, tok := range tokens {
		for _, rr := range routeRequests {
			w := h.do(rr.method, rr.path, `{}`, bearerAuth(tok), jsonBody)
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("%s on %s %s: status %d", name, rr.method, rr.path, w.Code)
			}
		}
	}
	if n := len(h.handlerCalls()); n != 0 {
		t.Fatalf("handler reached %d times", n)
	}
}

func TestPathHygiene(t *testing.T) {
	h := newHarness(t, baseConfig())
	key := testKeys["community-wiki"]
	for _, p := range []string{
		"/v1/articles/../x", "/v1//articles", "/v1/articles/%2e%2e/x", "/v1/articles/%2E", "/v1/%61rticles",
		"/v1/articles/", "/v1/articles/by-slug/rust%2Dasync", "/v1/./articles", "/v1/articles/by-slug/a/../b",
	} {
		w := h.do("GET", p, "", bearerAuth(key))
		if w.Code != http.StatusNotFound || w.Header().Get("Location") != "" || decodeErr(t, w).Code != "not_found" {
			t.Errorf("%s: status %d Location %q body %s", p, w.Code, w.Header().Get("Location"), w.Body.String())
		}
	}
	if n := len(h.handlerCalls()); n != 0 {
		t.Fatalf("handler reached %d times", n)
	}
	// A percent-encoded query is not a path escape.
	if w := h.do("GET", "/v1/articles?updated_since=2026-10-01T00%3A00%3A00Z", "", bearerAuth(key)); w.Code != http.StatusOK {
		t.Fatalf("query escape: status %d", w.Code)
	}
}

// rawGet sends a request line as written, bypassing any client-side cleaning.
func rawGet(t *testing.T, addr, target string) (int, http.Header, string) {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fmt.Fprintf(c, "%s HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n", target)
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, string(b)
}

// Go 1.26's ServeMux answers these with a 307; over a real socket the
// listener answers the JSON 404 with no redirect of any kind.
func TestNoRedirectOverTheWire(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.svc.Start(t.Context())
	addr := h.svc.Addr()
	if addr == "" {
		t.Fatal("listener not started")
	}
	for _, target := range []string{"GET /v1/articles/../x", "GET /v1//articles", "GET /v1/articles/%2e%2e/stats", "GET /v1/articles/", "OPTIONS *", "GET http://127.0.0.1/v1/articles/../stats", "GET /stats", "GET /admin/crawl-enqueue"} {
		code, hdr, body := rawGet(t, addr, target)
		if code != http.StatusNotFound || hdr.Get("Location") != "" || hdr.Get("Content-Type") != "application/json" || !strings.Contains(body, `"code":"not_found"`) {
			t.Errorf("%s: %d %v %s", target, code, hdr, body)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/articles/stats", func(http.ResponseWriter, *http.Request) {})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	if code, _, _ := rawGet(t, srv.Listener.Addr().String(), "GET /v1/articles/../articles/stats"); code/100 != 3 {
		t.Fatalf("bare ServeMux answered %d; the wrapper test is vacuous", code)
	}
}

func TestUnmatchedIsJSON404(t *testing.T) {
	h := newHarness(t, baseConfig())
	tok := h.token(synthSub, synthEmail)
	for _, rr := range []struct{ method, path string }{
		{"GET", "/v1/search"}, {"DELETE", "/v1/search"}, {"GET", "/v1/nope"}, {"GET", "/v1"}, {"GET", "/"},
		{"GET", "/stats"}, {"GET", "/metrics"}, {"POST", "/admin/crawl-enqueue"}, {"GET", "/healthz"}, {"PATCH", "/v1/articles/01J8ZC2Q7W4X9M3K5N6P8R0T2V"},
	} {
		w := h.do(rr.method, rr.path, "", bearerAuth(tok))
		if w.Code != http.StatusNotFound || decodeErr(t, w).Code != "not_found" || w.Header().Get("Allow") != "" {
			t.Errorf("%s %s: status %d %v", rr.method, rr.path, w.Code, w.Header())
		}
	}
	if !strings.Contains(h.logs.String(), "route=unmatched status=404") {
		t.Fatalf("log %s", h.logs.String())
	}
	if !strings.Contains(h.metrics(), `cosift_v1_requests_total{principal="-",route="unmatched",code="404"} 10`) {
		t.Fatalf("metrics %s", h.metrics())
	}
}

func TestResponseHeaders(t *testing.T) {
	h := newHarness(t, baseConfig(), withRoutes(func(func(string, v1.Principal, *http.Request)) []v1.Route {
		return []v1.Route{
			{Method: "GET", Path: "/v1/articles", Scopes: []v1.Scope{v1.ScopeArticlesRead}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Set-Cookie", "a=b")
				w.Header().Set("Access-Control-Allow-Origin", "*")
				w.Header().Set("Location", "/elsewhere")
				w.Header().Set("Content-Type", "text/html")
				_, _ = w.Write([]byte(`{"items":[]}`))
			})},
			{Method: "GET", Path: "/v1/articles/stats", Scopes: []v1.Scope{v1.ScopeArticlesReadAll}, Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				panic("boom")
			})},
		}
	}))
	key := testKeys["community-wiki"]
	for _, w := range []*httptest.ResponseRecorder{
		h.do("GET", "/v1/articles", "", bearerAuth(key)),
		h.do("GET", "/v1/articles", ""),
		h.do("GET", "/v1/articles/stats", "", bearerAuth(key)),
		h.do("GET", "/v1/articles/stats", "", bearerAuth(testKeys["dash-prod"])),
		h.do("GET", "/v1/nope", ""),
	} {
		hd := w.Header()
		if hd.Get("Cache-Control") != "no-store" || hd.Get("Content-Type") != "application/json" || hd.Get("X-Content-Type-Options") != "nosniff" {
			t.Errorf("status %d headers %v", w.Code, hd)
		}
		for k := range hd {
			if k == "Set-Cookie" || k == "Location" || strings.HasPrefix(k, "Access-Control-") {
				t.Errorf("status %d carries %s", w.Code, k)
			}
		}
	}
}

func TestPanicIsRecoveredWithoutTheRequest(t *testing.T) {
	h := newHarness(t, baseConfig(), withRoutes(func(func(string, v1.Principal, *http.Request)) []v1.Route {
		return []v1.Route{{Method: "POST", Path: "/v1/search", Scopes: []v1.Scope{v1.ScopeRetrieveRead}, BodyLimit: 8 << 10, Ungated: true, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			panic("handler failed on " + string(b))
		})}}
	}))
	tok := h.token(synthSub, synthEmail)
	w := h.do("POST", "/v1/search?topic_id=8624225ee5e2b9f9", `{"q":"private demand text"}`, bearerAuth(tok), jsonBody)
	if w.Code != http.StatusInternalServerError || decodeErr(t, w).Code != "internal_error" {
		t.Fatalf("status %d %s", w.Code, w.Body.String())
	}
	logs := h.logs.String()
	if !strings.Contains(logs, "v1 panic route=POST /v1/search") || !strings.Contains(logs, `status=500`) {
		t.Fatalf("log %s", logs)
	}
	for _, secret := range []string{"private demand text", "8624225ee5e2b9f9", tok, "handler failed on"} {
		if strings.Contains(logs, secret) {
			t.Fatalf("log holds %q:\n%s", secret, logs)
		}
	}
}

var auditLine = regexp.MustCompile(`^v1 req=[0-9A-HJKMNP-TV-Z]{26} principal=[a-z0-9-]+ route=("[A-Z]+ /v1/[^"]*"|unmatched) status=\d{3} ms=\d+( reason=[a-z_]+ client=\S+)?$`)

// One fixed-format line per response; nothing request-derived beyond
// the pattern, and the credential, q, titles, slugs, ids and URLs never.
func TestAuditLog(t *testing.T) {
	h := newHarness(t, baseConfig())
	tok := h.token(synthSub, synthEmail)
	key := testKeys["community-wiki"]
	secrets := []string{tok, key, "rust async runtimes secret", "Rust Async Title", "rust-async-runtimes", "01J8ZC2Q7W4X9M3K5N6P8R0T2V", "https://tokio.rs/tokio/tutorial", "reader9c1d0f6a", "8624225ee5e2b9f9", "X-Private-Header-Value"}
	h.do("POST", "/v1/search", `{"q":"rust async runtimes secret","title":"Rust Async Title","urls":["https://tokio.rs/tokio/tutorial"]}`, bearerAuth(tok), jsonBody, header("X-Private", "X-Private-Header-Value"))
	h.do("GET", "/v1/articles/by-slug/rust-async-runtimes?reader=reader9c1d0f6a", "", bearerAuth(key))
	h.do("GET", "/v1/articles/01J8ZC2Q7W4X9M3K5N6P8R0T2V", "", bearerAuth(key+"x"))
	forged := svcauthtest.NewSigner("kid-1").Token(svcauthtest.Claims(synthSub, synthEmail, h.clock.Now()))
	h.do("POST", "/v1/articles/match", `{"q":"rust async runtimes secret","topic_id":"8624225ee5e2b9f9"}`, bearerAuth(forged), remote("203.0.113.9:1234"))
	h.do("GET", "/v1/nope/rust-async-runtimes", "")
	lines := h.logs.Lines()
	if len(lines) != 5 {
		t.Fatalf("%d lines:\n%s", len(lines), h.logs.String())
	}
	for _, l := range lines {
		if !auditLine.MatchString(l) {
			t.Errorf("line %q", l)
		}
		for _, s := range secrets {
			if strings.Contains(l, s) {
				t.Errorf("line holds %q: %s", s, l)
			}
		}
	}
	if !strings.HasPrefix(lines[0], "v1 req=") || !strings.Contains(lines[0], ` principal=synth-prod route="POST /v1/search" status=200 `) {
		t.Errorf("line 0 %s", lines[0])
	}
	if !strings.Contains(lines[2], `principal=- route="GET /v1/articles/{id}" status=401`) || !strings.HasSuffix(lines[2], "reason=malformed_credential client=loopback-direct") {
		t.Errorf("line 2 %s", lines[2])
	}
	if !strings.HasSuffix(lines[3], "reason=bad_signature client=203.0.113.9") {
		t.Errorf("line 3 %s", lines[3])
	}
	if strings.Contains(lines[0], "reason=") || strings.Contains(lines[1], "client=") {
		t.Error("reason or client on a success line")
	}
}

func TestReadinessOrdering(t *testing.T) {
	h := newHarness(t, baseConfig())
	rd := &v1.Readiness{}
	h.svc.ready = rd
	if err := h.svc.Mount(stubRoutes(h.record)); err != nil {
		t.Fatal(err)
	}
	synth := h.token(synthSub, synthEmail)
	wantUnauthenticated(t, h.do("GET", "/v1/articles/stats", ""))
	for _, rr := range routeRequests {
		w := h.do(rr.method, rr.path, `{}`, bearerAuth(synth), jsonBody)
		ungated := rr.path == "/v1/search" || rr.path == "/v1/contents"
		switch {
		case ungated && w.Code != http.StatusOK:
			t.Errorf("%s before ready: %d", rr.path, w.Code)
		case !ungated && (w.Code != http.StatusServiceUnavailable || decodeErr(t, w).Code != "index_unavailable" || w.Header().Get("Retry-After") != "5"):
			t.Errorf("%s %s before ready: %d %s", rr.method, rr.path, w.Code, w.Body.String())
		}
	}
	rd.SetReady()
	w := h.do("GET", "/v1/articles/stats", "", bearerAuth(synth))
	if e := decodeErr(t, w); w.Code != http.StatusForbidden || e.Code != "missing_scope" || e.Detail != "requires scope articles:read_all" {
		t.Fatalf("after ready: %d %+v", w.Code, e)
	}
}

func TestRouteScopeCheck(t *testing.T) {
	h := newHarness(t, baseConfig())
	cases := []struct {
		cred, method, path, want string
	}{
		{h.token(mcpSub, mcpEmail), "POST", "/v1/search", "requires scope retrieve:read"},
		{h.token(resolverSub, resolverEmail), "POST", "/v1/articles/01J8ZC2Q7W4X9M3K5N6P8R0T2V/status", "requires scope articles:moderate"},
		{testKeys["community-wiki"], "GET", "/v1/articles/stats", "requires scope articles:read_all"},
		{testKeys["dash-prod"], "POST", "/v1/articles/purge-prelive", "requires scope articles:admin"},
		{testKeys["dash-prod"], "POST", "/v1/articles/match", "requires scope articles:read"},
		{h.token(synthSub, synthEmail), "DELETE", "/v1/articles/01J8ZC2Q7W4X9M3K5N6P8R0T2V", "requires scope articles:stub"},
		{testKeys["dash-staging"], "PUT", "/v1/articles/01J8ZC2Q7W4X9M3K5N6P8R0T2V", "requires scope articles:write or articles:stub"},
	}
	for _, tc := range cases {
		w := h.do(tc.method, tc.path, `{}`, bearerAuth(tc.cred), jsonBody)
		if e := decodeErr(t, w); w.Code != http.StatusForbidden || e.Code != "missing_scope" || e.Detail != tc.want {
			t.Errorf("%s %s: %d %+v", tc.method, tc.path, w.Code, e)
		}
	}
	if n := len(h.handlerCalls()); n != 0 {
		t.Fatalf("handler reached %d times", n)
	}
	for _, ok := range []struct{ cred, method, path string }{
		{testKeys["dash-prod"], "GET", "/v1/articles/01J8ZC2Q7W4X9M3K5N6P8R0T2V"},
		{testKeys["community-wiki"], "GET", "/v1/articles/01J8ZC2Q7W4X9M3K5N6P8R0T2V"},
		{h.token(resolverSub, resolverEmail), "PUT", "/v1/articles/01J8ZC2Q7W4X9M3K5N6P8R0T2V"},
	} {
		if w := h.do(ok.method, ok.path, `{}`, bearerAuth(ok.cred), jsonBody); w.Code != http.StatusOK {
			t.Errorf("%s %s: %d", ok.method, ok.path, w.Code)
		}
	}
}

func TestBodyCap(t *testing.T) {
	h := newHarness(t, baseConfig(), withRoutes(func(func(string, v1.Principal, *http.Request)) []v1.Route {
		return []v1.Route{{Method: "POST", Path: "/v1/search", Scopes: []v1.Scope{v1.ScopeRetrieveRead}, BodyLimit: 64, Ungated: true, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var body map[string]any
			if v1.Decode(w, r, &body, 1<<20) {
				v1.WriteJSON(w, http.StatusOK, body)
			}
		})}}
	}))
	tok := h.token(synthSub, synthEmail)
	if w := h.do("POST", "/v1/search", `{"q":"`+strings.Repeat("a", 40)+`"}`, bearerAuth(tok), jsonBody); w.Code != http.StatusOK {
		t.Fatalf("small body: %d", w.Code)
	}
	w := h.do("POST", "/v1/search", `{"q":"`+strings.Repeat("a", 80)+`"}`, bearerAuth(tok), jsonBody)
	if w.Code != http.StatusRequestEntityTooLarge || decodeErr(t, w).Code != "body_too_large" {
		t.Fatalf("large body: %d %s", w.Code, w.Body.String())
	}
}

func TestMountRouteTable(t *testing.T) {
	h := newHarness(t, nil)
	if err := h.svc.Mount(stubRoutes(nil)); err != nil {
		t.Fatalf("the full table: %v", err)
	}
	conflict := append(stubRoutes(nil), v1.Route{Method: "GET", Path: "/v1/articles/{id}/versions", Handler: http.NotFoundHandler()})
	if err := h.svc.Mount(conflict); err == nil || !strings.Contains(err.Error(), "route table") {
		t.Fatalf("conflicting table: %v", err)
	}
	if err := h.svc.Mount([]v1.Route{{Method: "GET", Path: "/v1/x"}}); err == nil {
		t.Fatal("a route without a handler mounted")
	}
}

func TestServerErrorLogDropsText(t *testing.T) {
	m := NewMetrics()
	l := log.New(serverErrorWriter{m}, "", 0)
	l.Printf("http: TLS handshake error from 203.0.113.9: GET /v1/secret?q=x")
	l.Printf("second")
	var b strings.Builder
	m.WritePrometheus(&b)
	if !strings.Contains(b.String(), "cosift_v1_server_errors_total 2") || strings.Contains(b.String(), "secret") {
		t.Fatal(b.String())
	}
	h := newHarness(t, baseConfig())
	h.svc.Start(t.Context())
	srv := h.svc.srv
	if _, ok := srv.ErrorLog.Writer().(serverErrorWriter); !ok {
		t.Fatal("listener ErrorLog is not the counting writer")
	}
	if srv.ReadHeaderTimeout.Seconds() != 5 || srv.ReadTimeout.Seconds() != 15 || srv.WriteTimeout.Seconds() != 30 || srv.IdleTimeout.Seconds() != 60 || srv.MaxHeaderBytes != 16<<10 || !srv.DisableGeneralOptionsHandler {
		t.Fatalf("server limits %+v", srv)
	}
}

func TestRequestID(t *testing.T) {
	re := regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)
	now := newClock().Now()
	a, b := requestID(now), requestID(now)
	if !re.MatchString(a) || a[:10] != b[:10] || a[10:] == b[10:] {
		t.Fatalf("%s %s", a, b)
	}
}
