package svcauth

import (
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

func TestPrincipalRateLimit(t *testing.T) {
	cfg := baseConfig()
	p := principalByID(cfg, "community-wiki")
	p["rpm"], p["burst"] = 2, 2
	h := newHarness(t, cfg)
	key := testKeys["community-wiki"]
	for i := range 2 {
		if w := h.do("GET", "/v1/articles", "", bearerAuth(key)); w.Code != http.StatusOK {
			t.Fatalf("request %d: %d", i, w.Code)
		}
	}
	w := h.do("GET", "/v1/articles", "", bearerAuth(key))
	if e := decodeErr(t, w); w.Code != http.StatusTooManyRequests || e.Code != "rate_limited" || w.Header().Get("Retry-After") != "30" {
		t.Fatalf("third request: %d %+v %v", w.Code, e, w.Header())
	}
	if w := h.do("GET", "/v1/articles/stats", "", bearerAuth(testKeys["dash-prod"])); w.Code != http.StatusOK {
		t.Fatalf("another principal: %d", w.Code)
	}
	for range 20 {
		h.do("GET", "/v1/articles", "", bearerAuth(key+"x"))
	}
	h.clock.Advance(30 * time.Second)
	if w := h.do("GET", "/v1/articles", "", bearerAuth(key)); w.Code != http.StatusOK {
		t.Fatalf("after refill (failed attempts must not drain the bucket): %d", w.Code)
	}
	if !strings.Contains(h.metrics(), `cosift_v1_rate_limited_total{principal="community-wiki",kind="principal"} 1`) {
		t.Fatal(h.metrics())
	}
}

// §6: failures from one client turn into 429 auth_throttled after the burst,
// logged once a minute, and never block a request that authenticates.
func TestFailedAuthLimiter(t *testing.T) {
	h := newHarness(t, baseConfig())
	tok := h.token(synthSub, synthEmail)
	edge := []reqOpt{remote("127.0.0.1:5000"), header("X-Forwarded-For", "198.51.100.7")}
	for i := range 10 {
		wantUnauthenticated(t, h.do("POST", "/v1/search", `{}`, append(edge, bearerAuth("bogus"))...))
		_ = i
	}
	w := h.do("POST", "/v1/search", `{}`, append(edge, bearerAuth("bogus"))...)
	if e := decodeErr(t, w); w.Code != http.StatusTooManyRequests || e.Code != "auth_throttled" || w.Header().Get("Retry-After") != "2" {
		t.Fatalf("11th failure: %d %+v %v", w.Code, e, w.Header())
	}
	for range 5 {
		if w := h.do("POST", "/v1/search", `{}`, append(edge, bearerAuth(tok))...); w.Code != http.StatusOK {
			t.Fatalf("an authenticating request from the throttled client: %d", w.Code)
		}
	}
	if w := h.do("POST", "/v1/search", `{}`, remote("127.0.0.1:5000"), header("X-Forwarded-For", "198.51.100.8"), bearerAuth("bogus")); w.Code != http.StatusUnauthorized {
		t.Fatalf("another client: %d", w.Code)
	}
	for range 30 {
		h.do("POST", "/v1/search", `{}`, append(edge, bearerAuth("bogus"))...)
	}
	summaries := strings.Count(h.logs.String(), "v1 auth_throttled client=198.51.100.7")
	throttledLines := 0
	for _, l := range h.logs.Lines() {
		if strings.Contains(l, "status=429") {
			throttledLines++
		}
	}
	if summaries != 1 || throttledLines != 0 {
		t.Fatalf("%d summaries, %d per-request 429 lines:\n%s", summaries, throttledLines, h.logs.String())
	}
	h.clock.Advance(time.Minute)
	h.do("POST", "/v1/search", `{}`, append(edge, bearerAuth("bogus"))...)
	for range 40 {
		h.do("POST", "/v1/search", `{}`, append(edge, bearerAuth("bogus"))...)
	}
	if n := strings.Count(h.logs.String(), "v1 auth_throttled client=198.51.100.7"); n != 2 {
		t.Fatalf("%d summaries after a minute", n)
	}
	if !strings.Contains(h.logs.String(), "v1 auth_throttled client=198.51.100.7 count=30") {
		t.Fatalf("summary counts:\n%s", h.logs.String())
	}
	if !strings.Contains(h.metrics(), `cosift_v1_rate_limited_total{principal="-",kind="failed_auth"}`) {
		t.Fatal(h.metrics())
	}
}

func TestClientKey(t *testing.T) {
	cases := []struct {
		remote string
		hdr    map[string][]string
		want   string
	}{
		{"127.0.0.1:1", nil, "loopback-direct"},
		{"[::1]:1", nil, "loopback-direct"},
		{"127.0.0.1:1", map[string][]string{"X-Forwarded-For": {"203.0.113.9"}}, "203.0.113.9"},
		{"127.0.0.1:1", map[string][]string{"X-Forwarded-For": {" 2001:db8::1 "}}, "2001:db8::1"},
		{"127.0.0.1:1", map[string][]string{"X-Forwarded-For": {"203.0.113.9, 198.51.100.1"}}, "127.0.0.1"},
		{"127.0.0.1:1", map[string][]string{"X-Forwarded-For": {"203.0.113.9", "198.51.100.1"}}, "127.0.0.1"},
		{"127.0.0.1:1", map[string][]string{"X-Forwarded-For": {"not an ip"}}, "127.0.0.1"},
		{"127.0.0.1:1", map[string][]string{"Cf-Ray": {"x"}}, "127.0.0.1"},
		{"203.0.113.50:1", map[string][]string{"X-Forwarded-For": {"198.51.100.1"}}, "203.0.113.50"},
		{"203.0.113.50:1", nil, "203.0.113.50"},
	}
	for _, tc := range cases {
		r := newRequest("GET", "/v1/articles", "", remote(tc.remote))
		for k, vs := range tc.hdr {
			r.Header[k] = vs
		}
		if got := clientKey(r, ""); got != tc.want {
			t.Errorf("%s %v: %q, want %q", tc.remote, tc.hdr, got, tc.want)
		}
	}
}

func TestWriteBudget(t *testing.T) {
	cfg := baseConfig()
	p := principalByID(cfg, "synth-prod")
	p["writes_per_hour"], p["writes_burst"], p["writes_per_day"] = 60, 2, 3
	h := newHarness(t, cfg)
	pol := h.svc.Policy
	h.clock.t = time.Date(2026, 10, 1, 23, 0, 0, 0, time.UTC)
	for i := range 2 {
		if _, ok := pol.ChargeWrite("synth-prod"); !ok {
			t.Fatalf("charge %d refused", i)
		}
	}
	retry, ok := pol.ChargeWrite("synth-prod")
	if ok || retry != time.Minute {
		t.Fatalf("hourly bucket empty: %v %s", ok, retry)
	}
	h.clock.Advance(time.Minute)
	if _, ok := pol.ChargeWrite("synth-prod"); !ok {
		t.Fatal("refilled bucket refused")
	}
	if !pol.WritesExhausted("synth-prod") {
		t.Fatal("daily allowance of 3 spent but not exhausted")
	}
	h.clock.Advance(10 * time.Minute)
	retry, ok = pol.ChargeWrite("synth-prod")
	if ok || retry != 49*time.Minute {
		t.Fatalf("daily allowance: %v %s, want the time to UTC midnight", ok, retry)
	}
	if !pol.WritesExhausted("synth-prod") {
		t.Fatal("exhausted check charged or reset")
	}
	h.clock.Advance(49 * time.Minute)
	if pol.WritesExhausted("synth-prod") {
		t.Fatal("allowance not reset at UTC midnight")
	}
	if _, ok := pol.ChargeWrite("synth-prod"); !ok {
		t.Fatal("new day refused")
	}
	for _, id := range []string{"mcp-prod", "community-wiki", "nobody"} {
		if _, ok := pol.ChargeWrite(id); ok {
			t.Errorf("%s charged a write", id)
		}
		if !pol.WritesExhausted(id) {
			t.Errorf("%s has a write allowance", id)
		}
	}
	if !strings.Contains(h.metrics(), `cosift_v1_writes_total{principal="synth-prod"} 4`) {
		t.Fatal(h.metrics())
	}
}

// fakeArticles stands in for W3's handlers: it applies the SA §5.1, §5.3 and
// §6 rules through internal/v1 and the Policy, so the auth half is proven.
type fakeArticles struct {
	pol     v1.Policy
	lock    sync.Mutex
	prelive map[string]bool
	status  map[string]string
	golive  bool
	gate    chan struct{}
	entered chan struct{}
	writes  int
	embeds  int
}

func (f *fakeArticles) put(w http.ResponseWriter, r *http.Request) {
	p, _ := v1.PrincipalFrom(r.Context())
	var body struct {
		Status string `json:"status"`
		Title  string `json:"title"`
	}
	if !v1.Decode(w, r, &body, 512<<10) {
		return
	}
	need := v1.ScopeArticlesWrite
	if body.Status == "pending" {
		need = v1.ScopeArticlesStub
	}
	if !p.Has(need) {
		v1.WriteError(w, v1.MissingScope(need))
		return
	}
	if f.pol.WritesFrozen() {
		v1.WriteError(w, v1.WritesFrozen())
		return
	}
	if retry, ok := f.pol.ChargeWrite(p.ID); !ok {
		v1.WriteError(w, v1.WriteBudget(retry))
		return
	}
	if body.Title == "" {
		v1.WriteJSON(w, http.StatusUnprocessableEntity, map[string]string{"code": "invalid_field"})
		return
	}
	if f.entered != nil {
		f.entered <- struct{}{}
	}
	if f.gate != nil {
		<-f.gate
	}
	f.embeds++
	f.lock.Lock()
	defer f.lock.Unlock()
	active, ok := f.pol.Principal(p.ID)
	switch id := r.PathValue("id"); {
	case !ok || !active.Has(need):
		v1.WriteError(w, v1.MissingScope(need))
	case f.golive && active.Env == v1.EnvStaging:
		v1.WriteError(w, v1.EnvGolive())
	case f.status[id] != "" && !active.SameEnv(f.prelive[id]):
		v1.WriteError(w, v1.EnvMismatch())
	case f.status[id] == "tombstoned":
		v1.WriteJSON(w, http.StatusConflict, map[string]string{"code": "moderation_locked"})
	default:
		f.writes++
		v1.WriteJSON(w, http.StatusOK, map[string]string{"result": "updated"})
	}
}

func (f *fakeArticles) moderate(w http.ResponseWriter, r *http.Request) {
	p, _ := v1.PrincipalFrom(r.Context())
	var body struct {
		Action string `json:"action"`
	}
	if !v1.Decode(w, r, &body, 4<<10) {
		return
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	id := r.PathValue("id")
	switch {
	case f.golive && p.Env == v1.EnvStaging:
		v1.WriteError(w, v1.EnvGolive())
	case !p.SameEnv(f.prelive[id]):
		v1.WriteError(w, v1.EnvMismatch())
	case f.status[id] == "tombstoned":
		v1.WriteJSON(w, http.StatusConflict, map[string]string{"code": "illegal_transition"})
	default:
		v1.WriteJSON(w, http.StatusOK, map[string]string{"result": "ok"})
	}
}

func (f *fakeArticles) match(w http.ResponseWriter, r *http.Request) {
	p, _ := v1.PrincipalFrom(r.Context())
	var body struct {
		Q       string `json:"q"`
		Purpose string `json:"purpose"`
	}
	if !v1.Decode(w, r, &body, 8<<10) {
		return
	}
	if body.Purpose == "build" && !p.HasAny(v1.ScopeArticlesWrite, v1.ScopeArticlesStub) {
		v1.WriteError(w, v1.MissingScope(v1.ScopeArticlesWrite))
		return
	}
	writes := "open"
	if f.pol.WritesFrozen() {
		writes = "frozen"
	} else if body.Purpose == "build" && f.pol.WritesExhausted(p.ID) {
		writes = "budget_exhausted"
	}
	v1.WriteJSON(w, http.StatusOK, map[string]string{"verdict": "none", "writes": writes})
}

const (
	prodID    = "01J8ZC2Q7W4X9M3K5N6P8R0T2V"
	preliveID = "01J8ZD9R1S2T3V4W5X6Y7Z8A9B"
	stoneID   = "01J8ZE1A2B3C4D5E6F7G8H9J0K"
	pstoneID  = "01J8ZF1A2B3C4D5E6F7G8H9J0K"
)

func newFakeArticlesHarness(t *testing.T, cfg map[string]any) (*harness, *fakeArticles) {
	f := &fakeArticles{
		prelive: map[string]bool{prodID: false, preliveID: true, stoneID: false, pstoneID: true},
		status:  map[string]string{prodID: "published", preliveID: "published", stoneID: "tombstoned", pstoneID: "tombstoned"},
	}
	h := newHarness(t, cfg, withRoutes(func(func(string, v1.Principal, *http.Request)) []v1.Route {
		return []v1.Route{
			{Method: "PUT", Path: "/v1/articles/{id}", Scopes: []v1.Scope{v1.ScopeArticlesWrite, v1.ScopeArticlesStub}, BodyLimit: 512 << 10, Handler: http.HandlerFunc(f.put)},
			{Method: "POST", Path: "/v1/articles/{id}/status", Scopes: []v1.Scope{v1.ScopeArticlesModerate}, BodyLimit: 4 << 10, Handler: http.HandlerFunc(f.moderate)},
			{Method: "POST", Path: "/v1/articles/match", Scopes: []v1.Scope{v1.ScopeArticlesRead}, BodyLimit: 8 << 10, Handler: http.HandlerFunc(f.match)},
			{Method: "GET", Path: "/v1/articles", Scopes: []v1.Scope{v1.ScopeArticlesRead, v1.ScopeArticlesReadAll}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				v1.WriteJSON(w, http.StatusOK, map[string]any{"items": []any{}})
			})},
		}
	}))
	f.pol = h.svc.Policy
	h.svc.Policy.SetSwapLocker(&f.lock)
	return h, f
}

const article = `{"status":"published","title":"Rust async runtimes"}`

// §5.1 auth half: the stub-only principal cannot publish, and the build match
// needs write or stub.
func TestPerOperationScope(t *testing.T) {
	h, _ := newFakeArticlesHarness(t, baseConfig())
	resolver := h.token(resolverSub, resolverEmail)
	w := h.do("PUT", "/v1/articles/"+prodID, article, bearerAuth(resolver), jsonBody)
	if e := decodeErr(t, w); w.Code != http.StatusForbidden || e.Code != "missing_scope" || e.Detail != "requires scope articles:write" {
		t.Fatalf("stub token publishing: %d %+v", w.Code, e)
	}
	if w := h.do("PUT", "/v1/articles/"+prodID, `{"status":"pending","title":"x"}`, bearerAuth(resolver), jsonBody); w.Code != http.StatusOK {
		t.Fatalf("stub token writing a stub: %d %s", w.Code, w.Body.String())
	}
	synth := h.token(synthSub, synthEmail)
	if w := h.do("PUT", "/v1/articles/"+prodID, `{"status":"pending","title":"x"}`, bearerAuth(synth), jsonBody); w.Code != http.StatusForbidden {
		t.Fatalf("write token writing a stub: %d", w.Code)
	}
	w = h.do("POST", "/v1/articles/match", `{"q":"x","purpose":"build"}`, bearerAuth(h.token(mcpSub, mcpEmail)), jsonBody)
	if e := decodeErr(t, w); w.Code != http.StatusForbidden || e.Code != "missing_scope" {
		t.Fatalf("build match by a reader: %d %+v", w.Code, e)
	}
	for _, tok := range []string{synth, resolver} {
		if w := h.do("POST", "/v1/articles/match", `{"q":"x","purpose":"build"}`, bearerAuth(tok), jsonBody); w.Code != http.StatusOK {
			t.Fatalf("build match by a writer: %d", w.Code)
		}
	}
}

// §5.3 auth half: environment binding in both directions, on writes and on
// moderation with the two dashboard keys, always before any 409.
func TestEnvBinding(t *testing.T) {
	h, _ := newFakeArticlesHarness(t, baseConfig())
	cases := []struct {
		name, method, path, body, cred string
		code                           int
		errCode                        string
	}{
		{"staging writer on a prod record", "PUT", prodID, article, h.token(synthStgSub, synthStgEmail), 403, "env_mismatch"},
		{"prod writer on a prelive record", "PUT", preliveID, article, h.token(synthSub, synthEmail), 403, "env_mismatch"},
		{"staging writer on a prod tombstone", "PUT", stoneID, article, h.token(synthStgSub, synthStgEmail), 403, "env_mismatch"},
		{"prod writer on a prelive tombstone", "PUT", pstoneID, article, h.token(synthSub, synthEmail), 403, "env_mismatch"},
		{"prod writer on its own tombstone", "PUT", stoneID, article, h.token(synthSub, synthEmail), 409, ""},
		{"staging resolver on a prod record", "PUT", prodID, `{"status":"pending","title":"x"}`, h.token(resolverStSub, resolverStEm), 403, "env_mismatch"},
		{"staging writer on a prelive record", "PUT", preliveID, article, h.token(synthStgSub, synthStgEmail), 200, ""},
		{"dash-staging moderating a prod record", "POST", prodID + "/status", `{"action":"hold"}`, testKeys["dash-staging"], 403, "env_mismatch"},
		{"dash-prod moderating a prelive record", "POST", preliveID + "/status", `{"action":"hold"}`, testKeys["dash-prod"], 403, "env_mismatch"},
		{"dash-staging on a prod tombstone", "POST", stoneID + "/status", `{"action":"approve"}`, testKeys["dash-staging"], 403, "env_mismatch"},
		{"dash-prod on a prelive tombstone", "POST", pstoneID + "/status", `{"action":"approve"}`, testKeys["dash-prod"], 403, "env_mismatch"},
		{"dash-prod on its own record", "POST", prodID + "/status", `{"action":"hold"}`, testKeys["dash-prod"], 200, ""},
		{"dash-staging on its own record", "POST", preliveID + "/status", `{"action":"hold"}`, testKeys["dash-staging"], 200, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := h.do(tc.method, "/v1/articles/"+tc.path, tc.body, bearerAuth(tc.cred), jsonBody)
			if w.Code != tc.code {
				t.Fatalf("status %d, want %d (%s)", w.Code, tc.code, w.Body.String())
			}
			if tc.errCode != "" {
				if e := decodeErr(t, w); e.Code != tc.errCode || e.Detail != "record belongs to another environment" {
					t.Fatalf("%+v", e)
				}
			}
		})
	}
}

// A staging write authorised before the go-live reload and released after it
// is refused inside the store lock, and no swap lands while a mutation holds
// that lock.
func TestGoliveLocksAndSwapUnderLock(t *testing.T) {
	cfg := baseConfig()
	h, f := newFakeArticlesHarness(t, cfg)
	f.gate, f.entered = make(chan struct{}), make(chan struct{}, 1)
	staging := h.token(synthStgSub, synthStgEmail)
	res := make(chan int, 1)
	go func() { res <- h.do("PUT", "/v1/articles/"+preliveID, article, bearerAuth(staging), jsonBody).Code }()
	<-f.entered
	principalByID(cfg, "synth-staging")["scopes"] = []string{"articles:read", "retrieve:read"}
	delete(principalByID(cfg, "synth-staging"), "writes_per_hour")
	delete(principalByID(cfg, "synth-staging"), "writes_burst")
	delete(principalByID(cfg, "synth-staging"), "writes_per_day")
	writeConfig(t, h.path, cfg)
	if err := h.svc.Reload(t.Context()); err != nil {
		t.Fatal(err)
	}
	close(f.gate)
	if code := <-res; code != http.StatusForbidden {
		t.Fatalf("staging write across the reload: %d", code)
	}
	if f.writes != 0 {
		t.Fatal("the write landed")
	}

	f.gate, f.entered = nil, nil
	f.golive = true
	principalByID(cfg, "synth-staging")["scopes"] = []string{"articles:read", "articles:write", "retrieve:read"}
	writeConfig(t, h.path, cfg)
	if err := h.svc.Reload(t.Context()); err != nil {
		t.Fatal(err)
	}
	w := h.do("PUT", "/v1/articles/"+preliveID, article, bearerAuth(staging), jsonBody)
	if e := decodeErr(t, w); w.Code != http.StatusForbidden || e.Code != "env_golive" {
		t.Fatalf("staging write after go-live with the scope restored: %d %+v", w.Code, e)
	}
	w = h.do("POST", "/v1/articles/"+preliveID+"/status", `{"action":"hold"}`, bearerAuth(testKeys["dash-staging"]), jsonBody)
	if e := decodeErr(t, w); w.Code != http.StatusForbidden || e.Code != "env_golive" {
		t.Fatalf("dash-staging after go-live: %d %+v", w.Code, e)
	}

	f.lock.Lock()
	swapped := make(chan struct{})
	go func() {
		_ = h.svc.Reload(t.Context())
		close(swapped)
	}()
	select {
	case <-swapped:
		t.Fatal("a reload swapped the config while a mutation held the store lock")
	case <-time.After(200 * time.Millisecond):
	}
	f.lock.Unlock()
	<-swapped
}

// §6: the write budget is charged after decoding and before validation, so a
// refused write costs as much as an accepted one; reads pass while it is spent.
func TestWriteBudgetThroughHandlers(t *testing.T) {
	cfg := baseConfig()
	p := principalByID(cfg, "synth-prod")
	p["writes_per_hour"], p["writes_burst"], p["writes_per_day"] = 600, 600, 3
	h, f := newFakeArticlesHarness(t, cfg)
	synth := h.token(synthSub, synthEmail)
	if w := h.do("PUT", "/v1/articles/"+prodID, `{"status":"published"}`, bearerAuth(synth), jsonBody); w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("invalid write: %d", w.Code)
	}
	if w := h.do("PUT", "/v1/articles/"+stoneID, article, bearerAuth(synth), jsonBody); w.Code != http.StatusConflict {
		t.Fatalf("refused write: %d", w.Code)
	}
	if w := h.do("PUT", "/v1/articles/"+prodID, `{"status":`, bearerAuth(synth), jsonBody); w.Code != http.StatusBadRequest {
		t.Fatalf("undecodable write: %d", w.Code)
	}
	if w := h.do("PUT", "/v1/articles/"+prodID, article, bearerAuth(synth), jsonBody); w.Code != http.StatusOK {
		t.Fatalf("third charged write: %d", w.Code)
	}
	w := h.do("PUT", "/v1/articles/"+prodID, article, bearerAuth(synth), jsonBody)
	if e := decodeErr(t, w); w.Code != http.StatusTooManyRequests || e.Code != "write_budget" || w.Header().Get("Retry-After") == "" {
		t.Fatalf("fourth write: %d %+v", w.Code, e)
	}
	if f.embeds != 2 {
		t.Fatalf("%d writes reached the embedder, want 2", f.embeds)
	}
	for _, rr := range []struct{ m, p, b string }{{"GET", "/v1/articles", ""}, {"POST", "/v1/articles/match", `{"q":"x"}`}} {
		if w := h.do(rr.m, rr.p, rr.b, bearerAuth(synth), jsonBody); w.Code != http.StatusOK {
			t.Fatalf("read with the write budget spent: %d", w.Code)
		}
	}
	w = h.do("POST", "/v1/articles/match", `{"q":"x","purpose":"build"}`, bearerAuth(synth), jsonBody)
	if !strings.Contains(w.Body.String(), `"writes":"budget_exhausted"`) {
		t.Fatalf("build match: %s", w.Body.String())
	}
}

func TestWritesFrozen(t *testing.T) {
	cfg := baseConfig()
	cfg["writes_frozen"] = true
	h, _ := newFakeArticlesHarness(t, cfg)
	if !h.svc.Policy.WritesFrozen() {
		t.Fatal("not frozen")
	}
	w := h.do("PUT", "/v1/articles/"+prodID, article, bearerAuth(h.token(synthSub, synthEmail)), jsonBody)
	if e := decodeErr(t, w); w.Code != http.StatusServiceUnavailable || e.Code != "writes_frozen" || w.Header().Get("Retry-After") != "60" {
		t.Fatalf("frozen write: %d %+v", w.Code, e)
	}
	if w := h.do("POST", "/v1/articles/"+prodID+"/status", `{"action":"hold"}`, bearerAuth(testKeys["dash-prod"]), jsonBody); w.Code != http.StatusOK {
		t.Fatalf("moderation while frozen: %d", w.Code)
	}
	if !strings.Contains(h.do("POST", "/v1/articles/match", `{"q":"x","purpose":"build"}`, bearerAuth(h.token(synthSub, synthEmail)), jsonBody).Body.String(), `"writes":"frozen"`) {
		t.Fatal("build match does not report frozen")
	}
	if _, ok := h.svc.Policy.ChargeWrite("synth-prod"); !ok {
		t.Fatal("frozen writes were charged by the policy itself")
	}
}

func TestPolicyAccessors(t *testing.T) {
	cfg := baseConfig()
	cfg["golive_at"] = "2026-10-12T16:00:00Z"
	h := newHarness(t, cfg)
	pol := h.svc.Policy
	if at, ok := pol.GoliveAt(); !ok || !at.Equal(time.Date(2026, 10, 12, 16, 0, 0, 0, time.UTC)) {
		t.Fatalf("golive %v %v", at, ok)
	}
	if got := pol.StagingWriters(); strings.Join(got, ",") != "resolver-staging,synth-staging" {
		t.Fatalf("staging writers %v", got)
	}
	p, ok := pol.Principal("mcp-prod")
	if !ok || p.Env != v1.EnvProd || !p.CountsReads || !p.Has(v1.ScopeArticlesRead) || p.Kind != v1.KindOIDC {
		t.Fatalf("mcp-prod %+v", p)
	}
	p.Scopes[0] = v1.ScopeArticlesAdmin
	if again, _ := pol.Principal("mcp-prod"); again.Has(v1.ScopeArticlesAdmin) {
		t.Fatal("caller mutated the active config")
	}
	if _, ok := pol.Principal("nobody"); ok {
		t.Fatal("unknown principal")
	}
	empty := NewPolicy(nil, nil)
	if !empty.WritesFrozen() || len(empty.StagingWriters()) != 0 || empty.StagingWriters() == nil {
		t.Fatal("empty policy must be frozen with no writers")
	}
	if _, ok := empty.GoliveAt(); ok {
		t.Fatal("empty policy golive")
	}
}
