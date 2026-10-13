package community

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

const claimReply = `{"leases":[{"lease_id":"c1b7e9f04a2d83657f0e1a9b3c5d7824","url":"https://example.org/blocked-page","source":"errored","expires_at":"2026-10-14T09:40:00Z","extract_version":1},{"lease_id":"d2c8fa05b3e94768a01f2b4c6d8e9035","url":"https://example.org/two","source":"pushed","expires_at":"2026-10-14T09:40:00Z","extract_version":1}]}`

type codeBody struct {
	Error string `json:"error"`
	Code  string `json:"code"`
	Field string `json:"field"`
	Rule  string `json:"rule"`
	Retry int    `json:"retry_after_seconds"`
}

func errBody(t *testing.T, w *httptest.ResponseRecorder) codeBody {
	t.Helper()
	var b codeBody
	json.Unmarshal(w.Body.Bytes(), &b)
	return b
}

func TestClaimIsNotFoundWhileWorkIsOffAndNeverReachesTheEngine(t *testing.T) {
	r := newWorkRig(t)
	r.s.cfg.Work = false
	w := r.claim(map[string]any{"max": 1})
	if w.Code != 404 || errBody(t, w).Code != "not_found" {
		t.Errorf("%d %s", w.Code, w.Body)
	}
	if len(r.engine.calls) != 0 {
		t.Fatal("the engine was called")
	}
}

func TestClaimNeedsABearerAndTheClientHeader(t *testing.T) {
	r := newWorkRig(t)
	cookie := func(req *http.Request) { req.AddCookie(&http.Cookie{Name: cookieName, Value: sharedTestToken}) }
	for _, tc := range []struct {
		name   string
		token  string
		mutate func(*http.Request)
		status int
		code   string
	}{
		{"no credential", "", nil, 401, "bearer_required"},
		{"a cookie only", "", cookie, 401, "bearer_required"},
		{"an unknown token", "ck_1_BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", nil, 401, "unauthorized"},
		{"no client header", sharedTestToken, func(req *http.Request) { req.Header.Del("X-Cosift-Client") }, 403, ""},
	} {
		w := r.post("/api/work/claim", tc.token, map[string]any{"max": 1}, tc.mutate)
		if w.Code != tc.status || (tc.code != "" && errBody(t, w).Code != tc.code) {
			t.Errorf("%s: %d %s", tc.name, w.Code, w.Body)
		}
	}
	if len(r.engine.calls) != 0 {
		t.Fatal("the engine was called")
	}
}

func TestClaimRefusesWhoHasNotJoinedIsBannedOrOnOldTermsBeforeTheEngine(t *testing.T) {
	r := newWorkRig(t)
	for _, tc := range []struct {
		state  sharedaccount.JoinState
		err    error
		status int
		code   string
	}{
		{sharedaccount.JoinState{}, nil, 403, "not_joined"},
		{sharedaccount.JoinState{CID: testCID, TermsVersion: termsVersion, Banned: true}, nil, 403, "banned"},
		{sharedaccount.JoinState{CID: testCID, TermsVersion: 0}, nil, 403, "terms_outdated"},
		{sharedaccount.JoinState{}, sharedaccount.ErrUnavailable, 503, "contributors_unavailable"},
	} {
		r.s.work.joins = newJoinCache(joinCacheKeys)
		r.f.joins[r.uid], r.f.joinErr = tc.state, tc.err
		w := r.claim(map[string]any{"max": 2})
		if w.Code != tc.status || errBody(t, w).Code != tc.code {
			t.Errorf("%s: %d %s", tc.code, w.Code, w.Body)
		}
	}
	if len(r.engine.calls) != 0 || r.rows(`SELECT count(*) FROM work_workers`) != 0 {
		t.Fatal("a refused claim reached the engine or registered a worker")
	}
}

func TestClaimMintsIdentitiesCallsTheEngineWithTheBrokerKeyAndRelaysWithoutThem(t *testing.T) {
	r := newWorkRig(t)
	r.engine.answer(fixed(200, claimReply))
	w := r.claim(map[string]any{"max": 8})
	if w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	calls := r.engine.on("/v1/work/claim")
	if len(calls) != 1 || calls[0].method != "POST" || calls[0].auth != "Bearer "+testBrokerKey {
		t.Fatalf("%+v", calls)
	}
	var sent struct {
		Worker, Net string
		Max         int
	}
	json.Unmarshal([]byte(strings.ToLower(string(calls[0].body))), &sent)
	net, _ := r.s.netID("203.0.113.9")
	if sent.Worker != r.s.workerID(r.userID) || sent.Net != net || sent.Max != 8 || !opaqueID.MatchString(sent.Worker) || !opaqueID.MatchString(sent.Net) {
		t.Fatalf("sent %+v", sent)
	}
	if strings.Contains(string(calls[0].body), "203.0.113") || strings.Contains(string(calls[0].body), r.userID) || strings.Contains(string(calls[0].body), "example.com") {
		t.Fatalf("the engine saw an address or an account: %s", calls[0].body)
	}
	if strings.Contains(w.Body.String(), sent.Worker) || strings.Contains(w.Body.String(), sent.Net) {
		t.Fatal("the answer carries a worker or net id")
	}
	var out struct {
		Leases []struct {
			LeaseID        string `json:"lease_id"`
			URL            string `json:"url"`
			Source         string `json:"source"`
			ExpiresAt      string `json:"expires_at"`
			ExtractVersion int    `json:"extract_version"`
		} `json:"leases"`
	}
	json.Unmarshal(w.Body.Bytes(), &out)
	if len(out.Leases) != 2 || out.Leases[0].LeaseID != "c1b7e9f04a2d83657f0e1a9b3c5d7824" || out.Leases[1].Source != "pushed" || out.Leases[0].ExtractVersion != 1 {
		t.Fatalf("%s", w.Body)
	}
	var user, cid string
	if err := r.s.db.QueryRow(`SELECT user_id,cid FROM work_workers WHERE worker=?`, sent.Worker).Scan(&user, &cid); err != nil || user != r.userID || cid != testCID {
		t.Fatalf("work_workers: %q %q %v", user, cid, err)
	}
	r.claim(map[string]any{"max": 1})
	if r.rows(`SELECT count(*) FROM work_workers`) != 1 {
		t.Fatal("a second claim added a second worker row")
	}
	r.s.flushFetchCounters(t.Context())
	if got := r.f.counter(testCID, "fetch_claimed"); got != 4 {
		t.Fatalf("fetch_claimed %d, want 4 leases over two claims", got)
	}
}

func TestClaimBoundsMaxAndRejectsAnythingButMax(t *testing.T) {
	r := newWorkRig(t)
	r.engine.answer(fixed(200, `{"leases":[]}`))
	for _, tc := range []struct {
		body     any
		status   int
		wantMax  int
		wantCode string
	}{
		{map[string]any{}, 200, 1, ""},
		{map[string]any{"max": 1}, 200, 1, ""},
		{map[string]any{"max": 32}, 200, 32, ""},
		{map[string]any{"max": 0}, 422, 0, "invalid_field"},
		{map[string]any{"max": 33}, 422, 0, "invalid_field"},
		{map[string]any{"max": -1}, 422, 0, "invalid_field"},
		{map[string]any{"max": "8"}, 400, 0, "invalid_body"},
		{map[string]any{"max": 2, "worker": strings.Repeat("a", 32)}, 400, 0, "invalid_body"},
		{[]byte(`not json`), 400, 0, "invalid_body"},
	} {
		r.clk.advance(2 * time.Minute)
		before := len(r.engine.on("/v1/work/claim"))
		w := r.claim(tc.body)
		if w.Code != tc.status || errBody(t, w).Code != tc.wantCode {
			t.Errorf("%v: %d %s", tc.body, w.Code, w.Body)
			continue
		}
		calls := r.engine.on("/v1/work/claim")
		if tc.status != 200 {
			if len(calls) != before {
				t.Errorf("%v reached the engine", tc.body)
			}
			if tc.status == 422 {
				if b := errBody(t, w); b.Field != "max" || b.Rule != "range" {
					t.Errorf("%v: %+v", tc.body, b)
				}
			}
			continue
		}
		var sent struct{ Max int }
		json.Unmarshal([]byte(strings.ToLower(string(calls[len(calls)-1].body))), &sent)
		if sent.Max != tc.wantMax {
			t.Errorf("%v: engine saw max %d, want %d", tc.body, sent.Max, tc.wantMax)
		}
	}
}

func TestClaimLimitsAUserToSixAMinuteAndKeepsUsersApart(t *testing.T) {
	r := newWorkRig(t)
	r.engine.answer(fixed(200, `{"leases":[]}`))
	for i := 0; i < 6; i++ {
		if w := r.claim(map[string]any{"max": 1}); w.Code != 200 {
			t.Fatalf("claim %d: %d %s", i, w.Code, w.Body)
		}
	}
	w := r.claim(map[string]any{"max": 1})
	if b := errBody(t, w); w.Code != 429 || b.Code != "rate_limited" || b.Retry < 1 || w.Header().Get("Retry-After") == "" {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	other := "fedcba9876543210"
	r.f.joins[other] = sharedaccount.JoinState{CID: "1111111111111111", TermsVersion: termsVersion}
	if w := r.post("/api/work/claim", sharedOtherToken, map[string]any{"max": 1}, nil); w.Code != 200 {
		t.Fatalf("another user: %d %s", w.Code, w.Body)
	}
}

func TestClaimRelaysTheEnginesWaitAndCeilingAndQueuesTheAlert(t *testing.T) {
	r := newWorkRig(t)
	r.engine.answer(fixed(200, `{"leases":[],"retry_after_s":1800,"moderation_ceiling":true}`))
	w := r.claim(map[string]any{"max": 4})
	var out struct {
		Leases     []any `json:"leases"`
		RetryAfter int   `json:"retry_after_s"`
		Ceiling    bool  `json:"moderation_ceiling"`
	}
	json.Unmarshal(w.Body.Bytes(), &out)
	if w.Code != 200 || out.Leases == nil || len(out.Leases) != 0 || out.RetryAfter != 1800 || !out.Ceiling {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	r.s.drainAlerts(t.Context())
	if len(r.f.alerts) != 1 || r.f.alerts[0].Subject != "2026-10-14" {
		t.Fatalf("alerts %+v", r.f.alerts)
	}
	r.engine.answer(fixed(200, `{"leases":[],"retry_after_s":30}`))
	r.clk.advance(time.Minute)
	if w := r.claim(map[string]any{"max": 1}); strings.Contains(w.Body.String(), "moderation_ceiling") {
		t.Fatalf("a ceiling the engine did not report: %s", w.Body)
	}
}

func TestClaimMapsEngineFailuresWithoutLeakingThem(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		header map[string]string
		want   int
		code   string
	}{
		{"rate limited", 429, `{"error":"x","status":429,"code":"rate_limited","detail":"d"}`, map[string]string{"Retry-After": "17"}, 429, "rate_limited"},
		{"work disabled", 503, `{"error":"x","status":503,"code":"work_disabled","detail":"d"}`, nil, 503, "work_disabled"},
		{"index unavailable", 503, `{"error":"x","status":503,"code":"index_unavailable"}`, nil, 503, "engine_unavailable"},
		{"invalid field", 422, `{"error":"x","status":422,"code":"invalid_field","field":"max","rule":"range"}`, nil, 422, "invalid_field"},
		{"unauthorised", 401, `{"error":"x","code":"unauthorized"}`, nil, 503, "engine_unavailable"},
		{"forbidden", 403, `{"error":"x","code":"missing_scope"}`, nil, 503, "engine_unavailable"},
		{"a body it should not have sent", 400, `{"error":"x","code":"invalid_body"}`, nil, 503, "engine_unavailable"},
		{"a server error", 500, `oops`, nil, 503, "engine_unavailable"},
	} {
		r := newWorkRig(t)
		logs := captureLog(t)
		r.engine.answer(fixed(tc.status, tc.body))
		r.engine.header = tc.header
		w := r.claim(map[string]any{"max": 1})
		if w.Code != tc.want || errBody(t, w).Code != tc.code {
			t.Errorf("%s: %d %s", tc.name, w.Code, w.Body)
		}
		if tc.name == "rate limited" && (w.Header().Get("Retry-After") != "17" || errBody(t, w).Retry != 17) {
			t.Errorf("%s: wait %q %s", tc.name, w.Header().Get("Retry-After"), w.Body)
		}
		if strings.Contains(w.Body.String(), "detail") || strings.Contains(logs.String(), testBrokerKey) {
			t.Errorf("%s: leaked: %s / %s", tc.name, w.Body, logs)
		}
		if tc.status == 401 || tc.status == 403 {
			if !strings.Contains(logs.String(), "/v1/work/claim") || !strings.Contains(logs.String(), "HTTP "+strconv.Itoa(tc.status)) {
				t.Errorf("%s: no operator-visible line: %s", tc.name, logs)
			}
		}
	}
}

func TestClaimIsUnavailableWhenTheEngineIsUnreachable(t *testing.T) {
	r := newWorkRig(t)
	engine, _ := newEngineClient("http://127.0.0.1:1", testBrokerKey)
	r.s.engine = engine
	w := r.claim(map[string]any{"max": 1})
	if w.Code != 503 || errBody(t, w).Code != "engine_unavailable" {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
}

func TestClaimHashesTheNetworkAtTheDocumentedBoundaries(t *testing.T) {
	r := newWorkRig(t)
	r.engine.answer(fixed(200, `{"leases":[]}`))
	r.s.trustedProxies = nil
	netOf := func(remote string) string {
		t.Helper()
		r.clk.advance(2 * time.Minute)
		before := len(r.engine.on("/v1/work/claim"))
		w := r.post("/api/work/claim", sharedTestToken, map[string]any{"max": 1}, func(req *http.Request) { req.RemoteAddr = remote })
		calls := r.engine.on("/v1/work/claim")
		if w.Code != 200 || len(calls) != before+1 {
			t.Fatalf("%s: %d %s", remote, w.Code, w.Body)
		}
		var sent struct{ Net string }
		json.Unmarshal(calls[len(calls)-1].body, &sent)
		return sent.Net
	}
	base := netOf("203.0.113.5:1")
	if netOf("203.0.113.250:1") != base || netOf("[::ffff:203.0.113.77]:1") != base {
		t.Error("addresses of one /24 give different nets")
	}
	if netOf("203.0.114.5:1") == base {
		t.Error("two /24s share a net")
	}
	v6 := netOf("[2001:db8:1:1::1]:1")
	if netOf("[2001:db8:1:2::9]:1") != v6 {
		t.Error("two /64s of one /48 give different nets")
	}
	if netOf("[2001:db8:2::1]:1") == v6 {
		t.Error("the next /48 shares a net")
	}
}

func TestClaimRefusesAClientAddressThatDoesNotParse(t *testing.T) {
	r := newWorkRig(t)
	w := r.post("/api/work/claim", sharedTestToken, map[string]any{"max": 1}, func(req *http.Request) { req.RemoteAddr = "pipe" })
	if w.Code != 400 || errBody(t, w).Code != "invalid_client_address" || len(r.engine.calls) != 0 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
}

func TestClaimStopsAtTheDailyFetchCapUntilMidnightUTC(t *testing.T) {
	r := newWorkRig(t)
	r.s.cfg.FetchDailyCap = 3
	r.engine.answer(fixed(200, `{"leases":[]}`))
	credit := func(id string, at time.Time) {
		if _, err := r.s.db.Exec(`INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES(?,?,2,'fetch_verified',?)`, id, r.userID, at.Unix()); err != nil {
			t.Fatal(err)
		}
	}
	yesterday := time.Date(2026, 10, 13, 23, 59, 59, 0, time.UTC)
	credit("fetch:old1", yesterday)
	credit("fetch:old2", yesterday)
	credit("fetch:old3", yesterday)
	credit("fetch:t1", time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC))
	credit("fetch:t2", time.Date(2026, 10, 14, 6, 0, 0, 0, time.UTC))
	if w := r.claim(map[string]any{"max": 1}); w.Code != 200 {
		t.Fatalf("two of three today: %d %s", w.Code, w.Body)
	}
	credit("fetch:t3", time.Date(2026, 10, 14, 8, 0, 0, 0, time.UTC))
	r.clk.advance(14*time.Hour + 29*time.Minute + 30*time.Second)
	w := r.claim(map[string]any{"max": 1})
	b := errBody(t, w)
	if w.Code != 429 || b.Code != "daily_cap" || b.Retry != 30 || w.Header().Get("Retry-After") != "30" {
		t.Fatalf("%d %s retry-after %q", w.Code, w.Body, w.Header().Get("Retry-After"))
	}
	calls := len(r.engine.on("/v1/work/claim"))
	r.clk.advance(31 * time.Second)
	if w := r.claim(map[string]any{"max": 1}); w.Code != 200 || len(r.engine.on("/v1/work/claim")) != calls+1 {
		t.Fatalf("after midnight: %d %s", w.Code, w.Body)
	}
}
