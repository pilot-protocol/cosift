package community

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

const leaseID = "c1b7e9f04a2d83657f0e1a9b3c5d7824"

func attestation(text string) map[string]any {
	return map[string]any{"lease_id": leaseID, "final_url": "https://example.org/blocked-page", "http_status": 200, "title": "Blocked page <&>", "text": text,
		"lang": "en", "content_hash": strings.Repeat("ab", 32), "simhash": "0123456789abcdef", "extract_version": 1}
}

func (r *workRig) submit(body any) *httptest.ResponseRecorder {
	return r.post("/api/work/submit", sharedTestToken, body, nil)
}

type submitReply struct {
	Verdict      string   `json:"verdict"`
	Agreeing     []string `json:"agreeing"`
	AgreeingN    *int     `json:"agreeing_count"`
	Indexed      bool     `json:"indexed"`
	WindowExpiry string   `json:"window_expires_at"`
	Reason       string   `json:"reason"`
}

func TestSubmitIsNotFoundWhileWorkIsOffAndNeedsABearer(t *testing.T) {
	r := newWorkRig(t)
	r.s.cfg.Work = false
	if w := r.submit(attestation("x")); w.Code != 404 || errBody(t, w).Code != "not_found" {
		t.Fatalf("off: %d %s", w.Code, w.Body)
	}
	r.s.cfg.Work = true
	if w := r.post("/api/work/submit", "", attestation("x"), nil); w.Code != 401 || errBody(t, w).Code != "bearer_required" {
		t.Fatalf("no bearer: %d %s", w.Code, w.Body)
	}
	cookie := func(req *http.Request) { req.AddCookie(&http.Cookie{Name: cookieName, Value: sharedTestToken}) }
	if w := r.post("/api/work/submit", "", attestation("x"), cookie); w.Code != 401 || errBody(t, w).Code != "bearer_required" {
		t.Fatalf("cookie: %d %s", w.Code, w.Body)
	}
	if len(r.engine.calls) != 0 {
		t.Fatal("the engine was called")
	}
}

func TestSubmitRefusesWhoHasNotJoinedIsBannedOrOnOldTerms(t *testing.T) {
	r := newWorkRig(t)
	for _, tc := range []struct {
		state sharedaccount.JoinState
		code  string
	}{
		{sharedaccount.JoinState{}, "not_joined"},
		{sharedaccount.JoinState{CID: testCID, TermsVersion: termsVersion, Banned: true}, "banned"},
		{sharedaccount.JoinState{CID: testCID, TermsVersion: 0}, "terms_outdated"},
	} {
		r.s.work.joins = newJoinCache(joinCacheKeys)
		r.f.joins[r.uid] = tc.state
		if w := r.submit(attestation("x")); w.Code != 403 || errBody(t, w).Code != tc.code {
			t.Errorf("%s: %d %s", tc.code, w.Code, w.Body)
		}
	}
	if len(r.engine.calls) != 0 {
		t.Fatal("the engine was called")
	}
}

func TestSubmitAddsWorkerAndNetKeepsTheTextExactAndRelaysACountNotTheWorkers(t *testing.T) {
	r := newWorkRig(t)
	r.engine.answer(fixed(200, `{"verdict":"verified","agreeing":["3f9a1c0b7d2e48a6915c0d3b7e2f4a18","9c1d0f6a2b3e4c5d6e7f8091a2b3c4d5"],"indexed":true}`))
	text := "Café \"quoted\" <b>bold</b> & more\nline two\ttabbed   end"
	w := r.submit(attestation(text))
	if w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	calls := r.engine.on("/v1/work/submit")
	if len(calls) != 1 || calls[0].auth != "Bearer "+testBrokerKey {
		t.Fatalf("%+v", calls)
	}
	var sent map[string]any
	json.Unmarshal(calls[0].body, &sent)
	net, _ := r.s.netID("203.0.113.9")
	if sent["worker"] != r.s.workerID(r.userID) || sent["net"] != net || sent["text"] != text || sent["lease_id"] != leaseID || sent["http_status"] != float64(200) || sent["extract_version"] != float64(1) || sent["title"] != "Blocked page <&>" || len(sent) != 11 {
		t.Fatalf("sent %v", sent)
	}
	var out submitReply
	json.Unmarshal(w.Body.Bytes(), &out)
	if out.Verdict != "verified" || out.AgreeingN == nil || *out.AgreeingN != 2 || !out.Indexed || out.Agreeing != nil || strings.Contains(w.Body.String(), "3f9a1c0b") || strings.Contains(w.Body.String(), "agreeing\"") {
		t.Fatalf("%s", w.Body)
	}
	r.s.flushFetchCounters(t.Context())
	if got := r.f.counter(testCID, "fetch_submitted"); got != 1 {
		t.Fatalf("fetch_submitted %d", got)
	}
}

func TestSubmitRelaysWindowAndReasonAndCountsOnlyRecordedAttestations(t *testing.T) {
	r := newWorkRig(t)
	for _, tc := range []struct {
		engine    string
		wantCount int64
	}{
		{`{"verdict":"awaiting_quorum","agreeing":[],"indexed":false,"window_expires_at":"2026-10-20T10:30:00Z"}`, 1},
		{`{"verdict":"mismatch","agreeing":[],"indexed":false}`, 1},
		{`{"verdict":"rejected","agreeing":[],"indexed":false,"reason":"spam"}`, 1},
		{`{"verdict":"released","agreeing":[],"indexed":false}`, 0},
	} {
		r.f.counters = map[string]map[string]int64{}
		r.s.work.counters = fetchCounters{}
		r.engine.answer(fixed(200, tc.engine))
		r.clk.advance(2 * time.Minute)
		w := r.submit(attestation("x"))
		var in, out map[string]any
		json.Unmarshal([]byte(tc.engine), &in)
		json.Unmarshal(w.Body.Bytes(), &out)
		for _, key := range []string{"verdict", "indexed", "window_expires_at", "reason"} {
			if in[key] != out[key] {
				t.Errorf("%s: %s = %v, engine said %v", tc.engine, key, out[key], in[key])
			}
		}
		if out["agreeing_count"] != float64(0) {
			t.Errorf("%s: agreeing_count %v", tc.engine, out["agreeing_count"])
		}
		r.s.flushFetchCounters(t.Context())
		if got := r.f.counter(testCID, "fetch_submitted"); got != tc.wantCount {
			t.Errorf("%s: fetch_submitted %d, want %d", tc.engine, got, tc.wantCount)
		}
	}
}

func TestSubmitRaisesTheCeilingAlertForADeferredVerdict(t *testing.T) {
	r := newWorkRig(t)
	r.engine.answer(fixed(200, `{"verdict":"deferred","agreeing":["3f9a1c0b7d2e48a6915c0d3b7e2f4a18"],"indexed":false,"reason":"moderation_ceiling"}`))
	w := r.submit(attestation("x"))
	var out submitReply
	json.Unmarshal(w.Body.Bytes(), &out)
	if w.Code != 200 || out.Verdict != "deferred" || out.Reason != "moderation_ceiling" || *out.AgreeingN != 1 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	r.s.drainAlerts(t.Context())
	if len(r.f.alerts) != 1 || r.f.alerts[0].Subject != "2026-10-14" {
		t.Fatalf("%+v", r.f.alerts)
	}
	r.engine.answer(fixed(200, `{"verdict":"deferred","agreeing":[],"indexed":false,"reason":"other"}`))
	r.clk.advance(24 * time.Hour)
	r.submit(attestation("x"))
	r.s.drainAlerts(t.Context())
	if len(r.f.alerts) != 1 {
		t.Fatal("a deferred verdict for another reason raised the ceiling alert")
	}
}

func TestSubmitTakesNoWorkerNetOrUnknownMemberFromTheClient(t *testing.T) {
	r := newWorkRig(t)
	for _, extra := range []string{"worker", "net", "agreeing", "verdict", "x"} {
		body := attestation("x")
		body[extra] = strings.Repeat("a", 32)
		if w := r.submit(body); w.Code != 400 || errBody(t, w).Code != "unknown_field" {
			t.Errorf("%s: %d %s", extra, w.Code, w.Body)
		}
	}
	for name, raw := range map[string]string{
		"an array": `[1]`, "null": `null`, "a string": `"x"`, "trailing data": `{"lease_id":"a"} {}`, "broken": `{"lease_id":`,
	} {
		if w := r.post("/api/work/submit", sharedTestToken, []byte(raw), nil); w.Code != 400 || errBody(t, w).Code != "invalid_body" {
			t.Errorf("%s: %d %s", name, w.Code, w.Body)
		}
	}
	if len(r.engine.calls) != 0 {
		t.Fatal("a refused body reached the engine")
	}
}

func TestSubmitForwardsAFailureReportWithoutInventingMembers(t *testing.T) {
	r := newWorkRig(t)
	r.engine.answer(fixed(200, `{"verdict":"released","agreeing":[],"indexed":false}`))
	if w := r.submit(map[string]any{"lease_id": leaseID, "http_status": 403}); w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	var sent map[string]any
	json.Unmarshal(r.engine.on("/v1/work/submit")[0].body, &sent)
	if len(sent) != 4 || sent["http_status"] != float64(403) || sent["lease_id"] != leaseID || sent["worker"] == nil || sent["net"] == nil {
		t.Fatalf("sent %v", sent)
	}
}

func TestSubmitChecksTheEnvelopeSizesAndTypes(t *testing.T) {
	r := newWorkRig(t)
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
		status int
		code   string
		field  string
	}{
		{"long title", func(m map[string]any) { m["title"] = strings.Repeat("t", 1001) }, 422, "invalid_field", "title"},
		{"long text", func(m map[string]any) { m["text"] = strings.Repeat("t", 512<<10+1) }, 422, "invalid_field", "text"},
		{"long final url", func(m map[string]any) { m["final_url"] = "https://example.org/" + strings.Repeat("p", 2048) }, 422, "invalid_field", "final_url"},
		{"long lease id", func(m map[string]any) { m["lease_id"] = strings.Repeat("a", 65) }, 422, "invalid_field", "lease_id"},
		{"long lang", func(m map[string]any) { m["lang"] = strings.Repeat("a", 36) }, 422, "invalid_field", "lang"},
		{"long hash", func(m map[string]any) { m["content_hash"] = strings.Repeat("a", 65) }, 422, "invalid_field", "content_hash"},
		{"long simhash", func(m map[string]any) { m["simhash"] = strings.Repeat("a", 17) }, 422, "invalid_field", "simhash"},
		{"a string status", func(m map[string]any) { m["http_status"] = "200" }, 400, "invalid_body", ""},
		{"a float version", func(m map[string]any) { m["extract_version"] = 1.5 }, 400, "invalid_body", ""},
		{"a number title", func(m map[string]any) { m["title"] = 7 }, 400, "invalid_body", ""},
		{"an array artifact", func(m map[string]any) { m["artifact"] = []int{1} }, 400, "invalid_body", ""},
		{"text at the limit", func(m map[string]any) { m["text"] = strings.Repeat("t", 512<<10) }, 200, "", ""},
		{"a null artifact", func(m map[string]any) { m["artifact"] = nil }, 200, "", ""},
	} {
		r.engine.answer(fixed(200, `{"verdict":"awaiting_quorum","agreeing":[],"indexed":false}`))
		r.clk.advance(2 * time.Minute)
		body := attestation("x")
		tc.mutate(body)
		w := r.submit(body)
		b := errBody(t, w)
		if w.Code != tc.status || b.Code != tc.code || b.Field != tc.field || (tc.field != "" && b.Rule != "length") {
			t.Errorf("%s: %d %s", tc.name, w.Code, w.Body)
		}
	}
}

func TestSubmitBodyCapIsTwoMiBForSubmitAndOneMiBElsewhere(t *testing.T) {
	r := newWorkRig(t)
	r.engine.answer(fixed(200, `{"verdict":"awaiting_quorum","agreeing":[],"indexed":false}`))
	artifact := func(n int) map[string]any {
		body := attestation("x")
		body["artifact"] = map[string]any{"model": "m", "chunks": []any{map[string]any{"text": strings.Repeat("a", n)}}}
		return body
	}
	if w := r.submit(artifact(1500 << 10)); w.Code != 200 {
		t.Fatalf("1.5 MiB: %d %.200s", w.Code, w.Body)
	}
	r.clk.advance(2 * time.Minute)
	w := r.submit(artifact(2100 << 10))
	if w.Code != 413 || errBody(t, w).Code != "body_too_large" {
		t.Fatalf("2.1 MiB: %d %.200s", w.Code, w.Body)
	}
	meter := meterBody("body-cap-request-01")
	meter["pad"] = strings.Repeat("a", 1500<<10)
	if w := meterCall(r.s, sharedTestToken, meter, nil); w.Code == 200 || w.Code == 402 {
		t.Fatalf("1.5 MiB meter body: %d", w.Code)
	}
}

func TestSubmitLimitsAUserToSixtyFourAMinute(t *testing.T) {
	r := newWorkRig(t)
	r.engine.answer(fixed(200, `{"verdict":"awaiting_quorum","agreeing":[],"indexed":false}`))
	for i := 0; i < workSubmitPerMinute; i++ {
		if w := r.submit(attestation("x")); w.Code != 200 {
			t.Fatalf("submit %d: %d %s", i, w.Code, w.Body)
		}
	}
	w := r.submit(attestation("x"))
	if b := errBody(t, w); w.Code != 429 || b.Code != "rate_limited" || b.Retry < 1 || w.Header().Get("Retry-After") == "" {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
}

func TestSubmitDerivesNetFromThisRequestSoAMovedWorkerMeetsLeaseNotOwner(t *testing.T) {
	r := newWorkRig(t)
	leased := ""
	r.engine.answer(func(call engineRequest) (int, string) {
		var in struct{ Net string }
		json.Unmarshal(call.body, &in)
		if call.path == "/v1/work/claim" {
			leased = in.Net
			return 200, `{"leases":[]}`
		}
		if in.Net != leased {
			return 409, `{"error":"x","status":409,"code":"lease_not_owner"}`
		}
		return 200, `{"verdict":"awaiting_quorum","agreeing":[],"indexed":false}`
	})
	from := func(addr string, mutate func(*http.Request)) *httptest.ResponseRecorder {
		return r.post("/api/work/submit", sharedTestToken, attestation("x"), func(req *http.Request) { req.RemoteAddr = addr })
	}
	r.post("/api/work/claim", sharedTestToken, map[string]any{"max": 1}, func(req *http.Request) { req.RemoteAddr = "203.0.113.9:1" })
	if w := from("203.0.113.200:1", nil); w.Code != 200 {
		t.Fatalf("same /24: %d %s", w.Code, w.Body)
	}
	if w := from("198.51.100.4:1", nil); w.Code != 409 || errBody(t, w).Code != "lease_not_owner" {
		t.Fatalf("moved: %d %s", w.Code, w.Body)
	}
}

func TestSubmitPassesTheEnginesLeaseAndFieldRefusalsThrough(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		want   int
		code   string
		extra  string
	}{
		{409, `{"error":"x","status":409,"code":"lease_expired","detail":"secret"}`, 409, "lease_expired", ""},
		{409, `{"error":"x","status":409,"code":"lease_not_owner"}`, 409, "lease_not_owner", ""},
		{409, `{"error":"x","status":409,"code":"extract_version_mismatch","extract_version":3}`, 409, "extract_version_mismatch", `"extract_version":3`},
		{422, `{"error":"x","status":422,"code":"invalid_field","field":"content_hash","rule":"mismatch"}`, 422, "invalid_field", `"field":"content_hash"`},
		{422, `{"error":"x","status":422,"code":"invalid_field","field":"<script>","rule":"mismatch"}`, 422, "invalid_field", ""},
		{429, `{"code":"rate_limited"}`, 429, "rate_limited", ""},
		{503, `{"code":"work_disabled"}`, 503, "work_disabled", ""},
		{503, `{"code":"index_unavailable"}`, 503, "engine_unavailable", ""},
		{413, `{"code":"body_too_large"}`, 503, "engine_unavailable", ""},
		{401, `{"code":"unauthorized"}`, 503, "engine_unavailable", ""},
		{500, `boom`, 503, "engine_unavailable", ""},
	} {
		r := newWorkRig(t)
		logs := captureLog(t)
		r.engine.answer(fixed(tc.status, tc.body))
		w := r.submit(attestation("x"))
		if w.Code != tc.want || errBody(t, w).Code != tc.code || (tc.extra != "" && !strings.Contains(w.Body.String(), tc.extra)) {
			t.Errorf("%d %s: %d %s", tc.status, tc.code, w.Code, w.Body)
		}
		if strings.Contains(w.Body.String(), "secret") || strings.Contains(w.Body.String(), "<script>") || strings.Contains(logs.String(), testBrokerKey) {
			t.Errorf("%d: leaked %s %s", tc.status, w.Body, logs)
		}
		r.s.flushFetchCounters(t.Context())
		if r.f.counter(testCID, "fetch_submitted") != 0 {
			t.Errorf("%d: a refused submit moved fetch_submitted", tc.status)
		}
	}
}
