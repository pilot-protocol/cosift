package community

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testRef = "01J8ZC2Q7W4X9M3K5N6P8R0T2V"

func meterServer(t *testing.T, balance int) (*Server, string) {
	t.Helper()
	s := testServer(t, nil)
	s.cfg.Shared = &fakeShared{}
	w := bearerRequest(s, "/api/me", sharedTestToken)
	expect(t, w, 200)
	var u User
	json.Unmarshal(w.Body.Bytes(), &u)
	if err := s.grantMonthlyCredits(t.Context(), u.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES('adjust',?,?,'test',0)`, u.ID, balance-monthlyFreeCredits); err != nil {
		t.Fatal(err)
	}
	return s, u.ID
}

func meterCall(s *Server, token string, body any, headers map[string]string) *httptest.ResponseRecorder {
	raw, _ := json.Marshal(body)
	r := httptest.NewRequest("POST", "/api/meter", bytes.NewReader(raw))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Cosift-Client", "community")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}

func meterBody(id string) map[string]string {
	return map[string]string{"kind": "article", "ref": testRef, "request_id": id}
}

func decodeMeter(t *testing.T, w *httptest.ResponseRecorder) (a struct {
	Charged         int    `json:"charged"`
	Balance         int    `json:"balance"`
	RequestID       string `json:"request_id"`
	Replayed        bool   `json:"replayed"`
	Code            string `json:"code"`
	CreditsRequired int    `json:"credits_required"`
	TopupURL        string `json:"topup_url"`
}) {
	t.Helper()
	if err := json.Unmarshal(w.Body.Bytes(), &a); err != nil {
		t.Fatalf("%v: %s", err, w.Body.String())
	}
	return a
}

func meterRows(t *testing.T, s *Server) int {
	t.Helper()
	var n int
	s.db.QueryRow(`SELECT count(*) FROM credit_ledger WHERE reason='article_read'`).Scan(&n)
	return n
}

func TestMeterChargesTwoOnceAndAnswersTheContractShape(t *testing.T) {
	s, uid := meterServer(t, 10)
	w := meterCall(s, sharedTestToken, meterBody("abcdefghijklmnop"), nil)
	expect(t, w, 200)
	a := decodeMeter(t, w)
	if a.Charged != 2 || a.Balance != 8 || a.RequestID != "abcdefghijklmnop" || a.Replayed {
		t.Fatalf("%+v", a)
	}
	var id, reason string
	var delta int
	s.db.QueryRow(`SELECT id,reason,delta FROM credit_ledger WHERE reason='article_read'`).Scan(&id, &reason, &delta)
	if id != "meter:"+uid+":abcdefghijklmnop" || delta != -2 {
		t.Fatalf("row %s %d", id, delta)
	}
}

func TestMeterRetryChargesOnceAndAnswersReplayedWhetherOrNotBroke(t *testing.T) {
	for _, start := range []int{3, 10} {
		s, _ := meterServer(t, start)
		expect(t, meterCall(s, sharedTestToken, meterBody("retry-request-0001"), nil), 200)
		for i := 0; i < 3; i++ {
			w := meterCall(s, sharedTestToken, meterBody("retry-request-0001"), nil)
			expect(t, w, 200)
			if a := decodeMeter(t, w); !a.Replayed || a.Charged != 2 || a.Balance != start-2 {
				t.Fatalf("balance %d, replay %d: %+v", start, i, a)
			}
		}
		if n := meterRows(t, s); n != 1 {
			t.Fatalf("%d rows for one request id", n)
		}
	}
}

func TestMeterRequestIDIsScopedToTheAccount(t *testing.T) {
	s, _ := meterServer(t, 10)
	expect(t, meterCall(s, sharedTestToken, meterBody("shared-request-id-1"), nil), 200)
	w := meterCall(s, sharedOtherToken, meterBody("shared-request-id-1"), nil)
	expect(t, w, 200)
	if decodeMeter(t, w).Replayed {
		t.Fatal("another account's request id replayed")
	}
	if n := meterRows(t, s); n != 2 {
		t.Fatalf("%d rows", n)
	}
}

func TestMeterConcurrentCallsNeverOverdraw(t *testing.T) {
	const k = 5
	s, _ := meterServer(t, 2*k+1)
	var ok, denied atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := meterCall(s, sharedTestToken, meterBody(fmt.Sprintf("concurrent-call-%04d", i)), nil)
			switch w.Code {
			case 200:
				ok.Add(1)
			case 402:
				denied.Add(1)
			default:
				t.Errorf("status %d: %s", w.Code, w.Body.String())
			}
		}()
	}
	wg.Wait()
	var balance int
	s.db.QueryRow(`SELECT sum(delta) FROM credit_ledger`).Scan(&balance)
	if ok.Load() != k || denied.Load() != 24-k || balance != 1 {
		t.Fatalf("charged %d denied %d balance %d", ok.Load(), denied.Load(), balance)
	}
}

func TestMeter402CarriesTheShapeEvenWithANegativeBalance(t *testing.T) {
	s, _ := meterServer(t, -50)
	w := meterCall(s, sharedTestToken, meterBody("negative-balance-1"), nil)
	expect(t, w, 402)
	a := decodeMeter(t, w)
	if a.Code != "insufficient_credits" || a.CreditsRequired != 2 || a.Balance != -50 || a.TopupURL != "http://localhost:7780/" {
		t.Fatalf("%+v", a)
	}
	if n := meterRows(t, s); n != 0 {
		t.Fatalf("%d rows written for a refusal", n)
	}
}

func TestMeterIsBearerOnlyAndGuardedByTheClientHeader(t *testing.T) {
	s, uid := meterServer(t, 10)
	cookie := &http.Cookie{Name: cookieName, Value: sharedTestToken}
	r := httptest.NewRequest("POST", "/api/meter", strings.NewReader(`{"kind":"article","ref":"`+testRef+`","request_id":"cookie-only-call-1"}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Cosift-Client", "community")
	r.AddCookie(cookie)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 401 || decodeMeter(t, w).Code != "bearer_required" {
		t.Fatalf("cookie-only: %d %s", w.Code, w.Body.String())
	}
	raw := `{"kind":"article","ref":"` + testRef + `","request_id":"no-client-header-1"}`
	r = httptest.NewRequest("POST", "/api/meter", strings.NewReader(raw))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+sharedTestToken)
	w = httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatalf("no client header: %d", w.Code)
	}
	if n := meterRows(t, s); n != 0 {
		t.Fatalf("%d rows charged for refused calls of %s", n, uid)
	}
	expect(t, meterCall(s, "invalid", meterBody("bad-token-call-001"), nil), 401)
	local := testServer(t, nil)
	expect(t, meterCall(local, sharedTestToken, meterBody("local-mode-call-01"), nil), 404)
}

func TestMeterValidatesTheBody(t *testing.T) {
	s, _ := meterServer(t, 10)
	for _, tc := range []struct {
		name string
		body any
		want int
		code string
	}{
		{"kind", map[string]string{"kind": "search", "ref": testRef, "request_id": "valid-request-id1"}, 422, "unsupported_kind"},
		{"no kind", map[string]string{"ref": testRef, "request_id": "valid-request-id1"}, 422, "unsupported_kind"},
		{"ref", map[string]string{"kind": "article", "ref": "not-a-ulid", "request_id": "valid-request-id1"}, 400, "invalid_ref"},
		{"lower ulid", map[string]string{"kind": "article", "ref": strings.ToLower(testRef), "request_id": "valid-request-id1"}, 400, "invalid_ref"},
		{"short id", map[string]string{"kind": "article", "ref": testRef, "request_id": "short"}, 400, "invalid_request_id"},
		{"long id", map[string]string{"kind": "article", "ref": testRef, "request_id": strings.Repeat("a", 65)}, 400, "invalid_request_id"},
		{"charset id", map[string]string{"kind": "article", "ref": testRef, "request_id": "has space in the id"}, 400, "invalid_request_id"},
		{"unknown member", map[string]any{"kind": "article", "ref": testRef, "request_id": "valid-request-id1", "extra": 1}, 400, "invalid_body"},
		{"not an object", []string{"x"}, 400, "invalid_body"},
	} {
		w := meterCall(s, sharedTestToken, tc.body, nil)
		if a := decodeMeter(t, w); w.Code != tc.want || a.Code != tc.code {
			t.Errorf("%s: %d %s", tc.name, w.Code, w.Body.String())
		}
	}
	if n := meterRows(t, s); n != 0 {
		t.Fatalf("%d rows charged for invalid bodies", n)
	}
}

func TestMeterLimitsEachUserSeparatelyAndOnlyAfterAuthentication(t *testing.T) {
	s, _ := meterServer(t, 1000)
	for i := 0; i < meterPerMinute; i++ {
		expect(t, meterCall(s, sharedTestToken, meterBody(fmt.Sprintf("limited-call-%05d", i)), nil), 200)
	}
	w := meterCall(s, sharedTestToken, meterBody("limited-call-over-0"), nil)
	if w.Code != 429 || w.Header().Get("Retry-After") == "" || decodeMeter(t, w).Code != "rate_limited" {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	expect(t, meterCall(s, sharedOtherToken, meterBody("limited-call-other"), nil), 200)
	before := len(s.routeLimits.keys)
	for i := 0; i < 50; i++ {
		meterCall(s, "ck_1_BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", meterBody(fmt.Sprintf("anonymous-flood-%03d", i)), nil)
	}
	if len(s.routeLimits.keys) != before {
		t.Fatalf("unauthenticated calls added %d limiter keys", len(s.routeLimits.keys)-before)
	}
}

func TestMeterNeverStoresOrLogsTheArticleRef(t *testing.T) {
	logs := captureLog(t)
	s, _ := meterServer(t, 10)
	ref := "01HZZZZZZZZZZZZZZZZZZZZZZZ"
	expect(t, meterCall(s, sharedTestToken, map[string]string{"kind": "article", "ref": ref, "request_id": "ref-privacy-check-1"}, nil), 200)
	meterCall(s, sharedTestToken, map[string]string{"kind": "article", "ref": ref, "request_id": "short"}, nil)
	meterCall(s, sharedTestToken, map[string]string{"kind": "article", "ref": "BADREF-" + ref, "request_id": "ref-privacy-check-2"}, nil)
	if strings.Contains(logs.String(), ref) {
		t.Fatal("the ref reached a log line")
	}
	rows, err := s.db.Query(`SELECT name FROM sqlite_master WHERE type='table'`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var name string
		rows.Scan(&name)
		tables = append(tables, name)
	}
	rows.Close()
	for _, name := range tables {
		cols, err := s.db.Query(`SELECT * FROM "` + name + `"`)
		if err != nil {
			t.Fatal(err)
		}
		names, _ := cols.Columns()
		for cols.Next() {
			vals := make([]any, len(names))
			ptrs := make([]any, len(names))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			cols.Scan(ptrs...)
			if strings.Contains(fmt.Sprint(vals...), ref) {
				t.Fatalf("table %s holds the ref", name)
			}
		}
		cols.Close()
	}
}

func TestNoCodePathDeletesOrRefundsALedgerRowButTheRequestReservation(t *testing.T) {
	files, _ := filepath.Glob("*.go")
	deletes := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, _ := os.ReadFile(f)
		for _, m := range regexp.MustCompile(`(?i)(DELETE\s+FROM\s+credit_ledger[^\x60"]*|UPDATE\s+credit_ledger)`).FindAll(src, -1) {
			deletes++
			if f != "credits.go" || !strings.Contains(string(m), "WHERE id=? AND user_id=? AND delta=?") {
				t.Errorf("%s: unexpected ledger mutation %q", f, m)
			}
		}
	}
	if deletes != 1 {
		t.Fatalf("%d ledger deletions, want only reserveCredit's own", deletes)
	}
}

func TestMeterChargesAtExactlyTheCostAndNeverFromAnotherAccountsCredits(t *testing.T) {
	s, uid := meterServer(t, 2)
	w := meterCall(s, sharedTestToken, meterBody("exact-balance-0001"), nil)
	expect(t, w, 200)
	if a := decodeMeter(t, w); a.Balance != 0 {
		t.Fatalf("%+v", a)
	}
	expect(t, meterCall(s, sharedTestToken, meterBody("exact-balance-0002"), nil), 402)
	expect(t, bearerRequest(s, "/api/me", sharedOtherToken), 200)
	var other string
	s.db.QueryRow(`SELECT id FROM users WHERE id<>?`, uid).Scan(&other)
	s.db.Exec(`INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES('rich',?,5000,'test',0)`, other)
	expect(t, meterCall(s, sharedTestToken, meterBody("exact-balance-0003"), nil), 402)
}
