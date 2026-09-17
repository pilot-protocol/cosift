package community

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFirstAuthenticatedRequestIsMeteredAcrossModesAndAliases(t *testing.T) {
	var calls atomic.Int32
	fail := false
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if fail {
			w.WriteHeader(503)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	s.cfg.Shared = &fakeShared{}
	s.cfg.MemberFreeRPM = 10000 // Legacy configuration must never restore free requests.
	balance := func() int {
		var n int
		if err := s.db.QueryRow(`SELECT COALESCE(sum(delta),0) FROM credit_ledger`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	expect(t, bearerRequest(s, "/api/research?q=first", sharedTestToken), 200)
	if balance() != 997 {
		t.Fatalf("first Research balance=%d want997", balance())
	}
	want := 997
	for _, item := range []struct {
		path string
		cost int
	}{{"/research", 3}, {"/api/answer", 2}, {"/answer", 2}, {"/api/search", 1}, {"/search", 1}} {
		expect(t, bearerRequest(s, item.path+"?q=test", sharedTestToken), 200)
		want -= item.cost
		if balance() != want {
			t.Fatalf("%s balance=%d want%d", item.path, balance(), want)
		}
	}
	fail = true
	expect(t, bearerRequest(s, "/api/research?q=failure", sharedTestToken), 502)
	if balance() != want {
		t.Fatal("failed Research retained3 credits")
	}
	for _, path := range []string{"/api/credits", "/api/limits"} {
		w := bearerRequest(s, path, sharedTestToken)
		expect(t, w, 200)
		var policy map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &policy); err != nil {
			t.Fatal(err)
		}
		if policy["all_authenticated_requests_metered"] != true {
			t.Fatal("API did not advertise metering", w.Body.String())
		}
		key := "free_requests_per_minute"
		if path == "/api/limits" {
			key = "member_free_requests_per_minute"
		}
		if policy[key] != float64(0) {
			t.Fatal("API advertised a free bypass", w.Body.String())
		}
	}
	var freeBuckets int
	if err := s.db.QueryRow(`SELECT count(*) FROM retrieval_usage WHERE mode='free'`).Scan(&freeBuckets); err != nil || freeBuckets != 0 {
		t.Fatal("legacy free allowance was used", err)
	}
	before := balance()
	fail = false
	expect(t, bearerRequest(s, "/api/search?q=guest", ""), 200)
	if balance() != before {
		t.Fatal("guest request charged an account")
	}
	if calls.Load() != 8 {
		t.Fatal("unexpected backend calls", calls.Load())
	}
}

func TestAuthenticatedConcurrentResearchCannotOverdrawOrBecomeGuest(t *testing.T) {
	var calls atomic.Int32
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); _, _ = w.Write([]byte(`{}`)) }))
	f := &fakeShared{}
	s.cfg.Shared = f
	s.cfg.ResearchPer10Min = 100
	s.cfg.MemberFreeRPM = 10000
	identity, err := f.Verify(context.Background(), sharedTestToken)
	if err != nil {
		t.Fatal(err)
	}
	u, err := s.sharedUser(context.Background(), identity)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.grantMonthlyCredits(context.Background(), u.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES('concurrent-research-budget',?,?,'fixture',0)`, u.ID, 5-monthlyFreeCredits); err != nil {
		t.Fatal(err)
	}
	var wins, blocked, unexpected atomic.Int32
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := bearerRequest(s, "/research?q=concurrent", sharedTestToken)
			switch w.Code {
			case 200:
				wins.Add(1)
			case 429:
				blocked.Add(1)
			default:
				unexpected.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 || blocked.Load() != 15 || unexpected.Load() != 0 || calls.Load() != 1 {
		t.Fatalf("wins=%d blocked=%d unexpected=%d engine=%d", wins.Load(), blocked.Load(), unexpected.Load(), calls.Load())
	}
	var balance, guests int
	if err = s.db.QueryRow(`SELECT sum(delta) FROM credit_ledger WHERE user_id=?`, u.ID).Scan(&balance); err != nil || balance != 2 {
		t.Fatal("concurrent debit overdraw", balance, err)
	}
	if err = s.db.QueryRow(`SELECT count(*) FROM guest_usage`).Scan(&guests); err != nil || guests != 0 {
		t.Fatal("authenticated request fell back to guest", guests, err)
	}
}
