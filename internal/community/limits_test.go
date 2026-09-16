package community

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestModeCapsShareAliasesAndCannotSpendPastCap(t *testing.T) {
	var backend atomic.Int32
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { backend.Add(1); w.Write([]byte(`{"results":[]}`)) }))
	cookie := account(t, s, "caps@example.com")
	var u User
	json.Unmarshal(request(t, s, "GET", "/api/me", nil, cookie).Body.Bytes(), &u)
	s.cfg.SearchRPM = 2
	s.cfg.AnswerRPM = 1
	s.cfg.ResearchPer10Min = 1
	s.cfg.MemberFreeRPM = 1
	if _, err := s.db.Exec(`INSERT INTO credit_ledger VALUES('seed',?,100,'test',0)`, u.ID); err != nil {
		t.Fatal(err)
	}
	expect(t, request(t, s, "GET", "/api/search?q=test", nil, cookie), 200)
	expect(t, request(t, s, "GET", "/search?q=test", nil, cookie), 200)
	blocked := request(t, s, "GET", "/api/search?q=test", nil, cookie)
	expect(t, blocked, 429)
	if blocked.Header().Get("Retry-After") == "" {
		t.Fatal("missing retry")
	}
	for _, mode := range []string{"answer", "research"} {
		expect(t, request(t, s, "GET", "/"+mode+"?q=test", nil, cookie), 200)
		expect(t, request(t, s, "GET", "/api/"+mode+"?q=test", nil, cookie), 429)
	}
	if backend.Load() != 4 {
		t.Fatalf("backend calls=%d", backend.Load())
	}
	var balance int
	s.db.QueryRow(`SELECT SUM(delta) FROM credit_ledger WHERE user_id=?`, u.ID).Scan(&balance)
	if balance != 97 {
		t.Fatalf("charged rejected request: %d", balance)
	}
	// A new process must not grant a new expensive Research allowance.
	reopened, err := Open(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	expect(t, request(t, reopened, "GET", "/research?q=test", nil, cookie), 429)
	if _, err = s.db.Exec(`UPDATE retrieval_usage SET expires_at=? WHERE mode='research'`, time.Now().Unix()-1); err != nil {
		t.Fatal(err)
	}
	expect(t, request(t, reopened, "GET", "/research?q=test", nil, cookie), 200)
}

func TestResearchCapAtomicAndFailureRefund(t *testing.T) {
	s := testServer(t, nil)
	s.cfg.ResearchPer10Min = 3
	var wins atomic.Int32
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			finish, ok := s.allowRetrieval(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), User{ID: "same"}, "research")
			if ok {
				wins.Add(1)
				finish(true)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 3 {
		t.Fatalf("admitted %d", wins.Load())
	}
	finish, ok := s.allowRetrieval(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), User{ID: "failure"}, "research")
	if !ok {
		t.Fatal("initial reservation denied")
	}
	finish(false)
	var count int
	s.db.QueryRow(`SELECT count FROM retrieval_usage WHERE identity='member:failure'`).Scan(&count)
	if count != 0 {
		t.Fatalf("failed request retained slot: %d", count)
	}
}

func TestGuestResearchSeparateFromSharedAllowance(t *testing.T) {
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{}`)) }))
	expect(t, request(t, s, "GET", "/api/research?q=test", nil, nil), 200)
	// Advance only the shared minute allowance; Research remains exhausted.
	s.db.Exec(`UPDATE guest_usage SET expires_at=0`)
	expect(t, request(t, s, "GET", "/research?q=test", nil, nil), 429)
	expect(t, request(t, s, "GET", "/search?q=test", nil, nil), 200)
}

func TestGuestPolicyMigrationPreservesRequestTime(t *testing.T) {
	s := testServer(t, nil)
	usedAt := time.Now().Unix() - 10
	s.db.Exec(`DELETE FROM settings WHERE key='guest_interval_seconds'`)
	s.db.Exec(`INSERT INTO guest_usage VALUES('legacy',?,'reservation')`, usedAt+1800)
	for range 2 {
		if err := s.migrateGuestInterval(); err != nil {
			t.Fatal(err)
		}
		var until int64
		s.db.QueryRow(`SELECT expires_at FROM guest_usage WHERE ip_hash='legacy'`).Scan(&until)
		if until != usedAt+60 {
			t.Fatalf("migration/reset changed original time: %d", until)
		}
	}
}
