package community

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestMonthlyGrantConcurrentReadsAndSpendingAcrossProcesses(t *testing.T) {
	s := testServer(t, nil)
	cookie := account(t, s, "monthly-race@example.com")
	var user User
	if err := json.Unmarshal(request(t, s, "GET", "/api/me", nil, cookie).Body.Bytes(), &user); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var wg sync.WaitGroup
	for i := range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			instance := s
			if i%3 == 0 {
				instance = reopened
			}
			if i%2 == 0 {
				expect(t, request(t, instance, "GET", "/api/credits", nil, cookie), 200)
				return
			}
			finish, ok := instance.reserveCredit(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/search", nil), user, "search")
			if !ok {
				t.Error("monthly spending failed")
				return
			}
			finish(true)
		}()
	}
	wg.Wait()
	var count, balance int
	if err := s.db.QueryRow(`SELECT count(*) FROM credit_ledger WHERE reason='monthly_free' AND user_id=?`, user.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("duplicate monthly grant: %d %v", count, err)
	}
	if err := s.db.QueryRow(`SELECT sum(delta) FROM credit_ledger WHERE user_id=?`, user.ID).Scan(&balance); err != nil || balance != monthlyFreeCredits-12 {
		t.Fatalf("wrong concurrent balance: %d %v", balance, err)
	}
	expect(t, request(t, reopened, "GET", "/api/credits", nil, cookie), 200)
	if err := s.db.QueryRow(`SELECT sum(delta) FROM credit_ledger WHERE user_id=?`, user.ID).Scan(&balance); err != nil || balance != monthlyFreeCredits-12 {
		t.Fatalf("restart/read regranted credit: %d %v", balance, err)
	}
}

func TestMonthlyGrantUsesUTCBoundaryAndPreservesCarryover(t *testing.T) {
	s := testServer(t, nil)
	cookie := account(t, s, "month-boundary@example.com")
	var user User
	json.Unmarshal(request(t, s, "GET", "/api/me", nil, cookie).Body.Bytes(), &user)
	january := time.Date(2026, 1, 31, 23, 59, 59, 0, time.UTC)
	february := time.Date(2026, 1, 31, 19, 0, 0, 0, time.FixedZone("west", -5*60*60))
	for _, now := range []time.Time{january, january, february, february} {
		if err := s.grantMonthlyCredits(context.Background(), user.ID, now); err != nil {
			t.Fatal(err)
		}
	}
	var count, balance int
	if err := s.db.QueryRow(`SELECT count(*),sum(delta) FROM credit_ledger WHERE user_id=?`, user.ID).Scan(&count, &balance); err != nil || count != 2 || balance != 2*monthlyFreeCredits {
		t.Fatalf("monthly carryover/reset: %d %d %v", count, balance, err)
	}
	var febCount int
	if err := s.db.QueryRow(`SELECT count(*) FROM credit_ledger WHERE id=?`, "monthly-free:"+user.ID+":2026-02").Scan(&febCount); err != nil || febCount != 1 {
		t.Fatal("local timezone changed grant month", err)
	}
	if err := s.grantMonthlyCredits(context.Background(), "", february); err == nil {
		t.Fatal("guest grant accepted")
	}
}
