package community

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fundedCreditAccount(t *testing.T, s *Server, balance int) (User, *http.Cookie) {
	t.Helper()
	cookie := account(t, s, "weighted@example.com")
	var u User
	if err := json.Unmarshal(request(t, s, "GET", "/api/me", nil, cookie).Body.Bytes(), &u); err != nil {
		t.Fatal(err)
	}
	if err := s.grantMonthlyCredits(context.Background(), u.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO credit_ledger VALUES('balance-adjustment',?,?,'test',0)`, u.ID, balance-monthlyFreeCredits); err != nil {
		t.Fatal(err)
	}
	return u, cookie
}

func TestModeCreditCostsInsufficientBalanceAndFullRefund(t *testing.T) {
	for mode, cost := range requestCreditCosts() {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			fail := false
			s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if fail {
					w.WriteHeader(503)
					return
				}
				w.Write([]byte(`{}`))
			}))
			u, cookie := fundedCreditAccount(t, s, cost-1)
			if _, err := s.db.Exec(`INSERT INTO retrieval_usage VALUES(?,'free',?,?)`, "member:"+u.ID, s.cfg.MemberFreeRPM, time.Now().Add(time.Minute).Unix()); err != nil {
				t.Fatal(err)
			}
			denied := request(t, s, "GET", "/api/"+mode+"?q=test", nil, cookie)
			expect(t, denied, 429)
			if calls != 0 || !strings.Contains(denied.Body.String(), "requires") {
				t.Fatal("insufficient credits reached backend or had no guidance")
			}
			if _, err := s.db.Exec(`INSERT INTO credit_ledger VALUES('top-up',?,1,'test',0)`, u.ID); err != nil {
				t.Fatal(err)
			}
			expect(t, request(t, s, "GET", "/api/"+mode+"?q=test", nil, cookie), 200)
			var balance int
			s.db.QueryRow(`SELECT sum(delta) FROM credit_ledger WHERE user_id=?`, u.ID).Scan(&balance)
			if balance != 0 {
				t.Fatalf("mode charge=%d remaining, expected0", balance)
			}
			if _, err := s.db.Exec(`INSERT INTO credit_ledger VALUES('refund-top-up',?,?,'test',0)`, u.ID, cost); err != nil {
				t.Fatal(err)
			}
			fail = true
			expect(t, request(t, s, "GET", "/api/"+mode+"?q=test", nil, cookie), 502)
			var result struct {
				Balance            int
				Monthly            struct{ Spent int }
				RequestCreditCosts map[string]int `json:"request_credit_costs"`
			}
			if err := json.Unmarshal(request(t, s, "GET", "/api/credits", nil, cookie).Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if result.Balance != cost || result.Monthly.Spent != cost || result.RequestCreditCosts[mode] != cost {
				t.Fatalf("incorrect full refund/accounting/cost map: %+v", result)
			}
		})
	}
}

func TestConcurrentWeightedDebitsNeverOverdrawAndRefundOnce(t *testing.T) {
	for mode, cost := range requestCreditCosts() {
		t.Run(mode, func(t *testing.T) {
			s := testServer(t, nil)
			u, _ := fundedCreditAccount(t, s, 4*cost-1)
			var wins atomic.Int32
			var wg sync.WaitGroup
			for range 16 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					finish, ok := s.reserveCredit(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), u, mode)
					if ok {
						wins.Add(1)
						finish(true)
					}
				}()
			}
			wg.Wait()
			if wins.Load() != 3 {
				t.Fatalf("%d concurrent weighted requests succeeded", wins.Load())
			}
			var balance int
			s.db.QueryRow(`SELECT sum(delta) FROM credit_ledger WHERE user_id=?`, u.ID).Scan(&balance)
			if balance != cost-1 {
				t.Fatalf("overdraw/incorrect balance=%d", balance)
			}
			if _, err := s.db.Exec(`INSERT INTO credit_ledger VALUES('refund-budget',?,1,'test',0)`, u.ID); err != nil {
				t.Fatal(err)
			}
			finish, ok := s.reserveCredit(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), u, mode)
			if !ok {
				t.Fatal("refund reservation denied")
			}
			finish(false)
			finish(false)
			s.db.QueryRow(`SELECT sum(delta) FROM credit_ledger WHERE user_id=?`, u.ID).Scan(&balance)
			if balance != cost {
				t.Fatalf("duplicate refund minted credits: %d", balance)
			}
		})
	}
}
