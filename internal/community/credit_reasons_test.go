package community

import (
	"encoding/json"
	"testing"
	"time"
)

type monthlySums struct {
	Balance int
	Monthly struct{ Free, Earned, Purchased, Spent int }
	Costs   map[string]int `json:"request_credit_costs"`
}

func TestEveryLedgerReasonLandsInItsMonthlyBucket(t *testing.T) {
	for _, tc := range []struct {
		reason        string
		delta         int
		earned, spent int
	}{
		{"verified_contribution", 10, 10, 0}, {"contribution_award", 1000, 1000, 0}, {"royalty", 3, 3, 0}, {"fetch_verified", 2, 2, 0},
		{"article_read", -2, 0, 2}, {"clawback", -300, 0, 300}, {"extra_request", -1, 0, 1},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			s := testServer(t, nil)
			u, cookie := fundedCreditAccount(t, s, monthlyFreeCredits)
			if _, err := s.db.Exec(`INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES('row',?,?,?,?)`, u.ID, tc.delta, tc.reason, time.Now().Unix()); err != nil {
				t.Fatal(err)
			}
			var got monthlySums
			json.Unmarshal(request(t, s, "GET", "/api/credits", nil, cookie).Body.Bytes(), &got)
			if got.Monthly.Earned != tc.earned || got.Monthly.Spent != tc.spent || got.Balance != monthlyFreeCredits+tc.delta {
				t.Fatalf("%s: earned %d spent %d balance %d", tc.reason, got.Monthly.Earned, got.Monthly.Spent, got.Balance)
			}
		})
	}
}

func TestEarnedStaysNonNegativeAfterAClawback(t *testing.T) {
	s := testServer(t, nil)
	u, cookie := fundedCreditAccount(t, s, monthlyFreeCredits)
	now := time.Now().Unix()
	s.db.Exec(`INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES('a',?,1000,'contribution_award',?),('b',?,-1000,'clawback',?)`, u.ID, now, u.ID, now)
	var got monthlySums
	json.Unmarshal(request(t, s, "GET", "/api/credits", nil, cookie).Body.Bytes(), &got)
	if got.Monthly.Earned != 1000 || got.Monthly.Spent != 1000 || got.Balance != monthlyFreeCredits {
		t.Fatalf("%+v", got)
	}
}

func TestReportedCostsAddArticleWithoutTouchingTheEnforcedMaps(t *testing.T) {
	s := testServer(t, nil)
	_, cookie := fundedCreditAccount(t, s, monthlyFreeCredits)
	var credits monthlySums
	json.Unmarshal(request(t, s, "GET", "/api/credits", nil, cookie).Body.Bytes(), &credits)
	var limits monthlySums
	json.Unmarshal(request(t, s, "GET", "/api/limits", nil, nil).Body.Bytes(), &limits)
	want := map[string]int{"search": 1, "answer": 2, "research": 3, "article": 2}
	for name, got := range map[string]map[string]int{"credits": credits.Costs, "limits": limits.Costs} {
		if len(got) != len(want) || got["article"] != 2 || got["search"] != 1 || got["answer"] != 2 || got["research"] != 3 {
			t.Fatalf("%s reports %v", name, got)
		}
	}
	if len(requestCreditCosts()) != 3 || requestCreditCosts()["article"] != 0 || len(s.guestIntervals()) != 3 {
		t.Fatal("the enforced cost map grew")
	}
	if _, ok := creditPack()["usd_per_1000_requests_by_mode"].(map[string]string)["article"]; ok {
		t.Fatal("a pack price was invented for the article")
	}
}
