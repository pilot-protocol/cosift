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

func TestCreditsRewardOnceSpendAndRefund(t *testing.T) {
	fail := false
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail {
			w.WriteHeader(503)
			return
		}
		w.Write([]byte(`{"results":[]}`))
	}))
	cookie := account(t, s, "credit@example.com")
	var u User
	json.Unmarshal(request(t, s, "GET", "/api/me", nil, cookie).Body.Bytes(), &u)
	s.db.Exec(`INSERT INTO submissions(id,user_id,url,created_at) VALUES('first',?,'https://example.com/a',0),('second',?,'https://example.com/b',0)`, u.ID, u.ID)
	for _, id := range []string{"first", "first", "second"} {
		if err := s.rewardContribution(context.Background(), id, strings.Repeat("a", 64)); err != nil {
			t.Fatal(err)
		}
	}
	balance := func() int {
		var v struct{ Balance int }
		json.Unmarshal(request(t, s, "GET", "/api/credits", nil, cookie).Body.Bytes(), &v)
		return v.Balance
	}
	if balance() != monthlyFreeCredits+10 {
		t.Fatal("duplicate reward")
	}
	s.db.Exec(`INSERT INTO retrieval_usage VALUES(?,'free',?,?)`, "member:"+u.ID, s.cfg.MemberFreeRPM, time.Now().Add(time.Minute).Unix())
	expect(t, request(t, s, "GET", "/api/search?q=test", nil, cookie), 200)
	if balance() != monthlyFreeCredits+9 {
		t.Fatal("extra request not charged")
	}
	fail = true
	expect(t, request(t, s, "GET", "/api/search?q=test", nil, cookie), 502)
	if balance() != monthlyFreeCredits+9 {
		t.Fatal("failed request not refunded")
	}
	expect(t, request(t, s, "GET", "/api/credits", nil, nil), 401)
}

func TestCreditConcurrentSpendingCannotOverdraw(t *testing.T) {
	s := testServer(t, nil)
	cookie := account(t, s, "atomic@example.com")
	var u User
	json.Unmarshal(request(t, s, "GET", "/api/me", nil, cookie).Body.Bytes(), &u)
	if err := s.grantMonthlyCredits(context.Background(), u.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	s.db.Exec(`INSERT INTO credit_ledger VALUES('spent-before-race',?,?,'test',0)`, u.ID, 1-monthlyFreeCredits)
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			finish, ok := s.reserveCredit(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), u)
			if ok {
				wins.Add(1)
				finish(true)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("charged %d requests for one credit", wins.Load())
	}
}

func TestLocalArtifactRequiresAccountAndPersists(t *testing.T) {
	s := testServer(t, nil)
	text := strings.Repeat("Useful scientific documentation. ", 4)
	body := map[string]any{"artifacts": []any{map[string]any{"url": "https://example.com/local", "title": "Guide", "text": text, "model": "test", "chunks": []any{map[string]any{"text": text, "embedding": []float32{1, 2}}}}}}
	expect(t, request(t, s, "POST", "/api/submissions", body, nil), 401)
	cookie := account(t, s, "local@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", body, cookie), 202)
	var n int
	s.db.QueryRow(`SELECT count(*) FROM submission_artifacts`).Scan(&n)
	if n != 1 {
		t.Fatal("artifact missing")
	}
	expect(t, request(t, s, "POST", "/api/submissions", body, cookie), 202)
	s.db.QueryRow(`SELECT count(*) FROM submission_artifacts`).Scan(&n)
	if n != 1 {
		t.Fatal("duplicate artifact")
	}
}
