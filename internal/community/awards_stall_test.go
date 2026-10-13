package community

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

func (r *awardRig) meterAccount(t *testing.T) {
	t.Helper()
	r.s.cfg.Shared = &fakeShared{}
	w := bearerRequest(r.s, "/api/me", sharedTestToken)
	expect(t, w, 200)
	var u User
	json.Unmarshal(w.Body.Bytes(), &u)
	r.s.grantMonthlyCredits(t.Context(), u.ID, time.Now())
}

// probeMeter calls /api/meter every 250 ms until done reports, returning the slowest answer and the number of calls.
func probeMeter(t *testing.T, s *Server, done <-chan error) (worst time.Duration, calls int) {
	t.Helper()
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			return
		case <-time.After(250 * time.Millisecond):
			start := time.Now()
			got := meterCall(s, sharedTestToken, meterBody(fmt.Sprintf("stall-probe-%06d", calls)), nil)
			if d := time.Since(start); d > worst {
				worst = d
			}
			if got.Code != 200 {
				t.Fatalf("meter answered %d: %s", got.Code, got.Body.String())
			}
			calls++
		}
	}
}

func TestMeterAnswersWhileThePollerStallsOnEveryFirestoreAndEngineRead(t *testing.T) {
	if testing.Short() {
		t.Skip("injects five 2 s stalls")
	}
	r := newAwardRig(t)
	r.meterAccount(t)
	a := r.articleAward(1, 1, 1000)

	var mu sync.Mutex
	var busy []string
	stalled := map[string]bool{}
	stall := func(op string) {
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		var one int
		if err := r.s.db.QueryRowContext(ctx, `SELECT 1`).Scan(&one); err != nil {
			mu.Lock()
			busy = append(busy, op)
			mu.Unlock()
		}
		mu.Lock()
		stalled[op] = true
		mu.Unlock()
		time.Sleep(2 * time.Second)
	}
	r.src.stall = stall
	r.art.stall = func() { stall("ArticleByID") }

	done := make(chan error, 1)
	go func() { done <- r.s.pollAwards(context.Background()) }()
	worst, calls := probeMeter(t, r.s, done)
	if worst > 200*time.Millisecond {
		t.Fatalf("a meter call took %v while the poller stalled", worst)
	}
	for _, op := range []string{"PendingAwards", "Contributor", "ArticleByID", "Account", "MarkApplied"} {
		if !stalled[op] {
			t.Errorf("%s never ran, so it was never stalled", op)
		}
	}
	if len(busy) > 0 {
		t.Fatalf("the database was held across the stalled %v", busy)
	}
	if calls < 30 {
		t.Fatalf("only %d meter probes during the run", calls)
	}
	if _, found := r.ledger(t, a.ID); !found || r.src.result(a.ID) == nil {
		t.Fatal("the stalled poll never finished the award")
	}
}

func TestMeterAnswersWhileTheTerminalMarkStalls(t *testing.T) {
	if testing.Short() {
		t.Skip("injects a 2 s stall")
	}
	r := newAwardRig(t)
	r.meterAccount(t)
	a := r.articleAward(1, 1, 1000)
	r.src.accountErr[uidN(1)] = sharedaccount.ErrUnavailable
	r.src.awards[a.ID].a.CreatedAt = r.now.Add(-8 * 24 * time.Hour)
	r.tick(t)
	r.now = r.now.Add(2 * time.Hour)

	var mu sync.Mutex
	var busy, stalled bool
	r.src.stall = func(op string) {
		if op != "MarkApplied" {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		var one int
		err := r.s.db.QueryRowContext(ctx, `SELECT 1`).Scan(&one)
		mu.Lock()
		busy, stalled = busy || err != nil, true
		mu.Unlock()
		time.Sleep(2 * time.Second)
	}
	done := make(chan error, 1)
	go func() { done <- r.s.pollAwards(context.Background()) }()
	worst, calls := probeMeter(t, r.s, done)
	if !stalled {
		t.Fatal("the terminal mark never ran, so it was never stalled")
	}
	if busy {
		t.Fatal("the database was held across the stalled terminal mark")
	}
	if worst > 200*time.Millisecond || calls < 5 {
		t.Fatalf("worst meter answer %v over %d calls while the terminal mark stalled", worst, calls)
	}
	if res := r.src.result(a.ID); res == nil || res.Status != "skipped" || res.SkipReason != "account_unresolvable" {
		t.Fatalf("result %+v", res)
	}
}
