package community

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

func TestValidateAwardNamesTheFirstBrokenRule(t *testing.T) {
	r := newAwardRig(t)
	article := r.articleAward(1, 1, 1000)
	royalty := r.royalty(2, 5, "2026-10-19")
	clawback := r.clawback(3, 1, 50)
	for name, tc := range map[string]struct {
		base   sharedaccount.Award
		mutate func(*sharedaccount.Award)
		want   string
	}{
		"article is valid":           {article, func(*sharedaccount.Award) {}, ""},
		"royalty is valid":           {royalty, func(*sharedaccount.Award) {}, ""},
		"clawback is valid":          {clawback, func(*sharedaccount.Award) {}, ""},
		"unknown reason":             {article, func(a *sharedaccount.Award) { a.Reason = "bonus" }, "reason"},
		"royalty day disagrees":      {royalty, func(a *sharedaccount.Award) { a.Ref.Day = "2026-10-18" }, "id_form"},
		"royalty article disagrees":  {royalty, func(a *sharedaccount.Award) { a.Ref.ArticleID = ulidN(5) }, "id_form"},
		"royalty contributor":        {royalty, func(a *sharedaccount.Award) { a.CID = cidN(9) }, "id_form"},
		"royalty id malformed":       {royalty, func(a *sharedaccount.Award) { a.ID = "roy:today" }, "id_form"},
		"royalty above the cap":      {royalty, func(a *sharedaccount.Award) { a.Amount = 101 }, "amount"},
		"royalty at the cap":         {royalty, func(a *sharedaccount.Award) { a.Amount = 100 }, ""},
		"clawback request disagrees": {clawback, func(a *sharedaccount.Award) { a.Ref.RequestID = ulidN(1) + "_" + ulidN(2) }, "id_form"},
		"clawback contributor":       {clawback, func(a *sharedaccount.Award) { a.CID = cidN(9) }, "id_form"},
		"clawback article":           {clawback, func(a *sharedaccount.Award) { a.Ref.ArticleID = "short" }, "id_form"},
		"clawback positive":          {clawback, func(a *sharedaccount.Award) { a.Amount = 1 }, "amount"},
		"clawback below the floor":   {clawback, func(a *sharedaccount.Award) { a.Amount = -(1 << 32) }, "amount"},
		"revision id on an article":  {article, func(a *sharedaccount.Award) { a.ID = "rev:" + a.Ref.ContributionID }, "id_form"},
		"article ref is not a ulid":  {article, func(a *sharedaccount.Award) { a.Ref.ArticleID = "short" }, "ref"},
		"award already applied":      {article, func(a *sharedaccount.Award) { a.Status = "applied" }, "state"},
	} {
		t.Run(name, func(t *testing.T) {
			a := tc.base
			tc.mutate(&a)
			got := validateAward(a)
			if got != tc.want {
				t.Fatalf("validateAward = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestLedgerReasonNamesTheAwardKind(t *testing.T) {
	for reason, want := range map[string]string{"article": "contribution_award", "revision": "contribution_award", "royalty": "royalty", "clawback": "clawback"} {
		if got := ledgerReason(reason); got != want {
			t.Errorf("ledgerReason(%q) = %q, want %q", reason, got, want)
		}
	}
}

func TestPollSurfacesASourceFailureAndTheNextPollRecovers(t *testing.T) {
	r := newAwardRig(t)
	a := r.articleAward(1, 1, 1000)
	r.src.pendingErr = sharedaccount.ErrUnavailable
	if err := r.s.pollAwards(context.Background()); !errors.Is(err, sharedaccount.ErrUnavailable) {
		t.Fatalf("poll error = %v", err)
	}
	if r.ledgerRows(t) != 0 {
		t.Fatal("an award was applied from a failed read")
	}
	r.src.pendingErr = nil
	r.tick(t)
	if _, found := r.ledger(t, a.ID); !found {
		t.Fatal("the poll did not recover")
	}
}

func TestPollStopsAtACancelledContextWithoutApplyingAnything(t *testing.T) {
	r := newAwardRig(t)
	r.articleAward(1, 1, 1000)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.s.pollAwards(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("poll error = %v", err)
	}
	if r.ledgerRows(t) != 0 || r.src.calls["MarkApplied"] != 0 {
		t.Fatalf("a cancelled poll applied %d rows, %d marks", r.ledgerRows(t), r.src.calls["MarkApplied"])
	}
}

func TestRetryStateExpiresAfterTwoHoursAndStaysBounded(t *testing.T) {
	r := newAwardRig(t)
	a := r.articleAward(1, 1, 1000)
	r.src.contribErr = sharedaccount.ErrUnavailable
	r.tick(t)
	tracked := func() int {
		r.s.awards.mu.Lock()
		defer r.s.awards.mu.Unlock()
		return len(r.s.awards.tries)
	}
	if tracked() != 1 {
		t.Fatalf("tracked = %d after a failure", tracked())
	}
	delete(r.src.awards, a.ID)
	r.now = r.now.Add(time.Hour)
	r.tick(t)
	if tracked() != 1 {
		t.Fatal("back-off state dropped within two hours")
	}
	r.now = r.now.Add(time.Hour + time.Minute)
	r.tick(t)
	if tracked() != 0 {
		t.Fatal("back-off state for a vanished award was kept")
	}

	p := newAwardPoller(r.src, r.art)
	for i := 0; i < awardMemory+25; i++ {
		p.fail(fmt.Sprintf("award-%d", i))
	}
	if len(p.tries) != awardMemory {
		t.Fatalf("retry state holds %d entries, want the cap %d", len(p.tries), awardMemory)
	}
}

func TestAwardStaysPendingWhenTheLedgerCannotBeRead(t *testing.T) {
	r := newAwardRig(t)
	a := r.articleAward(1, 1, 1000)
	delete(r.art.rec, a.Ref.ArticleID)
	r.s.db.Close()
	r.tick(t)
	if r.src.result(a.ID) != nil || r.src.calls["MarkApplied"] != 0 {
		t.Fatalf("a skip was written without knowing whether the award was credited: %+v", r.src.result(a.ID))
	}
	if !strings.Contains(r.logs.String(), "award_unresolved") || !strings.Contains(r.logs.String(), "class=ledger") {
		t.Fatalf("log %q", r.logs.String())
	}
}

func TestAwardStaysPendingWhenTheAccountCannotBeLinked(t *testing.T) {
	r := newAwardRig(t)
	a := r.articleAward(1, 1, 1000)
	r.s.db.Close()
	r.tick(t)
	if r.src.result(a.ID) != nil || !strings.Contains(r.logs.String(), "class=link") {
		t.Fatalf("result %+v log %q", r.src.result(a.ID), r.logs.String())
	}
}

func TestAwardStaysPendingWhenTheLedgerWriteFails(t *testing.T) {
	r := newAwardRig(t)
	a := r.articleAward(1, 1, 1000)
	r.s.awards.hook = func(step string) error {
		if step == "insert" {
			_, err := r.s.db.Exec(`DROP TABLE credit_ledger`)
			return err
		}
		return nil
	}
	r.tick(t)
	if r.src.result(a.ID) != nil || r.src.calls["MarkApplied"] != 0 || !strings.Contains(r.logs.String(), "class=ledger") {
		t.Fatalf("result %+v log %q", r.src.result(a.ID), r.logs.String())
	}
}

func TestAwardIsNeverMarkedAppliedWhenItsLedgerRowIsMissing(t *testing.T) {
	r := newAwardRig(t)
	a := r.articleAward(1, 1, 1000)
	r.s.awards.hook = func(step string) error {
		if step == "insert" {
			_, err := r.s.db.Exec(`CREATE TRIGGER drop_award BEFORE INSERT ON credit_ledger BEGIN SELECT RAISE(IGNORE); END`)
			return err
		}
		return nil
	}
	r.tick(t)
	if r.src.result(a.ID) != nil || r.src.calls["MarkApplied"] != 0 || !strings.Contains(r.logs.String(), "class=ledger") {
		t.Fatalf("result %+v log %q", r.src.result(a.ID), r.logs.String())
	}
}

func TestWeekOldAwardIsNotExpiredWhenTheLedgerCannotBeRead(t *testing.T) {
	r := newAwardRig(t)
	a := r.articleAward(1, 1, 1000)
	r.src.accountErr[uidN(1)] = sharedaccount.ErrUnavailable
	r.src.awards[a.ID].a.CreatedAt = r.now.Add(-8 * 24 * time.Hour)
	r.tick(t)
	r.s.db.Close()
	r.now = r.now.Add(2 * time.Hour)
	r.tick(t)
	if r.src.result(a.ID) != nil || r.src.calls["MarkApplied"] != 0 {
		t.Fatalf("expired without checking the ledger: %+v", r.src.result(a.ID))
	}
}

func TestRunAwardsLogsAFailedPollWithoutItsDetail(t *testing.T) {
	old := awardPollEvery
	awardPollEvery = 10 * time.Millisecond
	t.Cleanup(func() { awardPollEvery = old })
	r := newAwardRig(t)
	r.src.pendingErr = errors.New("rpc detail with a credential-like value")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.s.runAwards(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.src.mu.Lock()
		n := r.src.calls["PendingAwards"]
		r.src.mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	out := r.logs.String()
	if !strings.Contains(out, "community: award poll failed") || strings.Contains(out, "credential-like") {
		t.Fatalf("log %q", out)
	}
}
