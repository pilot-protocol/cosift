package community

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

func TestForgedAwardsStayPendingAndUntouched(t *testing.T) {
	r := newAwardRig(t)
	logs := r.logs
	good := r.articleAward(1, 1, 1000)
	r.person(2)
	forged := map[string]func(a *sharedaccount.Award){
		"id form":         func(a *sharedaccount.Award) { a.ID = "art:short" },
		"reason":          func(a *sharedaccount.Award) { a.Reason = "revision" },
		"contribution":    func(a *sharedaccount.Award) { a.Ref.ContributionID = ulidN(77) },
		"another uid":     func(a *sharedaccount.Award) { a.UID = uidN(2) },
		"bad uid":         func(a *sharedaccount.Award) { a.UID = "NOT-A-UID" },
		"unknown cid":     func(a *sharedaccount.Award) { a.CID = cidN(99) },
		"amount zero":     func(a *sharedaccount.Award) { a.Amount = 0 },
		"amount high":     func(a *sharedaccount.Award) { a.Amount = 600001 },
		"amount negative": func(a *sharedaccount.Award) { a.Amount = -5 },
		"applied fields":  func(a *sharedaccount.Award) { a.Applied = true },
		"malformed":       func(a *sharedaccount.Award) { a.Valid = false },
		"version zero":    func(a *sharedaccount.Award) { a.Ref.Version = 0 },
	}
	var ids []string
	i := 10
	for name, mutate := range forged {
		i++
		a := good
		a.ID, a.Ref.ContributionID = "art:"+ulidN(i), ulidN(i)
		mutate(&a)
		if name == "id form" || name == "malformed" {
			a.ID = fmt.Sprintf("%s-%d", a.ID, i)
		}
		a.CreatedAt = time.Time{}
		r.addRaw(a)
		ids = append(ids, a.ID)
	}
	r.addRaw(sharedaccount.Award{ID: "roy:2026-13-45:" + ulidN(1) + ":" + cidN(2), UID: uidN(2), CID: cidN(2), Amount: 5, Reason: "royalty", Valid: true, Status: "pending", Ref: sharedaccount.AwardRef{ArticleID: ulidN(1), Day: "2026-13-45"}})
	r.addRaw(sharedaccount.Award{ID: "roy:2026-10-19:" + ulidN(1) + ":" + cidN(2), UID: uidN(2), CID: cidN(2), Amount: 101, Reason: "royalty", Valid: true, Status: "pending", Ref: sharedaccount.AwardRef{ArticleID: ulidN(1), Day: "2026-10-19"}})
	r.addRaw(sharedaccount.Award{ID: "claw:" + ulidN(1) + "_" + ulidN(2) + ":" + cidN(2), UID: uidN(2), CID: cidN(2), Amount: 50, Reason: "clawback", Valid: true, Status: "pending", Ref: sharedaccount.AwardRef{ArticleID: ulidN(1), RequestID: ulidN(1) + "_" + ulidN(2)}})
	var usersBefore int
	r.s.db.QueryRow(`SELECT count(*) FROM users`).Scan(&usersBefore)
	for i := 0; i < 5; i++ {
		r.tick(t)
	}
	for _, id := range append(ids, "roy:2026-13-45:"+ulidN(1)+":"+cidN(2), "roy:2026-10-19:"+ulidN(1)+":"+cidN(2), "claw:"+ulidN(1)+"_"+ulidN(2)+":"+cidN(2)) {
		if r.src.result(id) != nil {
			t.Errorf("%s was marked", id)
		}
		if _, found := r.ledger(t, id); found {
			t.Errorf("%s reached the ledger", id)
		}
	}
	if _, found := r.ledger(t, good.ID); !found {
		t.Fatal("the genuine award was held back by the forged ones")
	}
	var usersAfter int
	r.s.db.QueryRow(`SELECT count(*) FROM users`).Scan(&usersAfter)
	if usersAfter != usersBefore+1 {
		t.Fatalf("a forged award created an account: %d -> %d users", usersBefore, usersAfter)
	}
	if n := strings.Count(logs.String(), "award_invalid"); n != len(ids)+3 {
		t.Fatalf("%d award_invalid lines for %d forged awards", n, len(ids)+3)
	}
}

func TestNoAwardCreatesAnAccountBeforeItIsValid(t *testing.T) {
	r := newAwardRig(t)
	r.person(1)
	r.addRaw(sharedaccount.Award{ID: "art:" + ulidN(1), UID: uidN(1), CID: cidN(1), Amount: 1000, Reason: "article", Valid: true, Status: "pending", Applied: true,
		Ref: sharedaccount.AwardRef{ContributionID: ulidN(1), ArticleID: ulidN(2), Version: 1}})
	r.tick(t)
	var users int
	r.s.db.QueryRow(`SELECT count(*) FROM users`).Scan(&users)
	if users != 0 || r.src.calls["Account"] != 0 {
		t.Fatalf("users %d, account reads %d", users, r.src.calls["Account"])
	}
}

func TestCursorPassesAnUnresolvableHead(t *testing.T) {
	r := newAwardRig(t)
	for i := 1; i <= 60; i++ {
		a := r.articleAward(i, i, 10)
		r.src.accountErr[a.UID] = sharedaccount.ErrUnavailable
	}
	good := r.articleAward(100, 100, 10)
	r.tick(t)
	if _, found := r.ledger(t, good.ID); found {
		t.Fatal("the good award is not on the first page")
	}
	r.tick(t)
	if _, found := r.ledger(t, good.ID); !found {
		t.Fatal("an unresolvable head hid a newer award")
	}
}

func TestOneContributorsBurstCannotHoldThePoller(t *testing.T) {
	r := newAwardRig(t)
	for i := 1; i <= 100; i++ {
		a := r.articleAward(1, i, 10)
		r.src.awards[a.ID].a.CreatedAt = r.now.Add(-time.Hour + time.Duration(i)*time.Second)
	}
	other := r.articleAward(2, 500, 10)
	r.src.awards[other.ID].a.CreatedAt = r.now.Add(-time.Hour + 9*time.Second + 500*time.Millisecond)
	r.tick(t)
	if _, found := r.ledger(t, other.ID); !found {
		t.Fatal("the other contributor's award waited behind the burst")
	}
	if n := r.ledgerRows(t); n != awardsPerCIDPerTick+1 {
		t.Fatalf("%d awards applied in the first tick, want %d", n, awardsPerCIDPerTick+1)
	}
	for tick := 2; tick <= 6; tick++ {
		before := r.ledgerRows(t)
		r.tick(t)
		if got := r.ledgerRows(t) - before; got > awardsPerCIDPerTick {
			t.Fatalf("tick %d applied %d awards of one contributor", tick, got)
		}
	}
}

func TestEveryStepCanCrashWithoutADoubleCredit(t *testing.T) {
	for _, step := range []string{"contributor", "article", "account", "link", "insert", "mark"} {
		t.Run(step, func(t *testing.T) {
			r := newAwardRig(t)
			a := r.articleAward(1, 1, 1000)
			r.s.awards.hook = func(s string) error {
				if s == step {
					return fmt.Errorf("crash")
				}
				return nil
			}
			r.tick(t)
			if r.src.result(a.ID) != nil {
				t.Fatal("marked despite the crash")
			}
			if step == "mark" && r.ledgerRows(t) != 1 {
				t.Fatalf("%d rows after a crash before the mark", r.ledgerRows(t))
			}
			r.restart()
			r.tick(t)
			r.tick(t)
			if r.ledgerRows(t) != 1 || r.balance(t, r.userID(t, 1)) != 1000 || r.src.result(a.ID) == nil {
				t.Fatalf("rows %d balance %d result %+v", r.ledgerRows(t), r.balance(t, r.userID(t, 1)), r.src.result(a.ID))
			}
		})
	}
}

func TestMarkFailureAndAMarkedAwardAreSafeToRepeat(t *testing.T) {
	r := newAwardRig(t)
	a := r.articleAward(1, 1, 1000)
	r.src.markErr = sharedaccount.ErrUnavailable
	for i := 0; i < 3; i++ {
		r.now = r.now.Add(2 * time.Hour)
		r.tick(t)
	}
	if r.ledgerRows(t) != 1 {
		t.Fatalf("%d rows after repeated mark failures", r.ledgerRows(t))
	}
	r.src.markErr = sharedaccount.ErrNotPending
	r.now = r.now.Add(2 * time.Hour)
	r.tick(t)
	if r.ledgerRows(t) != 1 {
		t.Fatal("a refused mark duplicated the credit")
	}
	if _, found := r.ledger(t, a.ID); !found {
		t.Fatal("no ledger row")
	}
}

func TestACreditedAwardIsNeverSkippedWithNothingApplied(t *testing.T) {
	worlds := map[string]func(*awardRig, sharedaccount.Award){
		"article gone": func(r *awardRig, a sharedaccount.Award) { delete(r.art.rec, a.Ref.ArticleID) },
		"account gone": func(r *awardRig, a sharedaccount.Award) { delete(r.src.accounts, a.UID) },
	}
	for name, change := range worlds {
		t.Run(name, func(t *testing.T) {
			r := newAwardRig(t)
			a := r.articleAward(1, 1, 1000)
			r.src.markErr = errors.New("firestore down")
			r.tick(t)
			if delta, found := r.ledger(t, a.ID); !found || delta != 1000 || r.src.result(a.ID) != nil {
				t.Fatalf("setup: ledger %d %v, result %+v", delta, found, r.src.result(a.ID))
			}
			change(r, a)
			r.src.markErr = nil
			r.now = r.now.Add(31 * time.Second)
			r.tick(t)
			res := r.src.result(a.ID)
			if res == nil || res.Status != "applied" || res.AppliedDelta != 1000 || res.LedgerID != "award:"+a.ID || res.SkipReason != "" {
				t.Fatalf("result %+v", res)
			}
			if r.ledgerRows(t) != 1 || r.balance(t, r.userID(t, 1)) != 1000 {
				t.Fatalf("rows %d balance %d", r.ledgerRows(t), r.balance(t, r.userID(t, 1)))
			}
		})
	}
}

func TestScanWrapsToTheStartWhenAPageComesBackShort(t *testing.T) {
	r := newAwardRig(t)
	late := r.articleAward(1, 1, 10)
	r.tick(t)
	early := r.articleAward(2, 2, 10)
	r.src.awards[early.ID].a.CreatedAt = r.src.awards[late.ID].a.CreatedAt.Add(-time.Hour)
	r.tick(t)
	if _, found := r.ledger(t, early.ID); !found {
		t.Fatal("an award older than the cursor was never met again")
	}
}

func TestInvalidAwardsAreLoggedOncePerHour(t *testing.T) {
	r := newAwardRig(t)
	a := r.articleAward(1, 1, 1000)
	r.src.awards[a.ID].a.Amount = 0
	count := func() int { return strings.Count(r.logs.String(), "award_invalid") }
	for i := 0; i < 40; i++ {
		r.now = r.now.Add(time.Minute)
		r.tick(t)
	}
	if count() != 1 {
		t.Fatalf("%d lines in 40 minutes", count())
	}
	for i := 0; i < 40; i++ {
		r.now = r.now.Add(time.Minute)
		r.tick(t)
	}
	if count() != 2 {
		t.Fatalf("%d lines in 80 minutes", count())
	}
}

func TestAnAlreadyMarkedAwardIsNotAFailure(t *testing.T) {
	r := newAwardRig(t)
	r.articleAward(1, 1, 1000)
	r.src.markErr = sharedaccount.ErrNotPending
	r.tick(t)
	if strings.Contains(r.logs.String(), "award_unresolved") || r.ledgerRows(t) != 1 {
		t.Fatalf("rows %d log %q", r.ledgerRows(t), r.logs.String())
	}
}
