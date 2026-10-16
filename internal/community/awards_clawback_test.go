package community

import (
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

func TestAClawbackDebitsASuspendedOrUnverifiedAccountAndLiftingItRestoresNothing(t *testing.T) {
	for name, hold := range map[string]func(r *awardRig){
		"suspended":  func(r *awardRig) { r.src.accountErr[uidN(1)] = sharedaccount.ErrBanned },
		"unverified": func(r *awardRig) { delete(r.src.accounts, uidN(1)) },
	} {
		t.Run(name, func(t *testing.T) {
			r := newAwardRig(t)
			r.articleAward(1, 1, 1000)
			r.tick(t)
			user := r.userID(t, 1)
			claw := r.clawback(1, 1, 1000)
			hold(r)
			r.tick(t)
			res := r.src.result(claw.ID)
			if delta, found := r.ledger(t, claw.ID); !found || delta != -1000 || r.balance(t, user) != 0 {
				t.Fatalf("ledger %d (%v), balance %d", delta, found, r.balance(t, user))
			}
			if res == nil || res.Status != "applied" || res.AppliedDelta != -1000 {
				t.Fatalf("result %+v", res)
			}
			delete(r.src.accountErr, uidN(1))
			r.person(1)
			for i := 0; i < 3; i++ {
				r.now = r.now.Add(2 * time.Hour)
				r.tick(t)
			}
			if r.balance(t, user) != 0 || r.ledgerRows(t) != 2 {
				t.Fatalf("balance %d, ledger rows %d after the hold was lifted", r.balance(t, user), r.ledgerRows(t))
			}
		})
	}
}

func TestAClawbackWithoutALinkStaysPendingPastAWeekAndIsRetried(t *testing.T) {
	for name, hold := range map[string]func(r *awardRig){
		"suspended":  func(r *awardRig) { r.src.accountErr[uidN(1)] = sharedaccount.ErrBanned },
		"unverified": func(r *awardRig) { delete(r.src.accounts, uidN(1)) },
	} {
		t.Run(name, func(t *testing.T) {
			r := newAwardRig(t)
			claw := r.clawback(1, 1, 500)
			r.src.awards[claw.ID].a.CreatedAt = r.now.Add(-8 * 24 * time.Hour)
			hold(r)
			reads := func() int { return r.src.calls["Account"] }
			var midway int
			for i := 0; i < 24; i++ {
				r.now = r.now.Add(2 * time.Hour)
				r.tick(t)
				if i == 11 {
					midway = reads()
				}
			}
			if res := r.src.result(claw.ID); res != nil || r.ledgerRows(t) != 0 {
				t.Fatalf("result %+v, ledger rows %d", res, r.ledgerRows(t))
			}
			if reads() <= midway {
				t.Fatalf("no retry in the second day: %d reads, %d midway", reads(), midway)
			}
			r.person(1)
			delete(r.src.accountErr, uidN(1))
			r.now = r.now.Add(2 * time.Hour)
			r.tick(t)
			if res := r.src.result(claw.ID); res == nil || res.Status != "applied" || res.AppliedDelta != 0 {
				t.Fatalf("result %+v once the account resolved", res)
			}
		})
	}
}

func TestSuspendedAccountsKeepWaitingForPositiveAwardsEvenWithALink(t *testing.T) {
	r := newAwardRig(t)
	r.articleAward(1, 1, 1000)
	r.tick(t)
	user := r.userID(t, 1)
	r.src.accountErr[uidN(1)] = sharedaccount.ErrBanned
	later := []sharedaccount.Award{r.articleAward(1, 2, 700), r.royalty(1, 5, "2026-10-19")}
	r.tick(t)
	for _, a := range later {
		if r.src.result(a.ID) != nil {
			t.Fatalf("%s was settled while the account was suspended", a.ID)
		}
		r.src.awards[a.ID].a.CreatedAt = r.now.Add(-8 * 24 * time.Hour)
	}
	for i := 0; i < 4; i++ {
		r.now = r.now.Add(2 * time.Hour)
		r.tick(t)
	}
	for _, a := range later {
		if res := r.src.result(a.ID); res == nil || res.Status != "skipped" || res.SkipReason != "account_unresolvable" {
			t.Fatalf("%s: %+v", a.ID, res)
		}
	}
	if r.balance(t, user) != 1000 || r.ledgerRows(t) != 1 {
		t.Fatalf("balance %d, ledger rows %d", r.balance(t, user), r.ledgerRows(t))
	}
}

func TestAnOldClawbackStaysPendingWhenTheLinkStepFails(t *testing.T) {
	for name, setup := range map[string]struct {
		fail    func(r *awardRig)
		restore func(r *awardRig)
	}{
		"the link is written": {
			fail:    func(r *awardRig) { r.s.awards.hook = failStep("link") },
			restore: func(r *awardRig) { r.s.awards.hook = nil },
		},
		"the link is read": {
			fail: func(r *awardRig) {
				if _, err := r.s.db.Exec(`ALTER TABLE shared_identities RENAME TO shared_identities_held`); err != nil {
					t.Fatal(err)
				}
			},
			restore: func(r *awardRig) {
				if _, err := r.s.db.Exec(`ALTER TABLE shared_identities_held RENAME TO shared_identities`); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := newAwardRig(t)
			if name == "the link is read" {
				r.articleAward(1, 1, 1000)
				r.tick(t)
			}
			claw := r.clawback(1, 1, 400)
			r.src.awards[claw.ID].a.CreatedAt = r.now.Add(-8 * 24 * time.Hour)
			setup.fail(r)
			for i := 0; i < 6; i++ {
				r.now = r.now.Add(2 * time.Hour)
				r.tick(t)
			}
			if res := r.src.result(claw.ID); res != nil {
				t.Fatalf("result %+v", res)
			}
			setup.restore(r)
			r.now = r.now.Add(2 * time.Hour)
			r.tick(t)
			if res := r.src.result(claw.ID); res == nil || res.Status != "applied" {
				t.Fatalf("result %+v once the link step worked", res)
			}
			if _, found := r.ledger(t, claw.ID); !found {
				t.Fatal("no ledger row")
			}
		})
	}
}

func failStep(name string) func(string) error {
	return func(step string) error {
		if step == name {
			return errEngineDown
		}
		return nil
	}
}
