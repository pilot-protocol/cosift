package community

import (
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

func userCount(t *testing.T, r *awardRig) (n int) {
	t.Helper()
	r.s.db.QueryRow(`SELECT count(*) FROM users`).Scan(&n)
	return
}

func TestAccountResolutionTable(t *testing.T) {
	t.Run("an unknown account is skipped without a ledger row or a user", func(t *testing.T) {
		r := newAwardRig(t)
		a := r.articleAward(1, 1, 1000)
		delete(r.src.accounts, uidN(1))
		r.tick(t)
		res := r.src.result(a.ID)
		if res == nil || res.Status != "skipped" || res.SkipReason != "unknown_account" || res.AppliedDelta != 0 || r.ledgerRows(t) != 0 || userCount(t, r) != 0 {
			t.Fatalf("%+v rows %d users %d", res, r.ledgerRows(t), userCount(t, r))
		}
	})
	t.Run("an existing link is used and the upstream email is not compared", func(t *testing.T) {
		r := newAwardRig(t)
		a := r.articleAward(1, 1, 1000)
		r.s.db.Exec(`INSERT INTO users(id,email,name,salt,password_hash,created_at) VALUES('local','old@example.com','Old','s','',0)`)
		r.s.db.Exec(`INSERT INTO shared_identities(uid,user_id) VALUES(?,'local')`, uidN(1))
		r.tick(t)
		if r.balance(t, "local") != 1000 || userCount(t, r) != 1 || r.src.result(a.ID) == nil {
			t.Fatalf("balance %d users %d", r.balance(t, "local"), userCount(t, r))
		}
		var email string
		r.s.db.QueryRow(`SELECT email FROM users WHERE id='local'`).Scan(&email)
		if email != "old@example.com" {
			t.Fatalf("the email was rewritten to %q", email)
		}
	})
	t.Run("the verified email links an existing user on the first award", func(t *testing.T) {
		r := newAwardRig(t)
		a := r.articleAward(1, 1, 1000)
		r.s.db.Exec(`INSERT INTO users(id,email,name,salt,password_hash,created_at) VALUES('local',?,'Local','s','hash',0)`, emailN(1))
		r.s.db.Exec(`INSERT INTO sessions(hash,user_id,expires_at) VALUES('h','local',9999999999)`)
		r.tick(t)
		var hash string
		r.s.db.QueryRow(`SELECT password_hash FROM users WHERE id='local'`).Scan(&hash)
		if r.balance(t, "local") != 1000 || r.userID(t, 1) != "local" || sessionCount(t, r.s, "local") != 0 || hash != "" || r.src.result(a.ID) == nil {
			t.Fatalf("balance %d sessions %d hash %q", r.balance(t, "local"), sessionCount(t, r.s, "local"), hash)
		}
	})
	t.Run("a new user is created from the email's local part", func(t *testing.T) {
		r := newAwardRig(t)
		r.articleAward(1, 1, 1000)
		r.tick(t)
		var name string
		r.s.db.QueryRow(`SELECT name FROM users WHERE id=?`, r.userID(t, 1)).Scan(&name)
		if name != "member1" || userCount(t, r) != 1 {
			t.Fatalf("name %q users %d", name, userCount(t, r))
		}
	})
	t.Run("a link conflict, a suspended account and an outage retry", func(t *testing.T) {
		for name, setup := range map[string]func(r *awardRig){
			"conflict": func(r *awardRig) {
				r.s.db.Exec(`INSERT INTO users(id,email,name,salt,password_hash,created_at) VALUES('local',?,'Local','s','',0)`, emailN(1))
				r.s.db.Exec(`INSERT INTO shared_identities(uid,user_id) VALUES('ffffffffffffffff','local')`)
			},
			"banned":      func(r *awardRig) { r.src.accountErr[uidN(1)] = sharedaccount.ErrBanned },
			"unavailable": func(r *awardRig) { r.src.accountErr[uidN(1)] = sharedaccount.ErrUnavailable },
		} {
			r := newAwardRig(t)
			a := r.articleAward(1, 1, 1000)
			setup(r)
			links := 0
			r.s.db.QueryRow(`SELECT count(*) FROM shared_identities`).Scan(&links)
			r.tick(t)
			var after int
			r.s.db.QueryRow(`SELECT count(*) FROM shared_identities`).Scan(&after)
			if r.src.result(a.ID) != nil || r.ledgerRows(t) != 0 || after != links || !strings.Contains(r.logs.String(), "award_unresolved") {
				t.Errorf("%s: result %+v rows %d links %d->%d log %q", name, r.src.result(a.ID), r.ledgerRows(t), links, after, r.logs.String())
			}
		}
	})
}

func TestArticleCrossCheck(t *testing.T) {
	for name, record := range map[string]ArticleRecord{
		"synth origin":    {Origin: "synth", Contributors: []string{cidN(1)}, Version: 1},
		"another cid":     {Origin: "community", Contributors: []string{cidN(2)}, Version: 1},
		"older version":   {Origin: "community", Contributors: []string{cidN(1)}, Version: 0},
		"no contributors": {Origin: "community", Version: 1},
	} {
		r := newAwardRig(t)
		a := r.articleAward(1, 1, 1000)
		r.art.rec[a.Ref.ArticleID] = record
		r.tick(t)
		if r.src.result(a.ID) != nil || r.ledgerRows(t) != 0 || userCount(t, r) != 0 || !strings.Contains(r.logs.String(), "rule=article_mismatch") {
			t.Errorf("%s: result %+v rows %d users %d", name, r.src.result(a.ID), r.ledgerRows(t), userCount(t, r))
		}
	}
	r := newAwardRig(t)
	a := r.articleAward(1, 1, 1000)
	r.art.rec[a.Ref.ArticleID] = ArticleRecord{Origin: "community", Contributors: []string{cidN(2), cidN(1)}, Version: 4}
	r.tick(t)
	if _, found := r.ledger(t, a.ID); !found {
		t.Fatal("a consistent record was refused")
	}
}

func TestVanishedArticleSkipsAndEngineErrorsRetry(t *testing.T) {
	r := newAwardRig(t)
	gone := r.articleAward(1, 1, 1000)
	delete(r.art.rec, gone.Ref.ArticleID)
	flaky := r.articleAward(2, 2, 1000)
	r.art.err[flaky.Ref.ArticleID] = errEngineDown
	r.tick(t)
	res := r.src.result(gone.ID)
	if res == nil || res.Status != "skipped" || res.SkipReason != "article_unavailable" || res.AppliedDelta != 0 {
		t.Fatalf("gone: %+v", res)
	}
	if r.src.result(flaky.ID) != nil || r.ledgerRows(t) != 0 || !strings.Contains(r.logs.String(), "class=article") {
		t.Fatalf("flaky: %+v rows %d", r.src.result(flaky.ID), r.ledgerRows(t))
	}
	delete(r.art.err, flaky.Ref.ArticleID)
	r.now = r.now.Add(2 * time.Minute)
	r.tick(t)
	if _, found := r.ledger(t, flaky.ID); !found {
		t.Fatal("the award did not recover once the engine did")
	}
}

type testError string

func (e testError) Error() string { return string(e) }

const errEngineDown = testError("engine down")

func TestRoyaltiesAndClawbacksAreNotCrossChecked(t *testing.T) {
	r := newAwardRig(t)
	r.royalty(1, 5, "2026-10-19")
	r.clawback(2, 1, 5)
	r.tick(t)
	if r.art.calls != 0 || r.ledgerRows(t) != 2 {
		t.Fatalf("article reads %d, rows %d", r.art.calls, r.ledgerRows(t))
	}
}

func TestSevenDayTerminalStates(t *testing.T) {
	old := func(r *awardRig, a sharedaccount.Award, age time.Duration) {
		r.src.awards[a.ID].a.CreatedAt = r.now.Add(-age)
	}
	run := func(r *awardRig, ticks int) {
		for i := 0; i < ticks; i++ {
			r.now = r.now.Add(2 * time.Hour)
			r.tick(t)
		}
	}
	t.Run("an unresolvable account is skipped after a week", func(t *testing.T) {
		r := newAwardRig(t)
		a := r.articleAward(1, 1, 1000)
		r.src.accountErr[uidN(1)] = sharedaccount.ErrUnavailable
		old(r, a, 8*24*time.Hour)
		r.tick(t)
		if r.src.result(a.ID) != nil {
			t.Fatal("skipped on the first failure")
		}
		run(r, 1)
		res := r.src.result(a.ID)
		if res == nil || res.Status != "skipped" || res.SkipReason != "account_unresolvable" || res.AppliedDelta != 0 || r.ledgerRows(t) != 0 {
			t.Fatalf("%+v", res)
		}
	})
	t.Run("an unreadable article is skipped as unavailable after a week", func(t *testing.T) {
		r := newAwardRig(t)
		a := r.articleAward(1, 1, 1000)
		r.art.err[a.Ref.ArticleID] = errEngineDown
		old(r, a, 8*24*time.Hour)
		r.tick(t)
		run(r, 1)
		if res := r.src.result(a.ID); res == nil || res.SkipReason != "article_unavailable" {
			t.Fatalf("%+v", res)
		}
	})
	t.Run("a young award keeps waiting", func(t *testing.T) {
		r := newAwardRig(t)
		a := r.articleAward(1, 1, 1000)
		r.src.accountErr[uidN(1)] = sharedaccount.ErrBanned
		old(r, a, 6*24*time.Hour)
		r.tick(t)
		run(r, 10)
		if r.src.result(a.ID) != nil {
			t.Fatal("skipped before a week")
		}
	})
	t.Run("a firestore read failure alone never ends an award", func(t *testing.T) {
		r := newAwardRig(t)
		a := r.articleAward(1, 1, 1000)
		r.src.contribErr = sharedaccount.ErrUnavailable
		old(r, a, 30*24*time.Hour)
		r.tick(t)
		run(r, 5)
		if r.src.result(a.ID) != nil {
			t.Fatal("skipped on a contributor read failure")
		}
	})
	t.Run("a credited award is marked applied, never skipped", func(t *testing.T) {
		r := newAwardRig(t)
		a := r.articleAward(1, 1, 1000)
		r.s.awards.hook = func(step string) error {
			if step == "mark" {
				return errEngineDown
			}
			return nil
		}
		old(r, a, 8*24*time.Hour)
		r.tick(t)
		r.s.awards.hook = nil
		r.src.accountErr[uidN(1)] = sharedaccount.ErrUnavailable
		run(r, 3)
		res := r.src.result(a.ID)
		if res == nil || res.Status != "applied" || res.AppliedDelta != 1000 || r.ledgerRows(t) != 1 {
			t.Fatalf("%+v rows %d", res, r.ledgerRows(t))
		}
	})
}

func TestRetryBacksOffFromThirtySecondsToAnHour(t *testing.T) {
	r := newAwardRig(t)
	r.articleAward(1, 1, 1000)
	r.src.accountErr[uidN(1)] = sharedaccount.ErrUnavailable
	reads := func() int { return r.src.calls["Account"] }
	r.tick(t)
	if reads() != 1 {
		t.Fatalf("%d reads", reads())
	}
	for _, step := range []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 16 * time.Minute, 32 * time.Minute, time.Hour, time.Hour} {
		before := reads()
		r.now = r.now.Add(step - time.Second)
		r.tick(t)
		if reads() != before {
			t.Fatalf("retried %v early", step)
		}
		r.now = r.now.Add(time.Second)
		r.tick(t)
		if reads() != before+1 {
			t.Fatalf("not retried after %v", step)
		}
	}
}

func TestAnAccountAnswerForAnotherUIDIsNeverCredited(t *testing.T) {
	r := newAwardRig(t)
	a := r.articleAward(1, 1, 1000)
	r.src.accounts[uidN(1)] = sharedaccount.Identity{UID: uidN(2), Email: emailN(2)}
	r.tick(t)
	if r.src.result(a.ID) != nil || r.ledgerRows(t) != 0 || userCount(t, r) != 0 {
		t.Fatalf("result %+v rows %d users %d", r.src.result(a.ID), r.ledgerRows(t), userCount(t, r))
	}
}
