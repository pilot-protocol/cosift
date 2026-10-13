package community

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

func TestRandomAwardSequencesNeverDriveABalanceBelowZero(t *testing.T) {
	for seed := int64(1); seed <= 25; seed++ {
		rng := rand.New(rand.NewSource(seed))
		r := newAwardRig(t)
		const people = 3
		starts := map[int]int{}
		for n := 1; n <= people; n++ {
			r.person(n)
			user, err := r.s.awardUser(t.Context(), sharedaccount.Identity{UID: uidN(n), Email: emailN(n)})
			if err != nil {
				t.Fatal(err)
			}
			starts[n] = []int{-100, 0, 50, 700, 1000}[rng.Intn(5)]
			r.s.db.Exec(`INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES('seed',?,?,'test',0)`, user, starts[n])
		}
		var ids []string
		for i := 1; i <= 40; i++ {
			n := 1 + rng.Intn(people)
			var a sharedaccount.Award
			switch rng.Intn(3) {
			case 0:
				a = r.articleAward(n, i, 1+rng.Intn(1000))
			case 1:
				a = r.royalty(n, 1+rng.Intn(100), fmt.Sprintf("2026-%02d-%02d", 8+i/28, 1+i%28))
			default:
				a = r.clawback(n, i, 1+rng.Intn(800))
			}
			ids = append(ids, a.ID)
		}
		for tick := 0; tick < 400 && !r.allMarked(ids); tick++ {
			if rng.Intn(3) == 0 {
				r.src.markErr = sharedaccount.ErrUnavailable
			} else {
				r.src.markErr = nil
			}
			r.now = r.now.Add(2 * time.Hour)
			r.tick(t)
		}
		r.src.markErr = nil
		for tick := 0; tick < 400 && !r.allMarked(ids); tick++ {
			r.now = r.now.Add(2 * time.Hour)
			r.tick(t)
		}
		if !r.allMarked(ids) {
			t.Fatalf("seed %d: awards left pending", seed)
		}
		if r.ledgerRows(t) != len(ids) {
			t.Fatalf("seed %d: %d ledger rows for %d awards", seed, r.ledgerRows(t), len(ids))
		}
		for n := 1; n <= people; n++ {
			r.checkNeverBelowZero(t, seed, r.userID(t, n), starts[n])
		}
		for _, id := range ids {
			delta, _ := r.ledger(t, id)
			if res := r.src.result(id); res == nil || res.AppliedDelta != int64(delta) {
				t.Fatalf("seed %d: %s marked %+v for ledger %d", seed, id, res, delta)
			}
		}
	}
}

func (r *awardRig) allMarked(ids []string) bool {
	for _, id := range ids {
		if r.src.result(id) == nil {
			return false
		}
	}
	return true
}

func (r *awardRig) checkNeverBelowZero(t *testing.T, seed int64, userID string, start int) {
	t.Helper()
	rows, err := r.s.db.Query(`SELECT id,delta,reason FROM credit_ledger WHERE user_id=? ORDER BY rowid`, userID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	balance := 0
	for rows.Next() {
		var id, reason string
		var delta int
		rows.Scan(&id, &delta, &reason)
		if reason == "clawback" {
			if delta > 0 || balance+delta < min(balance, 0) || (balance <= 0 && delta != 0) {
				t.Fatalf("seed %d: clawback %s took %d from a balance of %d (start %d)", seed, id, delta, balance, start)
			}
		}
		balance += delta
	}
	if start >= 0 && balance < 0 {
		t.Fatalf("seed %d: balance %d from a start of %d", seed, balance, start)
	}
}

func TestAClawbackAndConcurrentMeterDebitsNeverOverdraw(t *testing.T) {
	r := newAwardRig(t)
	for i := 1; i <= 100; i++ {
		user, err := r.s.awardUser(t.Context(), sharedaccount.Identity{UID: uidN(i), Email: emailN(i)})
		if err != nil {
			t.Fatal(err)
		}
		if err := r.s.grantMonthlyCredits(t.Context(), user, time.Now()); err != nil {
			t.Fatal(err)
		}
		if _, err := r.s.db.Exec(`INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES(?,?,?,'test',0)`, fmt.Sprintf("seed-%d", i), user, 10-monthlyFreeCredits); err != nil {
			t.Fatal(err)
		}
		if r.balance(t, user) != 10 {
			t.Fatalf("round %d starts with %d credits", i, r.balance(t, user))
		}
		req := ulidN(800+i) + "_" + ulidN(i)
		claw := sharedaccount.Award{ID: "claw:" + req + ":" + cidN(i), UID: uidN(i), CID: cidN(i), Amount: -10, Reason: "clawback"}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); <-start; r.s.writeAward(t.Context(), claw, user) }()
		for d := 0; d < 4; d++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				r.s.debitMeter(t.Context(), user, fmt.Sprintf("race-%04d-%04d", i, d), 2)
			}()
		}
		close(start)
		wg.Wait()
		if balance := r.balance(t, user); balance < 0 {
			t.Fatalf("round %d: a clawback and concurrent debits left a balance of %d", i, balance)
		}
	}
}
