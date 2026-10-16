package community

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestConcurrentCreditsOfOnePageToOneAccountPayOnce(t *testing.T) {
	r, _, _ := settledRig(t)
	var wg sync.WaitGroup
	paid := make(chan bool, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := r.s.creditFetch(context.Background(), sha(1), "acct-0")
			if err != nil {
				t.Error(err)
			}
			paid <- ok
		}()
	}
	wg.Wait()
	close(paid)
	won := 0
	for ok := range paid {
		if ok {
			won++
		}
	}
	if won != 1 || r.fetchRows("acct-0") != 1 {
		t.Fatalf("%d calls reported a payment, %d rows", won, r.fetchRows("acct-0"))
	}
}

func TestConcurrentCreditsNeverPassTheDailyCap(t *testing.T) {
	r, _, _ := settledRig(t)
	r.s.cfg.FetchDailyCap = 5
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := r.s.creditFetch(context.Background(), sha(i), "acct-0"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n := r.fetchRows("acct-0"); n != 5 {
		t.Fatalf("%d pages paid under a cap of 5", n)
	}
}

func (r *workRig) otherLedgerRowsToday(user string) {
	for i, row := range []struct {
		reason string
		delta  int
	}{{"monthly_free", 100}, {"extra_request", -1}, {"article_read", -2}, {"verified_contribution", 10}, {"royalty", 5}, {"clawback", -3}} {
		if _, err := r.s.db.Exec(`INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES(?,?,?,?,?)`, fmt.Sprintf("other:%s:%d", user, i), user, row.delta, row.reason, r.clk.now().Unix()); err != nil {
			panic(err)
		}
	}
}

func TestTheDailyCapCountsOnlyFetchCreditsNotOtherLedgerRows(t *testing.T) {
	r, fd, ws := settledRig(t)
	r.s.cfg.FetchDailyCap = 2
	r.otherLedgerRowsToday("acct-0")
	for i := 1; i <= 3; i++ {
		fd.add("verified", true, sha(i), ws.a)
	}
	if err := r.s.pollSettled(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := r.fetchRows("acct-0"); n != 2 {
		t.Fatalf("%d pages paid under a cap of 2 with six other rows today", n)
	}

	claims := newWorkRig(t)
	claims.s.cfg.FetchDailyCap = 3
	claims.engine.answer(fixed(200, `{"leases":[]}`))
	claims.otherLedgerRowsToday(claims.userID)
	for i := 0; i < 2; i++ {
		claims.s.db.Exec(`INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES(?,?,2,'fetch_verified',?)`, fmt.Sprintf("fetch:c%d", i), claims.userID, claims.clk.now().Unix())
	}
	if w := claims.claim(map[string]any{"max": 1}); w.Code != 200 {
		t.Fatalf("two of three pages today: %d %s", w.Code, w.Body)
	}
	claims.s.db.Exec(`INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES('fetch:c2',?,2,'fetch_verified',?)`, claims.userID, claims.clk.now().Unix())
	claims.clk.advance(2 * time.Minute)
	if w := claims.claim(map[string]any{"max": 1}); w.Code != 429 || errBody(t, w).Code != "daily_cap" {
		t.Fatalf("three of three pages today: %d %s", w.Code, w.Body)
	}
}

func TestRandomSettledStreamsNeverPayTwiceNorPastTheCapNorWithoutNewContent(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		rng := rand.New(rand.NewSource(seed))
		r, fd, ws := settledRig(t)
		ctx := context.Background()
		r.s.cfg.FetchDailyCap = 4
		everyone := []string{ws.a, ws.b, ws.c, "ffffffffffffffffffffffffffffffff"}
		novel := map[string]bool{}
		verified := map[string]bool{}
		for step := 0; step < 120; step++ {
			switch rng.Intn(10) {
			case 0:
				r.clk.advance(time.Duration(rng.Intn(30)) * time.Hour)
			case 1:
				r.s.db.Exec(`UPDATE settings SET value='0' WHERE key='work_settled_seq'`)
			default:
				page := rng.Intn(25)
				verdict := []string{"verified", "verified", "verified", "mismatch", "rejected", "expired"}[rng.Intn(6)]
				isNovel := rng.Intn(3) != 0
				var workers []string
				for _, w := range everyone {
					if rng.Intn(2) == 0 {
						workers = append(workers, w)
					}
				}
				fd.add(verdict, isNovel, sha(page), workers...)
				if verdict == "verified" {
					verified[sha(page)] = true
					novel[sha(page)] = novel[sha(page)] || isNovel
				}
			}
			if err := r.s.pollSettled(ctx); err != nil {
				t.Fatal(err)
			}
		}
		rows, err := r.s.db.Query(`SELECT id,user_id,delta,created_at FROM credit_ledger WHERE reason='fetch_verified'`)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		perDay := map[string]int{}
		for rows.Next() {
			var id, user string
			var delta, at int64
			rows.Scan(&id, &user, &delta, &at)
			var urlSha string
			fmt.Sscanf(id, "fetch:%64s", &urlSha)
			if seen[id] || delta != 2 || !verified[urlSha] || !novel[urlSha] {
				t.Fatalf("seed %d: row %s delta %d (paid twice or without new verified content)", seed, id, delta)
			}
			seen[id] = true
			day := fmt.Sprintf("%s/%d", user, at/86400)
			if perDay[day]++; perDay[day] > 4 {
				t.Fatalf("seed %d: %s passed the cap", seed, day)
			}
		}
		rows.Close()
	}
}

func TestClaimAndSubmitNeverWriteTheLedger(t *testing.T) {
	r := newWorkRig(t)
	r.engine.answer(func(call engineRequest) (int, string) {
		if call.path == "/v1/work/claim" {
			return 200, claimReply
		}
		return 200, `{"verdict":"verified","agreeing":["3f9a1c0b7d2e48a6915c0d3b7e2f4a18"],"indexed":true}`
	})
	before := r.rows(`SELECT count(*) FROM credit_ledger`)
	for i := 0; i < 5; i++ {
		r.clk.advance(2 * time.Minute)
		r.claim(map[string]any{"max": 4})
		r.submit(attestation("page text"))
	}
	if after := r.rows(`SELECT count(*) FROM credit_ledger`); after != before {
		t.Fatalf("the ledger grew from %d to %d rows without a settled event", before, after)
	}
}

func TestAClientCannotChooseItsNetByForgingForwardedFor(t *testing.T) {
	r := newWorkRig(t)
	r.engine.answer(fixed(200, `{"leases":[]}`))
	netFor := func(forwarded string) string {
		r.clk.advance(2 * time.Minute)
		r.post("/api/work/claim", sharedTestToken, map[string]any{"max": 1}, func(req *http.Request) {
			req.RemoteAddr = "203.0.113.9:1"
			if forwarded != "" {
				req.Header.Set("X-Forwarded-For", forwarded)
				req.Header.Set("X-Real-IP", forwarded)
			}
		})
		calls := r.engine.on("/v1/work/claim")
		var sent struct{ Net string }
		json.Unmarshal(calls[len(calls)-1].body, &sent)
		return sent.Net
	}
	if netFor("") != netFor("198.51.100.77") || netFor("") != netFor("2001:db8::1, 192.0.2.1") {
		t.Fatal("a forged forwarding header changed the net")
	}
}
