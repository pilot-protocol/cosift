package community

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type feedEvent struct {
	Seq          int64    `json:"seq"`
	URLSha       string   `json:"url_sha"`
	Verdict      string   `json:"verdict"`
	Workers      []string `json:"workers"`
	ContentNovel bool     `json:"content_novel"`
	At           string   `json:"at"`
}

// feed is a durable settled queue as the engine serves it.
type feed struct {
	mu      sync.Mutex
	events  []feedEvent
	last    int64
	ceiling bool
	fail    int
	afters  []int64
}

func (f *feed) add(verdict string, novel bool, sha string, workers ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.last++
	f.events = append(f.events, feedEvent{f.last, sha, verdict, workers, novel, "2026-10-14T09:00:00Z"})
}

func (f *feed) serve(call engineRequest) (int, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	q, _ := url.ParseQuery(call.query)
	after, _ := strconv.ParseInt(q.Get("after"), 10, 64)
	limit, _ := strconv.Atoi(q.Get("limit"))
	f.afters = append(f.afters, after)
	if f.fail != 0 {
		return f.fail, `{"code":"x"}`
	}
	out := struct {
		Events  []feedEvent `json:"events"`
		Next    int64       `json:"next"`
		LastSeq int64       `json:"last_seq"`
		Ceiling bool        `json:"moderation_ceiling,omitempty"`
	}{Events: []feedEvent{}, Next: after, LastSeq: f.last, Ceiling: f.ceiling}
	if after <= f.last {
		for _, e := range f.events {
			if e.Seq > after && len(out.Events) < limit {
				out.Events = append(out.Events, e)
				out.Next = e.Seq
			}
		}
	}
	b, _ := json.Marshal(out)
	return 200, string(b)
}

func sha(n int) string { return fmt.Sprintf("%064x", n) }

type workers struct{ a, b, c string }

// settledRig registers three workers of three accounts.
func settledRig(t *testing.T) (*workRig, *feed, workers) {
	t.Helper()
	r := newWorkRig(t)
	fd := &feed{}
	r.engine.answer(fd.serve)
	var ws workers
	for i, p := range []*string{&ws.a, &ws.b, &ws.c} {
		id := fmt.Sprintf("acct-%d", i)
		r.s.db.Exec(`INSERT INTO users(id,email,name,salt,password_hash,created_at) VALUES(?,?,?,?,?,0)`, id, id+"@example.com", id, "s", "")
		*p = r.s.workerID(id)
		if _, err := r.s.db.Exec(`INSERT INTO work_workers(worker,user_id,cid,first_seen) VALUES(?,?,?,0)`, *p, id, fmt.Sprintf("%016x", i+1)); err != nil {
			t.Fatal(err)
		}
	}
	return r, fd, ws
}

func (r *workRig) fetchRows(user string) int {
	return r.rows(`SELECT count(*) FROM credit_ledger WHERE user_id=? AND reason='fetch_verified'`, user)
}

func (r *workRig) cursor() string {
	var v string
	r.s.db.QueryRow(`SELECT value FROM settings WHERE key='work_settled_seq'`).Scan(&v)
	return v
}

func TestSettledVerifiedNovelPagesPayEachAgreeingWorkerTwoOnce(t *testing.T) {
	r, fd, ws := settledRig(t)
	ctx := context.Background()
	fd.add("verified", true, sha(1), ws.a, ws.b)
	if err := r.s.pollSettled(ctx); err != nil {
		t.Fatal(err)
	}
	for _, user := range []string{"acct-0", "acct-1"} {
		var id, reason string
		var delta int
		if err := r.s.db.QueryRow(`SELECT id,delta,reason FROM credit_ledger WHERE user_id=? AND reason='fetch_verified'`, user).Scan(&id, &delta, &reason); err != nil {
			t.Fatal(user, err)
		}
		if id != "fetch:"+sha(1)+":"+user || delta != 2 {
			t.Errorf("%s: %s %d", user, id, delta)
		}
	}
	if r.fetchRows("acct-2") != 0 {
		t.Fatal("a worker outside the event was paid")
	}
	if r.cursor() != "1" {
		t.Fatalf("cursor %q", r.cursor())
	}
	r.s.flushFetchCounters(ctx)
	if r.f.counter("0000000000000001", "fetch_verified") != 1 || r.f.counter("0000000000000001", "fetch_credits") != 2 || r.f.counter("0000000000000003", "fetch_verified") != 0 {
		t.Fatalf("%v", r.f.counters)
	}
	calls := r.engine.on("/v1/work/settled")
	if len(calls) != 1 || calls[0].method != "GET" || calls[0].auth != "Bearer "+testBrokerKey || calls[0].query != "after=0&limit=200" {
		t.Fatalf("%+v", calls)
	}
}

func TestSettledReplayAndCursorLossChangeNoLedgerRow(t *testing.T) {
	r, fd, ws := settledRig(t)
	ctx := context.Background()
	fd.add("verified", true, sha(1), ws.a)
	fd.add("verified", true, sha(2), ws.a, ws.b)
	r.s.pollSettled(ctx)
	r.s.db.Exec(`DELETE FROM settings WHERE key='work_settled_seq'`)
	r.s.pollSettled(ctx)
	r.s.db.Exec(`UPDATE settings SET value='0' WHERE key='work_settled_seq'`)
	r.s.pollSettled(ctx)
	if n := r.fetchRows("acct-0"); n != 2 {
		t.Fatalf("%d rows for two pages", n)
	}
	if n := r.fetchRows("acct-1"); n != 1 {
		t.Fatalf("%d rows for one page", n)
	}
	var total int
	r.s.db.QueryRow(`SELECT sum(delta) FROM credit_ledger WHERE reason='fetch_verified'`).Scan(&total)
	if total != 6 {
		t.Fatalf("total %d, want 6", total)
	}
}

func TestSettledPaysNothingWithoutNewContent(t *testing.T) {
	r, fd, ws := settledRig(t)
	ctx := context.Background()
	fd.add("verified", false, sha(1), ws.a, ws.b)
	r.s.pollSettled(ctx)
	if r.fetchRows("acct-0")+r.fetchRows("acct-1") != 0 {
		t.Fatal("a page that was not new content paid")
	}
	r.s.flushFetchCounters(ctx)
	if r.f.counter("0000000000000001", "fetch_verified") != 1 || r.f.counter("0000000000000001", "fetch_credits") != 0 {
		t.Fatalf("%v", r.f.counters)
	}
}

func TestSettledSkipsAnUnknownWorkerAndStillPaysTheOthers(t *testing.T) {
	r, fd, ws := settledRig(t)
	fd.add("verified", true, sha(1), strings.Repeat("f", 32), ws.a)
	if err := r.s.pollSettled(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.fetchRows("acct-0") != 1 || r.rows(`SELECT count(*) FROM credit_ledger WHERE reason='fetch_verified'`) != 1 {
		t.Fatal("the known worker was not paid exactly once")
	}
}

func TestSettledCountsAWorkerListedTwiceInOneEventOnce(t *testing.T) {
	r, fd, ws := settledRig(t)
	ctx := context.Background()
	fd.add("verified", true, sha(1), ws.a, ws.a, ws.b)
	fd.add("mismatch", true, sha(2), ws.a, ws.a)
	r.s.pollSettled(ctx)
	r.s.flushFetchCounters(ctx)
	a := "0000000000000001"
	if r.f.counter(a, "fetch_verified") != 1 || r.f.counter(a, "fetch_credits") != 2 || r.f.counter(a, "fetch_mismatch") != 1 || r.fetchRows("acct-0") != 1 {
		t.Fatalf("%v", r.f.counters)
	}
}

func TestSettledCountsMismatchAndRejectedWithoutPaying(t *testing.T) {
	r, fd, ws := settledRig(t)
	ctx := context.Background()
	fd.add("mismatch", true, sha(1), ws.a)
	fd.add("rejected", true, sha(2), ws.a, ws.b)
	fd.add("expired", true, sha(3), ws.a)
	r.s.pollSettled(ctx)
	r.s.flushFetchCounters(ctx)
	a, b := "0000000000000001", "0000000000000002"
	if r.f.counter(a, "fetch_mismatch") != 1 || r.f.counter(a, "fetch_rejected") != 1 || r.f.counter(b, "fetch_rejected") != 1 || r.f.counter(a, "fetch_verified") != 0 {
		t.Fatalf("%v", r.f.counters)
	}
	if r.rows(`SELECT count(*) FROM credit_ledger WHERE reason='fetch_verified'`) != 0 {
		t.Fatal("a mismatch, a rejection or an expiry paid")
	}
}

func TestSettledDailyCapSkipsWithoutRetryAndResetsAtMidnight(t *testing.T) {
	r, fd, ws := settledRig(t)
	ctx := context.Background()
	r.s.cfg.FetchDailyCap = 2
	for i := 1; i <= 3; i++ {
		fd.add("verified", true, sha(i), ws.a)
	}
	r.s.pollSettled(ctx)
	if n := r.fetchRows("acct-0"); n != 2 {
		t.Fatalf("%d pages paid under a cap of 2", n)
	}
	r.s.db.Exec(`UPDATE settings SET value='0' WHERE key='work_settled_seq'`)
	r.s.pollSettled(ctx)
	if n := r.fetchRows("acct-0"); n != 2 {
		t.Fatalf("a replay on the same day paid page %d", n)
	}
	fd.add("verified", true, sha(4), ws.a)
	r.s.pollSettled(ctx)
	if n := r.fetchRows("acct-0"); n != 2 {
		t.Fatal("a new page past the cap paid")
	}
	r.clk.advance(24 * time.Hour)
	fd.add("verified", true, sha(5), ws.a)
	r.s.pollSettled(ctx)
	if n := r.fetchRows("acct-0"); n != 3 {
		t.Fatalf("the next day's first page: %d rows", n)
	}
	if n := r.fetchRows("acct-1"); n != 0 {
		t.Fatal("one account's cap spent another's")
	}
}

func TestSettledResetsTheCursorWhenTheEngineWasRestoredToAnOlderSnapshot(t *testing.T) {
	r, fd, ws := settledRig(t)
	ctx := context.Background()
	fd.add("verified", true, sha(1), ws.a)
	fd.add("verified", true, sha(2), ws.b)
	r.s.db.Exec(`INSERT INTO settings(key,value) VALUES('work_settled_seq','50')`)
	if err := r.s.pollSettled(ctx); err != nil {
		t.Fatal(err)
	}
	if fd.afters[0] != 50 || fd.afters[1] != 0 {
		t.Fatalf("read after %v: want 50, then a re-read from 0", fd.afters)
	}
	if r.cursor() != "2" || r.fetchRows("acct-0") != 1 || r.fetchRows("acct-1") != 1 {
		t.Fatalf("cursor %q, rows %d %d", r.cursor(), r.fetchRows("acct-0"), r.fetchRows("acct-1"))
	}
}

func TestSettledDoesNotResetAnUpToDateCursorButResetsOneThatIsOnePast(t *testing.T) {
	r, fd, ws := settledRig(t)
	ctx := context.Background()
	fd.add("verified", true, sha(1), ws.a)
	fd.add("verified", true, sha(2), ws.a)
	a := "0000000000000001"
	r.s.pollSettled(ctx)
	r.s.flushFetchCounters(ctx)
	for i := 0; i < 3; i++ {
		if err := r.s.pollSettled(ctx); err != nil {
			t.Fatal(err)
		}
	}
	r.s.flushFetchCounters(ctx)
	if fmt.Sprint(fd.afters) != "[0 2 2 2]" || r.cursor() != "2" || r.f.counter(a, "fetch_verified") != 2 || r.f.counter(a, "fetch_credits") != 4 {
		t.Fatalf("reads after %v, cursor %q, counters %v", fd.afters, r.cursor(), r.f.counters)
	}
	r.s.db.Exec(`UPDATE settings SET value='3' WHERE key='work_settled_seq'`)
	r.s.pollSettled(ctx)
	if got := fd.afters[len(fd.afters)-2:]; fmt.Sprint(got) != "[3 0]" || r.cursor() != "2" {
		t.Fatalf("a cursor one past the engine's last: reads %v, cursor %q", fd.afters, r.cursor())
	}
}

func TestSettledKeepsTheCursorAndRetriesWhenAnEventCannotBeApplied(t *testing.T) {
	for _, tc := range []struct{ name, fail, repair string }{
		{"worker lookup", `ALTER TABLE work_workers RENAME TO work_workers_off`, `ALTER TABLE work_workers_off RENAME TO work_workers`},
		{"ledger insert", `CREATE TRIGGER no_credit BEFORE INSERT ON credit_ledger BEGIN SELECT RAISE(ABORT,'database is locked'); END`, `DROP TRIGGER no_credit`},
	} {
		r, fd, ws := settledRig(t)
		ctx := context.Background()
		fd.add("verified", true, sha(1), ws.a)
		if err := r.s.pollSettled(ctx); err != nil {
			t.Fatal(err)
		}
		fd.add("verified", true, sha(2), ws.a)
		if _, err := r.s.db.Exec(tc.fail); err != nil {
			t.Fatal(err)
		}
		if err := r.s.pollSettled(ctx); err == nil {
			t.Errorf("%s: no error", tc.name)
		}
		if r.cursor() != "1" {
			t.Errorf("%s: the cursor moved to %q past an event that was not applied", tc.name, r.cursor())
		}
		if _, err := r.s.db.Exec(tc.repair); err != nil {
			t.Fatal(err)
		}
		if err := r.s.pollSettled(ctx); err != nil || r.cursor() != "2" || r.fetchRows("acct-0") != 2 {
			t.Errorf("%s: after the repair: %v, cursor %q, %d rows", tc.name, err, r.cursor(), r.fetchRows("acct-0"))
		}
	}
}

func TestSettledReadsPagesOfTwoHundredUntilCaughtUp(t *testing.T) {
	r, fd, ws := settledRig(t)
	for i := 1; i <= 450; i++ {
		fd.add("rejected", false, sha(i), ws.a)
	}
	if err := r.s.pollSettled(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.cursor() != "450" || fmt.Sprint(fd.afters) != "[0 200 400]" {
		t.Fatalf("cursor %q after reads %v", r.cursor(), fd.afters)
	}
}

func TestSettledKeepsTheCursorWhenTheEngineFails(t *testing.T) {
	r, fd, ws := settledRig(t)
	ctx := context.Background()
	fd.add("verified", true, sha(1), ws.a)
	r.s.pollSettled(ctx)
	fd.add("verified", true, sha(2), ws.a)
	for _, status := range []int{500, 401, 403, 503} {
		fd.fail = status
		logs := captureLog(t)
		if err := r.s.pollSettled(ctx); err == nil {
			t.Errorf("status %d: no error", status)
		}
		if r.cursor() != "1" {
			t.Errorf("status %d moved the cursor to %q", status, r.cursor())
		}
		if (status == 401 || status == 403) && !strings.Contains(logs.String(), "HTTP "+strconv.Itoa(status)) {
			t.Errorf("status %d left no operator-visible line: %s", status, logs)
		}
		if strings.Contains(logs.String(), testBrokerKey) {
			t.Errorf("status %d: the key was logged", status)
		}
	}
	fd.fail = 0
	r.s.pollSettled(ctx)
	if r.cursor() != "2" || r.fetchRows("acct-0") != 2 {
		t.Fatalf("cursor %q rows %d after recovery", r.cursor(), r.fetchRows("acct-0"))
	}
}

func TestSettledIgnoresAMalformedEventButPassesIt(t *testing.T) {
	r, fd, ws := settledRig(t)
	fd.add("verified", true, "not-a-sha", ws.a)
	fd.add("verified", true, sha(2), "short")
	fd.add("celebrated", true, sha(3), ws.a)
	fd.add("verified", true, sha(4), ws.a)
	r.s.pollSettled(context.Background())
	if r.fetchRows("acct-0") != 1 || r.cursor() != "4" {
		t.Fatalf("rows %d cursor %q", r.fetchRows("acct-0"), r.cursor())
	}
}

func TestSettledQueuesTheCeilingAlert(t *testing.T) {
	r, fd, _ := settledRig(t)
	fd.ceiling = true
	r.s.pollSettled(context.Background())
	r.s.drainAlerts(context.Background())
	if len(r.f.alerts) != 1 {
		t.Fatalf("%+v", r.f.alerts)
	}
}

func TestSettledStopsAtAContextDeadlineWithoutAdvancing(t *testing.T) {
	r, fd, ws := settledRig(t)
	fd.add("verified", true, sha(1), ws.a)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.s.pollSettled(ctx); err == nil || r.cursor() != "" || r.fetchRows("acct-0") != 0 {
		t.Fatalf("%v cursor %q", err, r.cursor())
	}
}

func TestMeterAnswersWhileTheSettledReadStalls(t *testing.T) {
	if testing.Short() {
		t.Skip("injects a 2 s stall")
	}
	r, fd, ws := settledRig(t)
	fd.add("verified", true, sha(1), ws.a)
	stalled := make(chan struct{})
	var once sync.Once
	var busy bool
	r.engine.answer(func(call engineRequest) (int, string) {
		once.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			var one int
			busy = r.s.db.QueryRowContext(ctx, `SELECT 1`).Scan(&one) != nil
			close(stalled)
			time.Sleep(2 * time.Second)
		})
		return fd.serve(call)
	})
	r.s.grantMonthlyCredits(t.Context(), r.userID, time.Now())
	done := make(chan error, 1)
	go func() { done <- r.s.pollSettled(context.Background()) }()
	worst, calls := probeMeter(t, r.s, done)
	select {
	case <-stalled:
	default:
		t.Fatal("the read was never stalled")
	}
	if busy || worst > 200*time.Millisecond || calls < 5 {
		t.Fatalf("busy=%v worst=%v calls=%d", busy, worst, calls)
	}
}
