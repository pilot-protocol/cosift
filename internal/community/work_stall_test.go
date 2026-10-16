package community

import (
	"context"
	"sync"
	"testing"
	"time"
)

// stallProbe sleeps 2 s in a remote call and records whether the database was free while it did.
type stallProbe struct {
	mu      sync.Mutex
	s       *Server
	stalled map[string]bool
	busy    []string
}

func (p *stallProbe) hit(op string) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	var one int
	err := p.s.db.QueryRowContext(ctx, `SELECT 1`).Scan(&one)
	p.mu.Lock()
	p.stalled[op] = true
	if err != nil {
		p.busy = append(p.busy, op)
	}
	p.mu.Unlock()
	time.Sleep(2 * time.Second)
}

func TestMeterAnswersWhileEveryWorkCallStalls(t *testing.T) {
	if testing.Short() {
		t.Skip("injects 2 s stalls")
	}
	r, fd, ws := settledRig(t)
	p := &stallProbe{s: r.s, stalled: map[string]bool{}}
	r.f.stall = p.hit
	r.engine.answer(func(call engineRequest) (int, string) {
		p.hit(call.path)
		switch call.path {
		case "/v1/work/claim":
			return 200, `{"leases":[],"moderation_ceiling":true}`
		case "/v1/work/push":
			return 200, `{"accepted":1,"duplicates":0,"rejected":[]}`
		}
		return fd.serve(call)
	})
	fd.add("verified", true, sha(1), ws.a)
	r.submission("alice", "https://example.org/blocked", "unverified", failFetch, r.clk.now())
	r.s.grantMonthlyCredits(t.Context(), r.userID, time.Now())
	r.s.noteCeiling(ceilingModerate)
	r.s.bumpFetch(testCID, fetchClaimed, 1)

	done := make(chan error, 1)
	go func() {
		var wg sync.WaitGroup
		for _, run := range []func(){
			func() { r.s.pollSettled(context.Background()) },
			func() { r.s.pushBlocked(context.Background()) },
			func() { r.s.flushFetchCounters(context.Background()) },
			func() { r.s.drainAlerts(context.Background()) },
			func() { r.claim(map[string]any{"max": 1}) },
		} {
			wg.Add(1)
			go func() { defer wg.Done(); run() }()
		}
		wg.Wait()
		done <- nil
	}()
	worst, calls := probeMeter(t, r.s, done)
	for _, op := range []string{"JoinState", "AddCounters", "RaiseAlert", "/v1/work/claim", "/v1/work/push", "/v1/work/settled"} {
		if !p.stalled[op] {
			t.Errorf("%s never ran, so it was never stalled", op)
		}
	}
	if len(p.busy) > 0 {
		t.Fatalf("the database was held across the stalled %v", p.busy)
	}
	if worst > 200*time.Millisecond || calls < 5 {
		t.Fatalf("worst meter answer %v over %d calls", worst, calls)
	}
}
