package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerStopsAtTheModerationCeilingAndWaitsOnAnEmptyPool(t *testing.T) {
	app := newFakeApp(t)
	app.onClaim = func(int, int) (int, http.Header, string) {
		return ok(`{"leases":[],"retry_after_s":3600,"moderation_ceiling":true}`)
	}
	r := newWorkerRun(t, app, workerOptions{MaxPages: 5})
	if err := r.run(t); err != nil || r.w.summary().StopReason != "moderation_ceiling" || len(app.claimCalls()) != 1 {
		t.Fatalf("ceiling: %v %+v", err, r.w.summary())
	}

	empty := newFakeApp(t)
	empty.onClaim = func(int, int) (int, http.Header, string) { return ok(`{"leases":[],"retry_after_s":45}`) }
	r = newWorkerRun(t, empty, workerOptions{MaxPages: 5})
	if err := r.run(t); err != nil || r.w.summary().StopReason != "no_work" {
		t.Fatalf("empty pool: %v %+v", err, r.w.summary())
	}
	slept := r.sleeps.slept()
	if len(empty.claimCalls()) != 10 || len(slept) != 9 || slept[0] != 45*time.Second {
		t.Fatalf("%d claims, slept %v", len(empty.claimCalls()), slept)
	}
}

func TestWorkerAsksForAtMostWhatIsFreeAndAtMost32(t *testing.T) {
	app := newFakeApp(t)
	app.onClaim = func(int, int) (int, http.Header, string) { return ok(emptyPool) }
	r := newWorkerRun(t, app, workerOptions{MaxPages: 500, Concurrency: 100})
	r.w.idleLimit = 1
	r.run(t)
	if c := app.claimCalls(); len(c) != 1 || c[0] != 32 {
		t.Fatalf("claims %v", c)
	}

	app = newFakeApp(t)
	app.onClaim = func(int, int) (int, http.Header, string) { return ok(emptyPool) }
	r = newWorkerRun(t, app, workerOptions{MaxPages: 3, Concurrency: 8})
	r.w.idleLimit = 1
	r.run(t)
	if c := app.claimCalls(); len(c) != 1 || c[0] != 3 {
		t.Fatalf("max-pages not respected: %v", c)
	}
}

func TestWorkerKeepsAtMostConcurrencyLeasesInFlightAndTopsUpFreedSlots(t *testing.T) {
	var inflight, peak atomic.Int32
	release := make(chan struct{})
	gate := make(chan struct{}, 16)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := inflight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		gate <- struct{}{}
		<-release
		inflight.Add(-1)
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, "<html><title>T</title><body><article>"+strings.Repeat("A sentence of ordinary words for the page body. ", 6)+"</article></body></html>")
	}))
	defer origin.Close()
	app := newFakeApp(t)
	app.onClaim = func(n, max int) (int, http.Header, string) {
		if n > 2 {
			return ok(emptyPool)
		}
		paths := make([]string, max)
		for i := range paths {
			paths[i] = fmt.Sprintf("/p%d-%d", n, i)
		}
		return ok(leasesJSON(ev, paths...))
	}
	app.onSubmit = func(int, map[string]any) (int, http.Header, string) { return ok(verdictWaiting) }
	r := newWorkerRun(t, app, workerOptions{MaxPages: 4, Concurrency: 3})
	r.w.fetch = originClient(origin)
	done := make(chan error, 1)
	go func() { done <- r.run(t) }()
	for i := 0; i < 3; i++ {
		<-gate
	}
	time.Sleep(100 * time.Millisecond)
	if c := app.claimCalls(); len(c) != 1 || c[0] != 3 {
		t.Fatalf("a claim was made while every slot was busy: %v", c)
	}
	release <- struct{}{}
	<-gate
	for i := 0; i < 3; i++ {
		release <- struct{}{}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if c := app.claimCalls(); len(c) != 2 || c[1] != 1 {
		t.Fatalf("the freed slot was not topped up by exactly one: %v", c)
	}
	if peak.Load() > 3 {
		t.Fatalf("%d fetches at once, concurrency is 3", peak.Load())
	}
	if r.w.summary().Claimed != 4 || r.w.summary().Submitted != 4 {
		t.Fatalf("%+v", r.w.summary())
	}
}

func TestWorkerStopsClaimingAtTheDuration(t *testing.T) {
	app := newFakeApp(t)
	app.onClaim = func(int, int) (int, http.Header, string) { return ok(emptyPool) }
	r := newWorkerRun(t, app, workerOptions{Duration: 150 * time.Millisecond})
	r.w.sleep = sleepContext
	start := time.Now()
	if err := r.run(t); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 3*time.Second || r.w.summary().StopReason != "duration" {
		t.Fatalf("took %v, %+v", took, r.w.summary())
	}
}

func TestWorkerRunNeedsABound(t *testing.T) {
	app := newFakeApp(t)
	r := newWorkerRun(t, app, workerOptions{})
	if err := r.run(t); err == nil || len(app.claimCalls()) != 0 {
		t.Fatalf("got %v", err)
	}
}
