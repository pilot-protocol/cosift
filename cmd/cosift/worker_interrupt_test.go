package main

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestWorkerInterruptLetsInFlightLeasesFinishWithinTheGraceAndSummarises(t *testing.T) {
	app := newFakeApp(t)
	app.onClaim = queue(ev, "/a", "/b", "/c", "/d")
	app.onSubmit = func(int, map[string]any) (int, http.Header, string) { return ok(verdictWaiting) }
	r := newWorkerRun(t, app, workerOptions{MaxPages: 10, Concurrency: 2})
	origin, gate, release := blockingOrigin(t)
	r.w.fetch = originClient(origin)
	ctx, interrupt := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.w.run(ctx) }()
	<-gate
	<-gate
	interrupt()
	time.Sleep(50 * time.Millisecond)
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	s := r.w.summary()
	if s.Submitted != 2 || s.Abandoned != 0 || s.StopReason != "interrupted" || len(app.claimCalls()) != 1 {
		t.Fatalf("%+v after %d claims", s, len(app.claimCalls()))
	}
	events := r.events(t)
	if events[len(events)-1]["event"] != "summary" || events[len(events)-1]["stop_reason"] != "interrupted" {
		t.Fatalf("last line: %v", events[len(events)-1])
	}
}

func TestWorkerInterruptAbandonsLeasesThatOutlastTheGrace(t *testing.T) {
	app := newFakeApp(t)
	app.onClaim = queue(ev, "/a", "/b")
	app.onSubmit = func(int, map[string]any) (int, http.Header, string) { return ok(verdictWaiting) }
	r := newWorkerRun(t, app, workerOptions{MaxPages: 10, Concurrency: 2})
	r.w.grace = 100 * time.Millisecond
	origin, gate, _ := blockingOrigin(t)
	r.w.fetch = originClient(origin)
	ctx, interrupt := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.w.run(ctx) }()
	<-gate
	<-gate
	start := time.Now()
	interrupt()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("took %v to give up after the grace", took)
	}
	if s := r.w.summary(); s.Abandoned != 2 || s.Submitted != 0 || s.StopReason != "interrupted" {
		t.Fatalf("%+v", s)
	}
	events := r.events(t)
	if events[len(events)-1]["event"] != "summary" || events[len(events)-1]["abandoned"] != float64(2) {
		t.Fatalf("last line: %v", events[len(events)-1])
	}
}
