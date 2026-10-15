//go:build !windows

package main

import (
	"context"
	"net/http"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

func TestWorkerStopsOnARealSIGINT(t *testing.T) {
	app := newFakeApp(t)
	app.onClaim = queue(ev, "/a")
	app.onSubmit = func(int, map[string]any) (int, http.Header, string) { return ok(verdictWaiting) }
	r := newWorkerRun(t, app, workerOptions{MaxPages: 10, Concurrency: 1})
	origin, gate, release := blockingOrigin(t)
	r.w.fetch = originClient(origin)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- r.w.run(ctx) }()
	<-gate
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if s := r.w.summary(); s.StopReason != "interrupted" || s.Submitted != 1 {
		t.Fatalf("%+v", s)
	}
}
