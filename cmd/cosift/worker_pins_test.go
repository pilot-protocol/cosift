package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWorkerKeepsOnlyTheLeasesItAskedFor(t *testing.T) {
	app := newFakeApp(t)
	app.onClaim = func(int, int) (int, http.Header, string) {
		return ok(leasesJSON(ev, "/plain.html", "/german.html", "/moved"))
	}
	app.onSubmit = func(int, map[string]any) (int, http.Header, string) { return ok(verdictWaiting) }
	r := newWorkerRun(t, app, workerOptions{MaxPages: 1, Concurrency: 1})
	if err := r.run(t); err != nil {
		t.Fatal(err)
	}
	if s := r.w.summary(); s.Claimed != 1 || s.Submitted != 1 || len(app.submitted()) != 1 {
		t.Fatalf("%+v, %d submissions", s, len(app.submitted()))
	}
}

func TestWorkerRefusesALeaseOfAnOlderExtractVersion(t *testing.T) {
	app := newFakeApp(t)
	app.onClaim = queue(ev-1, "/plain.html")
	app.onSubmit = func(int, map[string]any) (int, http.Header, string) { return ok(verdictWaiting) }
	r := newWorkerRun(t, app, workerOptions{MaxPages: 1})
	if err := r.run(t); err == nil || !strings.Contains(err.Error(), "upgrade cosift") || len(r.hits.seen) != 0 {
		t.Fatalf("got %v after %d fetches", err, len(r.hits.seen))
	}
}

func TestWorkerFetchesPagesUpToTheServersBodyCap(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, "<html><title>Big</title><body><!--"+strings.Repeat("x", 2<<20)+"--><article>"+strings.Repeat("A sentence of ordinary words for the page body. ", 6)+"</article></body></html>")
	}))
	t.Cleanup(origin.Close)
	app := newFakeApp(t)
	app.onClaim = queue(ev, "/big.html")
	app.onSubmit = func(int, map[string]any) (int, http.Header, string) { return ok(verdictWaiting) }
	r := newWorkerRun(t, app, workerOptions{MaxPages: 1})
	r.w.fetch = originClient(origin)
	if err := r.run(t); err != nil {
		t.Fatal(err)
	}
	if s := r.w.summary(); s.Submitted != 1 || s.Skipped != 0 {
		t.Fatalf("%+v", s)
	}
}

func TestWorkerLeaseRetryWaitsGrow(t *testing.T) {
	app := newFakeApp(t)
	app.onClaim = queue(ev, "/plain.html")
	app.onSubmit = func(int, map[string]any) (int, http.Header, string) { return 409, nil, `{"code":"lease_expired"}` }
	r := newWorkerRun(t, app, workerOptions{MaxPages: 1})
	if err := r.run(t); err != nil {
		t.Fatal(err)
	}
	if got, want := r.sleeps.slept(), []time.Duration{time.Second, 2 * time.Second}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("slept %v, want %v", got, want)
	}
}

func TestClassifyTreatsAnUnknownConflictAsASkip(t *testing.T) {
	if d := classifyWorkErr(&workAPIError{Status: 409, Code: "something_else"}); d.act != actSkip {
		t.Fatalf("got %+v", d)
	}
}

func TestWorkerCapsAServerSuppliedIdleWait(t *testing.T) {
	app := newFakeApp(t)
	app.onClaim = func(int, int) (int, http.Header, string) { return ok(`{"leases":[],"retry_after_s":1000000000}`) }
	r := newWorkerRun(t, app, workerOptions{MaxPages: 1})
	r.w.idleLimit = 2
	if err := r.run(t); err != nil {
		t.Fatal(err)
	}
	if got := r.sleeps.slept(); len(got) != 1 || got[0] != idleWaitCap {
		t.Fatalf("slept %v", got)
	}
}

func TestWorkerCountsOnlyConsecutiveEmptyClaimsTowardsTheIdleLimit(t *testing.T) {
	app := newFakeApp(t)
	app.onClaim = func(n, _ int) (int, http.Header, string) {
		if n%3 == 0 {
			return ok(leasesJSON(ev, "/plain.html"))
		}
		return ok(emptyPool)
	}
	app.onSubmit = func(int, map[string]any) (int, http.Header, string) { return ok(verdictWaiting) }
	r := newWorkerRun(t, app, workerOptions{MaxPages: 3, Concurrency: 1})
	r.w.idleLimit = 3
	if err := r.run(t); err != nil {
		t.Fatal(err)
	}
	if s := r.w.summary(); s.Claimed != 3 || s.StopReason != "max_pages" {
		t.Fatalf("%+v", s)
	}
}
