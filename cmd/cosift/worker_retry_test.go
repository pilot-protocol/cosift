package main

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestWorkerRetriesAnExpiredLeaseAndGivesUpAfterABoundedNumberOfTries(t *testing.T) {
	for _, code := range []string{"lease_expired", "lease_not_owner"} {
		app := newFakeApp(t)
		app.onClaim = queue(ev, "/plain.html")
		app.onSubmit = func(n int, _ map[string]any) (int, http.Header, string) {
			if n == 1 {
				return 409, nil, `{"code":"` + code + `"}`
			}
			return ok(verdictWaiting)
		}
		r := newWorkerRun(t, app, workerOptions{MaxPages: 1})
		if err := r.run(t); err != nil {
			t.Fatal(err)
		}
		if len(app.submitted()) != 2 || r.w.summary().Submitted != 1 || r.w.summary().LeaseLost != 0 {
			t.Errorf("%s: %d submissions, %+v", code, len(app.submitted()), r.w.summary())
		}
		if s := r.sleeps.slept(); len(s) != 1 || s[0] != time.Second {
			t.Errorf("%s: slept %v", code, s)
		}

		always := newFakeApp(t)
		always.onClaim = queue(ev, "/plain.html", "/german.html")
		always.onSubmit = func(int, map[string]any) (int, http.Header, string) { return 409, nil, `{"code":"` + code + `"}` }
		r = newWorkerRun(t, always, workerOptions{MaxPages: 2, Concurrency: 1})
		if err := r.run(t); err != nil {
			t.Fatalf("%s: a lost lease must not end the run: %v", code, err)
		}
		if len(always.submitted()) != 6 || r.w.summary().LeaseLost != 2 {
			t.Errorf("%s: %d submissions, %+v", code, len(always.submitted()), r.w.summary())
		}
	}
}

func TestWorkerStopsWithUpgradeGuidanceOnAVersionMismatch(t *testing.T) {
	cases := map[string]func(*fakeApp){
		"submit 409": func(a *fakeApp) {
			a.onClaim = queue(ev, "/plain.html", "/german.html")
			a.onSubmit = func(int, map[string]any) (int, http.Header, string) {
				return 409, nil, fmt.Sprintf(`{"code":"extract_version_mismatch","extract_version":%d}`, ev+1)
			}
		},
		"content_hash 422": func(a *fakeApp) {
			a.onClaim = queue(ev, "/plain.html", "/german.html")
			a.onSubmit = func(int, map[string]any) (int, http.Header, string) {
				return 422, nil, `{"code":"invalid_field","field":"content_hash","rule":"mismatch"}`
			}
		},
		"simhash 422": func(a *fakeApp) {
			a.onClaim = queue(ev, "/plain.html", "/german.html")
			a.onSubmit = func(int, map[string]any) (int, http.Header, string) {
				return 422, nil, `{"code":"invalid_field","field":"simhash","rule":"mismatch"}`
			}
		},
	}
	for name, setup := range cases {
		app := newFakeApp(t)
		setup(app)
		r := newWorkerRun(t, app, workerOptions{MaxPages: 10, Concurrency: 1})
		err := r.run(t)
		if err == nil || !strings.Contains(err.Error(), "upgrade cosift") {
			t.Errorf("%s: want upgrade guidance, got %v", name, err)
		}
		if len(app.claimCalls()) != 1 || len(app.submitted()) != 1 {
			t.Errorf("%s: kept going: %d claims, %d submissions", name, len(app.claimCalls()), len(app.submitted()))
		}
		if r.w.summary().StopReason != "upgrade_required" {
			t.Errorf("%s: stop reason %q", name, r.w.summary().StopReason)
		}
	}
}

func TestWorkerRefusesALeaseOfAnotherExtractVersionBeforeFetching(t *testing.T) {
	app := newFakeApp(t)
	app.onClaim = queue(ev+1, "/plain.html")
	app.onSubmit = func(int, map[string]any) (int, http.Header, string) { return ok(verdictWaiting) }
	r := newWorkerRun(t, app, workerOptions{MaxPages: 1})
	err := r.run(t)
	if err == nil || !strings.Contains(err.Error(), "upgrade cosift") {
		t.Fatalf("got %v", err)
	}
	if len(r.hits.seen) != 0 || len(app.submitted()) != 0 {
		t.Fatal("the page was fetched or submitted")
	}
}

func TestWorkerOtherFieldRefusalsSkipTheLeaseAndContinue(t *testing.T) {
	app := newFakeApp(t)
	app.onClaim = queue(ev, "/plain.html", "/german.html")
	app.onSubmit = func(n int, _ map[string]any) (int, http.Header, string) {
		if n == 1 {
			return 422, nil, `{"code":"invalid_field","field":"final_url","rule":"off_host"}`
		}
		return ok(verdictWaiting)
	}
	r := newWorkerRun(t, app, workerOptions{MaxPages: 2, Concurrency: 1})
	if err := r.run(t); err != nil {
		t.Fatal(err)
	}
	if s := r.w.summary(); s.Submitted != 1 || s.Skipped != 1 || len(app.submitted()) != 2 {
		t.Fatalf("%+v", s)
	}
}

func TestWorkerStopsWithAClearMessageOnAccountRefusals(t *testing.T) {
	for _, tc := range []struct{ code, want string }{
		{"not_joined", "contributor network"},
		{"banned", "suspended"},
		{"terms_outdated", "accept the current contributor terms"},
	} {
		for _, onSubmit := range []bool{false, true} {
			app := newFakeApp(t)
			refuse := func() (int, http.Header, string) { return 403, nil, `{"code":"` + tc.code + `"}` }
			app.onClaim = func(int, int) (int, http.Header, string) {
				if !onSubmit {
					return refuse()
				}
				return ok(leasesJSON(ev, "/plain.html"))
			}
			app.onSubmit = func(int, map[string]any) (int, http.Header, string) { return refuse() }
			r := newWorkerRun(t, app, workerOptions{MaxPages: 5})
			err := r.run(t)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("%s (submit=%v): got %v", tc.code, onSubmit, err)
			}
			if len(app.claimCalls()) != 1 || len(app.submitted()) > 1 || len(r.sleeps.slept()) != 0 {
				t.Errorf("%s: retried a refusal", tc.code)
			}
			if r.w.summary().StopReason != tc.code {
				t.Errorf("%s: stop reason %q", tc.code, r.w.summary().StopReason)
			}
		}
	}
}

func TestWorkerStopsOnAnUnauthorizedToken(t *testing.T) {
	app := newFakeApp(t)
	app.onClaim = func(int, int) (int, http.Header, string) { return 401, nil, `{"code":"bearer_required"}` }
	r := newWorkerRun(t, app, workerOptions{MaxPages: 5})
	err := r.run(t)
	if err == nil || !strings.Contains(err.Error(), "bearer") || len(app.claimCalls()) != 1 {
		t.Fatalf("got %v after %d claims", err, len(app.claimCalls()))
	}
}

func TestWorkerStopsAtTheDailyCapAndSaysWhenItResets(t *testing.T) {
	app := newFakeApp(t)
	app.onClaim = func(int, int) (int, http.Header, string) {
		return 429, http.Header{"Retry-After": {"7200"}}, `{"code":"daily_cap","retry_after_seconds":7200}`
	}
	r := newWorkerRun(t, app, workerOptions{MaxPages: 5})
	r.w.now = func() time.Time { return time.Date(2026, 10, 14, 22, 0, 0, 0, time.UTC) }
	if err := r.run(t); err != nil {
		t.Fatalf("a spent daily cap is a normal end: %v", err)
	}
	if !strings.Contains(r.errOut.String(), "2026-10-15T00:00:00Z") || len(app.claimCalls()) != 1 || r.w.summary().StopReason != "daily_cap" {
		t.Fatalf("%q after %d claims, %+v", r.errOut.String(), len(app.claimCalls()), r.w.summary())
	}
}

func TestWorkerBacksOffOnRateLimitsAndUnavailability(t *testing.T) {
	app := newFakeApp(t)
	app.onClaim = func(n, _ int) (int, http.Header, string) {
		switch n {
		case 1:
			return 429, http.Header{"Retry-After": {"7"}}, `{"code":"rate_limited","retry_after_seconds":7}`
		case 2, 3, 4:
			return 503, nil, `{"code":"engine_unavailable"}`
		}
		return ok(emptyPool)
	}
	r := newWorkerRun(t, app, workerOptions{MaxPages: 1})
	r.w.idleLimit = 1
	if err := r.run(t); err != nil {
		t.Fatal(err)
	}
	want := []time.Duration{7 * time.Second, time.Second, 2 * time.Second, 4 * time.Second}
	if got := r.sleeps.slept(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("slept %v, want %v", got, want)
	}
}

func TestWorkerGivesUpAfterBoundedRetries(t *testing.T) {
	for _, status := range []int{429, 503, 500} {
		app := newFakeApp(t)
		app.onClaim = func(int, int) (int, http.Header, string) { return status, nil, `{"code":"rate_limited"}` }
		r := newWorkerRun(t, app, workerOptions{MaxPages: 1})
		err := r.run(t)
		if err == nil || len(app.claimCalls()) != 5 {
			t.Errorf("%d: got %v after %d claims", status, err, len(app.claimCalls()))
		}
	}
	app := newFakeApp(t)
	app.onClaim = queue(ev, "/plain.html")
	app.onSubmit = func(int, map[string]any) (int, http.Header, string) { return 503, nil, `{"code":"engine_unavailable"}` }
	r := newWorkerRun(t, app, workerOptions{MaxPages: 1})
	if err := r.run(t); err != nil {
		t.Fatalf("a submit that stays unavailable abandons only its lease: %v", err)
	}
	if len(app.submitted()) != 5 || r.w.summary().Abandoned != 1 {
		t.Fatalf("%d submissions, %+v", len(app.submitted()), r.w.summary())
	}
}
