package main

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestWorkerStopsOnAClaimRefusalItDoesNotClassify(t *testing.T) {
	for _, status := range []int{301, 308, 400, 405, 410, 413, 415, 422, 201} {
		app := newFakeApp(t)
		app.onClaim = func(int, int) (int, http.Header, string) {
			return status, http.Header{"Location": {"http://elsewhere.example.com/"}}, `{"code":"invalid_body"}`
		}
		r := newWorkerRun(t, app, workerOptions{MaxPages: 5, Duration: 300 * time.Millisecond})
		err := r.run(t)
		if err == nil || !strings.Contains(err.Error(), "refused the claim") {
			t.Errorf("%d: got %v", status, err)
		}
		if n := len(app.claimCalls()); n != 1 {
			t.Errorf("%d: %d claims", status, n)
		}
		if r.w.summary().StopReason != "claim_refused" {
			t.Errorf("%d: stop reason %q", status, r.w.summary().StopReason)
		}
	}
}
