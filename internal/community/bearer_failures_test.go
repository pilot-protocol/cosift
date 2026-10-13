package community

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

func bearerFrom(s *Server, remote, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", "/api/me", nil)
	r.RemoteAddr = remote
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}

func TestFailedBearersAreThrottledPerNetworkAndNeverAnAuthenticatingOne(t *testing.T) {
	logs := captureLog(t)
	s := testServer(t, nil)
	s.cfg.Shared = &fakeShared{}
	bogus := "ck_1_BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
	for i := 0; i < bearerFailuresPerMinute; i++ {
		if w := bearerFrom(s, fmt.Sprintf("10.1.2.%d:5000", i+1), bogus); w.Code != 401 {
			t.Fatalf("failure %d answered %d", i, w.Code)
		}
	}
	w := bearerFrom(s, "10.1.2.200:5000", bogus)
	if w.Code != 429 || w.Header().Get("Retry-After") == "" || !strings.Contains(w.Body.String(), `"rate_limited"`) {
		t.Fatalf("over the budget: %d %s", w.Code, w.Body.String())
	}
	bearerFrom(s, "10.1.2.201:5000", bogus)
	if n := strings.Count(logs.String(), "failed bearers a minute"); n != 1 {
		t.Fatalf("%d summary lines in the minute: %q", n, logs.String())
	}
	if w := bearerFrom(s, "10.1.2.202:5000", sharedTestToken); w.Code != 200 {
		t.Fatalf("a valid token from the throttled network answered %d", w.Code)
	}
	if w := bearerFrom(s, "10.1.3.1:5000", bogus); w.Code != 401 {
		t.Fatalf("another /24 answered %d", w.Code)
	}
	if w := bearerFrom(s, "10.1.2.9:5000", "not-a-token"); w.Code != 429 {
		t.Fatalf("a malformed bearer from the throttled network answered %d", w.Code)
	}
}

func TestAnOutageIsNeverCountedAgainstTheNetworkThatSawIt(t *testing.T) {
	s := testServer(t, nil)
	shared := &fakeShared{err: sharedaccount.ErrUnavailable}
	s.cfg.Shared = shared
	for i := 0; i < 2*bearerFailuresPerMinute; i++ {
		if w := bearerFrom(s, fmt.Sprintf("10.1.2.%d:5000", i+1), sharedTestToken); w.Code != 503 {
			t.Fatalf("request %d answered %d", i, w.Code)
		}
	}
	shared.err = nil
	if w := bearerFrom(s, "10.1.2.250:5000", "ck_1_BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"); w.Code != 401 {
		t.Fatalf("a bad bearer after the outage answered %d", w.Code)
	}
}

func TestFailureNetAggregatesIPv4At24AndIPv6At64(t *testing.T) {
	for in, want := range map[string]string{
		"203.0.113.77": "203.0.113.0/24", "::ffff:203.0.113.77": "203.0.113.0/24",
		"2001:db8:1:2:aaaa:bbbb:cccc:dddd": "2001:db8:1:2::/64", "unknown": "unknown",
	} {
		if got := failureNet(in); got != want {
			t.Errorf("%s: %s, want %s", in, got, want)
		}
	}
}
