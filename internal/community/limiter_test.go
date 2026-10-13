package community

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

func TestLimiterEnforcesAndResetsTheWindow(t *testing.T) {
	l := newLimiter(10)
	now := time.Unix(1000, 0)
	for i := 0; i < 3; i++ {
		if !l.allow("k", 3, time.Minute, now) {
			t.Fatalf("hit %d refused", i)
		}
	}
	if l.allow("k", 3, time.Minute, now.Add(59*time.Second)) {
		t.Fatal("fourth hit inside the window allowed")
	}
	if !l.allow("k", 3, time.Minute, now.Add(61*time.Second)) {
		t.Fatal("window did not reset")
	}
}

func TestLimiterDropsExpiredKeys(t *testing.T) {
	l := newLimiter(100)
	now := time.Unix(1000, 0)
	for i := 0; i < 50; i++ {
		l.allow(fmt.Sprint("old", i), 1, time.Minute, now)
	}
	l.allow("fresh", 1, time.Minute, now.Add(2*time.Minute))
	if len(l.keys) != 1 || l.order.Len() != 1 {
		t.Fatalf("expired keys kept: %d keys, %d queued", len(l.keys), l.order.Len())
	}
}

func TestLimiterNeverFailsClosedAtCapacity(t *testing.T) {
	l := newLimiter(100)
	now := time.Unix(1000, 0)
	for i := 0; i < 5000; i++ {
		if !l.allow(fmt.Sprint("flood", i), 1, time.Minute, now) {
			t.Fatalf("new key %d refused at capacity", i)
		}
		if len(l.keys) > 100 || l.order.Len() > 100 {
			t.Fatalf("store grew to %d keys", len(l.keys))
		}
	}
	if !l.allow("legitimate", 1, time.Minute, now) {
		t.Fatal("a new key was refused after the flood")
	}
}

func TestLimiterEvictsTheOldestLiveKey(t *testing.T) {
	l := newLimiter(3)
	now := time.Unix(1000, 0)
	for _, k := range []string{"a", "b", "c"} {
		l.allow(k, 1, time.Minute, now)
	}
	l.allow("d", 1, time.Minute, now)
	if _, ok := l.keys["a"]; ok {
		t.Fatal("oldest key survived")
	}
	if l.allow("b", 1, time.Minute, now) {
		t.Fatal("a surviving key lost its count")
	}
}

func floodAnonymous(s *Server, n int) {
	for i := 0; i < n; i++ {
		r := httptest.NewRequest("POST", "/api/auth/start", strings.NewReader(`{}`))
		r.RemoteAddr = fmt.Sprintf("[2001:db8:%x:%x::1]:443", 0x100+i>>16, i&0xffff)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Cosift-Client", "community")
		s.ServeHTTP(httptest.NewRecorder(), r)
	}
}

func TestAnonymousFloodDoesNotLockOutLoginOrMembers(t *testing.T) {
	s := testServer(t, nil)
	s.cfg.Shared = &fakeShared{}
	submit := func(peer string) int {
		return postFrom(s, "/api/submissions", `{"urls":["https://example.com/guide"]}`, sharedTestToken, peer).Code
	}
	for i := 0; i < 20; i++ {
		if c := submit("198.51.100.8:1234"); c != 202 {
			t.Fatalf("submit %d: %d", i, c)
		}
	}
	floodAnonymous(s, 12000)
	if c := postFrom(s, "/api/auth/start", `{"email":"someone@example.com"}`, "", "198.51.100.7:1234").Code; c != 200 {
		t.Fatalf("login start after the flood: %d", c)
	}
	if c := postFrom(s, "/api/auth/verify", `{"request_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","code":"123456"}`, "", "198.51.100.7:1234").Code; c == 429 {
		t.Fatal("verify locked out after the flood")
	}
	if c := submit("198.51.100.8:1234"); c != 429 {
		t.Fatalf("anonymous traffic reset a member's submission window: %d", c)
	}
	if c := submit("198.51.100.9:1234"); c != 202 {
		t.Fatalf("a member from another address was refused: %d", c)
	}
}

func TestAnonymousLimitsAggregateByIPv6Prefix(t *testing.T) {
	s := testServer(t, nil)
	s.cfg.Shared = &fakeShared{}
	start := `{"email":"someone@example.com"}`
	codes := map[int]int{}
	for i := 1; i <= 25; i++ {
		codes[postFrom(s, "/api/auth/start", start, "", fmt.Sprintf("[2001:db8:1:1::%x]:443", i)).Code]++
	}
	if codes[200] != 10 || codes[429] != 15 {
		t.Fatalf("one /64 got %v, want 10 allowed and 15 limited", codes)
	}
	if c := postFrom(s, "/api/auth/start", start, "", "[2001:db8:1:2::1]:443").Code; c != 200 {
		t.Fatalf("another /64 shares the limit: %d", c)
	}
}

func postFrom(s *Server, path, body, bearer, peer string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Cosift-Client", "community")
	if bearer != "" {
		r.Header.Set("Authorization", "Bearer "+bearer)
	}
	r.RemoteAddr = peer
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}

func TestEveryClientNetworkLimitAggregatesByIPv6Prefix(t *testing.T) {
	count := func(n int, call func(i int) int) map[int]int {
		codes := map[int]int{}
		for i := 1; i <= n; i++ {
			codes[call(i)]++
		}
		return codes
	}
	addr := func(i int) string { return fmt.Sprintf("[2001:db8:7:7::%x]:443", i) }
	t.Run("local login", func(t *testing.T) {
		s := testServer(t, nil)
		got := count(40, func(i int) int { return postFrom(s, "/api/login", `{}`, "", addr(i)).Code })
		if got[429] != 10 {
			t.Fatalf("one /64 got %v, want 30 allowed and 10 limited", got)
		}
	})
	t.Run("shared verify", func(t *testing.T) {
		s := testServer(t, nil)
		s.cfg.Shared = &fakeShared{}
		body := `{"request_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","code":"123456"}`
		got := count(30, func(i int) int { return postFrom(s, "/api/auth/verify", body, "", addr(i)).Code })
		if got[429] != 10 {
			t.Fatalf("one /64 got %v, want 20 allowed and 10 limited", got)
		}
	})
	t.Run("shared password", func(t *testing.T) {
		s := testServer(t, nil)
		s.cfg.SharedPasswordEnabled = true
		s.cfg.Shared = &passwordShared{passwordFn: func(context.Context, string, string) (sharedaccount.Issued, error) {
			return sharedaccount.Issued{}, sharedaccount.ErrUnauthorized
		}}
		got := count(30, func(i int) int {
			return postFrom(s, "/api/auth/password", passwordBody("shared@example.com", "password-valid-123"), "", addr(i)).Code
		})
		if got[429] != 20 {
			t.Fatalf("one /64 got %v, want 10 allowed and 20 limited", got)
		}
	})
	t.Run("submissions", func(t *testing.T) {
		s := testServer(t, nil)
		s.cfg.Shared = &fakeShared{}
		got := count(30, func(i int) int {
			return postFrom(s, "/api/submissions", `{"urls":["https://example.com/guide"]}`, sharedTestToken, addr(i)).Code
		})
		if got[429] != 10 {
			t.Fatalf("one /64 got %v, want 20 allowed and 10 limited", got)
		}
	})
}

func TestAnonymousFloodDoesNotResetCheckoutPortalOrSharedToolLimits(t *testing.T) {
	s := testServer(t, nil)
	s.cfg.Shared = &fakeShared{}
	s.cfg.StripeSecretKey, s.cfg.StripeWebhookSecret, s.cfg.AllowTestPayments = "sk_test_fake_for_unit_tests", "whsec_fake_for_unit_tests", true
	s.cfg.StripePortalConfigurationID = "bpc_test_configuration"
	routes := []struct {
		path, body string
		limit      int
	}{
		{"/api/payments/checkout", `{"idempotency_key":"test-checkout-key-0001"}`, 10},
		{"/api/payments/portal", `{}`, 10},
		{"/api/shared", `{}`, 30},
	}
	peer := "198.51.100.8:1234"
	for _, route := range routes {
		for i := 0; i < route.limit; i++ {
			if c := postFrom(s, route.path, route.body, sharedTestToken, peer).Code; c == 429 {
				t.Fatalf("%s limited after %d requests, want %d", route.path, i, route.limit)
			}
		}
	}
	floodAnonymous(s, 12000)
	for _, route := range routes {
		if c := postFrom(s, route.path, route.body, sharedTestToken, peer).Code; c != 429 {
			t.Errorf("%s: an anonymous flood reset the member's limit: %d", route.path, c)
		}
	}
}
