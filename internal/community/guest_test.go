package community

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestGuestSharedCooldownAndRestart(t *testing.T) {
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"hits":[]}`) }))
	expect(t, request(t, s, "GET", "/api/search?q=science", nil, nil), 200)
	w := request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, nil)
	expect(t, w, 429)
	if w.Header().Get("Retry-After") == "" || !strings.Contains(w.Body.String(), "retry_at") {
		t.Fatal("guest has no retry guidance")
	}
	cfg := s.cfg
	s.Close()
	reopened, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	expect(t, request(t, reopened, "GET", "/api/search?q=science", nil, nil), 429)
	// Spoofing XFF on an untrusted connection cannot get a fresh quota.
	r := httptest.NewRequest("GET", "/api/search?q=science", nil)
	r.Header.Set("X-Forwarded-For", "8.8.8.8")
	w = httptest.NewRecorder()
	reopened.ServeHTTP(w, r)
	expect(t, w, 429)
	if _, err := reopened.db.Exec(`UPDATE guest_usage SET expires_at=?`, time.Now().Unix()-1); err != nil {
		t.Fatal(err)
	}
	expect(t, request(t, reopened, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, nil), 202)
	expect(t, request(t, reopened, "GET", "/api/search?q=science", nil, nil), 429)
	var count int
	reopened.db.QueryRow(`SELECT count(*) FROM submissions WHERE user_id IS NULL`).Scan(&count)
	if count != 1 {
		t.Fatal("guest contribution missing")
	}
	expect(t, request(t, reopened, "GET", "/api/saved", nil, nil), 401)
	cookie := account(t, reopened, "member@example.com")
	expect(t, request(t, reopened, "GET", "/api/search?q=science", nil, cookie), 200)
	expect(t, request(t, reopened, "GET", "/api/search?q=design", nil, cookie), 200)
}

func TestGuestConcurrentSearchesOnlyOneSucceeds(t *testing.T) {
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"hits":[]}`) }))
	var wg sync.WaitGroup
	codes := make(chan int, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); w := request(t, s, "GET", "/api/search?q=science", nil, nil); codes <- w.Code }()
	}
	wg.Wait()
	close(codes)
	ok := 0
	for code := range codes {
		if code == 200 {
			ok++
		} else if code != 429 {
			t.Errorf("unexpected status %d", code)
		}
	}
	if ok != 1 {
		t.Fatalf("%d concurrent requests succeeded; want 1", ok)
	}
}

func TestGuestFailuresDoNotConsumeAllowance(t *testing.T) {
	s := testServer(t, nil)
	expect(t, request(t, s, "GET", "/api/search?q=", nil, nil), 400)
	expect(t, request(t, s, "GET", "/api/search?q=science", nil, nil), 502)
	expect(t, request(t, s, "GET", "/api/search?q=science", nil, nil), 502)
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"http://localhost/private"}}, nil), 400)
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, nil), 202)
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/second"}}, nil), 429)
}

func TestTrustedProxyClientIP(t *testing.T) {
	s := testServer(t, nil)
	s.trustedProxies = []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32"), netip.MustParsePrefix("10.0.0.0/8")}
	for _, tc := range []struct{ remote, xff, want string }{
		{"127.0.0.1:5000", "1.1.1.1, 8.8.8.8, 10.0.0.2", "8.8.8.8"},
		{"8.8.8.8:5000", "1.1.1.1", "8.8.8.8"},
		{"127.0.0.1:5000", "invalid", "127.0.0.1"},
		{"127.0.0.1:5000", "::ffff:8.8.8.8", "8.8.8.8"},
	} {
		r := httptest.NewRequest("GET", "/api/guest", nil)
		r.RemoteAddr = tc.remote
		r.Header.Set("X-Forwarded-For", tc.xff)
		if got := s.clientIP(r); got != tc.want {
			t.Errorf("clientIP %q with XFF %q = %q want %q", tc.remote, tc.xff, got, tc.want)
		}
	}
}

func TestInvalidSessionDoesNotSilentlyBecomeGuest(t *testing.T) {
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"hits":[]}`) }))
	stale := &http.Cookie{Name: cookieName, Value: "revoked-session"}
	for _, tc := range []struct {
		method, path string
		body         any
	}{
		{"GET", "/api/search?q=science", nil},
		{"POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}},
	} {
		w := request(t, s, tc.method, tc.path, tc.body, stale)
		expect(t, w, 401)
		cookies := w.Result().Cookies()
		if len(cookies) != 1 || cookies[0].Name != cookieName || cookies[0].MaxAge != -1 {
			t.Fatal("stale browser cookie was not cleared")
		}
	}
	// Failed authentication must not use guest allowance or enqueue unowned work.
	expect(t, request(t, s, "GET", "/api/search?q=science", nil, nil), 200)
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM submissions`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("unexpected submission: %d %v", count, err)
	}
}
