package community

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
)

func guestGet(s *Server, path, xff string) int {
	r := httptest.NewRequest("GET", path, nil)
	r.RemoteAddr = "127.0.0.1:5555"
	r.Header.Set("X-Forwarded-For", xff)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w.Code
}

func TestGuestMeteringSharesOneAllowancePerIPv6Prefix(t *testing.T) {
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{}`) }))
	s.trustedProxies = []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}
	if c := guestGet(s, "/api/research?q=x", "2001:db8:1:2::1"); c != 200 {
		t.Fatalf("first guest research: %d", c)
	}
	for i := 2; i < 12; i++ {
		if c := guestGet(s, "/api/research?q=x", fmt.Sprintf("2001:db8:1:2:%x::%x", i, i)); c != 429 {
			t.Fatalf("address %d in the same /64 got %d, want 429", i, c)
		}
	}
	if c := guestGet(s, "/api/research?q=x", "2001:db8:1:3::1"); c != 200 {
		t.Fatalf("a different /64 must keep its own allowance: %d", c)
	}
	if c := guestGet(s, "/api/research?q=x", "198.51.100.7"); c != 200 {
		t.Fatalf("IPv4 guest: %d", c)
	}
	if c := guestGet(s, "/api/research?q=x", "198.51.100.8"); c != 200 {
		t.Fatalf("IPv4 is metered per address, neighbour got %d", c)
	}
	if c := guestGet(s, "/api/research?q=x", "198.51.100.7"); c != 429 {
		t.Fatalf("repeat IPv4 guest: %d", c)
	}
}

func TestGuestStatusReportsThePrefixAllowance(t *testing.T) {
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{}`) }))
	s.trustedProxies = []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}
	guestGet(s, "/api/search?q=x", "2001:db8:9:9::1")
	r := httptest.NewRequest("GET", "/api/guest", nil)
	r.RemoteAddr = "127.0.0.1:5555"
	r.Header.Set("X-Forwarded-For", "2001:db8:9:9::beef")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	expect(t, w, 200)
	if body := w.Body.String(); !strings.Contains(body, `"available":false`) {
		t.Fatalf("a sibling address must see the spent allowance: %s", body)
	}
}

func TestNetKey(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		same bool
	}{
		{"2001:db8:1:2::1", "2001:db8:1:2:ffff:ffff:ffff:ffff", true},
		{"2001:db8:1:2::1", "2001:db8:1:3::1", false},
		{"fe80::1%eth0", "fe80::2", true},
		{"198.51.100.7", "198.51.100.8", false},
		{"::ffff:198.51.100.7", "198.51.100.7", true},
	} {
		if got := netKey(tc.a) == netKey(tc.b); got != tc.same {
			t.Errorf("netKey(%q)==netKey(%q) is %v, want %v", tc.a, tc.b, got, tc.same)
		}
	}
	if got := netKey("198.51.100.7"); got != "198.51.100.7" {
		t.Errorf("IPv4 key %q", got)
	}
	for _, raw := range []string{"unknown", ""} {
		if got := netKey(raw); got != raw {
			t.Errorf("unparsable %q mapped to %q", raw, got)
		}
	}
}
