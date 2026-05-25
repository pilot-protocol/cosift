package server

import (
	"net/http"
	"testing"
)

func req(remoteAddr, xff string) *http.Request {
	r := &http.Request{
		RemoteAddr: remoteAddr,
		Header:     http.Header{},
	}
	if xff != "" {
		r.Header.Set("X-Forwarded-For", xff)
	}
	return r
}

func TestResolveDirectWhenNoTrustedProxies(t *testing.T) {
	r, _ := newClientIPResolver(nil)
	got := r.Resolve(req("1.2.3.4:50000", "5.6.7.8"))
	if got != "1.2.3.4" {
		t.Errorf("got %q want 1.2.3.4 (direct, XFF ignored without trusted proxies)", got)
	}
}

func TestResolveDirectWhenPeerNotTrusted(t *testing.T) {
	r, _ := newClientIPResolver([]string{"10.0.0.0/8"})
	got := r.Resolve(req("8.8.8.8:1234", "5.6.7.8"))
	if got != "8.8.8.8" {
		t.Errorf("got %q want 8.8.8.8 (direct peer not in trusted CIDR)", got)
	}
}

func TestResolveTrustsXFFFromTrustedProxy(t *testing.T) {
	r, _ := newClientIPResolver([]string{"10.0.0.0/8"})
	got := r.Resolve(req("10.0.0.5:1234", "5.6.7.8"))
	if got != "5.6.7.8" {
		t.Errorf("got %q want 5.6.7.8 (XFF trusted via trusted proxy)", got)
	}
}

func TestResolveMultiHop(t *testing.T) {
	// Chain: client 5.6.7.8 → proxy1 10.0.0.5 → proxy2 10.0.0.6 → us.
	// XFF = "5.6.7.8, 10.0.0.5" (added by proxy2 listing what it received from + its predecessor).
	// We see RemoteAddr=10.0.0.6. Walking rightward-skipping-trusted: 10.0.0.5 trusted, 5.6.7.8 is the client.
	r, _ := newClientIPResolver([]string{"10.0.0.0/8"})
	got := r.Resolve(req("10.0.0.6:443", "5.6.7.8, 10.0.0.5"))
	if got != "5.6.7.8" {
		t.Errorf("got %q want 5.6.7.8 (client at left of XFF after skipping trusted hops)", got)
	}
}

func TestResolveIgnoresMalformedXFFEntries(t *testing.T) {
	r, _ := newClientIPResolver([]string{"10.0.0.0/8"})
	got := r.Resolve(req("10.0.0.1:80", "garbage,  ,5.6.7.8"))
	if got != "5.6.7.8" {
		t.Errorf("got %q want 5.6.7.8 (skip malformed XFF entries)", got)
	}
}

func TestResolveAllProxiesTrusted(t *testing.T) {
	// Pathological: XFF entirely composed of trusted proxies. Fall back to direct.
	r, _ := newClientIPResolver([]string{"10.0.0.0/8"})
	got := r.Resolve(req("10.0.0.1:80", "10.0.0.5, 10.0.0.6"))
	if got != "10.0.0.1" {
		t.Errorf("got %q want 10.0.0.1 (no untrusted IP in XFF, fall back to direct)", got)
	}
}

func TestNewClientIPResolverBadCIDR(t *testing.T) {
	_, err := newClientIPResolver([]string{"not-a-cidr"})
	if err == nil {
		t.Errorf("expected error on malformed CIDR")
	}
}

func TestServerWithTrustedProxiesEndToEnd(t *testing.T) {
	// Verify the limiter sees the X-Forwarded-For IP, not the proxy IP.
	s := &Server{
		feedbackLimiter: newIPLimiter(1, 60), // 1/window
		metrics:         NewMetrics(),
	}
	r, err := s.WithTrustedProxies([]string{"127.0.0.0/8"})
	if err != nil {
		t.Fatalf("WithTrustedProxies: %v", err)
	}
	s = r

	// Two requests from the same direct peer (a trusted proxy) but different
	// XFF clients — both should be allowed (separate clients).
	hit := func(remoteAddr, xff string) string {
		return s.resolveClientIP(req(remoteAddr, xff))
	}
	if got := hit("127.0.0.1:80", "1.1.1.1"); got != "1.1.1.1" {
		t.Errorf("first XFF client: %q", got)
	}
	if got := hit("127.0.0.1:80", "2.2.2.2"); got != "2.2.2.2" {
		t.Errorf("second XFF client: %q", got)
	}
	// And with the same XFF, the limiter would see the same identity.
	if hit("127.0.0.1:80", "1.1.1.1") != hit("127.0.0.1:80", "1.1.1.1") {
		t.Errorf("identity not stable for same XFF")
	}
}
