package server

import (
	"net/http"
	"testing"
	"time"
)

func TestIPLimiterAllowsUpToLimit(t *testing.T) {
	l := newIPLimiter(3, time.Minute)
	for i := 0; i < 3; i++ {
		if !l.Allow("1.2.3.4") {
			t.Errorf("hit %d should be allowed", i+1)
		}
	}
	if l.Allow("1.2.3.4") {
		t.Errorf("4th hit must be denied (limit=3)")
	}
}

func TestIPLimiterSeparateIPs(t *testing.T) {
	l := newIPLimiter(1, time.Minute)
	if !l.Allow("1.1.1.1") {
		t.Errorf("first hit from A allowed")
	}
	if l.Allow("1.1.1.1") {
		t.Errorf("second hit from A denied (limit=1)")
	}
	if !l.Allow("2.2.2.2") {
		t.Errorf("first hit from B independent of A")
	}
}

func TestIPLimiterWindowSlides(t *testing.T) {
	l := newIPLimiter(1, 30*time.Millisecond)
	if !l.Allow("x") {
		t.Errorf("first hit")
	}
	if l.Allow("x") {
		t.Errorf("denied within window")
	}
	time.Sleep(50 * time.Millisecond)
	if !l.Allow("x") {
		t.Errorf("allowed after window slid past previous hit")
	}
}

func TestClientIPStripsPort(t *testing.T) {
	r := &http.Request{RemoteAddr: "203.0.113.5:54321"}
	if got := clientIP(r); got != "203.0.113.5" {
		t.Errorf("got %q want 203.0.113.5", got)
	}
}

func TestClientIPIPv6(t *testing.T) {
	r := &http.Request{RemoteAddr: "[2001:db8::1]:443"}
	if got := clientIP(r); got != "2001:db8::1" {
		t.Errorf("got %q want 2001:db8::1", got)
	}
}
