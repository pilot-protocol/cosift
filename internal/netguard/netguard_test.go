package netguard

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"
)

func TestAllowedRejectionClasses(t *testing.T) {
	cases := []struct {
		addr  string
		class string
		allow bool
	}{
		{"8.8.8.8", "public v4", true},
		{"93.184.216.34", "public v4", true},
		{"2606:4700::1111", "public v6", true},

		{"127.0.0.1", "loopback", false},
		{"127.255.255.254", "loopback", false},
		{"::1", "loopback", false},
		{"10.1.2.3", "rfc1918", false},
		{"172.16.0.1", "rfc1918", false},
		{"172.31.255.255", "rfc1918", false},
		{"192.168.1.1", "rfc1918", false},
		{"fc00::1", "unique local", false},
		{"fd00::1", "unique local", false},
		{"169.254.169.254", "cloud metadata", false},
		{"169.254.0.1", "link-local", false},
		{"fe80::1", "link-local", false},
		{"224.0.0.1", "multicast", false},
		{"239.255.255.250", "multicast", false},
		{"ff02::1", "multicast", false},
		{"ff01::1", "interface-local multicast", false},
		{"0.0.0.0", "unspecified", false},
		{"::", "unspecified", false},
		{"0.1.2.3", "0.0.0.0/8", false},
		{"240.0.0.1", "class E", false},
		{"255.255.255.255", "broadcast", false},
		{"192.0.0.1", "192.0.0.0/24", false},
		{"192.0.2.5", "test-net-1", false},
		{"198.18.0.1", "benchmarking", false},
		{"198.51.100.5", "test-net-2", false},
		{"203.0.113.5", "test-net-3", false},
		{"100.64.0.1", "cgnat", false},
		{"100.127.255.255", "cgnat", false},
		{"100.128.0.1", "just above cgnat", true},
		{"fec0::1", "site-local", false},

		{"::ffff:127.0.0.1", "v4-mapped loopback", false},
		{"::ffff:8.8.8.8", "v4-mapped public", false},
		{"64:ff9b::7f00:1", "nat64", false},
		{"64:ff9b::808:808", "nat64", false},

		{"2002:7f00:1::", "6to4 over loopback", false},
		{"2002:a9fe:a9fe::", "6to4 over metadata", false},
		{"2002:c0a8:101::", "6to4 over rfc1918", false},
		{"2002:808:808::", "6to4 over public", true},

		{"2001:0:4136:e378:8000:63bf:3fff:fdd2", "teredo, client in test-net-1", false},
		{"2001:0:a00:1:0:0:fefe:fefe", "teredo, private server", false},
		{"2001:0:808:808:0:0:fefe:fefe", "teredo, both public", true},

		{"2001:db8::1", "documentation", false},
		{"2001:2::1", "benchmarking", false},
		{"2001:10::1", "orchid", false},
		{"3fff::1", "documentation", false},
		{"100::1", "discard-only", false},
		{"4000::1", "unallocated", false},
	}
	for _, tc := range cases {
		addr := netip.MustParseAddr(tc.addr)
		if got := Allowed(addr); got != tc.allow {
			t.Errorf("Allowed(%s) [%s] = %v, want %v", tc.addr, tc.class, got, tc.allow)
		}
	}
}

func TestAllowedRejectsZonedAndInvalid(t *testing.T) {
	if Allowed(netip.MustParseAddr("2606:4700::1111").WithZone("eth0")) {
		t.Error("a zoned address must be refused")
	}
	if Allowed(netip.Addr{}) {
		t.Error("the zero Addr must be refused")
	}
}

func TestControl(t *testing.T) {
	if err := Control("tcp", "8.8.8.8:443", nil); err != nil {
		t.Errorf("public address refused: %v", err)
	}
	if err := Control("tcp6", "[2606:4700::1111]:443", nil); err != nil {
		t.Errorf("public v6 address refused: %v", err)
	}
	for _, tc := range []struct{ network, address string }{
		{"tcp", "169.254.169.254:80"},
		{"tcp4", "127.0.0.1:7777"},
		{"tcp6", "[::1]:7777"},
		{"tcp6", "[::ffff:127.0.0.1]:80"},
		{"tcp6", "[64:ff9b::7f00:1]:80"},
		{"udp", "8.8.8.8:53"},
		{"tcp", "not-an-address"},
	} {
		err := Control(tc.network, tc.address, nil)
		if !errors.Is(err, ErrBlocked) {
			t.Errorf("Control(%q, %q) = %v, want ErrBlocked", tc.network, tc.address, err)
		}
	}
}

func TestEnabledEnvOverridesConfig(t *testing.T) {
	cases := []struct {
		env        string
		cfgDefault bool
		want       bool
	}{
		{"", true, true},
		{"", false, false},
		{"1", true, false},
		{"true", true, false},
		{"0", false, true},
		{"false", false, true},
		{"nonsense", true, true},
		{"nonsense", false, false},
	}
	for _, tc := range cases {
		t.Setenv(AllowPrivateEnv, tc.env)
		if got := Enabled(tc.cfgDefault); got != tc.want {
			t.Errorf("Enabled(%v) with %s=%q = %v, want %v", tc.cfgDefault, AllowPrivateEnv, tc.env, got, tc.want)
		}
	}
}

func TestClientRefusesLoopbackUnlessOptedOut(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Setenv(AllowPrivateEnv, "")
	if _, err := Client(5 * time.Second).Get(srv.URL); !errors.Is(err, ErrBlocked) {
		t.Fatalf("guarded Client reached %s: err = %v", srv.URL, err)
	}

	t.Setenv(AllowPrivateEnv, "1")
	resp, err := Client(5 * time.Second).Get(srv.URL)
	if err != nil {
		t.Fatalf("escape hatch did not re-enable loopback: %v", err)
	}
	_ = resp.Body.Close()
}

func TestCheckHost(t *testing.T) {
	ctx := context.Background()
	t.Setenv(AllowPrivateEnv, "")
	refused := []string{
		"127.0.0.1", "::1", "169.254.169.254", "10.0.0.1", "localhost",
		// Spellings a handler will paste into a URL unchanged.
		"[::1]", "[fe80::1]", "127.0.0.1:7777", "10.0.0.1:8080", "localhost:8080",
		"127.0.0.1.", "[::ffff:127.0.0.1]", "::ffff:169.254.169.254",
	}
	for _, h := range refused {
		if err := CheckHost(ctx, h); !errors.Is(err, ErrBlocked) {
			t.Errorf("CheckHost(%q) = %v, want ErrBlocked", h, err)
		}
	}
	for _, h := range []string{"", "8.8.8.8", "2606:4700::1111", "nonexistent.invalid", "8.8.8.8:443", "[2606:4700::1111]:443"} {
		if err := CheckHost(ctx, h); err != nil {
			t.Errorf("CheckHost(%q) = %v, want nil", h, err)
		}
	}
	// Handlers echo this string to unauthenticated callers.
	if err := CheckHost(ctx, "localhost"); err == nil || strings.Contains(err.Error(), "::1") || strings.Contains(err.Error(), "127.0.0.1") {
		t.Errorf("CheckHost leaked the resolved address: %v", err)
	}
	t.Setenv(AllowPrivateEnv, "1")
	if err := CheckHost(ctx, "127.0.0.1"); err != nil {
		t.Errorf("escape hatch should disable CheckHost, got %v", err)
	}
}

func TestVetTargetsRefusesProxiedTarget(t *testing.T) {
	t.Setenv(AllowPrivateEnv, "")
	var reached bool
	rt := VetTargets(roundTripperFunc(func(*http.Request) (*http.Response, error) {
		reached = true
		return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
	}))
	// A proxied transport dials the proxy, so nothing resolves the target.
	client := &http.Client{Transport: rt}
	if _, err := client.Get("http://169.254.169.254/latest/meta-data/"); !errors.Is(err, ErrBlocked) {
		t.Fatalf("proxied metadata fetch = %v, want ErrBlocked", err)
	}
	if reached {
		t.Error("request reached the proxy transport")
	}
	resp, err := client.Get("http://8.8.8.8/")
	if err != nil {
		t.Fatalf("public target refused: %v", err)
	}
	_ = resp.Body.Close()
	if !reached {
		t.Error("public target never reached the proxy transport")
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestProtectHonoursConfigAndEnv(t *testing.T) {
	t.Setenv(AllowPrivateEnv, "")
	if Protect(&http.Transport{}, false).DialContext != nil {
		t.Error("config false should leave the transport unguarded")
	}
	if Protect(&http.Transport{}, true).DialContext == nil {
		t.Error("config true should install the guarded dialer")
	}
	t.Setenv(AllowPrivateEnv, "0")
	if Protect(&http.Transport{}, false).DialContext == nil {
		t.Error("env must be able to force the guard on over config false")
	}
}
