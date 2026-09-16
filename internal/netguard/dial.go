package netguard

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// AllowPrivateEnv turns the guard off when truthy and back on when falsy,
// overriding the config field either way.
const AllowPrivateEnv = "COSIFT_ALLOW_PRIVATE_NETWORKS"

// Enabled reports whether dials should be guarded, given the config-file
// setting. AllowPrivateEnv wins when it parses as a bool.
func Enabled(cfgDefault bool) bool {
	if v := os.Getenv(AllowPrivateEnv); v != "" {
		if allow, err := strconv.ParseBool(v); err == nil {
			return !allow
		}
	}
	return cfgDefault
}

// Control vets the resolved ip:port immediately before connect(2).
func Control(network, address string, _ syscall.RawConn) error {
	switch network {
	case "tcp", "tcp4", "tcp6":
	default:
		return fmt.Errorf("%w: network %q", ErrBlocked, network)
	}
	ap, err := netip.ParseAddrPort(address)
	if err != nil {
		return fmt.Errorf("%w: unparsable address %q", ErrBlocked, address)
	}
	if !Allowed(ap.Addr()) {
		return fmt.Errorf("%w: %s", ErrBlocked, ap.Addr())
	}
	return nil
}

// CheckHost refuses a host that parses as, or resolves to, a non-public
// address; an unresolvable host passes so the caller reports the DNS failure.
func CheckHost(ctx context.Context, host string) error {
	if host == "" || !Enabled(true) {
		return nil
	}
	h := bareHost(host)
	if addr, err := netip.ParseAddr(h); err == nil {
		if !Allowed(addr.Unmap()) {
			return fmt.Errorf("%w: %s", ErrBlocked, host)
		}
		return nil
	}
	addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip", h)
	if err != nil {
		return nil
	}
	for _, addr := range addrs {
		// Keep the resolved address out: handlers echo this to the caller.
		if !Allowed(addr.Unmap()) {
			return fmt.Errorf("%w: %s", ErrBlocked, host)
		}
	}
	return nil
}

// bareHost strips a :port, IPv6 brackets and a trailing dot.
func bareHost(host string) string {
	h := strings.TrimSpace(host)
	if hp, _, err := net.SplitHostPort(h); err == nil && hp != "" {
		h = hp
	}
	h = strings.TrimSuffix(strings.TrimPrefix(h, "["), "]")
	return strings.TrimSuffix(h, ".")
}

// VetTargets refuses a request whose target host is non-public, for transports
// that dial a proxy instead of the target and so never see the target address.
func VetTargets(inner http.RoundTripper) http.RoundTripper {
	return targetVetter{inner: inner}
}

type targetVetter struct{ inner http.RoundTripper }

func (t targetVetter) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := CheckHost(req.Context(), req.URL.Host); err != nil {
		return nil, err
	}
	return t.inner.RoundTrip(req)
}

var guardedDialer = &net.Dialer{
	Timeout:   30 * time.Second,
	KeepAlive: 30 * time.Second,
	Control:   Control,
}

// DialContext is a net.Dialer.DialContext that only reaches public addresses.
func DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return guardedDialer.DialContext(ctx, network, address)
}

// Protect installs the guarded dialer on t and returns t.
func Protect(t *http.Transport, cfgDefault bool) *http.Transport {
	if Enabled(cfgDefault) {
		t.DialContext = DialContext
	}
	return t
}

var guardedTransport = func() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DialContext = DialContext
	return t
}()

// Client returns a guarded client sharing one connection pool. For callers
// with no crawler config to consult; only AllowPrivateEnv turns it off.
func Client(timeout time.Duration) *http.Client {
	if !Enabled(true) {
		return &http.Client{Timeout: timeout}
	}
	return &http.Client{Timeout: timeout, Transport: guardedTransport}
}
