package crawler

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"time"
)

var nonPublicPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
}

func publicIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	// Excludes IPv6 translation, local and other special-purpose ranges.
	if ip.Is6() && !netip.MustParsePrefix("2000::/3").Contains(ip) {
		return false
	}
	for _, prefix := range nonPublicPrefixes {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}

type publicDialer struct {
	lookup func(context.Context, string, string) ([]netip.Addr, error)
	dial   func(context.Context, string, string) (net.Conn, error)
}

func newPublicDialer() publicDialer {
	d := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	return publicDialer{lookup: net.DefaultResolver.LookupNetIP, dial: d.DialContext}
}

// PublicHTTPClient fetches untrusted contributed webpages without proxy or
// private-network access. Callers may add stricter redirect/content policies.
func PublicHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, Transport: &http.Transport{DialContext: newPublicDialer().DialContext, ForceAttemptHTTP2: true, ResponseHeaderTimeout: 10 * time.Second, IdleConnTimeout: 30 * time.Second, MaxIdleConns: 16, MaxConnsPerHost: 2}, CheckRedirect: func(r *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return fmt.Errorf("too many redirects")
		}
		return nil
	}}
}

// DialContext validates every resolved address and connects to the checked IP
// itself. No second DNS lookup can rebind a public hostname to a private IP.
// net/http invokes this transport for redirects, robots and sitemaps too.
func (d publicDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if port != "80" && port != "443" {
		return nil, fmt.Errorf("public-only crawler: non-web port denied")
	}
	ips, err := d.lookup(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("public-only crawler: no addresses")
	}
	for _, ip := range ips {
		if !publicIP(ip) {
			return nil, fmt.Errorf("public-only crawler: non-public address denied")
		}
	}
	for _, ip := range ips {
		var conn net.Conn
		conn, err = d.dial(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, err
}
