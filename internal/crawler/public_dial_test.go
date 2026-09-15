package crawler

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
)

func TestPublicIPRanges(t *testing.T) {
	for _, raw := range []string{"127.0.0.1", "10.0.0.1", "172.16.0.1", "192.168.1.1", "169.254.169.254", "100.64.1.1", "0.0.0.0", "192.0.0.8", "198.18.1.1", "224.0.0.1", "240.0.0.1", "::1", "::ffff:127.0.0.1", "fe80::1", "fc00::1", "64:ff9b::7f00:1", "2002:7f00:1::", "2001:db8::1"} {
		if publicIP(netip.MustParseAddr(raw)) {
			t.Errorf("allowed non-public %s", raw)
		}
	}
	for _, raw := range []string{"8.8.8.8", "1.1.1.1", "2606:4700:4700::1111"} {
		if !publicIP(netip.MustParseAddr(raw)) {
			t.Errorf("denied public %s", raw)
		}
	}
}
func TestPublicDialPinsResolutionAndRejectsMixedAnswers(t *testing.T) {
	calls := 0
	sentinel := errors.New("fake dial")
	d := publicDialer{lookup: func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
	}, dial: func(_ context.Context, network, address string) (net.Conn, error) {
		calls++
		if address != "8.8.8.8:443" {
			t.Errorf("hostname resolved a second time: %s", address)
		}
		return nil, sentinel
	}}
	_, err := d.DialContext(context.Background(), "tcp", "example.com:443")
	if !errors.Is(err, sentinel) || calls != 1 {
		t.Fatalf("dial %v, calls %d", err, calls)
	}
	d.lookup = func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("127.0.0.1")}, nil
	}
	_, err = d.DialContext(context.Background(), "tcp", "rebind.example:443")
	if err == nil || calls != 1 {
		t.Fatal("mixed DNS answers allowed")
	}
	_, err = d.DialContext(context.Background(), "tcp", "example.com:22")
	if err == nil || calls != 1 {
		t.Fatal("non-web port allowed")
	}
}

func TestPublicCrawlerCannotReachLocalServices(t *testing.T) {
	hits := 0
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ }))
	defer local.Close()
	cfg := config.Default().Crawler
	cfg.PublicOnly = true
	// Even an explicitly configured local proxy or remote fetcher must not
	// bypass public-only transport rules.
	cfg.Proxies = []string{local.URL}
	cfg.RemoteFetcherURL = local.URL
	c := New(cfg, newStoreT(t))
	if res, err := c.http.Get(local.URL); err == nil {
		res.Body.Close()
		t.Fatal("local fetch accepted")
	}
	if hits != 0 {
		t.Fatal("private service reached")
	}
}

type publicRedirectTransport struct{ next http.RoundTripper }

func (t publicRedirectTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Host == "redirect.example" {
		return &http.Response{StatusCode: 302, Header: http.Header{"Location": []string{"http://127.0.0.1/private"}}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	}
	return t.next.RoundTrip(r)
}

func TestPublicCrawlerRejectsRedirectToPrivateIP(t *testing.T) {
	cfg := config.Default().Crawler
	cfg.PublicOnly = true
	c := New(cfg, newStoreT(t))
	c.http.Transport = publicRedirectTransport{next: c.http.Transport}
	res, err := c.http.Get("https://redirect.example/")
	if err == nil {
		res.Body.Close()
		t.Fatal("private redirect accepted")
	}
	if !strings.Contains(err.Error(), "non-public address denied") {
		t.Fatalf("wrong failure: %v", err)
	}
}
