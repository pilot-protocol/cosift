package crawler

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/netguard"
)

func loopbackServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("<html><body>ok</body></html>"))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestCrawlerRefusesLoopbackByDefault(t *testing.T) {
	t.Setenv(netguard.AllowPrivateEnv, "")
	srv := loopbackServer(t)

	c := newBare(config.Default().Crawler)
	_, err := c.http.Get(srv.URL)
	if !errors.Is(err, netguard.ErrBlocked) {
		t.Fatalf("crawler reached %s: err = %v", srv.URL, err)
	}
}

func TestCrawlerEscapeHatchReEnablesLoopback(t *testing.T) {
	t.Setenv(netguard.AllowPrivateEnv, "1")
	srv := loopbackServer(t)

	c := newBare(config.Default().Crawler)
	resp, err := c.http.Get(srv.URL)
	if err != nil {
		t.Fatalf("escape hatch did not re-enable loopback: %v", err)
	}
	_ = resp.Body.Close()
}

// FetchOne with a nil client backs /contents' fetch of a caller-supplied URL.
func TestFetchOneDefaultClientRefusesLoopback(t *testing.T) {
	t.Setenv(netguard.AllowPrivateEnv, "")
	srv := loopbackServer(t)

	_, err := FetchOne(context.Background(), nil, "TestBot/1.0", srv.URL, 0)
	if !errors.Is(err, netguard.ErrBlocked) {
		t.Fatalf("FetchOne reached %s: err = %v", srv.URL, err)
	}
}

// Only the direct bypasses leave from this box, so that is the path to pin.
func TestRemoteFetcherDirectPathIsGuarded(t *testing.T) {
	t.Setenv(netguard.AllowPrivateEnv, "0")
	srv := loopbackServer(t)
	host := strings.TrimPrefix(srv.URL, "http://")
	t.Setenv("COSIFT_DIRECT_HOSTS", host)

	cfg := config.Default().Crawler
	cfg.RemoteFetcherURLs = []string{"https://worker.example/fetch"}
	c := newBare(cfg)

	_, err := c.http.Get(srv.URL)
	if !errors.Is(err, netguard.ErrBlocked) {
		t.Fatalf("direct-host GET reached %s: err = %v", srv.URL, err)
	}
	if strings.Contains(err.Error(), "remote-fetcher") {
		t.Fatalf("request went to the worker, not the direct path: %v", err)
	}
}

// Drives the crawler's own client; only the loopback origin is exempted.
func TestRedirectToLinkLocalIsRefusedAtDial(t *testing.T) {
	t.Setenv(netguard.AllowPrivateEnv, "")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://169.254.169.254/latest/meta-data/", http.StatusFound)
	}))
	defer srv.Close()
	origin := strings.TrimPrefix(srv.URL, "http://")

	c := newBare(config.Default().Crawler)
	tr, ok := c.http.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("crawler transport is %T, want *http.Transport", c.http.Transport)
	}
	guarded := tr.DialContext
	if guarded == nil {
		t.Fatal("crawler transport has no guarded dialer")
	}
	plain := &net.Dialer{}
	tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address == origin {
			return plain.DialContext(ctx, network, address)
		}
		return guarded(ctx, network, address)
	}

	resp, err := c.http.Get(srv.URL)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("redirect to 169.254.169.254 was followed")
	}
	if !errors.Is(err, netguard.ErrBlocked) {
		t.Fatalf("redirect hop failed for the wrong reason: %v", err)
	}
	if !strings.Contains(err.Error(), "169.254.169.254") {
		t.Fatalf("refusal names %v, not the redirect target", err)
	}
}

// A proxied transport dials the proxy, so the target never reaches Control.
func TestProxiedCrawlerStillVetsTarget(t *testing.T) {
	t.Setenv(netguard.AllowPrivateEnv, "")
	cfg := config.Default().Crawler
	cfg.Proxies = []string{"http://proxy.example:8080"}
	c := newBare(cfg)

	_, err := c.http.Get("http://169.254.169.254/latest/meta-data/")
	if !errors.Is(err, netguard.ErrBlocked) {
		t.Fatalf("proxied metadata fetch = %v, want ErrBlocked", err)
	}
	if !strings.Contains(err.Error(), "169.254.169.254") {
		t.Fatalf("refusal names %v, not the target", err)
	}
}
