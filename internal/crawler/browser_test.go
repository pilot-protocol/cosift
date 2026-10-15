package crawler

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBrowserHeadersAreOneChromeSet(t *testing.T) {
	h := BrowserHeaders()
	for _, name := range []string{"User-Agent", "Accept", "Accept-Language", "Sec-Ch-Ua", "Sec-Ch-Ua-Mobile", "Sec-Ch-Ua-Platform", "Sec-Fetch-Dest", "Sec-Fetch-Mode", "Sec-Fetch-Site", "Sec-Fetch-User", "Upgrade-Insecure-Requests"} {
		if h.Get(name) == "" {
			t.Errorf("header %s missing", name)
		}
	}
	if !strings.Contains(h.Get("User-Agent"), "Chrome/155.") || !strings.Contains(h.Get("Sec-Ch-Ua"), `v="155"`) {
		t.Errorf("version drift: %q %q", h.Get("User-Agent"), h.Get("Sec-Ch-Ua"))
	}
	if h.Get("Accept-Encoding") != "" {
		t.Error("Accept-Encoding must be left to the transport")
	}
	h.Set("User-Agent", "changed")
	if BrowserHeaders().Get("User-Agent") != BrowserUserAgent {
		t.Error("BrowserHeaders shares state between calls")
	}
}

func TestCheckPublicTargetTable(t *testing.T) {
	for raw, ok := range map[string]bool{
		"https://example.com/a":         true,
		"http://news.example.org:80/a":  true,
		"https://Example.com:443/a?b=1": true,
		"ftp://example.com/a":           false,
		"https://user:pw@example.com/a": false,
		"http://127.0.0.1/a":            false,
		"http://8.8.8.8/a":              false,
		"http://[::1]/a":                false,
		"http://10.0.0.1/a":             false,
		"http://intranet.internal/a":    false,
		"http://printer.local/a":        false,
		"http://intranet/a":             false,
		"http://example.com./a":         false,
		"https://example.com:8443/a":    false,
		"http://2130706433/a":           false,
		"mailto:someone@example.com":    false,
	} {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := CheckPublicTarget(u) == nil; got != ok {
			t.Errorf("%s: allowed=%v, want %v", raw, got, ok)
		}
	}
}

type redirectOrigin struct {
	*httptest.Server
	mu    sync.Mutex
	hosts []string
}

func newRedirectOrigin(t *testing.T, next map[string]string) *redirectOrigin {
	t.Helper()
	o := &redirectOrigin{}
	o.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		o.mu.Lock()
		o.hosts = append(o.hosts, r.Host)
		o.mu.Unlock()
		if to, ok := next[r.Host+r.URL.Path]; ok {
			http.Redirect(w, r, to, http.StatusFound)
			return
		}
		w.Write([]byte("landed"))
	}))
	t.Cleanup(o.Close)
	return o
}

func (o *redirectOrigin) saw(host string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, h := range o.hosts {
		if h == host {
			return true
		}
	}
	return false
}

// client sends every hostname to the test origin, so only the redirect policy is under test.
func (o *redirectOrigin) client() *http.Client {
	addr := o.Listener.Addr().String()
	return PublicRedirects(&http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}}})
}

func TestPublicRedirectsFollowAPublicHopAndRefuseTheRest(t *testing.T) {
	o := newRedirectOrigin(t, map[string]string{
		"a.example.com/start":   "http://b.example.com/mid",
		"b.example.com/mid":     "http://c.example.com:80/end",
		"a.example.com/ip":      "http://127.0.0.1/secret",
		"a.example.com/suffix":  "http://intranet.internal/secret",
		"a.example.com/port":    "http://b.example.com:8080/secret",
		"a.example.com/scheme":  "ftp://b.example.com/secret",
		"a.example.com/loop":    "http://a.example.com/loop",
		"a.example.com/viaflip": "http://b.example.com/ip",
		"b.example.com/ip":      "http://[::1]/secret",
	})
	c := o.client()
	res, err := c.Get("http://a.example.com/start")
	if err != nil {
		t.Fatalf("public chain: %v", err)
	}
	res.Body.Close()
	if res.Request.URL.Hostname() != "c.example.com" {
		t.Errorf("landed on %s", res.Request.URL.Host)
	}
	for _, path := range []string{"ip", "suffix", "port", "scheme", "viaflip"} {
		res, err := c.Get("http://a.example.com/" + path)
		if err == nil {
			res.Body.Close()
			t.Errorf("%s: redirect followed", path)
			continue
		}
		if !errors.Is(err, errNonPublicTarget) {
			t.Errorf("%s: %v", path, err)
		}
	}
	for _, h := range []string{"127.0.0.1", "intranet.internal", "b.example.com:8080", "[::1]"} {
		if o.saw(h) {
			t.Errorf("the refused target %s was contacted", h)
		}
	}
	o.mu.Lock()
	o.hosts = nil
	o.mu.Unlock()
	if _, err := c.Get("http://a.example.com/loop"); err == nil || !strings.Contains(err.Error(), "too many redirects") {
		t.Errorf("loop: %v", err)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.hosts) != 5 {
		t.Errorf("the loop made %d requests, want 5", len(o.hosts))
	}
}

func TestBrowserClientDialsThroughThePublicOnlyDialer(t *testing.T) {
	o := newRedirectOrigin(t, nil)
	_, err := BrowserClient(5 * time.Second).Get(o.URL)
	if err == nil || !errors.Is(err, errNonPublicTarget) {
		t.Fatalf("a loopback origin was reachable: %v", err)
	}
	if len(o.hosts) != 0 {
		t.Fatal("the loopback origin received a request")
	}
	req, _ := http.NewRequest("GET", "http://127.0.0.1/x", nil)
	if err := BrowserClient(time.Second).CheckRedirect(req, nil); !errors.Is(err, errNonPublicTarget) {
		t.Fatalf("BrowserClient does not re-check redirect hops: %v", err)
	}
}
