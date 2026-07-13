package crawler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
)

func feedCrawlerT(t *testing.T, body string) (*Crawler, string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	cfg := config.Default().Crawler
	cfg.PerHostDelayMs = 0
	cfg.RespectRobots = false
	return New(cfg, newStoreT(t)), srv.URL
}

func TestFetchRSS2ItemLinks(t *testing.T) {
	c, feed := feedCrawlerT(t, `<?xml version="1.0"?>
<rss version="2.0"><channel><title>t</title>
<item><title>a</title><link> https://example.com/a </link></item>
<item><title>b</title><link>https://example.com/b</link></item>
</channel></rss>`)

	urls, err := c.fetchRSS(t.Context(), feed)
	if err != nil {
		t.Fatalf("fetchRSS: %v", err)
	}
	want := []string{"https://example.com/a", "https://example.com/b"}
	if len(urls) != 2 || urls[0] != want[0] || urls[1] != want[1] {
		t.Errorf("got %v want %v", urls, want)
	}
}

func TestFetchRSSAtomPrefersAlternate(t *testing.T) {
	c, feed := feedCrawlerT(t, `<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom"><title>t</title>
<entry>
  <link rel="enclosure" href="https://cdn.example.com/a.mp3"/>
  <link rel="alternate" href="https://example.com/post-1"/>
</entry>
<entry><link href="https://example.com/post-2"/></entry>
</feed>`)

	urls, err := c.fetchRSS(t.Context(), feed)
	if err != nil {
		t.Fatalf("fetchRSS: %v", err)
	}
	want := []string{"https://example.com/post-1", "https://example.com/post-2"}
	if len(urls) != 2 || urls[0] != want[0] || urls[1] != want[1] {
		t.Errorf("got %v want %v", urls, want)
	}
}

func TestFetchRSSNeitherFormat(t *testing.T) {
	c, feed := feedCrawlerT(t, `<html><body>not a feed</body></html>`)

	if _, err := c.fetchRSS(t.Context(), feed); err == nil {
		t.Error("expected error for non-feed body")
	}
}

func TestFetchRSSHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	defer srv.Close()

	cfg := config.Default().Crawler
	c := New(cfg, newStoreT(t))
	if _, err := c.fetchRSS(t.Context(), srv.URL); err == nil {
		t.Error("expected error for HTTP 418 feed")
	}
}
