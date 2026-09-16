package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/netguard"
)

func TestHandleRecrawlSitemapRefusesInternalURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`<urlset><url><loc>https://x.example/a</loc></url></urlset>`))
	}))
	defer srv.Close()

	var seen []string
	s := &pebbleHTTP{crawlRecrawl: func(_ context.Context, u string) error {
		seen = append(seen, u)
		return nil
	}}
	post := func() *httptest.ResponseRecorder {
		seen = nil
		rec := httptest.NewRecorder()
		s.handleRecrawlSitemap(rec, httptest.NewRequest(http.MethodPost, "/admin/recrawl-sitemap",
			strings.NewReader(`{"url":"`+srv.URL+`/sitemap.xml"}`)))
		return rec
	}

	t.Setenv(netguard.AllowPrivateEnv, "0")
	rec := post()
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("guarded: status = %d, want 502 (body=%s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "netguard") {
		t.Errorf("guarded: body should name the guard, got %s", rec.Body.String())
	}
	if len(seen) != 0 {
		t.Errorf("guarded: handler still parsed the sitemap: %v", seen)
	}

	t.Setenv(netguard.AllowPrivateEnv, "1")
	rec = post()
	if rec.Code != http.StatusOK {
		t.Fatalf("hatched: status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}
	if len(seen) != 1 || seen[0] != "https://x.example/a" {
		t.Errorf("hatched: recrawled %v, want the one <loc> entry", seen)
	}
}

// site-submit buries per-sitemap failures in a 200, so only CheckHost shows.
func TestHandleSiteSubmitRefusesInternalHost(t *testing.T) {
	var seeded int
	s := &pebbleHTTP{crawlSeedSitemapLane: func(context.Context, string, byte) (int, error) {
		seeded++
		return 0, nil
	}}
	post := func(host string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		s.handleSiteSubmit(rec, httptest.NewRequest(http.MethodPost, "/admin/site-submit",
			strings.NewReader(`{"host":"`+host+`"}`)))
		return rec
	}

	t.Setenv(netguard.AllowPrivateEnv, "0")
	// Every spelling normalizeBareHost lets through must be refused.
	for _, host := range []string{"localhost", "localhost:8080", "127.0.0.1:8080", "[::1]", "127.0.0.1."} {
		seeded = 0
		rec := post(host)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400 (body=%s)", host, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "netguard") {
			t.Errorf("%s: body should name the guard, got %s", host, rec.Body.String())
		}
		if seeded != 0 {
			t.Errorf("%s: handler still seeded %d sitemaps", host, seeded)
		}
	}
	if body := post("localhost").Body.String(); strings.Contains(body, "::1") || strings.Contains(body, "127.0.0.1") {
		t.Errorf("refusal leaked the resolved address to the caller: %s", body)
	}

	// An unresolvable host must still reach the fetch path.
	if rec := post("nonexistent.invalid"); rec.Code != http.StatusOK {
		t.Errorf("nonexistent.invalid: status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}
}

func TestHandleSitePackRefusesInternalHost(t *testing.T) {
	var seeded int
	s := &pebbleHTTP{
		crawlSeedSitemap: func(context.Context, string) (int, error) { seeded++; return 0, nil },
		crawlSeedRSS:     func(context.Context, string) (int, error) { seeded++; return 0, nil },
	}
	t.Setenv(netguard.AllowPrivateEnv, "0")
	rec := httptest.NewRecorder()
	s.handleSitePack(rec, httptest.NewRequest(http.MethodPost, "/admin/site-pack",
		strings.NewReader(`{"host":"localhost"}`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body=%s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "netguard") {
		t.Errorf("body should name the guard, got %s", rec.Body.String())
	}
	if seeded != 0 {
		t.Errorf("handler still seeded %d resources", seeded)
	}
}

func TestHandleWETImportBulkRefusesInternalManifest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("crawl-data/CC-MAIN/wet/x.warc.wet.gz\n"))
	}))
	defer srv.Close()

	var imported int
	s := &pebbleHTTP{crawlSeedWET: func(context.Context, string, bool, bool) (int, error) {
		imported++
		return 0, nil
	}}
	t.Setenv(netguard.AllowPrivateEnv, "0")
	rec := httptest.NewRecorder()
	s.handleWETImportBulk(rec, httptest.NewRequest(http.MethodPost, "/admin/wet-import-bulk",
		strings.NewReader(`{"manifest_url":"`+srv.URL+`/wet.paths.gz","count":1}`)))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 (body=%s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "netguard") {
		t.Errorf("body should name the guard, got %s", rec.Body.String())
	}
	if imported != 0 {
		t.Errorf("handler still imported %d WET files", imported)
	}
}

// discoverSitemaps builds its own client when the caller passes nil.
func TestDiscoverSitemapsDefaultClientRefusesLoopback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			_, _ = w.Write([]byte("Sitemap: https://internal.example/secret-sitemap.xml\n"))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	t.Setenv(netguard.AllowPrivateEnv, "0")
	sitemaps, fromRobots := discoverSitemaps(context.Background(), nil, srv.URL)
	if fromRobots {
		t.Errorf("robots.txt on loopback was fetched: %v", sitemaps)
	}
	for _, s := range sitemaps {
		if strings.Contains(s, "secret-sitemap") {
			t.Errorf("robots.txt on loopback was fetched: %v", sitemaps)
		}
	}
}

func TestCheckRobotsRefusesInternalURL(t *testing.T) {
	srv := robotsTestServer(t)
	defer srv.Close()

	cfg := &config.Config{Crawler: config.Crawler{UserAgent: "TestBot/1.0"}}
	t.Setenv(netguard.AllowPrivateEnv, "0")
	out := captureStdoutCosift(t, func() {
		if err := runCheckRobots(context.Background(), cfg, []string{srv.URL + "/blog/post-1"}); err != nil {
			t.Errorf("runCheckRobots: %v", err)
		}
	})
	if !strings.Contains(out, "netguard") {
		t.Errorf("check-robots reached the loopback server: %s", out)
	}
}
