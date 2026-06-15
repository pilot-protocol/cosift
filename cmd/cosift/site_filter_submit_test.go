package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/store"
)

func TestParseSiteScopes(t *testing.T) {
	cases := []struct {
		in   string
		want []siteScope
	}{
		{"", nil},
		{"pilotprotocol.network", []siteScope{{host: "pilotprotocol.network"}}},
		{"pilotprotocol.network/docs", []siteScope{{host: "pilotprotocol.network", path: "/docs"}}},
		{"https://pilotprotocol.network/docs/", []siteScope{{host: "pilotprotocol.network", path: "/docs"}}},
		{"http://EXAMPLE.com/Blog", []siteScope{{host: "example.com", path: "/Blog"}}},
		{"a.com , b.com/x", []siteScope{{host: "a.com"}, {host: "b.com", path: "/x"}}},
		{"  ,  ", nil},
	}
	for _, c := range cases {
		got := parseSiteScopes(c.in)
		if len(got) != len(c.want) {
			t.Errorf("parseSiteScopes(%q) len = %d, want %d (%v)", c.in, len(got), len(c.want), got)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("parseSiteScopes(%q)[%d] = %+v, want %+v", c.in, i, got[i], c.want[i])
			}
		}
	}
}

func TestMatchesAnySite(t *testing.T) {
	scopes := parseSiteScopes("pilotprotocol.network/docs")
	cases := []struct {
		url  string
		want bool
	}{
		{"https://pilotprotocol.network/docs", true},
		{"https://pilotprotocol.network/docs/", true},
		{"https://pilotprotocol.network/docs/getting-started", true},
		{"https://www.pilotprotocol.network/docs/x", true}, // subdomain matches host suffix
		{"https://pilotprotocol.network/blog", false},      // wrong path
		{"https://pilotprotocol.network/docsearch", false}, // not a segment boundary
		{"https://evil.com/docs", false},                   // wrong host
		{"https://notpilotprotocol.network/docs", false},   // suffix must be on dot boundary
	}
	for _, c := range cases {
		if got := matchesAnySite(c.url, scopes); got != c.want {
			t.Errorf("matchesAnySite(%q) = %v, want %v", c.url, got, c.want)
		}
	}

	// Empty scopes = match everything (no filter).
	if !matchesAnySite("https://anything.example/x", nil) {
		t.Error("nil scopes should match all URLs")
	}

	// Host-only scope matches any path on the host.
	hostOnly := parseSiteScopes("example.com")
	if !matchesAnySite("https://example.com/anything/here", hostOnly) {
		t.Error("host-only scope should match any path")
	}
}

// TestHandleSearchSiteFilter drives the real /search handler against the
// populated fixture (6 docs, all on x.example with distinct paths) to prove
// the `site` param scopes results by host+path end-to-end.
func TestHandleSearchSiteFilter(t *testing.T) {
	t.Setenv("COSIFT_DEFAULT_DECAY_DAYS", "0")
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)

	doSearch := func(query string) searchResponse {
		t.Helper()
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/search?k=10&"+query, nil)
		srv.handleSearch(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
		}
		var resp searchResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return resp
	}

	// Unscoped: "consensus" matches several docs (raft, paxos, distributed).
	all := doSearch("q=consensus")
	if len(all.Hits) < 2 {
		t.Fatalf("baseline: want ≥2 hits for 'consensus', got %d (%+v)", len(all.Hits), all.Hits)
	}

	// Path-scoped to /paxos: only the paxos doc may survive.
	scoped := doSearch("q=consensus&site=x.example/paxos")
	if len(scoped.Hits) == 0 {
		t.Fatal("site=x.example/paxos returned no hits")
	}
	for _, h := range scoped.Hits {
		if !strings.HasPrefix(h.URL, "https://x.example/paxos") {
			t.Errorf("site=x.example/paxos leaked %s", h.URL)
		}
	}
	if len(scoped.Hits) >= len(all.Hits) {
		t.Errorf("path scope did not narrow results: scoped=%d all=%d", len(scoped.Hits), len(all.Hits))
	}

	// Host scope that matches nothing → zero hits.
	none := doSearch("q=consensus&site=nonexistent.example")
	if len(none.Hits) != 0 {
		t.Errorf("site=nonexistent.example should yield 0 hits, got %d", len(none.Hits))
	}

	// Host-only scope (no path) keeps all the host's matches.
	host := doSearch("q=consensus&site=x.example")
	if len(host.Hits) != len(all.Hits) {
		t.Errorf("host-only scope changed result count: %d vs baseline %d", len(host.Hits), len(all.Hits))
	}
}

func TestParseLaneName(t *testing.T) {
	cases := map[string]byte{
		"":           store.LaneSubmitted,
		"priority":   store.LaneSubmitted,
		"submitted":  store.LaneSubmitted,
		"PRIORITY":   store.LaneSubmitted,
		"refresh":    store.LaneRefresh,
		"discovered": store.LaneDiscovered,
		"bulk":       store.LaneBulk,
		"nonsense":   store.LaneSubmitted, // unknown defaults to priority
	}
	for in, want := range cases {
		if got := parseLaneName(in); got != want {
			t.Errorf("parseLaneName(%q) = %d, want %d", in, got, want)
		}
	}
	// laneName round-trips the known lanes.
	for _, lane := range []byte{store.LaneSubmitted, store.LaneRefresh, store.LaneDiscovered, store.LaneBulk} {
		if parseLaneName(laneName(lane)) != lane {
			t.Errorf("laneName/parseLaneName round-trip failed for lane %d (%q)", lane, laneName(lane))
		}
	}
}

func TestNormalizeBareHost(t *testing.T) {
	cases := []struct {
		in   string
		host string
		ok   bool
	}{
		{"example.com", "example.com", true},
		{"https://example.com", "example.com", true},
		{"http://example.com/", "example.com", true},
		{"EXAMPLE.com", "example.com", true},
		{"example.com/docs", "", false}, // path present
		{"", "", false},
		{"  ", "", false},
	}
	for _, c := range cases {
		host, ok := normalizeBareHost(c.in)
		if host != c.host || ok != c.ok {
			t.Errorf("normalizeBareHost(%q) = (%q,%v), want (%q,%v)", c.in, host, ok, c.host, c.ok)
		}
	}
}

func TestDiscoverSitemapsRobots(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			_, _ = w.Write([]byte("User-agent: *\nSitemap: https://x.test/a.xml\nsitemap: https://x.test/b.xml\n"))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	sitemaps, fromRobots := discoverSitemaps(context.Background(), srv.Client(), srv.URL)
	if !fromRobots {
		t.Fatal("expected fromRobots=true")
	}
	if len(sitemaps) != 2 || sitemaps[0] != "https://x.test/a.xml" || sitemaps[1] != "https://x.test/b.xml" {
		t.Errorf("robots sitemaps = %v, want [a.xml b.xml]", sitemaps)
	}
}

func TestDiscoverSitemapsFallback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.NotFound(w, nil) // no robots.txt
	}))
	defer srv.Close()

	sitemaps, fromRobots := discoverSitemaps(context.Background(), srv.Client(), srv.URL)
	if fromRobots {
		t.Fatal("expected fromRobots=false when robots.txt has no Sitemap directives")
	}
	if len(sitemaps) == 0 || sitemaps[0] != srv.URL+"/sitemap.xml" {
		t.Errorf("fallback sitemaps = %v, want canonical list starting with /sitemap.xml", sitemaps)
	}
}

func TestHandleSiteSubmitAuthAndValidation(t *testing.T) {
	// 501 when no in-serve crawler is wired.
	s := &pebbleHTTP{}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/admin/site-submit", strings.NewReader(`{"host":"example.com"}`))
	s.handleSiteSubmit(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Errorf("no crawler: got %d want 501", rec.Code)
	}

	// 400 on missing host.
	s = &pebbleHTTP{crawlSeedSitemapLane: func(context.Context, string, byte) (int, error) { return 0, nil }}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/admin/site-submit", strings.NewReader(`{}`))
	s.handleSiteSubmit(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("missing host: got %d want 400", rec.Code)
	}

	// 400 on host with a path segment.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/admin/site-submit", strings.NewReader(`{"host":"example.com/docs"}`))
	s.handleSiteSubmit(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("host with path: got %d want 400", rec.Code)
	}
}

// TestHandleSiteSubmitLane drives the happy path against an unreachable host
// (.invalid never resolves, so discovery fast-falls to the canonical fallback
// list) and asserts the handler forwards the chosen lane to the seed function
// for every candidate sitemap.
func TestHandleSiteSubmitLane(t *testing.T) {
	var gotLanes []byte
	s := &pebbleHTTP{
		crawlSeedSitemapLane: func(_ context.Context, _ string, lane byte) (int, error) {
			gotLanes = append(gotLanes, lane)
			return 0, nil
		},
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/admin/site-submit",
		strings.NewReader(`{"host":"nonexistent.invalid","lane":"priority"}`))
	s.handleSiteSubmit(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("got %d want 200 (body=%s)", rec.Code, rec.Body.String())
	}
	if len(gotLanes) == 0 {
		t.Fatal("seed func never called")
	}
	for i, l := range gotLanes {
		if l != store.LaneSubmitted {
			t.Errorf("candidate %d: lane %d, want submitted(%d)", i, l, store.LaneSubmitted)
		}
	}
	var resp struct {
		Host string `json:"host"`
		Lane string `json:"lane"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if resp.Host != "nonexistent.invalid" || resp.Lane != "submitted" {
		t.Errorf("resp = %+v, want host=nonexistent.invalid lane=submitted", resp)
	}
}
