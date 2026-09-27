package wiki

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestConfigRefusals(t *testing.T) {
	logs := &logBuf{}
	base := testConfig(DefaultEngineURL, newClock(), logs)
	cases := []struct {
		name, field string
		edit        func(*Config)
	}{
		{"no key", "COSIFT_WIKI_ENGINE_KEY", func(c *Config) { c.EngineKey = "" }},
		{"short key", "COSIFT_WIKI_ENGINE_KEY", func(c *Config) { c.EngineKey = testKey[:40] }},
		{"lower-case secret", "COSIFT_WIKI_ENGINE_KEY", func(c *Config) { c.EngineKey = strings.ToLower(testKey) }},
		{"key with newline", "COSIFT_WIKI_ENGINE_KEY", func(c *Config) { c.EngineKey = testKey + "\n" }},
		{"no mailto", "COSIFT_REPORT_MAILTO", func(c *Config) { c.ReportMailto = "" }},
		{"display name", "COSIFT_REPORT_MAILTO", func(c *Config) { c.ReportMailto = `Reports <reports@example.org>` }},
		{"quoted local part", "COSIFT_REPORT_MAILTO", func(c *Config) { c.ReportMailto = `"a b"@example.org` }},
		{"mailto header injection", "COSIFT_REPORT_MAILTO", func(c *Config) { c.ReportMailto = "a@example.org?cc=b@example.org" }},
		{"no domain dot", "COSIFT_REPORT_MAILTO", func(c *Config) { c.ReportMailto = "a@localhost" }},
		{"bad gsc", "COSIFT_GSC_VERIFICATION", func(c *Config) { c.GSCVerification = `x"><script>` }},
		{"short gsc", "COSIFT_GSC_VERIFICATION", func(c *Config) { c.GSCVerification = "abc" }},
	}
	for _, u := range []string{"http://10.0.0.1:7779", "http://localhost:7779", "http://cosift.example:7779", "https://127.0.0.1:7779",
		"http://127.0.0.1:7779/v1", "http://user@127.0.0.1:7779", "http://127.0.0.1:7779?x=1", "http://[::ffff:127.0.0.1]:7779",
		"http://0.0.0.0:7779", "http://[fe80::1%25lo]:7779", "http://127.0.0.1:0", "127.0.0.1:7779", "http://192.168.1.2"} {
		cases = append(cases, struct {
			name, field string
			edit        func(*Config)
		}{"engine " + u, "COSIFT_WIKI_ENGINE_URL", func(c *Config) { c.EngineURL = u }})
	}
	for _, tc := range cases {
		cfg := base
		tc.edit(&cfg)
		_, err := New(cfg)
		if err == nil || !strings.Contains(err.Error(), tc.field) {
			t.Fatalf("%s: got %v, want a refusal naming %s", tc.name, err, tc.field)
		}
		if strings.Contains(err.Error(), testKey[20:]) || (cfg.EngineKey != "" && strings.Contains(err.Error(), cfg.EngineKey)) {
			t.Fatalf("%s: the error echoes the key", tc.name)
		}
	}
	for _, u := range []string{"", "http://127.0.0.1:7779", "http://127.9.8.7:1", "http://[::1]:7779", "http://127.0.0.1:7779/"} {
		cfg := base
		cfg.EngineURL = u
		if _, err := New(cfg); err != nil {
			t.Fatalf("engine URL %q refused: %v", u, err)
		}
	}
	dark, err := New(Config{Public: false, EngineKey: "not a key", ReportMailto: "nope", EngineURL: "http://10.0.0.1"})
	if err != nil || dark.on {
		t.Fatalf("switch off must start dark without validating: %v", err)
	}
	if strings.Contains(logs.all(), testKey) {
		t.Fatal("a log line carries the key")
	}
}

func TestEngineRequestsCarryOnlyTheKey(t *testing.T) {
	h := newHarness(t)
	rec := published("rust-async-runtimes", "Rust async runtimes")
	h.v1.list(item(rec))
	h.v1.set("rust-async-runtimes", jsonReply(200, rec))
	h.refresh()
	forwarded := []string{"X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "Forwarded", "X-Real-IP",
		"CF-Connecting-IP", "True-Client-IP", "CF-Ray", "CDN-Loop", "Via", "X-Client-IP"}
	var hdrs []string
	for _, k := range forwarded {
		hdrs = append(hdrs, k, "203.0.113.9")
	}
	hdrs = append(hdrs, "Authorization", "Bearer visitor-token", "Cookie", "cosift_session=abc")
	if got := h.get("/wiki/rust-async-runtimes", hdrs...); got.Code != 200 {
		t.Fatalf("article: %d", got.Code)
	}
	h.get("/wiki/unknown-slug", hdrs...)
	calls := h.v1.allCalls()
	if len(calls) != 3 {
		t.Fatalf("engine calls: %+v", calls)
	}
	for _, c := range calls {
		okPath := strings.HasPrefix(c.path, "/v1/articles/by-slug/") || (c.path == "/v1/articles" && c.query == "order=slug&limit=500")
		if c.method != http.MethodGet || !okPath {
			t.Fatalf("unexpected engine route %s %s?%s", c.method, c.path, c.query)
		}
		if c.header.Get("Authorization") != "Bearer "+testKey {
			t.Fatalf("%s: Authorization %q", c.path, c.header.Get("Authorization"))
		}
		for _, k := range forwarded {
			if _, ok := c.header[http.CanonicalHeaderKey(k)]; ok {
				t.Fatalf("%s carried %s to /v1", c.path, k)
			}
		}
		if c.header.Get("Cookie") != "" {
			t.Fatalf("%s carried the visitor's cookie", c.path)
		}
	}
}

func TestEngineRedirectNotFollowed(t *testing.T) {
	h := newHarness(t)
	h.refresh()
	h.v1.set("moved", reply{status: 302, body: `{}`, location: "/v1/articles/by-slug/elsewhere"})
	before := h.v1.count()
	wantPage(t, h.get("/wiki/moved"), 503, "no-store")
	if h.v1.count() != before+1 {
		t.Fatalf("the redirect was followed: %d calls", h.v1.count()-before)
	}
}

func TestSlugValidationBeforeAnyLookup(t *testing.T) {
	h := newHarness(t)
	h.refresh()
	before := h.v1.count()
	bad := []string{"/wiki/Rust", "/wiki/rust_async", "/wiki/-rust", "/wiki/rust-", "/wiki/rust--async", "/wiki/" + strings.Repeat("a", 81),
		"/wiki/rust/", "/wiki/", "/wiki/a%62c", "/wiki/r%C3%BCst", "/wiki/index", "/wiki/v", "/wiki/sitemap", "/wiki/search",
		"/wiki/api", "/wiki/admin", "/wiki/random", "/wiki/new", "/wiki/index/1", "/wiki/index/02", "/wiki/index/+2", "/wiki/index/two",
		"/wiki/v/unknown", "/wiki/v/dev-docs/1", "/wiki/v/dev-docs/01", "/wiki/v/dev-docs/2/3", "/wiki/a/b", "/sitemaps/wiki-0.xml",
		"/sitemaps/wiki-01.xml", "/sitemaps/wiki-1.xml.gz", "/sitemaps/other.xml", "/sitemaps/wiki-%31.xml"}
	for _, p := range bad {
		rec := h.get(p)
		if rec.Code != 404 || cacheControl(rec) != "no-store" {
			t.Fatalf("%s: %d %q", p, rec.Code, cacheControl(rec))
		}
	}
	if h.v1.count() != before {
		t.Fatalf("invalid paths reached the engine: %+v", h.v1.allCalls()[before:])
	}
	if ok := h.get("/wiki/" + strings.Repeat("a", 80)); ok.Code != 404 || h.v1.count() != before+1 {
		t.Fatal("an 80-byte slug is valid and must be looked up")
	}
}

func TestDispatchOnStatus(t *testing.T) {
	art := published("topic", "Topic")
	cases := []struct {
		name   string
		rep    reply
		status int
	}{
		{"article", jsonReply(200, art), 200},
		{"stub", jsonReply(200, with(stub("topic", "Topic"))), 200},
		{"held", jsonReply(200, with(art, "status", "held")), 503},
		{"rejected", jsonReply(200, with(art, "status", "rejected")), 503},
		{"tombstoned 200", jsonReply(200, with(art, "status", "tombstoned")), 503},
		{"residue", jsonReply(200, map[string]any{"schema": 1, "id": art["id"], "slug": "topic", "status": "tombstoned", "residue": true, "reason_code": "privacy"}), 503},
		{"no status", jsonReply(200, with(art, "status", nil)), 503},
		{"not ai generated", jsonReply(200, with(art, "ai_generated", false)), 503},
		{"bad id", jsonReply(200, with(art, "id", "not-a-ulid")), 503},
		{"slug mismatch", jsonReply(200, with(art, "slug", "other")), 503},
		{"bad tier", jsonReply(200, with(art, "quality_tier", "great")), 503},
		{"bad vertical", jsonReply(200, with(art, "vertical", "sports")), 503},
		{"citation order", jsonReply(200, with(art, "citations", []any{map[string]any{"n": 2, "url": "https://a.example", "title": "t", "host": "a.example", "quote": "q"}})), 503},
		{"read_all full projection", jsonReply(200, with(art, "status", "held", "prelive", false, "aliases", []string{}, "moderation", nil)), 503},
		{"prelive on a published record", jsonReply(200, with(art, "prelive", false)), 503},
		{"prelive null", reply{status: 200, body: strings.TrimSuffix(jsonReply(200, art).body, "}") + `,"prelive":null}`}, 503},
		{"redirect", jsonReply(200, map[string]any{"redirect_slug": "new-topic"}), 301},
		{"redirect with extras", jsonReply(200, map[string]any{"redirect_slug": "new-topic", "status": "published"}), 503},
		{"redirect to itself", jsonReply(200, map[string]any{"redirect_slug": "topic"}), 503},
		{"redirect to a bad slug", jsonReply(200, map[string]any{"redirect_slug": "../admin"}), 503},
		{"redirect to a reserved slug", jsonReply(200, map[string]any{"redirect_slug": "index"}), 503},
		{"not json", reply{status: 200, body: "<html>challenge</html>"}, 503},
		{"html content type", reply{status: 200, body: jsonReply(200, art).body, ctype: "text/html"}, 503},
		{"array", reply{status: 200, body: "[]"}, 503},
		{"type mismatch", jsonReply(200, with(art, "citations", "none")), 503},
		{"404", errReply(404, "not_found"), 404},
		{"404 without code", reply{status: 404, body: "not found", ctype: "text/plain"}, 503},
		{"404 unmatched route", errReply(404, "unmatched"), 503},
		{"410", errReply(410, "gone"), 410},
		{"410 other code", errReply(410, "golive"), 503},
		{"400", errReply(400, "invalid_param"), 503},
		{"401", errReply(401, "unauthenticated"), 503},
		{"403", errReply(403, "missing_scope"), 503},
		{"405", errReply(405, "method_not_allowed"), 503},
		{"415", errReply(415, "unsupported_media_type"), 503},
		{"301", reply{status: 301, body: "{}"}, 503},
		{"429", errReply(429, "rate_limited"), 503},
		{"500", errReply(500, "internal"), 503},
		{"503", errReply(503, "index_unavailable"), 503},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.refresh()
			h.v1.set("topic", tc.rep)
			rec := h.get("/wiki/topic")
			if rec.Code != tc.status {
				t.Fatalf("got %d, want %d:\n%s", rec.Code, tc.status, rec.Body)
			}
			if tc.status == 503 {
				wantPage(t, rec, 503, "no-store")
				if strings.Contains(rec.Body.String(), "<h1>Topic</h1>") || strings.Contains(rec.Body.String(), "A lead about") {
					t.Fatal("a refused record's text reached the page")
				}
				if _, cached := inspect(h.w, "topic"); cached {
					t.Fatal("a 503 outcome was cached")
				}
				h.get("/wiki/topic")
				if h.v1.slugCalls("topic") != 2 {
					t.Fatal("a 503 outcome was not retried at the engine")
				}
			}
			if tc.status == 301 && rec.Header().Get("Location") != "https://cosift.example/wiki/new-topic" {
				t.Fatalf("Location %q", rec.Header().Get("Location"))
			}
		})
	}
}

func TestEngineTimeoutIs503(t *testing.T) {
	h := newHarness(t)
	h.refresh()
	h.w.engine.slugTimeout = 50 * time.Millisecond
	h.v1.set("slow", reply{status: 200, body: "{}", delay: 300 * time.Millisecond})
	wantPage(t, h.get("/wiki/slow"), 503, "no-store")
	if !strings.Contains(h.logs.all(), "engine_timeout") {
		t.Fatalf("no timeout class logged: %s", h.logs.all())
	}
}

func TestFailureLogIsRateLimited(t *testing.T) {
	h := newHarness(t)
	h.refresh()
	h.v1.set("a", reply{status: 500, body: `{"detail":"secret body text"}`})
	for range 5 {
		h.get("/wiki/a")
	}
	if n := strings.Count(h.logs.all(), "engine_5xx"); n != 1 {
		t.Fatalf("%d log lines in a minute, want 1", n)
	}
	h.clock.Add(61 * time.Second)
	h.get("/wiki/a")
	if n := strings.Count(h.logs.all(), "engine_5xx"); n != 2 {
		t.Fatalf("%d log lines after a minute, want 2", n)
	}
	if strings.Contains(h.logs.all(), "secret body text") || strings.Contains(h.logs.all(), testKey) {
		t.Fatal("the log carries the body or the key")
	}
}

func TestCacheLifetimes(t *testing.T) {
	h := newHarness(t)
	rec := published("rust-async-runtimes", "Rust async runtimes")
	h.v1.list(item(rec))
	h.v1.set("rust-async-runtimes", jsonReply(200, rec))
	h.refresh()
	wantPage(t, h.get("/wiki/rust-async-runtimes"), 200, public(300))
	h.clock.Add(200 * time.Second)
	wantPage(t, h.get("/wiki/rust-async-runtimes"), 200, public(100))
	if n := h.v1.slugCalls("rust-async-runtimes"); n != 1 {
		t.Fatalf("a cache hit called the engine: %d", n)
	}
	h.clock.Add(100 * time.Second)
	wantPage(t, h.get("/wiki/rust-async-runtimes"), 200, public(300))
	if n := h.v1.slugCalls("rust-async-runtimes"); n != 2 {
		t.Fatalf("an expired entry was served: %d calls", n)
	}

	h.v1.set("gone-topic", errReply(410, "gone"))
	wantPage(t, h.get("/wiki/gone-topic"), 410, public(300))
	h.v1.set("moved-topic", jsonReply(200, map[string]any{"redirect_slug": "rust-async-runtimes"}))
	wantPage(t, h.get("/wiki/moved-topic"), 301, public(300))
	h.clock.Add(120 * time.Second)
	wantPage(t, h.get("/wiki/gone-topic"), 410, public(180))
	wantPage(t, h.get("/wiki/moved-topic"), 301, public(180))
}

func TestHubAndSitemapLifetimes(t *testing.T) {
	h := newHarness(t)
	h.v1.list(item(published("a-topic", "A topic")))
	h.refresh()
	wantPage(t, h.get("/wiki"), 200, public(300))
	wantPage(t, h.get("/sitemap.xml"), 200, public(3600))
	wantPage(t, h.get("/robots.txt"), 200, public(3600))
	h.clock.Add(250 * time.Second)
	wantPage(t, h.get("/wiki"), 200, public(50))
	wantPage(t, h.get("/wiki/v/dev-docs"), 200, public(50))
	wantPage(t, h.get("/sitemaps/wiki-1.xml"), 200, public(3350))
	h.clock.Add(50 * time.Second)
	wantPage(t, h.get("/wiki"), 503, "no-store")
	wantPage(t, h.get("/wiki/v/dev-docs"), 503, "no-store")
	wantPage(t, h.get("/sitemap.xml"), 200, public(3300))
	h.clock.Add(3300 * time.Second)
	wantPage(t, h.get("/sitemap.xml"), 503, "no-store")
	wantPage(t, h.get("/sitemaps/wiki-1.xml"), 503, "no-store")
	wantPage(t, h.get("/robots.txt"), 200, public(3600))
}

func TestNotReadyIs503(t *testing.T) {
	h := newHarness(t)
	for _, p := range []string{"/wiki", "/wiki/index/2", "/wiki/v/research", "/sitemap.xml", "/sitemaps/wiki-1.xml"} {
		wantPage(t, h.get(p), 503, "no-store")
	}
	if h.v1.count() != 0 {
		t.Fatal("a hub walked the engine")
	}
}

func TestNegativeEntryLifetime(t *testing.T) {
	h := newHarness(t)
	h.refresh()
	wantPage(t, h.get("/wiki/nothing-here"), 404, public(60))
	h.clock.Add(59 * time.Second)
	wantPage(t, h.get("/wiki/nothing-here"), 404, public(1))
	if n := h.v1.slugCalls("nothing-here"); n != 1 {
		t.Fatalf("calls %d", n)
	}
	h.clock.Add(time.Second)
	if _, live := inspect(h.w, "nothing-here"); live {
		t.Fatal("a negative entry outlived 60 s")
	}
	wantPage(t, h.get("/wiki/nothing-here"), 404, public(60))
	if n := h.v1.slugCalls("nothing-here"); n != 2 {
		t.Fatalf("calls %d", n)
	}
}

func TestAuthFailureCachesNothing(t *testing.T) {
	for _, status := range []int{401, 403} {
		h := newHarness(t)
		rec := published("known", "Known")
		h.v1.list(item(rec))
		h.refresh()
		h.v1.set("known", errReply(status, "unauthenticated"))
		wantPage(t, h.get("/wiki/known"), 503, "no-store")
		if _, cached := inspect(h.w, "known"); cached || negatives(h.w) != 0 {
			t.Fatalf("%d left a cache entry", status)
		}
		if !known(h.w, "known") {
			t.Fatalf("%d removed the slug from the known set", status)
		}
	}
}

func TestUnknownSlugBudget(t *testing.T) {
	h := newHarness(t)
	wantPage(t, h.get("/wiki/first"), 404, public(60))
	wantPage(t, h.get("/wiki/second"), 404, public(60))
	wantPage(t, h.get("/wiki/third"), 503, "no-store")
	h.refresh()
	h.clock.Add(time.Second)
	before := h.v1.count()
	h.get("/wiki/fourth")
	h.get("/wiki/fifth")
	wantPage(t, h.get("/wiki/sixth"), 404, "no-store")
	if h.v1.count() != before+2 {
		t.Fatalf("the budget let %d calls through", h.v1.count()-before)
	}
	if _, cached := inspect(h.w, "sixth"); cached {
		t.Fatal("a budget 404 was cached")
	}
	before = h.v1.count()
	for i := range 3000 {
		h.clock.Add(50 * time.Millisecond)
		h.get(fmt.Sprintf("/wiki/random-%d", i), "RemoteAddr", fmt.Sprintf("198.51.100.%d:1", i%250))
		if n := negatives(h.w); n > 2*60 {
			t.Fatalf("negative cache at %d entries", n)
		}
	}
	if calls := h.v1.count() - before; calls < 2*150-3 || calls > 2*150+3 {
		t.Fatalf("%d engine calls over 150 s of unknown slugs, want 2 a second", calls)
	}
}

func TestKnownSlugSkipsTheBudget(t *testing.T) {
	h := newHarness(t)
	rec := published("known", "Known")
	h.v1.list(item(rec))
	h.v1.set("known", jsonReply(200, rec))
	h.refresh()
	h.get("/wiki/x-1")
	h.get("/wiki/x-2")
	wantPage(t, h.get("/wiki/x-3"), 404, "no-store")
	wantPage(t, h.get("/wiki/known"), 200, public(300))
}

func TestOutOfRangePagesMakeNoCalls(t *testing.T) {
	h := newHarness(t)
	h.v1.list(item(published("a-topic", "A topic")))
	h.refresh()
	before := h.v1.count()
	for _, p := range []string{"/wiki/index/999", "/wiki/index/2", "/sitemaps/wiki-999.xml", "/sitemaps/wiki-2.xml", "/wiki/v/research/2", "/wiki/v/dev-docs/999"} {
		wantPage(t, h.get(p), 404, "no-store")
	}
	if h.v1.count() != before {
		t.Fatal("an out-of-range page called the engine")
	}
}

func TestRefreshFailureKeepsThePreviousSet(t *testing.T) {
	h := newHarness(t)
	old := []map[string]any{item(published("alpha", "Alpha")), item(published("beta", "Beta")), item(published("gamma", "Gamma"))}
	h.v1.list(old...)
	h.refresh()
	h.v1.list(item(published("aardvark", "Aardvark")), item(published("alpha", "Alpha")), item(published("zeta", "Zeta")))
	h.v1.failPage(1, errReply(500, "internal"))
	if err := h.w.Refresh(context.Background()); err == nil {
		t.Fatal("a walk failing on page 2 succeeded")
	}
	if !strings.Contains(h.logs.all(), "WARN article list refresh failed") {
		t.Fatal("no WARN for the failed refresh")
	}
	for _, s := range []string{"alpha", "beta", "gamma"} {
		if !known(h.w, s) {
			t.Fatalf("%s left the known set after a failed refresh", s)
		}
	}
	if known(h.w, "aardvark") {
		t.Fatal("a partial walk was swapped in")
	}
	body := h.get("/wiki").Body.String()
	if !strings.Contains(body, "Gamma") || strings.Contains(body, "Aardvark") {
		t.Fatal("the hub is not the previous complete set")
	}
	h.v1.clearPages()
	h.refresh()
	if known(h.w, "beta") || !known(h.w, "zeta") {
		t.Fatal("the next successful walk was not swapped in")
	}
}

func TestListPreliveRefusesTheWalk(t *testing.T) {
	h := newHarness(t)
	h.v1.list(item(published("alpha", "Alpha")), with(item(published("beta", "Beta")), "prelive", false))
	if err := h.w.Refresh(context.Background()); err == nil || !strings.Contains(h.logs.all(), "engine_overscoped") {
		t.Fatalf("an over-scoped list was accepted: %v", err)
	}
	wantPage(t, h.get("/wiki"), 503, "no-store")
}

func TestListItemsEnterOnlyPublished(t *testing.T) {
	h := newHarness(t)
	h.v1.list(item(published("alpha", "Alpha")), with(item(published("held-one", "Held one")), "status", "held"),
		with(item(published("ok-one", "OK one")), "promoted", false, "quality_tier", "ok"),
		with(item(published("bad-id", "Bad id")), "id", "x"), with(item(published("index", "Index")), "status", "published"))
	h.refresh()
	for s, want := range map[string]bool{"alpha": true, "held-one": false, "ok-one": true, "bad-id": false, "index": false} {
		if known(h.w, s) != want {
			t.Fatalf("%s known = %v", s, !want)
		}
	}
	hub := h.get("/wiki").Body.String()
	sitemap := h.get("/sitemaps/wiki-1.xml").Body.String()
	if !strings.Contains(hub, "Alpha") || strings.Contains(hub, "OK one") || strings.Contains(sitemap, "ok-one") || !strings.Contains(sitemap, "alpha") {
		t.Fatalf("hub/sitemap not promoted-only:\n%s\n%s", hub, sitemap)
	}
}

func TestEngine404And410DropTheSlug(t *testing.T) {
	for _, tc := range []struct {
		rep    reply
		status int
	}{{errReply(410, "gone"), 410}, {errReply(404, "not_found"), 404}} {
		h := newHarness(t)
		rec := published("taken-down", "Taken down title")
		h.v1.list(item(rec), item(published("other", "Other")))
		h.v1.set("taken-down", jsonReply(200, rec))
		h.refresh()
		h.get("/wiki/taken-down")
		if !strings.Contains(h.get("/wiki").Body.String(), "Taken down title") || !strings.Contains(h.get("/sitemaps/wiki-1.xml").Body.String(), "taken-down") {
			t.Fatal("setup: the hub does not list the article")
		}
		h.clock.Add(250 * time.Second)
		h.refresh()
		h.v1.set("taken-down", tc.rep)
		h.clock.Add(51 * time.Second)
		wantPage(t, h.get("/wiki/taken-down"), tc.status, public(map[int]int{410: 300, 404: 60}[tc.status]))
		if known(h.w, "taken-down") {
			t.Fatalf("%d left the slug known", tc.status)
		}
		if strings.Contains(h.get("/wiki").Body.String(), "Taken down title") || strings.Contains(h.get("/sitemaps/wiki-1.xml").Body.String(), "taken-down") {
			t.Fatalf("%d left the slug on the hub or the sitemap", tc.status)
		}
		if e, ok := inspect(h.w, "taken-down"); !ok || e.status != tc.status {
			t.Fatalf("the slug's entry is %+v", e)
		}
	}
}

func TestRemovalDuringAWalkSticks(t *testing.T) {
	h := newHarness(t)
	rec := published("taken-down", "Taken down title")
	h.v1.list(item(rec), item(published("zeta", "Zeta")))
	h.refresh()
	block := make(chan struct{})
	h.v1.failPage(0, reply{status: 200, body: jsonReply(200, map[string]any{"items": []any{item(rec)}, "next_cursor": nil}).body, block: block})
	done := make(chan error)
	h.clock.Add(time.Second)
	go func() { done <- h.w.Refresh(context.Background()) }()
	for h.v1.count() < 2 {
		time.Sleep(time.Millisecond)
	}
	h.clock.Add(time.Second)
	h.v1.set("taken-down", errReply(410, "gone"))
	wantPage(t, h.get("/wiki/taken-down"), 410, public(300))
	close(block)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if known(h.w, "taken-down") || strings.Contains(h.get("/wiki").Body.String(), "Taken down title") {
		t.Fatal("a walk that began before the 410 brought the slug back")
	}
}

func TestSwapEvictsArticlesThatLeftTheList(t *testing.T) {
	h := newHarness(t)
	rec := published("held-later", "Held later")
	h.v1.list(item(rec))
	h.v1.set("held-later", jsonReply(200, rec))
	h.refresh()
	h.get("/wiki/held-later")
	h.v1.list()
	h.clock.Add(10 * time.Second)
	h.refresh()
	if _, ok := inspect(h.w, "held-later"); ok {
		t.Fatal("an article that left the published list is still cached")
	}
}

func TestPerIPLimit(t *testing.T) {
	h := newHarness(t)
	rec := published("cached", "Cached")
	h.v1.list(item(rec))
	h.v1.set("cached", jsonReply(200, rec))
	h.refresh()
	for i := range 3 * ipLimit {
		if got := h.get("/wiki/cached", "RemoteAddr", "192.0.2.1:5"); got.Code != 200 {
			t.Fatalf("cache hit %d refused: %d", i, got.Code)
		}
	}
	for i := range 3 * ipLimit {
		if got := h.get("/wiki", "RemoteAddr", "192.0.2.1:5"); got.Code != 200 {
			t.Fatalf("hub hit %d refused: %d", i, got.Code)
		}
	}
	misses, limited := 0, false
	for i := 0; i < 2*ipLimit && !limited; i++ {
		got := h.get(fmt.Sprintf("/wiki/miss-%d", i), "RemoteAddr", "192.0.2.7:5")
		if limited = got.Code == 429; limited {
			wantPage(t, got, 429, "no-store")
			if ra := got.Header().Get("Retry-After"); ra != "59" {
				t.Fatalf("Retry-After %q, want 59 (a 60 s window opened 1.2 s ago)", ra)
			}
			break
		}
		misses++
		h.clock.Add(10 * time.Millisecond)
	}
	if !limited {
		t.Fatalf("no 429 after %d misses", misses)
	}
	if misses != ipLimit {
		t.Fatalf("%d misses before the limit, want %d", misses, ipLimit)
	}
	if got := h.get("/wiki/miss-x", "RemoteAddr", "192.0.2.8:5"); got.Code == 429 {
		t.Fatal("another IP was limited")
	}
	h.clock.Add(ipWindow)
	if got := h.get("/wiki/miss-y", "RemoteAddr", "192.0.2.7:5"); got.Code == 429 {
		t.Fatal("the window did not reset")
	}
}

func TestIPLimiterEvictsTheOldestWhenFull(t *testing.T) {
	var l ipLimiter
	start := newClock().Now()
	at := func(ms int) time.Time { return start.Add(time.Duration(ms) * time.Millisecond) }
	for i := range ipCap {
		if ok, _ := l.allow(fmt.Sprintf("ip-%d", i), at(i)); !ok {
			t.Fatalf("client %d refused below the cap", i)
		}
	}
	l.allow("ip-7", at(ipCap))
	if ok, _ := l.allow("one-more", at(ipCap+1)); !ok || l.size() != ipCap {
		t.Fatalf("a new client was refused at the cap (size %d)", l.size())
	}
	if _, kept := l.m["ip-0"]; kept {
		t.Fatal("the oldest client was not the one evicted")
	}
	for _, k := range []string{"ip-1", "ip-7", fmt.Sprintf("ip-%d", ipCap-1), "one-more"} {
		if _, kept := l.m[k]; !kept {
			t.Fatalf("%s was evicted", k)
		}
	}
	for i := range ipLimit - 1 {
		if ok, _ := l.allow("ip-1", at(ipCap+2+i)); !ok {
			t.Fatal("a tracked client lost its window")
		}
	}
	now := at(ipCap + 2 + ipLimit)
	if ok, retry := l.allow("ip-1", now); ok || retry != at(1).Add(ipWindow).Sub(now) {
		t.Fatalf("the request past the limit passed, or got retry %v", retry)
	}
	if ok, _ := l.allow("late", at(ipCap+1).Add(ipWindow)); !ok || l.size() != 1 {
		t.Fatalf("expired clients were not swept: %d", l.size())
	}
}

func TestClientKey(t *testing.T) {
	for in, want := range map[string]string{
		"198.51.100.9": "198.51.100.9", "::ffff:198.51.100.9": "198.51.100.9", "2001:db8:1:2::1": "2001:db8:1:2::/64",
		"2001:db8:1:2:ffff:ffff:ffff:ffff": "2001:db8:1:2::/64", "2001:db8:1:3::1": "2001:db8:1:3::/64",
		"fe80::1%eth0": "fe80::/64", "unknown": "unknown",
	} {
		if got := clientKey(in); got != want {
			t.Fatalf("clientKey(%q) = %q, want %q", in, got, want)
		}
	}
}

// The review's flood: rotating addresses must not lock new clients out of cold pages.
func TestIPv6FloodCannotLockOutNewClients(t *testing.T) {
	for _, tc := range []struct {
		name    string
		addr    func(i int) string
		limited bool
	}{
		{"one /64", func(i int) string { return fmt.Sprintf("[2001:db8:1:2::%x]:1", i+1) }, true},
		{"a /64 each inside one /48", func(i int) string { return fmt.Sprintf("[2001:db8:1:%x::1]:1", i+1) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			a, b := published("cold-one", "Cold one"), published("cold-two", "Cold two")
			h.v1.list(item(a), item(b))
			h.v1.set("cold-one", jsonReply(200, a))
			h.v1.set("cold-two", jsonReply(200, b))
			h.refresh()
			refused := 0
			for i := range ipCap {
				h.clock.Add(5 * time.Millisecond)
				if h.get(fmt.Sprintf("/wiki/flood-%d", i), "RemoteAddr", tc.addr(i)).Code == 429 {
					refused++
				}
			}
			if tc.limited && (refused != ipCap-ipLimit || h.w.ips.size() != 1) {
				t.Fatalf("one /64 is not one client: %d refused, %d tracked", refused, h.w.ips.size())
			}
			if !tc.limited && (refused != 0 || h.w.ips.size() != ipCap) {
				t.Fatalf("%d refused, %d tracked", refused, h.w.ips.size())
			}
			for _, c := range []struct{ addr, slug string }{{"198.51.100.9:1", "cold-one"}, {"66.249.66.1:1", "cold-two"}} {
				h.refresh()
				if got := h.get("/wiki/"+c.slug, "RemoteAddr", c.addr); got.Code != 200 {
					t.Fatalf("%s: cold article %d", c.addr, got.Code)
				}
				if got := h.get("/wiki", "RemoteAddr", c.addr); got.Code != 200 {
					t.Fatalf("%s: hub after a swap %d", c.addr, got.Code)
				}
			}
		})
	}
}

// A hub rendered from the old snapshot must not be cached after a racing takedown.
func TestHubRenderedBeforeATakedownIsNotKept(t *testing.T) {
	h := newHarness(t)
	h.v1.list(item(published("taken-down", "Taken down title")), item(published("other", "Other")))
	h.refresh()
	racing := httptest.NewRecorder()
	h.w.fromSnapshot(racing, httptest.NewRequest(http.MethodGet, "/wiki", nil), htmlType, hubBound, func(s *snapshot) []byte {
		h.w.mu.Lock()
		now := h.w.now()
		h.w.store("taken-down", &entry{kind: outGone, status: http.StatusGone, body: h.w.gone, fetched: now, bound: articleBound}, now)
		h.w.mu.Unlock()
		return h.w.renderHub("Articles A–Z", "/wiki", s.promoted, 1)
	})
	if !strings.Contains(racing.Body.String(), "Taken down title") {
		t.Fatal("setup: the racing render does not predate the takedown")
	}
	if strings.Contains(h.get("/wiki").Body.String(), "Taken down title") {
		t.Fatal("a hub rendered before the takedown was cached after it")
	}
}

func TestPageBytesIgnoreRequestData(t *testing.T) {
	rec := published("rust-async-runtimes", "Rust async runtimes")
	fresh := func() *harness {
		h := newHarness(t)
		h.v1.list(item(rec))
		h.v1.set("rust-async-runtimes", jsonReply(200, rec))
		h.v1.set("a-stub", jsonReply(200, stub("a-stub", "A stub")))
		h.refresh()
		return h
	}
	variants := [][]string{
		{"Cookie", "cosift_session=abc; other=1", "Host", "evil.example"},
		nil,
		{"X-Forwarded-Host", "evil.example", "X-Forwarded-Proto", "http", "X-Forwarded-For", "203.0.113.5", "Forwarded", "host=evil.example"},
		{"Authorization", "Bearer x", "Accept-Language", "ro"},
	}
	for _, p := range []string{"/wiki/rust-async-runtimes", "/wiki/a-stub", "/wiki", "/wiki/v/dev-docs", "/sitemap.xml", "/sitemaps/wiki-1.xml", "/robots.txt", "/wiki/nope"} {
		var first string
		for i, v := range variants {
			for _, q := range []string{"", "?utm_source=x&page=2", "?canonical=https://evil.example/"} {
				got := fresh().get(p+q, append(v, "RemoteAddr", fmt.Sprintf("192.0.2.%d:1", i+10))...)
				body := got.Body.String()
				if first == "" {
					first = body
				}
				if body != first {
					t.Fatalf("%s%s bytes differ", p, q)
				}
				if _, ok := got.Header()["Set-Cookie"]; ok {
					t.Fatalf("%s sets a cookie", p)
				}
				if _, ok := got.Header()["Vary"]; ok {
					t.Fatalf("%s varies", p)
				}
				if strings.Contains(body, "evil.example") || strings.Contains(body, "utm_source") {
					t.Fatalf("%s reflects request data", p)
				}
			}
		}
	}
	if !strings.Contains(fresh().get("/wiki/rust-async-runtimes").Body.String(), `<link rel="canonical" href="https://cosift.example/wiki/rust-async-runtimes">`) {
		t.Fatal("the canonical is not the configured public URL")
	}
}

func TestRobots(t *testing.T) {
	h := newHarness(t)
	got := h.get("/robots.txt")
	want := "User-agent: *\nAllow: /\nDisallow: /api/\nSitemap: https://cosift.example/sitemap.xml\n"
	if got.Body.String() != want || got.Header().Get("Content-Type") != "text/plain; charset=utf-8" {
		t.Fatalf("robots.txt:\n%s", got.Body)
	}
	if h.v1.count() != 0 {
		t.Fatal("robots.txt called the engine")
	}
}

func TestSharedFetchForConcurrentMisses(t *testing.T) {
	h := newHarness(t)
	rec := published("popular", "Popular")
	h.v1.list(item(rec))
	block := make(chan struct{})
	h.v1.set("popular", reply{status: 200, body: jsonReply(200, rec).body, block: block})
	h.refresh()
	done := make(chan int, 8)
	for i := range 8 {
		go func() { done <- h.get("/wiki/popular", "RemoteAddr", fmt.Sprintf("192.0.2.%d:1", i+1)).Code }()
	}
	for h.v1.slugCalls("popular") < 1 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	close(block)
	for range 8 {
		if code := <-done; code != 200 {
			t.Fatalf("shared fetch answered %d", code)
		}
	}
	if n := h.v1.slugCalls("popular"); n != 1 {
		t.Fatalf("%d engine calls for concurrent misses", n)
	}
}

func TestRunRefreshCadence(t *testing.T) {
	h := newHarness(t)
	h.w.poll = time.Millisecond
	h.v1.failPage(0, errReply(503, "index_unavailable"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.w.Run(ctx)
	waitFor(t, func() bool { return h.v1.count() >= 1 })
	h.clock.Add(retryEvery - time.Second)
	time.Sleep(20 * time.Millisecond)
	if h.v1.count() != 1 {
		t.Fatal("retried before 15 s")
	}
	h.v1.clearPages()
	h.clock.Add(time.Second)
	waitFor(t, func() bool { return h.w.ready() })
	n := h.v1.count()
	h.clock.Add(refreshEvery - time.Second)
	time.Sleep(20 * time.Millisecond)
	if h.v1.count() != n {
		t.Fatal("refreshed before 240 s once ready")
	}
	h.clock.Add(time.Second)
	waitFor(t, func() bool { return h.v1.count() == n+1 })

	h.v1.failPage(0, errReply(503, "index_unavailable"))
	h.clock.Add(refreshEvery)
	waitFor(t, func() bool { return h.v1.count() == n+2 })
	h.clock.Add(retryEvery - time.Second)
	time.Sleep(20 * time.Millisecond)
	if h.v1.count() != n+2 {
		t.Fatal("retried a failed refresh before 15 s")
	}
	h.v1.clearPages()
	h.v1.list(item(published("after-retry", "After retry")))
	h.clock.Add(time.Second)
	waitFor(t, func() bool { return known(h.w, "after-retry") })
	if h.v1.count() != n+3 {
		t.Fatalf("a failed refresh was not retried after 15 s: %d calls", h.v1.count()-n)
	}
	h.clock.Add(refreshEvery - time.Second)
	time.Sleep(20 * time.Millisecond)
	if h.v1.count() != n+3 {
		t.Fatal("kept the 15 s cadence after a successful retry")
	}
	h.clock.Add(time.Second)
	waitFor(t, func() bool { return h.v1.count() == n+4 })
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(time.Millisecond)
	}
}
