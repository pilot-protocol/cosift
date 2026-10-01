package wiki

import (
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/html"
)

var update = flag.Bool("update", false, "rewrite the golden pages")

func find(n *html.Node, match func(*html.Node) bool) []*html.Node {
	var out []*html.Node
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && match(n) {
			out = append(out, n)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return out
}

func tag(name string) func(*html.Node) bool { return func(n *html.Node) bool { return n.Data == name } }

// ancestor finds the nearest matching ancestor of n, or nil.
func ancestor(n *html.Node, match func(*html.Node) bool) *html.Node {
	for p := n.Parent; p != nil; p = p.Parent {
		if p.Type == html.ElementNode && match(p) {
			return p
		}
	}
	return nil
}

func textOf(n *html.Node) string {
	var b strings.Builder
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.TextNode {
			b.WriteString(n.Data)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return b.String()
}

var citeID = regexp.MustCompile(`^cite-[1-9][0-9]*$`)

// structure checks a full page: lang, one h1, landmarks, heading order, link
// rels, scripts, and no style or event-handler attributes.
func structure(t *testing.T, name, page string) *html.Node {
	t.Helper()
	doc, err := html.Parse(strings.NewReader(page))
	if err != nil {
		t.Fatal(err)
	}
	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("%s: "+format+"\n%s", append([]any{name}, append(args, page)...)...)
	}
	if h := find(doc, tag("html")); len(h) != 1 || attr(h[0], "lang") != "en" {
		fail("no html lang")
	}
	if n := len(find(doc, tag("h1"))); n != 1 {
		fail("%d h1 elements", n)
	}
	for _, l := range []string{"header", "nav", "main", "footer"} {
		if len(find(doc, tag(l))) == 0 {
			fail("no <%s> landmark", l)
		}
	}
	if len(find(doc, tag("main"))) != 1 {
		fail("more than one main")
	}
	last := 0
	for _, h := range find(doc, func(n *html.Node) bool {
		return len(n.Data) == 2 && n.Data[0] == 'h' && n.Data[1] >= '1' && n.Data[1] <= '6'
	}) {
		level := int(h.Data[1] - '0')
		if (last == 0 && level != 1) || level > last+1 {
			fail("heading h%d after h%d", level, last)
		}
		last = level
	}
	for _, n := range find(doc, func(*html.Node) bool { return true }) {
		for _, a := range n.Attr {
			if a.Key == "style" || strings.HasPrefix(a.Key, "on") {
				fail("attribute %s on <%s>", a.Key, n.Data)
			}
			if a.Key == "id" && !citeID.MatchString(a.Val) {
				fail("id %q", a.Val)
			}
		}
		switch n.Data {
		case "style", "iframe", "img", "object", "embed", "form", "base":
			fail("element <%s>", n.Data)
		case "script":
			src, typ := attr(n, "src"), attr(n, "type")
			if !(src == "/wiki.js" && n.FirstChild == nil && typ == "") && typ != "application/ld+json" {
				fail("inline or foreign script (src %q, type %q)", src, typ)
			}
		case "a":
			href := attr(n, "href")
			if strings.HasPrefix(href, "http:") || strings.HasPrefix(href, "https:") {
				if !strings.HasPrefix(href, "https://cosift.example/") && attr(n, "rel") != "nofollow noopener" {
					fail("outbound link %q without rel=nofollow noopener", href)
				}
			} else if !strings.HasPrefix(href, "/") && !strings.HasPrefix(href, "#cite-") && !strings.HasPrefix(href, "mailto:") {
				fail("link %q", href)
			}
		}
	}
	return doc
}

func rustArticle() map[string]any {
	body, err := os.ReadFile("testdata/rust-async-runtimes.md")
	if err != nil {
		panic(err)
	}
	rec := published("rust-async-runtimes", "Rust async runtimes")
	rec["id"] = "01J8ZC2Q7W4X9M3K5N6P8R0T2V"
	rec["lead"] = "Rust's async functions compile to state machines that do nothing until an executor polls them. Runtimes supply that executor together with I/O reactors, timers and task scheduling [1]."
	rec["body_md"] = string(body)
	rec["citations"] = []any{
		map[string]any{"n": 1, "url": "https://docs.example.org/async/runtime-tutorial", "title": "Async runtime tutorial", "host": "docs.example.org", "quote": "A runtime drives futures to completion by polling them when they can make progress."},
		map[string]any{"n": 2, "url": "https://docs.example.org/async/executors", "title": "Executors and wakers", "host": "docs.example.org", "quote": "A waker tells the executor that a task is ready to be polled again."},
		map[string]any{"n": 3, "url": "https://blog.example.net/work-stealing", "title": "Work-stealing schedulers", "host": "blog.example.net", "quote": "Idle worker threads steal queued tasks from busy ones."},
	}
	return rec
}

// pages renders one of each page type from the fake engine.
func pages(t *testing.T, h *harness) map[string]string {
	t.Helper()
	rec := rustArticle()
	var items []map[string]any
	for i, title := range []string{"Actor model", "Bloom filters", "Consensus protocols", "Rust async runtimes"} {
		slug := strings.ToLower(strings.ReplaceAll(title, " ", "-"))
		r := with(published(slug, title), "updated_at", fmt.Sprintf("2026-10-0%dT10:00:00Z", i+1))
		if title == "Rust async runtimes" {
			r = rec
		}
		items = append(items, item(r))
	}
	items = append(items, item(with(published("zero-copy-parsing", "Zero-copy parsing"), "vertical", "research")))
	h.v1.list(items...)
	h.v1.set("rust-async-runtimes", jsonReply(200, rec))
	h.v1.set("webassembly-components", jsonReply(200, with(stub("webassembly-components", "WebAssembly components"))))
	h.v1.set("removed-topic", errReply(410, "gone"))
	h.v1.set("renamed-topic", jsonReply(200, map[string]any{"redirect_slug": "rust-async-runtimes"}))
	h.v1.set("broken", errReply(500, "internal"))
	h.w.hubPage = 3
	h.refresh()
	out := map[string]string{}
	for name, path := range map[string]string{
		"article": "/wiki/rust-async-runtimes", "stub": "/wiki/webassembly-components", "hub": "/wiki", "hub-2": "/wiki/index/2",
		"vertical": "/wiki/v/research", "vertical-empty": "/wiki/v/other", "gone": "/wiki/removed-topic", "moved": "/wiki/renamed-topic",
		"not-found": "/wiki/nothing-here", "unavailable": "/wiki/broken", "sitemap-index": "/sitemap.xml", "sitemap": "/sitemaps/wiki-1.xml",
		"robots": "/robots.txt",
	} {
		h.clock.Add(time.Second)
		out[name] = h.get(path, "RemoteAddr", "192.0.2.50:1").Body.String()
	}
	out["too-many"] = string(h.w.tooMany)
	return out
}

func TestPageStructure(t *testing.T) {
	for _, ga := range []string{"", "G-TEST123"} {
		h := newHarness(t, func(c *Config) { c.GAMeasurementID, c.GSCVerification = ga, "gsc-token_0123456789" })
		for name, page := range pages(t, h) {
			if strings.HasPrefix(name, "sitemap") || name == "robots" {
				continue
			}
			doc := structure(t, name, page)
			gsc := find(doc, func(n *html.Node) bool { return n.Data == "meta" && attr(n, "name") == "google-site-verification" })
			if len(gsc) != 1 || attr(gsc[0], "content") != "gsc-token_0123456789" {
				t.Fatalf("%s: no Search Console tag", name)
			}
			scripts := find(doc, func(n *html.Node) bool { return n.Data == "script" && attr(n, "src") == "/wiki.js" })
			if (ga == "") != (len(scripts) == 0) || (ga != "" && attr(scripts[0], "data-ga") != ga) {
				t.Fatalf("%s: GA script present=%d with id %q", name, len(scripts), ga)
			}
		}
	}
}

func TestArticlePage(t *testing.T) {
	h := newHarness(t)
	p := pages(t, h)["article"]
	doc := structure(t, "article", p)
	if got := textOf(find(doc, tag("h1"))[0]); got != "Rust async runtimes" {
		t.Fatalf("h1 %q", got)
	}
	statusLine := find(doc, func(n *html.Node) bool { return n.Data == "p" && attr(n, "class") == "wiki-meta" })
	if len(statusLine) != 1 {
		t.Fatal("no status line under the title")
	}
	label := find(statusLine[0], func(n *html.Node) bool { return n.Data == "span" && attr(n, "class") == "wiki-agent" })
	if len(label) != 1 || textOf(label[0]) != "Agent submitted" || attr(label[0], "title") != "Written by an AI agent from the sources listed below" || label[0].NextSibling != nil {
		t.Fatal("no Agent submitted label at the end of the status line")
	}
	if strings.Contains(p, "wiki-badge") || strings.Contains(p, "AI-generated") {
		t.Fatal("the old top badge is still there")
	}
	if !strings.Contains(p, `<time datetime="2026-10-06T10:00:00Z">Oct 6, 2026</time>`) || !strings.Contains(p, "Developer docs") || !strings.Contains(p, "High quality") {
		t.Fatal("no updated date, vertical or tier")
	}
	lis := find(doc, func(n *html.Node) bool { return n.Data == "li" && strings.HasPrefix(attr(n, "id"), "cite-") })
	src3 := ancestor(lis[2], func(n *html.Node) bool { return n.Data == "li" && attr(n, "class") == "wiki-source" })
	if len(lis) != 3 || attr(lis[2], "id") != "cite-3" || !strings.Contains(textOf(lis[2]), "Idle worker threads steal queued tasks") ||
		src3 == nil || !strings.Contains(textOf(src3), "blog.example.net") {
		t.Fatalf("citations: %d", len(lis))
	}
	src1 := ancestor(lis[0], func(n *html.Node) bool { return n.Data == "li" && attr(n, "class") == "wiki-source" })
	out := find(src1, tag("a"))
	if len(out) != 1 || attr(out[0], "href") != "https://docs.example.org/async/runtime-tutorial" || attr(out[0], "rel") != "nofollow noopener" {
		t.Fatal("citation link")
	}
	report := find(doc, func(n *html.Node) bool { return n.Data == "a" && strings.HasPrefix(attr(n, "href"), "mailto:") })
	if len(report) != 1 || attr(report[0], "href") != "mailto:reports@example.org?subject=Report%3A%20https%3A%2F%2Fcosift.example%2Fwiki%2Frust-async-runtimes" {
		t.Fatalf("report link %v", report)
	}
	if len(find(doc, func(n *html.Node) bool { return n.Data == "a" && attr(n, "href") == "/#agents" })) == 0 {
		t.Fatal("no install call to action")
	}
	meta := func(key, val string) string {
		m := find(doc, func(n *html.Node) bool { return n.Data == "meta" && attr(n, key) == val })
		if len(m) != 1 {
			t.Fatalf("meta %s=%s: %d", key, val, len(m))
		}
		return attr(m[0], "content")
	}
	if d := meta("name", "description"); !strings.HasPrefix(d, "Rust's async functions compile") || len([]rune(d)) > 161 {
		t.Fatalf("description %q", d)
	}
	if meta("property", "og:url") != "https://cosift.example/wiki/rust-async-runtimes" || meta("property", "og:type") != "article" || meta("property", "og:title") != "Rust async runtimes" {
		t.Fatal("OpenGraph")
	}
	markers := find(doc, func(n *html.Node) bool { return n.Data == "a" && strings.HasPrefix(attr(n, "href"), "#cite-") })
	if len(markers) < 3 {
		t.Fatalf("%d citation markers", len(markers))
	}
}

// TestSourceTitleIsTheFirstCited pins: a URL cited under differing titles shows its first-cited title.
func TestSourceTitleIsTheFirstCited(t *testing.T) {
	h := newHarness(t)
	rec := with(published("source-title", "Source title"), "citations", []any{
		map[string]any{"n": 1, "url": "https://a.example/page", "title": "First title", "host": "a.example", "quote": "First quote."},
		map[string]any{"n": 2, "url": "https://b.example/page", "title": "B title", "host": "b.example", "quote": "B quote."},
		map[string]any{"n": 3, "url": "https://a.example/page", "title": "Later title", "host": "a.example", "quote": "Second quote."},
	})
	h.refresh()
	h.v1.set("source-title", jsonReply(200, rec))
	doc := structure(t, "source-title", h.get("/wiki/source-title").Body.String())

	titles := find(doc, func(n *html.Node) bool { return n.Data == "p" && attr(n, "class") == "wiki-cite-title" })
	if len(titles) != 2 || !strings.Contains(textOf(titles[0]), "First title") || strings.Contains(textOf(titles[0]), "Later title") {
		t.Fatalf("source titles: %d, first %q", len(titles), textOf(titles[0]))
	}
}

// TestSourcesGroupedByURL pins: one Sources entry per URL, each quote showing its own [n].
func TestSourcesGroupedByURL(t *testing.T) {
	h := newHarness(t)
	cite := func(n int, src string) map[string]any {
		return map[string]any{"n": n, "url": "https://" + src + ".example/page", "title": strings.ToUpper(src) + " title", "host": src + ".example", "quote": fmt.Sprintf("Quote %d from %s.", n, src)}
	}
	rec := with(published("grouped-sources", "Grouped sources"), "citations", []any{
		cite(1, "a"), cite(2, "b"), cite(3, "a"), cite(4, "c"), cite(5, "a"), cite(6, "b"),
	}, "body_md", "## Overview\n\nA claim [4] and another [5].\n")
	h.refresh()
	h.v1.set("grouped-sources", jsonReply(200, rec))
	doc := structure(t, "grouped-sources", h.get("/wiki/grouped-sources").Body.String())

	section := find(doc, func(n *html.Node) bool { return n.Data == "section" && attr(n, "class") == "wiki-sources" })
	if len(section) != 1 || len(find(section[0], tag("ol"))) != 0 {
		t.Fatal("the Sources list is not unnumbered")
	}
	srcs := find(doc, func(n *html.Node) bool { return n.Data == "li" && attr(n, "class") == "wiki-source" })
	want := []struct {
		src string
		ns  []int
	}{{"a", []int{1, 3, 5}}, {"b", []int{2, 6}}, {"c", []int{4}}}
	if len(srcs) != len(want) {
		t.Fatalf("%d sources, want %d (one per URL)", len(srcs), len(want))
	}
	for i, w := range want {
		titles := find(srcs[i], func(n *html.Node) bool { return n.Data == "p" && attr(n, "class") == "wiki-cite-title" })
		if len(titles) != 1 || textOf(titles[0]) != strings.ToUpper(w.src)+" title "+w.src+".example" {
			t.Fatalf("source %d: %d title lines, text %q", i, len(titles), textOf(srcs[i]))
		}
		quotes := find(srcs[i], func(n *html.Node) bool { return n.Data == "li" && strings.HasPrefix(attr(n, "id"), "cite-") })
		if len(quotes) != len(w.ns) {
			t.Fatalf("source %d: %d quotes, want %d", i, len(quotes), len(w.ns))
		}
		for k, c := range w.ns {
			num := find(quotes[k], func(n *html.Node) bool { return n.Data == "span" && attr(n, "class") == "wiki-cite-n" })
			if attr(quotes[k], "id") != fmt.Sprintf("cite-%d", c) || len(num) != 1 || quotes[k].FirstChild != num[0] ||
				textOf(quotes[k]) != fmt.Sprintf("[%d]Quote %d from %s.", c, c, w.src) {
				t.Fatalf("source %d quote %d: id %q, text %q", i, k, attr(quotes[k], "id"), textOf(quotes[k]))
			}
		}
	}
	markers := find(doc, func(n *html.Node) bool { return n.Data == "a" && strings.HasPrefix(attr(n, "href"), "#cite-") })
	for _, m := range markers {
		target := find(doc, func(n *html.Node) bool { return n.Data == "li" && "#"+attr(n, "id") == attr(m, "href") })
		if len(target) != 1 || !strings.HasPrefix(textOf(target[0]), textOf(m)) {
			t.Fatalf("body marker %s does not land on a quote showing it", textOf(m))
		}
	}
	if len(markers) != 3 {
		t.Fatalf("%d body markers, want 3", len(markers))
	}
	for _, id := range []string{"cite-1", "cite-2", "cite-3", "cite-4", "cite-5", "cite-6"} {
		if len(find(doc, func(n *html.Node) bool { return n.Data == "li" && attr(n, "id") == id })) != 1 {
			t.Fatalf("missing anchor %s", id)
		}
	}
}

// TestQuotesShownVerbatimOnThePage pins: a displayed quote keeps its brackets and loses only stray spacing.
func TestQuotesShownVerbatimOnThePage(t *testing.T) {
	h := newHarness(t)
	quotes := []struct{ in, want string }{
		{"rolled oats. [ 51 ]", "rolled oats. [ 51 ]"},
		{"sys.argv[1] holds the first argument .", "sys.argv[1] holds the first argument."},
		{"The law passed in [2019] changed it [a] .", "The law passed in [2019] changed it [a]."},
		{"[3]", "[3]"},
		{"Built on .NET ( see ./configure ) , too", "Built on .NET (see ./configure), too"},
	}
	var cites []any
	for i, q := range quotes {
		cites = append(cites, map[string]any{"n": i + 1, "url": fmt.Sprintf("https://a.example/%d", i+1), "title": "A title", "host": "a.example", "quote": q.in})
	}
	rec := with(published("quote-cleanup", "Quote cleanup"), "citations", cites)
	h.refresh()
	h.v1.set("quote-cleanup", jsonReply(200, rec))
	doc := structure(t, "quote-cleanup", h.get("/wiki/quote-cleanup").Body.String())

	for i, q := range quotes {
		li := find(doc, func(n *html.Node) bool { return n.Data == "li" && attr(n, "id") == fmt.Sprintf("cite-%d", i+1) })
		if len(li) != 1 {
			t.Fatalf("no cite-%d", i+1)
		}
		if got := textOf(find(li[0], tag("blockquote"))[0]); got != q.want {
			t.Fatalf("quote %q displayed as %q, want %q", q.in, got, q.want)
		}
	}
}

func jsonLD(t *testing.T, doc *html.Node) map[string]any {
	t.Helper()
	blocks := find(doc, func(n *html.Node) bool { return n.Data == "script" && attr(n, "type") == "application/ld+json" })
	if len(blocks) != 1 {
		t.Fatalf("%d JSON-LD blocks", len(blocks))
	}
	var ld map[string]any
	if err := json.Unmarshal([]byte(textOf(blocks[0])), &ld); err != nil {
		t.Fatalf("JSON-LD: %v", err)
	}
	return ld
}

func TestJSONLD(t *testing.T) {
	h := newHarness(t)
	doc := structure(t, "article", pages(t, h)["article"])
	ld := jsonLD(t, doc)
	for k, want := range map[string]string{"@type": "Article", "headline": "Rust async runtimes", "dateModified": "2026-10-06T10:00:00Z",
		"datePublished": "2026-09-29T10:00:00Z", "url": "https://cosift.example/wiki/rust-async-runtimes", "@context": "https://schema.org"} {
		if ld[k] != want {
			t.Fatalf("%s = %v, want %s", k, ld[k], want)
		}
	}
	canon := find(doc, func(n *html.Node) bool { return n.Data == "link" && attr(n, "rel") == "canonical" })
	if len(canon) != 1 || attr(canon[0], "href") != ld["url"] {
		t.Fatal("JSON-LD url is not the canonical")
	}
}

func TestBreakoutAttemptsStayInPlace(t *testing.T) {
	h := newHarness(t)
	evil := `</script><script>alert(1)</script> --> <!--`
	rec := with(published("breakout", "Breakout "+evil), "lead", "Lead "+evil+" [1].",
		"citations", []any{
			map[string]any{"n": 1, "url": "https://docs.example.org/guide", "title": "Title " + evil, "host": "docs.example.org", "quote": "Quote " + evil},
			map[string]any{"n": 2, "url": "https://news.example.com/item?id=7", "title": "Item", "host": "news.example.com", "quote": "Another quote."},
		})
	h.refresh()
	h.v1.set("breakout", jsonReply(200, rec))
	p := h.get("/wiki/breakout").Body.String()
	doc := structure(t, "breakout", p)
	if n := len(find(doc, tag("script"))); n != 1 {
		t.Fatalf("%d script elements", n)
	}
	ld := jsonLD(t, doc)
	if ld["headline"] != "Breakout "+evil || !strings.HasPrefix(ld["description"].(string), "Lead </script>") {
		t.Fatalf("JSON-LD text changed: %v", ld["headline"])
	}
	if title := textOf(find(doc, tag("title"))[0]); title != "Breakout "+evil+" · Cosift" {
		t.Fatalf("title %q", title)
	}
	if got := len(find(find(doc, tag("head"))[0], tag("meta"))); got < 6 {
		t.Fatalf("the head was cut short: %d meta", got)
	}
	for _, want := range []string{"Title " + evil, "Quote " + evil, "Lead " + evil} {
		if !strings.Contains(textOf(find(doc, tag("main"))[0]), want) {
			t.Fatalf("visible text lost %q", want)
		}
	}
}

func TestCorpusInsideThePage(t *testing.T) {
	h := newHarness(t)
	h.refresh()
	for i, tc := range loadCorpus(t) {
		slug := fmt.Sprintf("case-%d", i)
		cites := []any{
			map[string]any{"n": 1, "url": corpusCites[0], "title": "One", "host": "docs.example.org", "quote": "q"},
			map[string]any{"n": 2, "url": corpusCites[1], "title": "Two", "host": "docs.example.org", "quote": "q"},
		}
		h.v1.set(slug, jsonReply(200, with(published(slug, "Case"), "body_md", tc.Text, "citations", cites)))
		h.clock.Add(time.Second)
		rec := h.get("/wiki/"+slug, "RemoteAddr", fmt.Sprintf("192.0.2.%d:1", i%200+1))
		if rec.Code != 200 {
			t.Fatalf("%s: %d", tc.Name, rec.Code)
		}
		doc := structure(t, tc.Name, rec.Body.String())
		body := find(doc, func(n *html.Node) bool { return n.Data == "div" && attr(n, "class") == "wiki-body" })
		if len(body) != 1 {
			t.Fatalf("%s: no body", tc.Name)
		}
		var b strings.Builder
		for c := body[0].FirstChild; c != nil; c = c.NextSibling {
			_ = html.Render(&b, c)
		}
		inert(t, b.String(), corpusCites)
	}
}

func TestStubRendersOnlyStubFields(t *testing.T) {
	h := newHarness(t)
	h.refresh()
	canaries := map[string]any{"lead": "CANARY-LEAD", "body_md": "## CANARY-BODY\n\n[1]", "quality_tier": "strong", "updated_at": "2026-10-09T09:09:09Z",
		"citations": []any{map[string]any{"n": 1, "url": "https://canary.example/", "title": "CANARY-CITE", "host": "canary.example", "quote": "CANARY-QUOTE"}},
		"aliases":   []string{"CANARY-ALIAS"}, "topic_ids": []string{"cafecafecafecafe"}, "version": 0}
	rec := stub("being-researched", "Being researched topic")
	for k, v := range canaries {
		rec[k] = v
	}
	h.v1.set("being-researched", jsonReply(200, rec))
	p := h.get("/wiki/being-researched")
	wantPage(t, p, 200, public(300))
	body := p.Body.String()
	structure(t, "stub", body)
	for _, c := range []string{"CANARY", "canary.example", "cafecafe", "Strong", "High", " quality", "Oct 9", "2026-10-09", "ld+json"} {
		if strings.Contains(body, c) {
			t.Fatalf("the stub rendered %q", c)
		}
	}
	for _, want := range []string{"<h1>Being researched topic</h1>", "Research", `<time datetime="2026-10-01T08:00:00Z">Oct 1, 2026</time>`, "Being researched"} {
		if !strings.Contains(body, want) {
			t.Fatalf("the stub lacks %q", want)
		}
	}
}

func TestNoFieldOutsideTheProjections(t *testing.T) {
	h := newHarness(t)
	rec := rustArticle()
	extra := map[string]any{"aliases": []string{"CANARY-aliases"}, "topic_ids": []string{"CANARY-topic_ids"}, "cluster_ids": []string{"CANARY-cluster_ids"},
		"sensitivity": map[string]any{"class": "none", "reason": "CANARY-sensitivity"}, "moderation": map[string]any{"action": "approve", "by": "CANARY-moderation"},
		"models": map[string]any{"triage": "CANARY-models", "writer": "CANARY-models-w"}, "build": map[string]any{"retrieval_params": map[string]any{"k": "CANARY-build"}},
		"base_slug": "CANARY-base_slug", "residue": "CANARY-residue", "reason_code": "CANARY-reason_code", "content_sha256": "CANARY-content_sha256",
		"readers_7d": "CANARY-readers_7d", "readers_by_day": map[string]any{"CANARY-readers_by_day": 1}, "has_fingerprint": "CANARY-has_fingerprint"}
	for k, v := range extra {
		rec[k] = v
	}
	cites := rec["citations"].([]any)
	for i := range cites {
		cites[i].(map[string]any)["content_sha"] = "CANARY-content_sha"
	}
	it := item(rec)
	for k, v := range extra {
		it[k] = v
	}
	h.v1.list(it)
	h.v1.set("rust-async-runtimes", jsonReply(200, rec))
	h.refresh()
	var all strings.Builder
	for _, p := range []string{"/wiki/rust-async-runtimes", "/wiki", "/wiki/v/dev-docs", "/sitemap.xml", "/sitemaps/wiki-1.xml"} {
		got := h.get(p)
		if got.Code != 200 {
			t.Fatalf("%s: %d", p, got.Code)
		}
		all.WriteString(got.Body.String())
		for _, v := range got.Header() {
			all.WriteString(strings.Join(v, " "))
		}
	}
	if strings.Contains(all.String(), "CANARY") {
		i := strings.Index(all.String(), "CANARY")
		t.Fatalf("a canary was rendered: %s", all.String()[i:i+30])
	}
	h.v1.set("prelive-canary", jsonReply(200, with(rec, "slug", "prelive-canary", "prelive", "CANARY-prelive")))
	got := h.get("/wiki/prelive-canary")
	wantPage(t, got, 503, "no-store")
	if strings.Contains(got.Body.String(), "CANARY") || strings.Contains(got.Body.String(), "Rust async") {
		t.Fatal("the over-scoped record reached the page")
	}
}

func TestSitemapXML(t *testing.T) {
	h := newHarness(t)
	var items []map[string]any
	for i, slug := range []string{"alpha", "beta", "gamma", "delta", "epsilon"} {
		updated := fmt.Sprintf("2026-10-0%dT10:00:00Z", 5-i)
		rec := with(published(slug, strings.ToUpper(slug[:1])+slug[1:]), "updated_at", updated)
		if slug == "delta" {
			rec = with(rec, "updated_at", "2026-10-03T10:00:00Z", "id", "01AAAAAAAAAAAAAAAAAAAAAAAA")
		}
		items = append(items, item(rec))
	}
	items = append(items, item(with(published("not-promoted", "Not promoted"), "promoted", false, "quality_tier", "ok")))
	h.v1.list(items...)
	h.w.sitemapPage = 2
	h.refresh()
	type loc struct {
		Loc     string `xml:"loc"`
		Lastmod string `xml:"lastmod"`
	}
	var index struct {
		XMLName  xml.Name
		Sitemaps []loc `xml:"sitemap"`
	}
	idx := h.get("/sitemap.xml")
	if idx.Header().Get("Content-Type") != "application/xml; charset=utf-8" || xml.Unmarshal(idx.Body.Bytes(), &index) != nil {
		t.Fatalf("sitemap index:\n%s", idx.Body)
	}
	if index.XMLName.Space != "http://www.sitemaps.org/schemas/sitemap/0.9" || index.XMLName.Local != "sitemapindex" || len(index.Sitemaps) != 3 {
		t.Fatalf("index %+v", index)
	}
	var got []loc
	for n, sm := range index.Sitemaps {
		if sm.Loc != fmt.Sprintf("https://cosift.example/sitemaps/wiki-%d.xml", n+1) {
			t.Fatalf("index loc %q", sm.Loc)
		}
		var set struct {
			XMLName xml.Name
			URLs    []loc `xml:"url"`
		}
		body := h.get(strings.TrimPrefix(sm.Loc, "https://cosift.example")).Body.Bytes()
		if err := xml.Unmarshal(body, &set); err != nil || set.XMLName.Space != "http://www.sitemaps.org/schemas/sitemap/0.9" || set.XMLName.Local != "urlset" {
			t.Fatalf("urlset %d: %v\n%s", n+1, err, body)
		}
		if len(set.URLs) == 0 || sm.Lastmod != set.URLs[0].Lastmod {
			t.Fatalf("index lastmod %q for file %d", sm.Lastmod, n+1)
		}
		got = append(got, set.URLs...)
	}
	want := []string{"alpha", "beta", "delta", "gamma", "epsilon"}
	var gotSlugs []string
	for _, u := range got {
		gotSlugs = append(gotSlugs, strings.TrimPrefix(u.Loc, "https://cosift.example/wiki/"))
		if _, err := time.Parse(time.RFC3339, u.Lastmod); err != nil {
			t.Fatalf("lastmod %q", u.Lastmod)
		}
	}
	if strings.Join(gotSlugs, ",") != strings.Join(want, ",") {
		t.Fatalf("sitemap order %v, want %v", gotSlugs, want)
	}
	if got[0].Lastmod != "2026-10-05T10:00:00Z" {
		t.Fatalf("lastmod %q", got[0].Lastmod)
	}
	h.v1.list()
	h.clock.Add(time.Second)
	h.refresh()
	empty := h.get("/sitemaps/wiki-1.xml")
	if empty.Code != 200 || !strings.Contains(empty.Body.String(), "<urlset") || strings.Contains(empty.Body.String(), "<url>") {
		t.Fatalf("empty promoted set:\n%s", empty.Body)
	}
	if !strings.Contains(h.get("/sitemap.xml").Body.String(), "wiki-1.xml") {
		t.Fatal("the index is empty")
	}
}

func TestHubPagination(t *testing.T) {
	h := newHarness(t)
	var items []map[string]any
	for i := range 7 {
		slug := fmt.Sprintf("topic-%c", 'a'+i)
		items = append(items, item(published(slug, "Topic "+string(rune('A'+i)))))
	}
	h.v1.list(items...)
	h.w.hubPage = 3
	h.refresh()
	p1 := h.get("/wiki").Body.String()
	p3 := h.get("/wiki/index/3").Body.String()
	if !strings.Contains(p1, `<link rel="next" href="https://cosift.example/wiki/index/2">`) || strings.Contains(p1, `rel="prev"`) || !strings.Contains(p1, "Topic C") || strings.Contains(p1, "Topic D") {
		t.Fatalf("page 1:\n%s", p1)
	}
	if !strings.Contains(p3, `<link rel="prev" href="https://cosift.example/wiki/index/2">`) || strings.Contains(p3, `rel="next"`) || !strings.Contains(p3, "Topic G") {
		t.Fatalf("page 3:\n%s", p3)
	}
	if !strings.Contains(p3, `<link rel="canonical" href="https://cosift.example/wiki/index/3">`) {
		t.Fatal("page 3 canonical")
	}
	wantPage(t, h.get("/wiki/index/4"), 404, "no-store")
	v2 := h.get("/wiki/v/dev-docs/2").Body.String()
	if !strings.Contains(v2, `<link rel="prev" href="https://cosift.example/wiki/v/dev-docs">`) || !strings.Contains(v2, `<link rel="canonical" href="https://cosift.example/wiki/v/dev-docs/2">`) {
		t.Fatalf("vertical page 2:\n%s", v2)
	}
}

func TestGoldenPages(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.GSCVerification = "gsc-token_0123456789" })
	for name, page := range pages(t, h) {
		ext := ".html"
		switch {
		case strings.HasPrefix(name, "sitemap"):
			ext = ".xml"
		case name == "robots":
			ext = ".txt"
		}
		path := filepath.Join("testdata", "golden", name+ext)
		if *update {
			if err := os.WriteFile(path, []byte(page), 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		want, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%v (run with -update)", err)
		}
		if string(want) != page {
			t.Fatalf("%s differs from %s", name, path)
		}
	}
}

func TestControlCharactersStrippedFromEveryField(t *testing.T) {
	h := newHarness(t)
	dirty := func(s string) string { return "\u202e" + s + "\u2066\u200b\x07" }
	rec := with(published("dirty-fields", dirty("Dirty title")), "citations", []any{
		map[string]any{"n": 1, "url": "https://docs.example.org/guide", "title": dirty("Cite title"), "host": dirty("docs.example.org"), "quote": dirty("Cite quote")},
		map[string]any{"n": 2, "url": "https://news.example.com/item?id=7", "title": "Item", "host": "news.example.com", "quote": "Another quote."},
	})
	h.v1.list(item(rec))
	h.v1.set("dirty-fields", jsonReply(200, rec))
	h.refresh()
	for _, p := range []string{"/wiki/dirty-fields", "/wiki", "/wiki/v/dev-docs"} {
		if body := h.get(p).Body.String(); strings.ContainsAny(body, "\u202e\u2066\u200b\x07") || !strings.Contains(body, "Dirty title") {
			t.Fatalf("%s kept a control or bidi character", p)
		}
	}
	doc := structure(t, "dirty", h.get("/wiki/dirty-fields").Body.String())
	if ld := jsonLD(t, doc); ld["headline"] != "Dirty title" {
		t.Fatalf("JSON-LD headline %q", ld["headline"])
	}
	if got := textOf(find(doc, tag("h1"))[0]); got != "Dirty title" {
		t.Fatalf("h1 %q", got)
	}
	cite1 := find(doc, func(n *html.Node) bool { return n.Data == "li" && attr(n, "id") == "cite-1" })[0]
	src := ancestor(cite1, func(n *html.Node) bool { return n.Data == "li" && attr(n, "class") == "wiki-source" })
	if src == nil || !strings.Contains(textOf(src), "Cite title docs.example.org") || !strings.Contains(textOf(cite1), "Cite quote") {
		t.Fatalf("citation text src=%q quote=%q", textOf(src), textOf(cite1))
	}
}

func TestPublicQualityNames(t *testing.T) {
	h := newHarness(t)
	h.refresh()
	for tier, want := range map[string]string{"strong": "High quality", "ok": "Medium quality", "thin": "Low quality"} {
		slug := "tier-" + tier
		h.v1.set(slug, jsonReply(200, with(published(slug, "Tier "+tier), "quality_tier", tier)))
		h.clock.Add(time.Second)
		doc := structure(t, slug, h.get("/wiki/"+slug).Body.String())
		line := textOf(find(doc, func(n *html.Node) bool { return n.Data == "p" && attr(n, "class") == "wiki-meta" })[0])
		if !strings.Contains(line, "· "+want) || strings.Contains(strings.ToLower(line), tier+" quality") {
			t.Fatalf("%s: status line %q, want %q", tier, line, want)
		}
	}
}
