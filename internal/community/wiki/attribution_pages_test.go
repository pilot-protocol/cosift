package wiki

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/html"
)

func communityArticle(slug string, cids ...any) map[string]any {
	return with(published(slug, "Tidal power"), "origin", "community", "contributors", cids)
}

func statusLine(t *testing.T, doc *html.Node) *html.Node {
	t.Helper()
	line := find(doc, func(n *html.Node) bool { return n.Data == "p" && attr(n, "class") == "wiki-meta" })
	if len(line) != 1 {
		t.Fatal("no status line")
	}
	return line[0]
}

func contribSpan(doc *html.Node) []*html.Node {
	return find(doc, func(n *html.Node) bool { return n.Data == "span" && attr(n, "class") == "wiki-contrib" })
}

func TestCommunityArticleNamesItsContributorsAfterTheAgentLabel(t *testing.T) {
	src := &fakeHandles{known: map[string]Handle{cidA: {Name: "tidal-fan"}, cidB: {Name: "_edge_"}}}
	h := withHandles(t, src)
	h.v1.set("tidal-power", jsonReply(200, communityArticle("tidal-power", cidA, cidB)))
	doc := structure(t, "community", h.get("/wiki/tidal-power").Body.String())
	line := statusLine(t, doc)
	spans := contribSpan(line)
	agent := find(line, func(n *html.Node) bool { return n.Data == "span" && attr(n, "class") == "wiki-agent" })
	if len(spans) != 1 || len(agent) != 1 || textOf(agent[0]) != "Agent submitted" || agent[0].NextSibling == nil {
		t.Fatalf("status line %q", textOf(line))
	}
	if got := textOf(spans[0]); got != "· Contributed by @tidal-fan, @_edge_" {
		t.Fatalf("contributors %q", got)
	}
	if spans[0].NextSibling != nil {
		t.Fatal("the contributors do not end the status line")
	}
}

func TestSynthArticleAndEmptyCommunityListShowNoContributors(t *testing.T) {
	src := &fakeHandles{known: map[string]Handle{cidA: {Name: "tidal-fan"}}}
	h := withHandles(t, src)
	h.v1.set("synth-one", jsonReply(200, with(published("synth-one", "Synth one"), "origin", "synth", "contributors", []any{cidA})))
	h.v1.set("legacy-one", jsonReply(200, published("legacy-one", "Legacy one")))
	h.v1.set("empty-one", jsonReply(200, communityArticle("empty-one")))
	for _, slug := range []string{"synth-one", "legacy-one", "empty-one"} {
		h.clock.Add(time.Second)
		doc := structure(t, slug, h.get("/wiki/"+slug).Body.String())
		if len(contribSpan(doc)) != 0 {
			t.Fatalf("%s: contributors shown", slug)
		}
	}
	if src.callCount() != 0 {
		t.Fatalf("%d handle lookups for articles without contributors", src.callCount())
	}
}

func TestBannedUnknownAndLocalContributorsAreACommunityMember(t *testing.T) {
	src := &fakeHandles{known: map[string]Handle{cidA: {Name: "tidal-fan", Banned: true}, cidB: {Name: "rusty"}}}
	h := withHandles(t, src)
	h.v1.set("tidal-power", jsonReply(200, communityArticle("tidal-power", cidA, cidB, cidC)))
	body := h.get("/wiki/tidal-power").Body.String()
	doc := structure(t, "banned", body)
	if got := textOf(contribSpan(doc)[0]); got != "· Contributed by a community member, @rusty, a community member" {
		t.Fatalf("contributors %q", got)
	}
	if strings.Contains(body, "tidal-fan") {
		t.Fatal("a banned handle was rendered")
	}
	local := newHarness(t)
	local.v1.set("tidal-power", jsonReply(200, communityArticle("tidal-power", cidA, cidB)))
	doc = structure(t, "local", local.get("/wiki/tidal-power").Body.String())
	if got := textOf(contribSpan(doc)[0]); got != "· Contributed by a community member, a community member" {
		t.Fatalf("local mode %q", got)
	}
}

func TestMalformedAttributionFieldsAreIgnored(t *testing.T) {
	src := &fakeHandles{known: map[string]Handle{cidA: {Name: "tidal-fan"}}}
	h := withHandles(t, src)
	for slug, rec := range map[string]map[string]any{
		"other-origin":  with(published("other-origin", "Other origin"), "origin", "staff", "contributors", []any{cidA}),
		"bad-cid":       communityArticle("bad-cid", cidA, "not-a-cid"),
		"not-an-array":  with(published("not-an-array", "Not an array"), "origin", "community", "contributors", "oops"),
		"origin-number": with(published("origin-number", "Origin number"), "origin", 7, "contributors", []any{cidA}),
	} {
		h.v1.set(slug, jsonReply(200, rec))
		h.clock.Add(time.Second)
		rec := h.get("/wiki/" + slug)
		if rec.Code != 200 {
			t.Fatalf("%s: %d", slug, rec.Code)
		}
		if len(contribSpan(structure(t, slug, rec.Body.String()))) != 0 {
			t.Fatalf("%s: contributors shown", slug)
		}
	}
}

func TestHandleTextCannotInjectMarkup(t *testing.T) {
	for _, name := range []string{"<script>alert(1)</script>", `"><img src=x onerror=alert(1)>`, "a b", "x‮y", "javascript:alert(1)"} {
		src := &fakeHandles{known: map[string]Handle{cidA: {Name: name}}}
		h := withHandles(t, src)
		h.v1.set("tidal-power", jsonReply(200, communityArticle("tidal-power", cidA)))
		body := h.get("/wiki/tidal-power").Body.String()
		doc := structure(t, "handle", body)
		if got := textOf(contribSpan(doc)[0]); got != "· Contributed by a community member" || strings.Contains(body, "alert(1)") {
			t.Fatalf("handle %q rendered as %q", name, got)
		}
	}
	src := &fakeHandles{known: map[string]Handle{cidA: {Name: "_a_"}}}
	h := withHandles(t, src)
	h.v1.set("tidal-power", jsonReply(200, communityArticle("tidal-power", cidA)))
	doc := structure(t, "underscore", h.get("/wiki/tidal-power").Body.String())
	if got := textOf(contribSpan(doc)[0]); got != "· Contributed by @_a_" {
		t.Fatalf("%q", got)
	}
}

func TestARenameShowsWhenTheCachesExpire(t *testing.T) {
	src := &fakeHandles{known: map[string]Handle{cidA: {Name: "old-name"}}}
	h := withHandles(t, src)
	h.v1.set("tidal-power", jsonReply(200, communityArticle("tidal-power", cidA)))
	h.get("/wiki/tidal-power")
	src.set(cidA, Handle{Name: "new-name"})
	h.clock.Add(articleBound - 1)
	if got := h.get("/wiki/tidal-power").Body.String(); !strings.Contains(got, "@old-name") {
		t.Fatal("the page changed before its cache expired")
	}
	h.clock.Add(2)
	if got := h.get("/wiki/tidal-power").Body.String(); !strings.Contains(got, "@new-name") {
		t.Fatal("the rename did not show once the caches expired")
	}
}

func TestAPageRenderedWithoutHandlesIsServedBrieflyNotForFiveMinutes(t *testing.T) {
	src := &fakeHandles{known: map[string]Handle{cidA: {Name: "tidal-fan"}}, err: fmt.Errorf("unavailable")}
	h := withHandles(t, src)
	h.v1.set("tidal-power", jsonReply(200, communityArticle("tidal-power", cidA)))
	rec := h.get("/wiki/tidal-power")
	wantPage(t, rec, 200, public(int(retryEvery.Seconds())))
	if !strings.Contains(rec.Body.String(), "Contributed by a community member") {
		t.Fatal("no fallback label")
	}
	src.mu.Lock()
	src.err = nil
	src.mu.Unlock()
	h.clock.Add(retryEvery)
	if got := h.get("/wiki/tidal-power").Body.String(); !strings.Contains(got, "@tidal-fan") {
		t.Fatal("the page did not recover")
	}
}

func TestArticlePageCarriesTheLicenceLine(t *testing.T) {
	h := newHarness(t)
	h.v1.set("tidal-power", jsonReply(200, published("tidal-power", "Tidal power")))
	doc := structure(t, "licence", h.get("/wiki/tidal-power").Body.String())
	lic := find(doc, func(n *html.Node) bool { return n.Data == "a" && attr(n, "rel") == "license noopener" })
	if len(lic) != 1 || attr(lic[0], "href") != "https://creativecommons.org/licenses/by-sa/4.0/" || textOf(lic[0]) != "CC BY-SA 4.0" {
		t.Fatalf("licence link %v", lic)
	}
	p := ancestor(lic[0], tag("p"))
	if p == nil || !strings.Contains(textOf(p), "Creative Commons Attribution-ShareAlike 4.0 International") {
		t.Fatal("no licence sentence")
	}
	h.v1.set("pending-one", jsonReply(200, stub("pending-one", "Pending one")))
	if strings.Contains(h.get("/wiki/pending-one").Body.String(), "CC BY-SA") {
		t.Fatal("a stub carries the licence line")
	}
}

func TestVersionContributorAndRevertsToAreNeverRendered(t *testing.T) {
	src := &fakeHandles{known: map[string]Handle{cidA: {Name: "tidal-fan"}}}
	h := withHandles(t, src)
	h.v1.set("tidal-power", jsonReply(200, with(communityArticle("tidal-power", cidA), "version_contributor", "CANARY-vc", "reverts_to", "CANARY-rt")))
	if strings.Contains(h.get("/wiki/tidal-power").Body.String(), "CANARY") {
		t.Fatal("a canary was rendered")
	}
}

func checkGolden(t *testing.T, file, got string) {
	t.Helper()
	path := filepath.Join("testdata", "golden", file)
	if *update {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update)", err)
	}
	if string(want) != got {
		t.Fatalf("%s differs from %s", file, path)
	}
}

func TestGoldenCommunityArticle(t *testing.T) {
	src := &fakeHandles{known: map[string]Handle{cidA: {Name: "tidal-fan"}, cidB: {Name: "rusty", Banned: true}, cidC: {Name: "_edge_"}}}
	h := withHandles(t, src)
	h.v1.set("rust-async-runtimes", jsonReply(200, with(rustArticle(), "origin", "community", "contributors", []any{cidA, cidB, cidC})))
	checkGolden(t, "article-community.html", h.get("/wiki/rust-async-runtimes").Body.String())
}

func TestRendererEscapesAContributorNameEvenIfOneGotThrough(t *testing.T) {
	h := newHarness(t)
	rec := published("tidal-power", "Tidal power")
	eng := engineRecord{}
	body, _ := json.Marshal(rec)
	if err := json.Unmarshal(body, &eng); err != nil {
		t.Fatal(err)
	}
	page := string(h.w.renderArticle(&eng, []string{`@<script>alert(1)</script>`, `"><b>`}))
	if strings.Contains(page, "<script>alert") || strings.Contains(page, `"><b>`) || !strings.Contains(page, "@&lt;script&gt;alert(1)&lt;/script&gt;") {
		t.Fatal("a contributor name was not escaped")
	}
}

func TestABanOrRenameReachesAPageWithinTheArticleBound(t *testing.T) {
	src := &fakeHandles{known: map[string]Handle{cidA: {Name: "tidal-fan"}}}
	h := withHandles(t, src)
	h.v1.set("one", jsonReply(200, communityArticle("one", cidA)))
	h.v1.set("two", jsonReply(200, communityArticle("two", cidA)))
	h.get("/wiki/one")
	h.clock.Add(time.Second)
	src.set(cidA, Handle{Name: "tidal-fan", Banned: true})
	h.clock.Add(handleTTL - 2*time.Second)
	page := h.get("/wiki/two")
	if !strings.Contains(page.Body.String(), "@tidal-fan") {
		t.Fatal("the cached handle was not used")
	}
	wantPage(t, page, 200, public(int((articleBound-(handleTTL-time.Second))/time.Second)))
	h.clock.Add(articleBound - (handleTTL - time.Second) + time.Second)
	if body := h.get("/wiki/two").Body.String(); strings.Contains(body, "@tidal-fan") || !strings.Contains(body, anonymous) {
		t.Fatalf("a ban still shows past the article bound:\n%s", body)
	}
}
