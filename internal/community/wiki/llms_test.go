package wiki

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestLlmsTxtListsPromotedArticlesAsMarkdownLinks(t *testing.T) {
	h := newHarness(t)
	h.v1.list(item(published("alpha", "Alpha")), item(published("rust-async-runtimes", "Rust *async* [runtimes]")),
		with(item(published("ok-one", "OK one")), "promoted", false, "quality_tier", "ok"))
	h.refresh()
	rec := h.get("/llms.txt")
	wantPage(t, rec, 200, public(300))
	if rec.Header().Get("Content-Type") != mdType {
		t.Fatalf("content type %q", rec.Header().Get("Content-Type"))
	}
	want := "# Cosift articles\n\n" +
		"> Articles written by AI agents from cited sources. The text is available under the Creative Commons Attribution-ShareAlike 4.0 International licence (CC BY-SA 4.0): <" + licenceURL + ">\n\n" +
		"- [Contributor style guide](https://cosift.example/wiki/style.md)\n- [Contributor terms](https://cosift.example/wiki/terms)\n- [Wanted articles](https://cosift.example/wiki/wanted)\n\n" +
		"## Articles\n\n" +
		"- [Alpha](https://cosift.example/wiki/alpha.md)\n" +
		"- [Rust \\*async\\* \\[runtimes\\]](https://cosift.example/wiki/rust-async-runtimes.md)\n"
	if got := rec.Body.String(); got != want {
		t.Fatalf("llms.txt:\n%s\nwant:\n%s", got, want)
	}
}

func TestLlmsTxtFollowsTheSnapshotBound(t *testing.T) {
	h := newHarness(t)
	wantPage(t, h.get("/llms.txt"), 503, "no-store")
	h.v1.list(item(published("alpha", "Alpha")))
	h.refresh()
	h.clock.Add(250 * time.Second)
	wantPage(t, h.get("/llms.txt"), 200, public(50))
	h.clock.Add(50 * time.Second)
	wantPage(t, h.get("/llms.txt"), 503, "no-store")
}

func TestLlmsTxtDropsTakenDownArticles(t *testing.T) {
	h := newHarness(t)
	h.v1.list(item(published("alpha", "Alpha")), item(published("beta", "Beta")))
	h.refresh()
	if body := h.get("/llms.txt").Body.String(); !strings.Contains(body, "alpha.md") || !strings.Contains(body, "beta.md") {
		t.Fatalf("both articles expected in\n%s", body)
	}
	h.v1.set("beta", errReply(410, "gone"))
	h.get("/wiki/beta")
	h.clock.Add(time.Second)
	if body := h.get("/llms.txt").Body.String(); strings.Contains(body, "beta") {
		t.Fatalf("a removed article stays listed:\n%s", body)
	}
}

func TestInjectionCorpusInTitlesStaysInertInLlmsTxt(t *testing.T) {
	h := newHarness(t)
	var items []map[string]any
	for i, tc := range loadCorpus(t) {
		items = append(items, item(published(fmt.Sprintf("case-%03d", i), tc.Text)))
	}
	h.v1.list(items...)
	h.refresh()
	out := h.get("/llms.txt").Body.String()
	if mdBidi.MatchString(out) {
		t.Fatal("control or bidi character in llms.txt")
	}
	_, list, _ := strings.Cut(out, "## Articles\n\n")
	lines := strings.Split(strings.TrimSuffix(list, "\n"), "\n")
	if len(lines) != len(items) {
		t.Fatalf("%d lines for %d articles", len(lines), len(items))
	}
	line := regexp.MustCompile(`^- \[(.*)\]\(https://cosift\.example/wiki/case-\d{3}\.md\)$`)
	for _, l := range lines {
		m := line.FindStringSubmatch(l)
		if m == nil {
			t.Fatalf("not a bare link line: %q", l)
		}
		if bad := mdPunct.FindString(mdPair.ReplaceAllString(m[1], "x")); bad != "" {
			t.Fatalf("unescaped %q in %q", bad, l)
		}
	}
}

func TestGoldenLlmsTxt(t *testing.T) {
	h := newHarness(t)
	h.v1.list(item(published("rust-async-runtimes", "Rust async runtimes")), item(published("tidal-power", "Tidal power")),
		with(item(published("ok-one", "OK one")), "promoted", false, "quality_tier", "ok"))
	h.refresh()
	checkGolden(t, "llms.txt", h.get("/llms.txt").Body.String())
}
