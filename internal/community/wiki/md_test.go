package wiki

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"
)

var (
	mdEscape  = regexp.MustCompile(`\\.`)
	autolink  = regexp.MustCompile(`<https?://[^<>\s]+>`)
	mdSetext  = regexp.MustCompile(`^ {0,3}(=+|-+) *$`)
	mdFenceLn = regexp.MustCompile("^ {0,3}(```|~~~)")
	mdBidi    = regexp.MustCompile("[\u0000-\u0008\u000b-\u001f\u007f-\u009f\u200b-\u200f\u202a-\u202e\u2060-\u2069\ufeff]")
)

// mdInert fails when a Markdown renderer could find markup that is not plain text.
func mdInert(t *testing.T, name, out string) {
	t.Helper()
	if mdBidi.MatchString(out) {
		t.Fatalf("%s: control or bidi character in the output", name)
	}
	fenced, loose := false, false
	ours := len(out)
	if k := strings.LastIndex(out, "\n## Sources\n"); k >= 0 {
		ours = k
	} else if k := strings.LastIndex(out, "\n---\n"); k >= 0 {
		ours = k
	}
	offset := 0
	for i, line := range strings.Split(out, "\n") {
		generated := offset >= ours
		offset += len(line) + 1
		if mdFenceLn.MatchString(line) {
			if line[0] != '`' && line[0] != '~' {
				t.Fatalf("%s: indented fence on line %d: %q", name, i, line)
			}
			fenced, loose = !fenced, false
			continue
		}
		if fenced {
			continue
		}
		if strings.TrimSpace(line) == "" {
			loose = false
			continue
		}
		for k := strings.Index(line, "]:"); k >= 0; {
			if k == 0 || line[k-1] != '\\' {
				t.Fatalf("%s: reference definition on line %d: %q", name, i, line)
			}
			next := strings.Index(line[k+2:], "]:")
			if next < 0 {
				break
			}
			k += 2 + next
		}
		plain := mdEscape.ReplaceAllString(line, "")
		plain, lone := stripSpans(plain, loose)
		loose = loose || lone
		if generated {
			plain = autolink.ReplaceAllString(plain, "")
		}
		switch {
		case strings.Contains(plain, "<"):
			t.Fatalf("%s: raw < on line %d: %q", name, i, line)
		case strings.Contains(plain, "](") && !(generated && strings.Contains(line, licenceURL)):
			t.Fatalf("%s: inline link on line %d: %q", name, i, line)
		case mdSetext.MatchString(line) && !(generated && line == "---"):
			t.Fatalf("%s: setext rule on line %d: %q", name, i, line)
		case i > 0 && strings.HasPrefix(line, "# "):
			t.Fatalf("%s: second h1 on line %d: %q", name, i, line)
		}
	}
}

// stripSpans removes inline code that opens and closes on this line, unless an earlier line of the paragraph left a backtick run open; it reports whether a run stayed unmatched.
func stripSpans(s string, loose bool) (string, bool) {
	var b strings.Builder
	lone := false
	for i := 0; i < len(s); {
		if s[i] != '`' {
			b.WriteByte(s[i])
			i++
			continue
		}
		n := 0
		for i+n < len(s) && s[i+n] == '`' {
			n++
		}
		ticks := strings.Repeat("`", n)
		end := strings.Index(s[i+n:], ticks)
		for end >= 0 && i+n+end+n < len(s) && s[i+n+end+n] == '`' {
			rest := s[i+n+end:]
			skip := len(rest) - len(strings.TrimLeft(rest, "`"))
			next := strings.Index(s[i+n+end+skip:], ticks)
			if next < 0 {
				end = -1
				break
			}
			end += skip + next
		}
		switch {
		case end < 0:
			lone = true
			b.WriteString(ticks)
			i += n
		case loose || strings.Contains(s[i+n:i+n+end], "|"):
			b.WriteString(ticks)
			i += n
		default:
			i += n + end + n
		}
	}
	return b.String(), lone
}

func unescapeMD(s string) string {
	return mdEscape.ReplaceAllStringFunc(s, func(m string) string {
		if isPunct(m[1]) {
			return m[1:]
		}
		return m
	})
}

func TestInjectionCorpusInsideTheMarkdownForm(t *testing.T) {
	h := newHarness(t)
	h.refresh()
	for i, tc := range loadCorpus(t) {
		slug := fmt.Sprintf("case-%d", i)
		cites := []any{
			map[string]any{"n": 1, "url": corpusCites[0], "title": "One", "host": "docs.example.org", "quote": "q"},
			map[string]any{"n": 2, "url": corpusCites[1], "title": "Two", "host": "docs.example.org", "quote": "q"},
		}
		t.Run(tc.Name, func(t *testing.T) {
			h.v1.set(slug, jsonReply(200, with(published(slug, "Case"), "body_md", tc.Text, "citations", cites)))
			h.clock.Add(time.Second)
			rec := h.get("/wiki/"+slug+".md", "RemoteAddr", fmt.Sprintf("192.0.2.%d:1", i%200+1))
			if rec.Code != 200 {
				t.Fatalf("%d", rec.Code)
			}
			out := rec.Body.String()
			mdInert(t, tc.Name, out)
			plain := unescapeMD(out)
			for _, v := range tc.Visible {
				if !strings.Contains(plain, v) {
					t.Fatalf("lost visible text %q", v)
				}
			}
		})
	}
}

func TestHandlesInTheMarkdownFormAreEscaped(t *testing.T) {
	src := &fakeHandles{known: map[string]Handle{cidA: {Name: "_a_"}, cidB: {Name: "x_y-z"}, cidC: {Name: "<b>boo</b>"}}}
	h := withHandles(t, src)
	h.v1.set("tidal-power", jsonReply(200, communityArticle("tidal-power", cidA, cidB, cidC)))
	out := h.get("/wiki/tidal-power.md").Body.String()
	want := `Agent submitted · Contributed by @\_a\_, @x\_y-z, a community member`
	if !strings.Contains(out, "\n"+want+"\n") {
		t.Fatalf("attribution line missing in\n%s", out)
	}
	mdInert(t, "handles", out)
}

func TestMarkdownFormCarriesTheLicenceAndNoLinksButTheCitedOnes(t *testing.T) {
	h := newHarness(t)
	h.v1.set("tidal-power", jsonReply(200, published("tidal-power", "Tidal power")))
	out := h.get("/wiki/tidal-power.md").Body.String()
	if !strings.Contains(out, "Creative Commons Attribution-ShareAlike 4.0 International licence (CC BY-SA 4.0)](https://creativecommons.org/licenses/by-sa/4.0/)") {
		t.Fatal("no licence line")
	}
	for _, want := range []string{"# Tidal power\n\nAgent submitted\n\n", "## Sources\n", "- Guide (docs.example.org) <https://docs.example.org/guide>\n  - [1] A verbatim quote.\n", "Canonical page: <https://cosift.example/wiki/tidal-power>"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in\n%s", want, out)
		}
	}
	mdInert(t, "plain", out)
}

func TestMarkdownSourcesOmitAnUnsafeURLAndKeepGrouping(t *testing.T) {
	h := newHarness(t)
	cite := func(n int, url, quote string) map[string]any {
		return map[string]any{"n": n, "url": url, "title": "T*" + fmt.Sprint(n), "host": "a.example", "quote": quote}
	}
	rec := with(published("grouped", "Grouped"), "citations", []any{
		cite(1, "https://a.example/page", "First <b>quote</b>."), cite(2, "https://a.example/<x>", "Second."), cite(3, "https://a.example/page", "Third."),
		cite(4, "javascript:alert(1)", "Fourth."),
	})
	h.v1.set("grouped", jsonReply(200, rec))
	out := h.get("/wiki/grouped.md").Body.String()
	for _, want := range []string{
		"- T\\*1 (a.example) <https://a.example/page>\n  - [1] First \\<b\\>quote\\</b\\>.\n  - [3] Third.\n",
		"- T\\*2 (a.example)\n  - [2] Second.\n",
		"- T\\*4 (a.example)\n  - [4] Fourth.\n",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in\n%s", want, out)
		}
	}
	if strings.Contains(out, "javascript:") || strings.Contains(out, "<x>") {
		t.Fatal("an unsafe URL was printed")
	}
}

func TestMarkdownRouteServesTheSameEntryAsThePage(t *testing.T) {
	h := newHarness(t)
	h.v1.set("tidal-power", jsonReply(200, published("tidal-power", "Tidal power")))
	page := h.get("/wiki/tidal-power")
	md := h.get("/wiki/tidal-power.md")
	wantPage(t, page, 200, public(300))
	wantPage(t, md, 200, public(300))
	if md.Header().Get("Content-Type") != "text/markdown; charset=utf-8" || md.Header().Get("Set-Cookie") != "" {
		t.Fatalf("headers %v", md.Header())
	}
	if h.v1.slugCalls("tidal-power") != 1 {
		t.Fatalf("%d engine calls for one article", h.v1.slugCalls("tidal-power"))
	}
	h.clock.Add(100 * time.Second)
	wantPage(t, h.get("/wiki/tidal-power.md"), 200, public(200))
}

func TestMarkdownRouteStatuses(t *testing.T) {
	h := newHarness(t)
	h.v1.set("pending-one", jsonReply(200, stub("pending-one", "Pending one")))
	h.v1.set("removed-topic", errReply(410, "gone"))
	h.v1.set("renamed-topic", jsonReply(200, map[string]any{"redirect_slug": "tidal-power"}))
	h.v1.set("broken", errReply(500, "internal"))
	for _, tc := range []struct {
		path   string
		status int
		cache  string
	}{
		{"/wiki/pending-one.md", 404, "no-store"},
		{"/wiki/removed-topic.md", 410, "no-store"},
		{"/wiki/renamed-topic.md", 301, public(300)},
		{"/wiki/broken.md", 503, "no-store"},
		{"/wiki/nothing-here.md", 404, "no-store"},
	} {
		h.clock.Add(time.Second)
		rec := h.get(tc.path)
		wantPage(t, rec, tc.status, tc.cache)
		if tc.status != 301 && !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain") {
			t.Fatalf("%s: %q", tc.path, rec.Header().Get("Content-Type"))
		}
	}
	if loc := h.get("/wiki/renamed-topic.md").Header().Get("Location"); loc != "https://cosift.example/wiki/tidal-power.md" {
		t.Fatalf("location %q", loc)
	}
}

func TestMarkdownRouteIsRateLimitedPerClientAsPlainText(t *testing.T) {
	h := newHarness(t)
	var rec *struct{ code int }
	_ = rec
	for i := 0; i <= ipLimit; i++ {
		got := h.get(fmt.Sprintf("/wiki/missing-%d.md", i), "RemoteAddr", "192.0.2.77:1")
		if got.Code == http.StatusTooManyRequests {
			if !strings.HasPrefix(got.Header().Get("Content-Type"), "text/plain") || got.Header().Get("Retry-After") == "" {
				t.Fatalf("429 headers %v", got.Header())
			}
			return
		}
		h.clock.Add(10 * time.Millisecond)
	}
	t.Fatal("no 429")
}

func TestMarkdownPathsThatAreNotArticles(t *testing.T) {
	h := newHarness(t)
	for _, p := range []string{"/wiki/.md", "/wiki/UPPER.md", "/wiki/a.md.md", "/wiki/v.md", "/wiki/index.md", "/wiki/a/b.md"} {
		rec := h.get(p)
		if rec.Code != 404 || h.v1.count() != 0 {
			t.Fatalf("%s: %d with %d engine calls", p, rec.Code, h.v1.count())
		}
	}
}

func TestGoldenMarkdown(t *testing.T) {
	src := &fakeHandles{known: map[string]Handle{cidA: {Name: "tidal-fan"}, cidB: {Name: "rusty", Banned: true}, cidC: {Name: "_edge_"}}}
	h := withHandles(t, src)
	h.v1.set("rust-async-runtimes", jsonReply(200, with(rustArticle(), "origin", "community", "contributors", []any{cidA, cidB, cidC})))
	checkGolden(t, "article-community.md", h.get("/wiki/rust-async-runtimes.md").Body.String())
	plain := newHarness(t)
	plain.v1.set("rust-async-runtimes", jsonReply(200, rustArticle()))
	checkGolden(t, "article.md", plain.get("/wiki/rust-async-runtimes.md").Body.String())
}

func TestEscapeInline(t *testing.T) {
	for in, want := range map[string]string{"plain": "plain", "_a_": `\_a\_`, "a*b": `a\*b`, "[x](y)": `\[x\](y)`, "<b>": `\<b\>`, "a\\b": `a\\b`, "`c`": "\\`c\\`", "x|y~z": `x\|y\~z`} {
		if got := escapeInline(in); got != want {
			t.Errorf("%q -> %q, want %q", in, got, want)
		}
	}
}

func TestMarkdownBodyNormalisesHeadingsLikeThePage(t *testing.T) {
	in := "### Early\n\n# Top\n\n###### Deep\n\n## Real\n\n### Sub\n\n#\n\nText\n=====\n\n---\n"
	want := "## Early\n\n## Top\n\n### Deep\n\n## Real\n\n### Sub\n\n\nText\n\\=====\n\n\\---\n"
	if got := mdBody(in); got != want {
		t.Fatalf("got\n%q\nwant\n%q", got, want)
	}
}

func TestMarkdownBodyKeepsFencesAndCodeSpans(t *testing.T) {
	in := "Use `Vec<T>` and ``a<b``.\n\n```rust\nlet v: Vec<u8> = <x>;\n[a](b)\n```\n\nAfter <i>fence</i> [l](http://x)\n[1]: http://x\n"
	want := "Use `Vec<T>` and ``a<b``.\n\n```rust\nlet v: Vec<u8> = <x>;\n[a](b)\n```\n\nAfter \\<i>fence\\</i> [l]\\(http://x)\n[1]\\: http://x\n"
	if got := mdBody(in); got != want {
		t.Fatalf("got\n%q\nwant\n%q", got, want)
	}
}

func TestMarkdownBodyClosesAnOpenFence(t *testing.T) {
	if got, want := mdBody("Text [1].\n\n```\n<b>open"), "Text [1].\n\n```\n<b>open\n```"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	h := newHarness(t)
	h.v1.set("open", jsonReply(200, with(published("open", "Open"), "body_md", "Text [1].\n\n~~~\n<b>open", "citations", []any{
		map[string]any{"n": 1, "url": "https://a.example/p", "title": "T", "host": "a.example", "quote": "q"},
	})))
	out := h.get("/wiki/open.md").Body.String()
	if !strings.Contains(out, "<b>open\n~~~\n\n## Sources\n") {
		t.Fatalf("the fence runs into the sources:\n%s", out)
	}
}

func TestMarkdownBodyMovesFencesToColumnZero(t *testing.T) {
	in := "- item\n  ```go\n  a<b\n    c\n  ```\nafter <i>\n   ~~~\nx\n   ~~~\n"
	want := "- item\n```go\na<b\n  c\n```\nafter \\<i>\n~~~\nx\n~~~\n"
	if got := mdBody(in); got != want {
		t.Fatalf("got\n%q\nwant\n%q", got, want)
	}
}

func TestMarkdownBodyKeepsASpanOnlyWhenNoRunIsOpen(t *testing.T) {
	for in, want := range map[string]string{
		"x `a\nb` <i> `c`":         "x `a\nb` \\<i> `c`",
		"x `a\nb\nc` <i> `d<e>`":   "x `a\nb\nc` \\<i> `d\\<e>`",
		"x `a\n\nb `c<d>`":         "x `a\n\nb `c<d>`",
		"`ok<` x `a\nb` <i>":       "`ok<` x `a\nb` \\<i>",
		"| `x | <i>`":              "| `x | \\<i>`",
		"[x`]: u`":                 "[x`]\\: u`",
		"```\na\n```\n`b\n`c<d>`":  "```\na\n```\n`b\n`c\\<d>`",
		"`b\n```\na\n```\n`c<d>`":  "`b\n```\na\n```\n`c<d>`",
		"`b\n- `c` <d>":            "`b\n- `c` \\<d>",
		"a `b` `c\nd` `e` <f>":     "a `b` `c\nd` `e` \\<f>",
		"\\`a` <i>":                "\\`a` \\<i>",
		"x `a\n# h `b<c>`\n`d<e>`": "x `a\n## h `b\\<c>`\n`d\\<e>`",
	} {
		got := mdBody(in)
		if got != want {
			t.Errorf("%q -> %q, want %q", in, got, want)
		}
	}
}

func TestMarkdownBodyEscapesAnIndentedSetextRule(t *testing.T) {
	for in, want := range map[string]string{"T\n ===": "T\n \\===", "T\n   ---": "T\n   \\---", "T\n    ===": "T\n    ==="} {
		if got := mdBody(in); got != want {
			t.Errorf("%q -> %q, want %q", in, got, want)
		}
	}
}

func TestMarkdownFormEscapesTheCitationHost(t *testing.T) {
	h := newHarness(t)
	h.v1.set("host", jsonReply(200, with(published("host", "Host"), "citations", []any{
		map[string]any{"n": 1, "url": "https://a_b.example/p", "title": "T", "host": "a_b*c.example", "quote": "q"},
	})))
	if out := h.get("/wiki/host.md").Body.String(); !strings.Contains(out, "- T (a\\_b\\*c.example) <https://a_b.example/p>\n") {
		t.Fatalf("host not escaped:\n%s", out)
	}
}
