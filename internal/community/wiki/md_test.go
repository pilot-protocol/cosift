package wiki

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode"
)

var (
	mdPair     = regexp.MustCompile("\\\\[!-/:-@\\[-`{-~]")
	mdPunct    = regexp.MustCompile("[!-/:-@\\[-`{-~]")
	mdItemLink = regexp.MustCompile(`^(.*) <(https?://[^<>\s]+)>$`)
	mdBidi     = regexp.MustCompile("[\u0000-\u0008\u000b-\u001f\u007f-\u009f\u200b-\u200f\u202a-\u202e\u2060-\u2069\ufeff]")
)

// mdProblem says why a generated line could start Markdown syntax: apart from the code's own line prefix and a validated autolink after a list item, every text run must be free of unescaped ASCII punctuation, and the line must not be indented by four spaces.
func mdProblem(line string, urls map[string]bool) string {
	if strings.HasPrefix(line, "    ") || strings.ContainsAny(line, "\t\r") {
		return "indentation or control character"
	}
	text := line
	item := false
	for _, p := range []string{"### ", "## ", "# ", "  - ", "- "} {
		if rest, ok := strings.CutPrefix(text, p); ok {
			text, item = rest, strings.HasSuffix(p, "- ")
			break
		}
	}
	if m := mdItemLink.FindStringSubmatch(text); item && m != nil && urls[m[2]] {
		text = m[1]
	}
	if bad := mdPunct.FindString(mdPair.ReplaceAllString(text, "x")); bad != "" {
		return fmt.Sprintf("unescaped %q", bad)
	}
	return ""
}

// mdSafe fails unless every line of a .md page is a construct the code builds (the title, the status line, headings, paragraphs and items made of escaped text, validated autolinks, the licence line) and the page has one h1.
func mdSafe(t *testing.T, name, out string, urls ...string) {
	t.Helper()
	if mdBidi.MatchString(out) {
		t.Fatalf("%s: control or bidi character in the output", name)
	}
	allowed := map[string]bool{licenceURL: true}
	for _, u := range urls {
		allowed[u] = true
	}
	canonical := regexp.MustCompile(`^Canonical page: <(https://[^<>\s]+)>$`)
	for i, line := range strings.Split(strings.TrimSuffix(out, "\n"), "\n") {
		var problem string
		switch m := canonical.FindStringSubmatch(line); {
		case line == "" || line == licenceLine:
		case i > 0 && strings.HasPrefix(line, "# "):
			problem = "second h1"
		case i == 2 && strings.HasPrefix(line, "Agent submitted"):
			if bad := mdPunct.FindString(strings.ReplaceAll(mdPair.ReplaceAllString(line, "x"), ", ", " ")); bad != "" {
				problem = fmt.Sprintf("unescaped %q", bad)
			}
		case m != nil:
			if !allowed[m[1]] {
				problem = "autolink to " + m[1]
			}
		default:
			problem = mdProblem(line, allowed)
		}
		if problem != "" {
			t.Fatalf("%s: line %d %q: %s", name, i, line, problem)
		}
	}
}

func unescapeMD(s string) string {
	return mdPair.ReplaceAllStringFunc(s, func(m string) string { return m[1:] })
}

func TestInjectionCorpusInsideTheMarkdownForm(t *testing.T) {
	h := newHarness(t)
	h.refresh()
	for i, tc := range loadCorpus(t) {
		for _, field := range []string{"body", "title", "citation"} {
			slug := fmt.Sprintf("case-%d-%s", i, field)
			cites := []any{
				map[string]any{"n": 1, "url": corpusCites[0], "title": "One", "host": "docs.example.org", "quote": "q"},
				map[string]any{"n": 2, "url": corpusCites[1], "title": "Two", "host": "docs.example.org", "quote": "q"},
			}
			rec := published(slug, "Case")
			switch field {
			case "body":
				rec = with(rec, "body_md", tc.Text, "citations", cites)
			case "title":
				rec = with(rec, "title", tc.Text, "citations", cites)
			case "citation":
				rec = with(rec, "citations", []any{map[string]any{"n": 1, "url": corpusCites[0], "title": tc.Text, "host": tc.Text, "quote": tc.Text}})
			}
			t.Run(tc.Name+"/"+field, func(t *testing.T) {
				h.v1.set(slug, jsonReply(200, rec))
				h.clock.Add(time.Second)
				got := h.get("/wiki/"+slug+".md", "RemoteAddr", fmt.Sprintf("192.0.2.%d:1", i%200+1))
				if got.Code != 200 {
					t.Fatalf("%d", got.Code)
				}
				out := got.Body.String()
				mdSafe(t, tc.Name, out, corpusCites[0], "https://cosift.example/wiki/"+slug)
				if field != "body" {
					return
				}
				plain := unescapeMD(out)
				for _, v := range tc.Visible {
					if !strings.Contains(plain, v) {
						t.Fatalf("lost visible text %q", v)
					}
				}
			})
		}
	}
}

func TestHandlesInTheMarkdownFormAreEscaped(t *testing.T) {
	src := &fakeHandles{known: map[string]Handle{cidA: {Name: "_a_"}, cidB: {Name: "x_y-z"}, cidC: {Name: "<b>boo</b>"}}}
	h := withHandles(t, src)
	h.v1.set("tidal-power", jsonReply(200, communityArticle("tidal-power", cidA, cidB, cidC)))
	out := h.get("/wiki/tidal-power.md").Body.String()
	want := `Agent submitted · Contributed by \@\_a\_, \@x\_y\-z, a community member`
	if !strings.Contains(out, "\n"+want+"\n") {
		t.Fatalf("attribution line missing in\n%s", out)
	}
	mdSafe(t, "handles", out, "https://docs.example.org/guide", "https://news.example.com/item?id=7", "https://cosift.example/wiki/tidal-power")
}

func TestMarkdownFormCarriesTheLicenceAndLinksOnlyWhatTheCodeBuilds(t *testing.T) {
	h := newHarness(t)
	h.v1.set("tidal-power", jsonReply(200, published("tidal-power", "Tidal power")))
	out := h.get("/wiki/tidal-power.md").Body.String()
	for _, want := range []string{
		"# Tidal power\n\nAgent submitted\n\n",
		"## Sources\n",
		"- Guide \\(docs\\.example\\.org\\) <https://docs.example.org/guide>\n  - \\[1\\] A verbatim quote\\.\n",
		"\nThis article's text is available under the Creative Commons Attribution-ShareAlike 4.0 International licence (CC BY-SA 4.0): <https://creativecommons.org/licenses/by-sa/4.0/>\n",
		"\nCanonical page: <https://cosift.example/wiki/tidal-power>\n",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in\n%s", want, out)
		}
	}
	mdSafe(t, "plain", out, "https://docs.example.org/guide", "https://news.example.com/item?id=7", "https://cosift.example/wiki/tidal-power")
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
		"- T\\*1 \\(a\\.example\\) <https://a.example/page>\n  - \\[1\\] First \\<b\\>quote\\<\\/b\\>\\.\n  - \\[3\\] Third\\.\n",
		"- T\\*2 \\(a\\.example\\)\n  - \\[2\\] Second\\.\n",
		"- T\\*4 \\(a\\.example\\)\n  - \\[4\\] Fourth\\.\n",
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

func TestMdTextEscapesEveryASCIIPunctuationCharacter(t *testing.T) {
	for c := '!'; c <= '~'; c++ {
		want := "a" + string(c) + "b"
		if c < '0' || c > '9' && c < 'A' || c > 'Z' && c < 'a' || c > 'z' {
			want = `a\` + string(c) + "b"
		}
		if got := mdText("a" + string(c) + "b"); got != want {
			t.Errorf("%q -> %q, want %q", c, got, want)
		}
	}
	if got, want := mdText("é · — “q” 100%"), `é · — “q” 100\%`; got != want {
		t.Errorf("non-ASCII text: %q, want %q", got, want)
	}
}

func TestMdTextFoldsBreaksAndDropsControls(t *testing.T) {
	for in, want := range map[string]string{
		"a\rb":              "a b",
		"a\r\nb":            "a b",
		"a\nb":              "a b",
		"a\u2028b\u2029c":   "a b c",
		"a\t b   c":         "a b c",
		"  a  ":             "a",
		"\u00a0\u3000a":     "a",
		"a\x00b\x1bc\x7fd":  "abcd",
		"a\u202etxet\u2066": "atxet",
		"a\u200bb\ufeffc":   "abc",
		"\r\n\t ":           "",
	} {
		if got := mdText(in); got != want {
			t.Errorf("%q -> %q, want %q", in, got, want)
		}
	}
}

func TestMdLineKeepsAtMostThreeSpacesOfIndentation(t *testing.T) {
	for n := 0; n <= 8; n++ {
		if got, want := mdLine(strings.Repeat(" ", n)+"x"), strings.Repeat(" ", min(n, 3))+"x"; got != want {
			t.Errorf("%d spaces -> %q, want %q", n, got, want)
		}
	}
	for in, want := range map[string]string{"\tx": "   x", " \tx": "   x", "\t\t x": "   x", "  \u00a0 x": "  x", "   ": "", "": ""} {
		if got := mdLine(in); got != want {
			t.Errorf("%q -> %q, want %q", in, got, want)
		}
	}
}

func TestMdBodyBuildsOnlyHeadingsItemsAndParagraphs(t *testing.T) {
	for _, tc := range []struct{ name, in, want string }{
		{"headings are normalised like the page", "### Early\n\n# Top\n\n###### Deep\n\n## Real\n\n### Sub\n\n#\n\nText\n=====\n\n---\n",
			"## Early\n\n## Top\n\n### Deep\n\n## Real\n\n### Sub\n\nText\n\\=\\=\\=\\=\\=\n\n\\-\\-\\-"},
		{"heading text is escaped", "## A *b* `c` [d](e)", "## A \\*b\\* \\`c\\` \\[d\\]\\(e\\)"},
		{"items and paragraphs", "Intro:\n- a <b>\n* b\n+ c\n1. d\n2) e\n  - nested\nTail line\n\nNext",
			"Intro\\:\n\n- a \\<b\\>\n- b\n- c\n- d\n- e\n- nested\nTail line\n\nNext"},
		{"an ordered marker other than 1 does not interrupt a paragraph", "Born in\n1990. A year\n1. one\n2. two", "Born in\n1990\\. A year\n\n- one\n- two"},
		{"lookalike markers stay text", "-x\n--\n1.x\n10 apples\n1234567890. y", "\\-x\n\\-\\-\n1\\.x\n10 apples\n1234567890\\. y"},
		{"a bare marker is dropped", "-\ntext", "text"},
		{"a same-line code span with a pipe", "`a|b` <i> `c`", "\\`a\\|b\\` \\<i\\> \\`c\\`"},
		{"a fence at indent three with a de-indented closer", "   ```\n    ```\n<img src=x onerror=alert(7)>\n```\nafter",
			"   \\`\\`\\`\n\\<img src\\=x onerror\\=alert\\(7\\)\\>\n\nafter"},
		{"a fence is plain text", "Use `Vec<T>`:\n\n```rust\nlet v: Vec<u8> = <x>;\n[a](b)\n```\n\nAfter",
			"Use \\`Vec\\<T\\>\\`\\:\n\nlet v\\: Vec\\<u8\\> \\= \\<x\\>\\;\n\\[a\\]\\(b\\)\n\nAfter"},
		{"an unclosed fence", "Text [1].\n\n```\n<b>open", "Text \\[1\\]\\.\n\n\\<b\\>open"},
		{"a heading inside a fence is text", "~~~\n# not a heading\n- not an item\n~~~", "\\# not a heading\n\\- not an item"},
		{"indentation is capped", "a\n    b\n\tc\n  d", "a\n   b\n   c\n  d"},
		{"quotes, tables, definitions, setext rules and html", "> q\n| a | b |\n|---|---|\n[x]: u\nT\n==\n<div>",
			"\\> q\n\\| a \\| b \\|\n\\|\\-\\-\\-\\|\\-\\-\\-\\|\n\\[x\\]\\: u\nT\n\\=\\=\n\\<div\\>"},
		{"escapes in the source stand for their characters", `\<b\> \\ \a`, `\<b\> \\ \\a`},
		{"entities", "&lt;b&gt; &#60;", "\\&lt\\;b\\&gt\\; \\&\\#60\\;"},
		{"CR and CRLF end lines", "a\r\nb\rc", "a\nb\nc"},
		{"an empty text", " \n\t\n", ""},
	} {
		if got := mdBody(tc.in); got != tc.want {
			t.Errorf("%s: %q\n got %q\nwant %q", tc.name, tc.in, got, tc.want)
		}
	}
}

var mdTokens = []string{
	"```", "~~~", "`", "``", "<script>", "</", "<https://e.example/>", "](", "]:", "[1]", "![", "# ", "## ", "###### ", "#", "- ", "* ", "+ ", "1. ", "2) ", "10. ", "> ", "|", "---", "===", "***", "___",
	"    ", "  ", " ", "\t", "\r", "\n", "\r\n", "\n\n", "\u2028", "\u00a0", "\u200b", "\u202e", "\x00", "\x1b", "&lt;", "&#60;", "\\", "\\\\", "word", "Word9", "é", "http://x.example/a_b", "$", "~", "^", "{", "}", "@",
}

func randomMarkdown(rng *rand.Rand) string {
	var b strings.Builder
	for range 1 + rng.IntN(24) {
		b.WriteString(mdTokens[rng.IntN(len(mdTokens))])
		if rng.IntN(3) == 0 {
			b.WriteByte(' ')
		}
	}
	return b.String()
}

func TestMdTextAndBodyAreSafeForRandomInput(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	fold := func(s string) string {
		var b strings.Builder
		for _, r := range s {
			switch {
			case unicode.IsSpace(r) || r == '\u2028' || r == '\u2029':
				b.WriteByte(' ')
			case unicode.Is(unicode.Cc, r) || unicode.Is(unicode.Cf, r):
			default:
				b.WriteRune(r)
			}
		}
		return strings.Join(strings.Fields(b.String()), " ")
	}
	for range 4000 {
		in := randomMarkdown(rng)
		text := mdText(in)
		if p := mdProblem("- "+text, nil); p != "" || strings.ContainsAny(text, "\r\n") {
			t.Fatalf("mdText(%q) = %q: %s", in, text, p)
		}
		if got, want := unescapeMD(text), fold(in); got != want {
			t.Fatalf("mdText(%q) = %q reads back as %q, want %q", in, text, got, want)
		}
		for j, line := range strings.Split(mdBody(in), "\n") {
			if p := mdProblem(line, nil); p != "" || strings.HasPrefix(line, "# ") {
				t.Fatalf("mdBody(%q) line %d %q: %s", in, j, line, p)
			}
		}
	}
}

func TestAutolinkAcceptsOnlyStrictURLs(t *testing.T) {
	for raw, want := range map[string]bool{
		"https://a.example/p":              true,
		"http://a.example":                 true,
		"https://a.example/a%20b?q=1#f":    true,
		"HTTPS://A.EXAMPLE/":               true,
		"https://a_b.example/p":            true,
		"https://user:pw@a.example/x":      true,
		"javascript:alert(1)":              false,
		"data:text/html,x":                 false,
		"ftp://a.example/":                 false,
		"//a.example/":                     false,
		"https://":                         false,
		"https:///x":                       false,
		"https://:80/x":                    false,
		"https://a.example/<x>":            false,
		"https://a.example/a b":            false,
		"https://a.example/a\tb":           false,
		"https://a.example/a\"b":           false,
		"https://a.example/a\\b":           false,
		"https://a.example/a|b":            false,
		"https://a.example/a^b":            false,
		"https://a.example/a`b":            false,
		"https://a.example/{x}":            false,
		"https://a.example/\u00e9":         false,
		"https://a.example/\u202e":         false,
		"mailto:a@a.example":               false,
		"https://a.example/>":              false,
		"https://a.example/a>b<c":          false,
		"https://a.example/\x00":           false,
		"https://a.example/path?x=<script": false,
	} {
		got, ok := autolink(raw)
		if ok != want || (ok && got != "<"+raw+">") {
			t.Errorf("%q -> %q, %v; want ok=%v", raw, got, ok, want)
		}
	}
}

func TestMarkdownFormFoldsLineBreaksInTitleAndCitation(t *testing.T) {
	h := newHarness(t)
	h.v1.set("breaks", jsonReply(200, with(published("breaks", "Two\r\nlines\rand\nmore"), "citations", []any{
		map[string]any{"n": 1, "url": "https://a.example/p", "title": "T\r\n# x", "host": "a.example", "quote": "q\r- y\n1. z"},
	})))
	out := h.get("/wiki/breaks.md").Body.String()
	for _, want := range []string{"# Two lines and more\n", "- T \\# x \\(a\\.example\\) <https://a.example/p>\n  - \\[1\\] q \\- y 1\\. z\n"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in\n%s", want, out)
		}
	}
	mdSafe(t, "breaks", out, "https://a.example/p", "https://cosift.example/wiki/breaks")
}

func TestMarkdownFormEscapesTheCitationHost(t *testing.T) {
	h := newHarness(t)
	h.v1.set("host", jsonReply(200, with(published("host", "Host"), "citations", []any{
		map[string]any{"n": 1, "url": "https://a_b.example/p", "title": "T", "host": "a_b*c.example", "quote": "q"},
	})))
	if out := h.get("/wiki/host.md").Body.String(); !strings.Contains(out, "- T \\(a\\_b\\*c\\.example\\) <https://a_b.example/p>\n") {
		t.Fatalf("host not escaped:\n%s", out)
	}
}
