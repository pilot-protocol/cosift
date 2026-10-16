//go:build cmark

package wiki

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"os/exec"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/html"
)

var cmarkArticleTags = map[string]bool{"h1": true, "h2": true, "h3": true, "p": true, "ul": true, "li": true, "a": true}

var cmarkLlmsTags = map[string]bool{"h1": true, "h2": true, "p": true, "blockquote": true, "ul": true, "li": true, "a": true}

type cmarkFlavour struct {
	bin  string
	args []string
}

var cmarkFlavours = []cmarkFlavour{
	{"cmark", []string{"--unsafe"}},
	{"cmark-gfm", []string{"--unsafe", "-e", "table", "-e", "strikethrough", "-e", "autolink", "-e", "tasklist"}},
}

func cmarkRender(t *testing.T, f cmarkFlavour, md string) string {
	t.Helper()
	cmd := exec.Command(f.bin, f.args...)
	cmd.Stdin = strings.NewReader(md)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s: %v: %s", f.bin, err, errb.String())
	}
	return out.String()
}

// cmarkInspect fails unless the rendered HTML holds only the allowed tags, links only to the allowed URLs and, once whitespace is folded, every visible string.
func cmarkInspect(t *testing.T, name, rendered string, tags map[string]bool, urls map[string]bool, visible []string) {
	t.Helper()
	z := html.NewTokenizer(strings.NewReader(rendered))
	var text strings.Builder
	h1 := 0
	for {
		tt := z.Next()
		if tt == html.ErrorToken {
			if z.Err() != io.EOF {
				t.Fatalf("%s: %v", name, z.Err())
			}
			break
		}
		tok := z.Token()
		switch tt {
		case html.TextToken:
			text.WriteString(tok.Data)
		case html.StartTagToken, html.EndTagToken, html.SelfClosingTagToken:
			if !tags[tok.Data] || tt == html.SelfClosingTagToken {
				t.Fatalf("%s: element %v in\n%s", name, tok, rendered)
			}
			if tt == html.EndTagToken {
				continue
			}
			if tok.Data == "h1" {
				h1++
			}
			if tok.Data != "a" {
				if len(tok.Attr) != 0 {
					t.Fatalf("%s: attributes on %v", name, tok)
				}
				continue
			}
			if len(tok.Attr) != 1 || tok.Attr[0].Key != "href" || !urls[tok.Attr[0].Val] {
				t.Fatalf("%s: link %v", name, tok)
			}
		default:
			t.Fatalf("%s: %v token %q in\n%s", name, tt, tok.String(), rendered)
		}
	}
	if tags["h1"] && h1 != 1 {
		t.Fatalf("%s: %d h1 elements", name, h1)
	}
	plain := strings.Join(strings.Fields(text.String()), " ")
	for _, v := range visible {
		if !strings.Contains(plain, strings.Join(strings.Fields(v), " ")) {
			t.Fatalf("%s: lost visible text %q in\n%s", name, v, plain)
		}
	}
}

func urlSet(urls ...string) map[string]bool {
	m := map[string]bool{}
	for _, u := range urls {
		m[u] = true
	}
	return m
}

func TestCmarkRendersTheInjectionCorpusAsPlainText(t *testing.T) {
	h := newHarness(t)
	h.refresh()
	cites := func() []any {
		return []any{
			map[string]any{"n": 1, "url": corpusCites[0], "title": "One", "host": "docs.example.org", "quote": "q"},
			map[string]any{"n": 2, "url": corpusCites[1], "title": "Two", "host": "docs.example.org", "quote": "q"},
		}
	}
	allowed := urlSet(corpusCites[0], licenceURL)
	for _, f := range cmarkFlavours {
		for i, tc := range loadCorpus(t) {
			for _, field := range []string{"body_md", "title", "citation"} {
				t.Run(f.bin+"/"+tc.Name+"/"+field, func(t *testing.T) {
					slug := fmt.Sprintf("case-%d-%s", i, strings.ReplaceAll(field, "_", "-"))
					rec := with(published(slug, "Case"), "citations", cites())
					visible := tc.Visible
					switch field {
					case "body_md":
						rec = with(rec, "body_md", tc.Text)
					case "title":
						rec = with(rec, "title", tc.Text)
						visible = nil
					case "citation":
						rec = with(rec, "citations", []any{map[string]any{"n": 1, "url": corpusCites[0], "title": tc.Text, "host": tc.Text, "quote": tc.Text}})
						visible = nil
					}
					h.v1.set(slug, jsonReply(200, rec))
					h.clock.Add(time.Second)
					got := h.get("/wiki/"+slug+".md", "RemoteAddr", fmt.Sprintf("192.0.2.%d:1", i%200+1))
					if got.Code != 200 {
						t.Fatalf("%d", got.Code)
					}
					urls := urlSet("https://cosift.example/wiki/" + slug)
					for u := range allowed {
						urls[u] = true
					}
					cmarkInspect(t, tc.Name, cmarkRender(t, f, got.Body.String()), cmarkArticleTags, urls, visible)
				})
			}
		}
	}
}

func TestCmarkRendersTheGoldenPagesAsPlainText(t *testing.T) {
	src := &fakeHandles{known: map[string]Handle{cidA: {Name: "tidal-fan"}, cidB: {Name: "rusty", Banned: true}, cidC: {Name: "_edge_"}}}
	h := withHandles(t, src)
	h.v1.set("rust-async-runtimes", jsonReply(200, with(rustArticle(), "origin", "community", "contributors", []any{cidA, cidB, cidC})))
	md := h.get("/wiki/rust-async-runtimes.md").Body.String()
	urls := urlSet("https://docs.example.org/async/runtime-tutorial", "https://docs.example.org/async/executors", "https://blog.example.net/work-stealing",
		licenceURL, "https://cosift.example/wiki/rust-async-runtimes")
	for _, f := range cmarkFlavours {
		cmarkInspect(t, f.bin+"/golden", cmarkRender(t, f, md), cmarkArticleTags, urls,
			[]string{"Rust async runtimes", "Agent submitted · Contributed by @tidal-fan, a community member, @_edge_", "An `async fn` in Rust returns", "[1] A runtime drives futures to completion"})
	}
}

func TestCmarkRendersLlmsTxtAsPlainText(t *testing.T) {
	h := newHarness(t)
	var items []map[string]any
	for i, tc := range loadCorpus(t) {
		items = append(items, item(published(fmt.Sprintf("case-%03d", i), tc.Text)))
	}
	h.v1.list(items...)
	h.refresh()
	out := h.get("/llms.txt").Body.String()
	urls := urlSet(licenceURL, "https://cosift.example/wiki/style.md", "https://cosift.example/wiki/terms", "https://cosift.example/wiki/wanted")
	for i := range items {
		urls[fmt.Sprintf("https://cosift.example/wiki/case-%03d.md", i)] = true
	}
	for _, f := range cmarkFlavours {
		cmarkInspect(t, f.bin+"/llms.txt", cmarkRender(t, f, out), cmarkLlmsTags, urls, nil)
	}
}

func TestCmarkRendersRandomTextAsPlainText(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 5))
	for range 1500 {
		in := randomMarkdown(rng)
		doc := "# " + mdText(in) + "\n\n" + mdBody(in) + "\n"
		for _, f := range cmarkFlavours {
			cmarkInspect(t, fmt.Sprintf("%s %q", f.bin, in), cmarkRender(t, f, doc), cmarkArticleTags, nil, nil)
		}
	}
}
