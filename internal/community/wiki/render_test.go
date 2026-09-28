package wiki

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

var corpusCites = []string{"https://docs.example.org/guide", `https://docs.example.org/a"onmouseover="alert(1)`}

func corpusLinks() map[string]string {
	m := map[string]string{}
	for _, u := range corpusCites {
		if linkable(u) {
			m[u] = u
		}
	}
	return m
}

var bodyTags = map[string]bool{"h2": true, "h3": true, "p": true, "ul": true, "ol": true, "li": true, "pre": true, "code": true, "strong": true, "em": true, "a": true, "sup": true}

// inert parses a rendered body and fails unless it holds only the allow-listed
// elements and attributes, with every href a citation URL or #cite-n.
func inert(t *testing.T, fragment string, cites []string) (text string, links int) {
	t.Helper()
	nodes, err := html.ParseFragment(strings.NewReader(fragment), &html.Node{Type: html.ElementNode, Data: "div", DataAtom: atom.Div})
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	var walk func(n *html.Node)
	walk = func(n *html.Node) {
		switch n.Type {
		case html.TextNode:
			b.WriteString(n.Data)
		case html.ElementNode:
			if !bodyTags[n.Data] {
				t.Fatalf("element <%s> in the body:\n%s", n.Data, fragment)
			}
			for _, a := range n.Attr {
				switch {
				case n.Data == "a" && a.Key == "href":
					external := false
					for _, c := range cites {
						external = external || a.Val == c
					}
					k, err := strconv.Atoi(strings.TrimPrefix(a.Val, "#cite-"))
					internal := strings.HasPrefix(a.Val, "#cite-") && err == nil && k >= 1 && k <= len(cites)
					if !external && !internal {
						t.Fatalf("href %q is neither a citation URL nor #cite-n:\n%s", a.Val, fragment)
					}
					if external {
						links++
						if attr(n, "rel") != "nofollow noopener" {
							t.Fatalf("outbound link without rel=nofollow noopener:\n%s", fragment)
						}
					}
				case n.Data == "a" && a.Key == "rel":
				default:
					t.Fatalf("attribute %s on <%s>:\n%s", a.Key, n.Data, fragment)
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	for _, n := range nodes {
		walk(n)
	}
	return b.String(), links
}

func attr(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}

type corpusCase struct {
	Name    string   `json:"name"`
	Text    string   `json:"text"`
	Visible []string `json:"visible"`
	Links   int      `json:"links"`
}

func loadCorpus(t *testing.T) []corpusCase {
	b, err := os.ReadFile("testdata/injection.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []corpusCase
	if err := json.Unmarshal(b, &cases); err != nil {
		t.Fatal(err)
	}
	var nested strings.Builder
	for i := range 300 {
		nested.WriteString(strings.Repeat("  ", i) + "- level <b>" + strconv.Itoa(i) + "</b>\n")
	}
	var long strings.Builder
	long.WriteString("## Long\n\n")
	for i := range 2000 {
		fmt.Fprintf(&long, "word%d ", i)
		if i%100 == 99 {
			long.WriteString("[1] **strong** *em* `code`\n\n")
		}
	}
	return append(cases,
		corpusCase{Name: "deeply nested list", Text: nested.String(), Visible: []string{"level <b>0</b>", "level <b>299</b>"}},
		corpusCase{Name: "2,000-word body", Text: long.String(), Visible: []string{"word0", "word1999"}},
		corpusCase{Name: "many openers", Text: strings.Repeat("*a [b `c ", 5000), Visible: []string{"*a [b "}},
	)
}

func TestInjectionCorpusRendersInert(t *testing.T) {
	for _, tc := range loadCorpus(t) {
		t.Run(tc.Name, func(t *testing.T) {
			start := time.Now()
			out := string(renderBody(tc.Text, corpusLinks(), len(corpusCites)))
			if time.Since(start) > 2*time.Second {
				t.Fatal("rendering took too long")
			}
			text, links := inert(t, out, corpusCites)
			for _, v := range tc.Visible {
				if !strings.Contains(text, v) {
					t.Fatalf("visible text lost %q:\n%s", v, out)
				}
			}
			if links != tc.Links {
				t.Fatalf("%d outbound links, want %d:\n%s", links, tc.Links, out)
			}
		})
	}
}

func TestRenderShapes(t *testing.T) {
	links := map[string]string{"https://docs.example.org/guide": "https://docs.example.org/guide"}
	cases := []struct{ in, want string }{
		{"Lead text.\n\n## Section\n\nBody [1].", "<p>Lead text.</p>\n<h2>Section</h2>\n<p>Body <sup><a href=\"#cite-1\">[1]</a></sup>.</p>\n"},
		{"# Stray", "<h2>Stray</h2>\n"},
		{"### First\n\n## Then\n\n### Sub\n\n###### Deep", "<h2>First</h2>\n<h2>Then</h2>\n<h3>Sub</h3>\n<h3>Deep</h3>\n"},
		{"## Title ##\n\n## C#", "<h2>Title</h2>\n<h2>C#</h2>\n"},
		{"#hashtag", "<p>#hashtag</p>\n"},
		{"- a\n- b\n  - c\n- d", "<ul>\n<li>a</li>\n<li>b<ul>\n<li>c</li></ul>\n</li>\n<li>d</li></ul>\n"},
		{"* one\n* two", "<ul>\n<li>one</li>\n<li>two</li></ul>\n"},
		{"1. one\n2. two", "<ol>\n<li>one</li>\n<li>two</li></ol>\n"},
		{"- a\n1. b", "<ul>\n<li>a</li></ul>\n<ol>\n<li>b</li></ol>\n"},
		{"- a\n\n- b\n\nAfter", "<ul>\n<li>a</li>\n<li>b</li></ul>\n<p>After</p>\n"},
		{"```go\nfmt.Println(\"<x>\")\n```\nNext", "<pre><code>fmt.Println(&#34;&lt;x&gt;&#34;)</code></pre>\n<p>Next</p>\n"},
		{"**bold** and *em* and `co*de`", "<p><strong>bold</strong> and <em>em</em> and <code>co*de</code></p>\n"},
		{"2 * 3 * 4 and a*b", "<p>2 * 3 * 4 and a*b</p>\n"},
		{"[1] [2] [0]", "<p><sup><a href=\"#cite-1\">[1]</a></sup> [2] [0]</p>\n"},
		{"[guide](https://docs.example.org/guide) [other](https://x.example/)", "<p><a href=\"https://docs.example.org/guide\" rel=\"nofollow noopener\">guide</a> other</p>\n"},
		{"[**bold** *em*](https://docs.example.org/guide)", "<p><a href=\"https://docs.example.org/guide\" rel=\"nofollow noopener\"><strong>bold</strong> <em>em</em></a></p>\n"},
		{"line one\nline two", "<p>line one\nline two</p>\n"},
		{"\\*not em\\*", "<p>*not em*</p>\n"},
		{"## ", ""},
	}
	for _, tc := range cases {
		if got := string(renderBody(tc.in, links, 1)); got != tc.want {
			t.Fatalf("%q:\ngot  %q\nwant %q", tc.in, got, tc.want)
		}
	}
}

func TestCleanStripsControls(t *testing.T) {
	if got := clean("a\x00b\u202ec\u200bd\u2028e\r\nf\tg"); got != "abcd\ne\nf\tg" {
		t.Fatalf("clean: %q", got)
	}
	if got := cleanLine(" a \n\t b\u2066 "); got != "a b" {
		t.Fatalf("cleanLine: %q", got)
	}
}

func TestCleanQuote(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"plain", "A runtime drives futures to completion.", "A runtime drives futures to completion."},
		{"numeric marker verbatim", "rolled oats. [3]", "rolled oats. [3]"},
		{"padded marker verbatim", "rolled oats. [ 51 ]", "rolled oats. [ 51 ]"},
		{"letter marker verbatim", "a claim[a] follows", "a claim[a] follows"},
		{"index verbatim", "sys.argv[1] holds the first argument.", "sys.argv[1] holds the first argument."},
		{"bracketed year verbatim", "The law passed in [2019] changed it.", "The law passed in [2019] changed it."},
		{"citation needed verbatim", "a claim [citation needed] follows", "a claim [citation needed] follows"},
		{"space before comma", "word , next", "word, next"},
		{"space before comma at the end", "word ,", "word,"},
		{"space before period at the end", "the end .", "the end."},
		{"each mark", "a ; b : c ! d ? e", "a; b: c! d? e"},
		{"paren padding", "( word )", "(word)"},
		{"paren then period", "( word ) .", "(word)."},
		{"paren-period run", "the pipeline (CI/CD ).", "the pipeline (CI/CD)."},
		{"comma-paren run", "word ,)", "word,)"},
		{"nested paren run", "( a ( b ))", "(a (b))"},
		{"leading dot word", "built on .NET", "built on .NET"},
		{"file extension", "the extension .json", "the extension .json"},
		{"relative path", "Run ./configure first", "Run ./configure first"},
		{"pseudo-class", "The :hover state", "The :hover state"},
		{"css important", "Use !important sparingly", "Use !important sparingly"},
		{"two-mark operator", "The ?. operator", "The ?. operator"},
		{"null-coalescing operator", "the ?? operator", "the ?? operator"},
		{"scope operator", "the :: operator", "the :: operator"},
		{"ellipsis", "and so on ...", "and so on ..."},
		{"all marker keeps the original", "[51]", "[51]"},
		{"lone mark keeps the original", " . ", "."},
		{"whitespace collapsed", "  a \t b\n c  ", "a b c"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := cleanQuote(tc.in); got != tc.want {
				t.Fatalf("cleanQuote(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestLinkableCitations(t *testing.T) {
	for u, want := range map[string]bool{
		"https://docs.example.org/guide": true, "http://x.example/a?b=c#d": true, `https://x.example/a"b`: true,
		"javascript:alert(1)": false, "JAVASCRIPT:alert(1)": false, "data:text/html,x": false, "vbscript:x": false,
		"//x.example/a": false, "/relative": false, "https://": false, "https://x.example/a b": false,
		"https://x.example/\u00e9": false, "mailto:a@example.org": false, "ftp://x.example/": false, "https:x.example": false,
	} {
		if linkable(u) != want {
			t.Fatalf("linkable(%q) = %v", u, !want)
		}
	}
}

// The renderer must be the package's only unescaped-content conversion.
func TestSourceHasOneTemplateHTMLConversion(t *testing.T) {
	files, _ := filepath.Glob("*.go")
	forbidden := map[string]bool{"JS": true, "JSStr": true, "URL": true, "HTMLAttr": true, "CSS": true, "Srcset": true}
	conversions := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if id, ok := sel.X.(*ast.Ident); ok && id.Name == "template" {
				if forbidden[sel.Sel.Name] {
					t.Fatalf("%s uses template.%s", f, sel.Sel.Name)
				}
			}
			return true
		})
		ast.Inspect(file, func(n ast.Node) bool {
			c, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if sel, ok := c.Fun.(*ast.SelectorExpr); ok {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "template" && sel.Sel.Name == "HTML" {
					conversions++
					if f != "render.go" {
						t.Fatalf("template.HTML conversion in %s", f)
					}
				}
			}
			return true
		})
	}
	if conversions != 1 {
		t.Fatalf("%d template.HTML conversions, want 1", conversions)
	}
}
