package crawler

import (
	"strings"
	"testing"
)

func TestParseOGTitlePreferredOverBareTitle(t *testing.T) {
	html := `<!doctype html>
<html><head>
<title>Foo - Some Site Name</title>
<meta property="og:title" content="Foo: A Practical Guide">
</head><body>Article body content here.</body></html>`
	p, err := Parse([]byte(html), "https://x/")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if p.Title != "Foo: A Practical Guide" {
		t.Errorf("title: got %q want %q (og:title should override bare <title>)", p.Title, "Foo: A Practical Guide")
	}
}

func TestParseTwitterTitleFallback(t *testing.T) {
	// No og:title, no <title> — twitter:title should fill in.
	html := `<!doctype html>
<html><head><meta name="twitter:title" content="Page Title via Twitter Card"></head><body>Body.</body></html>`
	p, _ := Parse([]byte(html), "https://x/")
	if p.Title != "Page Title via Twitter Card" {
		t.Errorf("title: got %q want twitter card value", p.Title)
	}
}

func TestParseBareTitlePreservedWhenNoOG(t *testing.T) {
	// Backwards-compat: bare <title> still works when no meta tags present.
	html := `<!doctype html><html><head><title>Just A Title</title></head><body>Body.</body></html>`
	p, _ := Parse([]byte(html), "https://x/")
	if p.Title != "Just A Title" {
		t.Errorf("title: got %q want %q", p.Title, "Just A Title")
	}
}

func TestParseOGDescriptionPrependedToText(t *testing.T) {
	html := `<!doctype html>
<html><head>
<title>Tutorial</title>
<meta property="og:description" content="A concise summary mentioning quantum entanglement and unique product names.">
</head><body><p>Body discusses physics.</p></body></html>`
	p, err := Parse([]byte(html), "https://x/")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	// Description prepended → searching for "quantum entanglement" finds the doc
	// even though body only says "physics".
	if !strings.Contains(p.Text, "quantum entanglement") {
		t.Errorf("description should be prepended to text. got: %q", p.Text)
	}
	if !strings.Contains(p.Text, "Body discusses physics") {
		t.Errorf("body text should still be present. got: %q", p.Text)
	}
}

func TestParseDescriptionHierarchy(t *testing.T) {
	// og:description wins over twitter:description and meta name="description".
	html := `<!doctype html>
<html><head>
<meta name="description" content="standard description">
<meta name="twitter:description" content="twitter description">
<meta property="og:description" content="og description preferred">
</head><body>body</body></html>`
	p, _ := Parse([]byte(html), "https://x/")
	if !strings.Contains(p.Text, "og description preferred") {
		t.Errorf("og:description should win the hierarchy. got: %q", p.Text)
	}
	// Other descriptions should NOT also appear (they didn't make it through the hierarchy).
	if strings.Contains(p.Text, "standard description") || strings.Contains(p.Text, "twitter description") {
		t.Errorf("lower-priority descriptions leaked: %q", p.Text)
	}
}

func TestParseTwitterDescriptionFallback(t *testing.T) {
	// No og:description — twitter falls through.
	html := `<!doctype html>
<html><head><meta name="twitter:description" content="twitter fallback works"></head><body>body</body></html>`
	p, _ := Parse([]byte(html), "https://x/")
	if !strings.Contains(p.Text, "twitter fallback works") {
		t.Errorf("twitter:description should be used when og missing. got: %q", p.Text)
	}
}

func TestParseMetaDescriptionFallback(t *testing.T) {
	html := `<!doctype html>
<html><head><meta name="description" content="standard meta description"></head><body>body</body></html>`
	p, _ := Parse([]byte(html), "https://x/")
	if !strings.Contains(p.Text, "standard meta description") {
		t.Errorf("meta name=description should be used as last fallback. got: %q", p.Text)
	}
}

func TestParseEmptyMetaContentSkipped(t *testing.T) {
	// <meta property="og:title" content=""> should NOT clobber the bare <title>.
	html := `<!doctype html>
<html><head>
<title>Real Title</title>
<meta property="og:title" content="">
</head><body>body</body></html>`
	p, _ := Parse([]byte(html), "https://x/")
	if p.Title != "Real Title" {
		t.Errorf("empty og:title content should fall through; got %q", p.Title)
	}
}

// Iter 155: og:image preferred over twitter:image; both preferred over
// JSON-LD `image`. Absence everywhere → empty ParsedDoc.Image.
func TestParseImageOgPreferred(t *testing.T) {
	html := `<!doctype html>
<html><head>
<meta property="og:image" content="https://x/og.jpg">
<meta name="twitter:image" content="https://x/twitter.jpg">
</head><body>body</body></html>`
	p, _ := Parse([]byte(html), "https://x/")
	if p.Image != "https://x/og.jpg" {
		t.Errorf("og:image should win: got %q", p.Image)
	}
}

func TestParseImageTwitterFallback(t *testing.T) {
	html := `<!doctype html>
<html><head>
<meta name="twitter:image" content="https://x/twitter.jpg">
</head><body>body</body></html>`
	p, _ := Parse([]byte(html), "https://x/")
	if p.Image != "https://x/twitter.jpg" {
		t.Errorf("twitter:image fallback failed: got %q", p.Image)
	}
}

func TestParseImageJSONLDFallback(t *testing.T) {
	// No og: or twitter:; JSON-LD `image` should fill in. Test three shapes:
	// string, object with url, array of strings (each in its own subtest doc).
	cases := []struct {
		name, jsonld, want string
	}{
		{"string", `{"@type":"Article","image":"https://x/ld-str.jpg"}`, "https://x/ld-str.jpg"},
		{"object", `{"@type":"Article","image":{"@type":"ImageObject","url":"https://x/ld-obj.jpg"}}`, "https://x/ld-obj.jpg"},
		{"array", `{"@type":"Article","image":["https://x/first.jpg","https://x/second.jpg"]}`, "https://x/first.jpg"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			html := `<!doctype html><html><head>
<script type="application/ld+json">` + tc.jsonld + `</script>
</head><body>body</body></html>`
			p, _ := Parse([]byte(html), "https://x/")
			if p.Image != tc.want {
				t.Errorf("got %q want %q", p.Image, tc.want)
			}
		})
	}
}

func TestParseImageAbsent(t *testing.T) {
	html := `<!doctype html><html><head><title>no image</title></head><body>body</body></html>`
	p, _ := Parse([]byte(html), "https://x/")
	if p.Image != "" {
		t.Errorf("no image meta should produce empty Image; got %q", p.Image)
	}
}

// Iter 156 — favicon extraction from <link rel="...">.
func TestParseFavicon(t *testing.T) {
	cases := []struct {
		name, html, base, want string
	}{
		{
			"icon-relative",
			`<!doctype html><html><head><link rel="icon" href="/favicon.ico"></head><body>x</body></html>`,
			"https://example.com/article",
			"https://example.com/favicon.ico",
		},
		{
			"shortcut-icon-relative",
			`<!doctype html><html><head><link rel="shortcut icon" href="/static/fav.png"></head><body>x</body></html>`,
			"https://example.com/article",
			"https://example.com/static/fav.png",
		},
		{
			"apple-touch-icon-absolute",
			`<!doctype html><html><head><link rel="apple-touch-icon" href="https://cdn.example.com/apple.png"></head><body>x</body></html>`,
			"https://example.com/article",
			"https://cdn.example.com/apple.png",
		},
		{
			"first-link-wins",
			`<!doctype html><html><head>
<link rel="icon" href="/first.ico">
<link rel="apple-touch-icon" href="/second.png">
</head><body>x</body></html>`,
			"https://example.com/article",
			"https://example.com/first.ico",
		},
		{
			"unrelated-rel-ignored",
			`<!doctype html><html><head><link rel="stylesheet" href="/style.css"><title>t</title></head><body>x</body></html>`,
			"https://example.com/article",
			"",
		},
		{
			"absent",
			`<!doctype html><html><head><title>t</title></head><body>x</body></html>`,
			"https://example.com/article",
			"",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := Parse([]byte(tc.html), tc.base)
			if p.Favicon != tc.want {
				t.Errorf("favicon: got %q want %q", p.Favicon, tc.want)
			}
		})
	}
}
