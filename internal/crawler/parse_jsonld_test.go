package crawler

import (
	"strings"
	"testing"
)

func TestExtractJSONLDSingleObject(t *testing.T) {
	raw := []byte(`{
		"@context": "https://schema.org",
		"@type": "Article",
		"name": "Article Name",
		"headline": "Compact Article Headline",
		"description": "A short article description.",
		"keywords": "go, programming, concurrency",
		"author": {"@type": "Person", "name": "Jane Doe"}
	}`)
	f := extractJSONLD(raw)
	if f.Name != "Article Name" {
		t.Errorf("name: %q", f.Name)
	}
	if f.Headline != "Compact Article Headline" {
		t.Errorf("headline: %q", f.Headline)
	}
	if f.Description != "A short article description." {
		t.Errorf("description: %q", f.Description)
	}
	if f.Keywords != "go, programming, concurrency" {
		t.Errorf("keywords: %q", f.Keywords)
	}
	if f.AuthorName != "Jane Doe" {
		t.Errorf("authorName: %q", f.AuthorName)
	}
}

func TestExtractJSONLDStringAuthor(t *testing.T) {
	// Some sites use a bare string for author rather than a Person object.
	raw := []byte(`{"@type":"Article","name":"Foo","author":"John Smith"}`)
	f := extractJSONLD(raw)
	if f.AuthorName != "John Smith" {
		t.Errorf("string author: %q", f.AuthorName)
	}
}

func TestExtractJSONLDKeywordsAsArray(t *testing.T) {
	// keywords can be either a comma-string OR a JSON array.
	raw := []byte(`{"@type":"Article","keywords":["go","programming","concurrency"]}`)
	f := extractJSONLD(raw)
	if f.Keywords != "go, programming, concurrency" {
		t.Errorf("array keywords: %q", f.Keywords)
	}
}

func TestExtractJSONLDArrayShape(t *testing.T) {
	// Top-level array of multiple objects — common when the page describes
	// both the article AND its publisher.
	raw := []byte(`[
		{"@type":"Article","headline":"Article Headline","description":"Article desc"},
		{"@type":"Organization","name":"Publisher Name"}
	]`)
	f := extractJSONLD(raw)
	if f.Headline != "Article Headline" {
		t.Errorf("array headline: %q", f.Headline)
	}
	if f.Description != "Article desc" {
		t.Errorf("array description: %q", f.Description)
	}
	// Name takes the first non-empty value found across the array — Publisher's name here.
	if f.Name != "Publisher Name" {
		t.Errorf("array name (publisher): %q", f.Name)
	}
}

func TestExtractJSONLDGraphShape(t *testing.T) {
	// @graph nesting — Yoast SEO and other CMSs use this shape heavily.
	raw := []byte(`{
		"@context":"https://schema.org",
		"@graph":[
			{"@type":"WebPage","name":"Web Page","description":"Page summary"},
			{"@type":"Article","headline":"Article Headline"}
		]
	}`)
	f := extractJSONLD(raw)
	if f.Headline != "Article Headline" {
		t.Errorf("@graph headline: %q", f.Headline)
	}
	if f.Description != "Page summary" {
		t.Errorf("@graph description: %q", f.Description)
	}
}

func TestExtractJSONLDMalformedTolerated(t *testing.T) {
	// Invalid JSON should yield empty fields, not panic.
	raw := []byte(`{this is not valid json}`)
	f := extractJSONLD(raw)
	if f.Name != "" || f.Headline != "" || f.Description != "" {
		t.Errorf("malformed JSON should return zero-value, got %+v", f)
	}
}

// End-to-end: a page with a JSON-LD block should have its headline used as
// the title and its keywords/author prepended to indexed text.
func TestParseWithJSONLDBlock(t *testing.T) {
	html := `<!doctype html>
<html><head>
<title>Padded Site Title - Foo Site</title>
<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@type": "Article",
  "headline": "The Clean Article Headline",
  "description": "An article about tessellation patterns.",
  "keywords": "tessellation, mathematics, art",
  "author": {"@type": "Person", "name": "Dr. Octave Demanche"}
}
</script>
</head><body><p>Body content about geometry.</p></body></html>`
	p, err := Parse([]byte(html), "https://x/")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if p.Title != "The Clean Article Headline" {
		t.Errorf("title: got %q want JSON-LD headline", p.Title)
	}
	// Description, keywords, author all prepended to text.
	for _, want := range []string{"tessellation", "Dr. Octave Demanche", "mathematics", "Body content about geometry"} {
		if !strings.Contains(p.Text, want) {
			t.Errorf("missing %q in text. got: %q", want, p.Text)
		}
	}
}

// JSON-LD headline must lose to og:title (og: is more author-curated for
// previews than headline-which-may-be-article-specific-but-also-may-not-be).
func TestParseOGTitleBeatsJSONLDHeadline(t *testing.T) {
	html := `<!doctype html>
<html><head>
<title>Bare Title</title>
<meta property="og:title" content="OG Title Wins">
<script type="application/ld+json">{"@type":"Article","headline":"JSON-LD Headline"}</script>
</head><body>body</body></html>`
	p, _ := Parse([]byte(html), "https://x/")
	if p.Title != "OG Title Wins" {
		t.Errorf("og:title should beat JSON-LD headline, got: %q", p.Title)
	}
}

// JSON-LD headline must beat twitter:title (article-specific > social-platform).
func TestParseJSONLDHeadlineBeatsTwitterTitle(t *testing.T) {
	html := `<!doctype html>
<html><head>
<title>Bare</title>
<meta name="twitter:title" content="Twitter Title">
<script type="application/ld+json">{"@type":"Article","headline":"JSON-LD Headline Wins"}</script>
</head><body>body</body></html>`
	p, _ := Parse([]byte(html), "https://x/")
	if p.Title != "JSON-LD Headline Wins" {
		t.Errorf("JSON-LD headline should beat twitter:title, got: %q", p.Title)
	}
}

// Multiple JSON-LD blocks on one page (common with Yoast + manual additions):
// the first non-empty value wins per field.
func TestParseMultipleJSONLDBlocks(t *testing.T) {
	html := `<!doctype html>
<html><head>
<script type="application/ld+json">{"@type":"BreadcrumbList","itemListElement":[]}</script>
<script type="application/ld+json">{"@type":"Article","headline":"First Block Headline"}</script>
<script type="application/ld+json">{"@type":"Article","headline":"Second Block Headline"}</script>
</head><body>body</body></html>`
	p, _ := Parse([]byte(html), "https://x/")
	if p.Title != "First Block Headline" {
		t.Errorf("first non-empty wins; got: %q", p.Title)
	}
}
