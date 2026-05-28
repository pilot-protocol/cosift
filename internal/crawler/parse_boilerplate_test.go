package crawler

import (
	"strings"
	"testing"
)

// TestParseModernFeatureFlagClassesNotBoilerplate locks in the fix for the
// catastrophic class of bug where a substring-matching boilerplate filter
// dropped the entire document subtree when a modern feature-flag class name
// happened to contain a boilerplate token as a hyphen-bounded sub-word.
//
// Wikipedia's <html> element carries:
//
//	class="...vector-feature-main-menu-pinned-disabled..."
//
// Pre-fix, substring match "menu" against that class string returned true and
// the entire <html> subtree was dropped — every page produced empty text.
// Post-fix, token-level match treats `vector-feature-main-menu-pinned-disabled`
// as one token and only matches when that token equals a boilerplate name
// outright.
func TestParseModernFeatureFlagClassesNotBoilerplate(t *testing.T) {
	// Mimics the Wikipedia-shaped structure that triggered the bug: feature
	// flags on <html>, real <header>/<nav>/<aside> chrome to drop, real <main>
	// to keep.
	body := []byte(`<!doctype html>
<html class="vector-feature-main-menu-pinned-disabled vector-feature-language-in-main-page-header-disabled some-flag">
<head><title>Page About Goroutines</title></head>
<body class="ns-0 page-Goroutines">
  <header class="vector-header-container">Top chrome should be dropped.</header>
  <nav class="main-nav">Nav chrome should be dropped.</nav>
  <main id="content" class="mw-body">
    <h1>Goroutines</h1>
    <p>A goroutine is a lightweight thread managed by the Go runtime.</p>
    <p>Channels coordinate communication between goroutines.</p>
    <a href="/wiki/Channel_(programming)">See channels.</a>
  </main>
  <footer class="site-footer">Footer chrome should be dropped.</footer>
</body>
</html>`)

	p, err := Parse(body, "https://example.com/wiki/Goroutines")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if p.Title == "" {
		t.Errorf("expected non-empty title; got empty")
	}
	if !strings.Contains(p.Title, "Goroutines") {
		t.Errorf("title should mention Goroutines; got %q", p.Title)
	}
	if strings.TrimSpace(p.Text) == "" {
		t.Fatalf("parser returned empty text — feature-flag class on <html> still drops the document tree")
	}
	for _, kw := range []string{"lightweight thread", "Channels"} {
		if !strings.Contains(p.Text, kw) {
			t.Errorf("expected %q in extracted text; got: %q", kw, p.Text)
		}
	}
	// Boilerplate chrome that we DO want to drop must not leak into the text.
	for _, drop := range []string{"Top chrome", "Footer chrome", "Nav chrome"} {
		if strings.Contains(p.Text, drop) {
			t.Errorf("expected %q to be dropped as chrome; got: %q", drop, p.Text)
		}
	}
	if len(p.Links) == 0 {
		t.Errorf("expected at least one link from <main>; got 0")
	}
}

// TestBoilerplateTokenMatchExactness — direct unit test of hasBoilerplateAttr.
// Token-level match: classes that contain a boilerplate keyword AS A SUB-WORD
// must NOT trigger; classes whose whole-space-separated token equals one of
// the keywords MUST trigger.
func TestBoilerplateTokenMatchExactness(t *testing.T) {
	cases := []struct {
		class     string
		wantDrop  bool
		rationale string
	}{
		{"nav", true, "exact match"},
		{"navigation", true, "exact match against navigation"},
		{"main-content nav", true, "second token matches"},
		{"main-content navigation", true, "second token matches"},
		{"footer", true, "exact match"},
		{"site-footer", true, "exact match (we list site-footer)"},

		// These caused the bug — must NOT trigger.
		{"vector-feature-main-menu-pinned-disabled", false, "hyphen-bounded sub-word"},
		{"vector-feature-language-in-main-page-header-disabled", false, "hyphen-bounded sub-word"},
		{"navbar-link-active", false, "hyphen-bounded sub-word containing navbar"},
		{"site-banner-wrapper", false, "hyphen-bounded sub-word containing banner"},
		{"adverb-display", false, "must not match 'ad'/'adv'"},
		{"mw-body", false, "Wikipedia main content container"},
		{"page-Web_crawler", false, "page title in class"},
	}
	for _, c := range cases {
		// Use a minimal HTML node to exercise hasBoilerplateAttr.
		body := []byte(`<!doctype html><html><body><div class="` + c.class + `">probe text</div></body></html>`)
		p, err := Parse(body, "https://example.com/")
		if err != nil {
			t.Fatalf("parse %q: %v", c.class, err)
		}
		got := !strings.Contains(p.Text, "probe text")
		if got != c.wantDrop {
			verb := "should NOT have been"
			if c.wantDrop {
				verb = "should have been"
			}
			t.Errorf("class=%q %s dropped (%s); text=%q", c.class, verb, c.rationale, p.Text)
		}
	}
}
