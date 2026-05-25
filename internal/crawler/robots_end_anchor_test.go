package crawler

import "testing"

// TestPatternMatchesEndAnchorNoWildcard covers `pattern$` with no `*`.
// `/admin$` matches ONLY /admin, not /admin/foo.
func TestPatternMatchesEndAnchorNoWildcard(t *testing.T) {
	cases := []struct {
		pattern string
		path    string
		want    bool
	}{
		// /admin$ — exact match only
		{"/admin$", "/admin", true},
		{"/admin$", "/admin/", false}, // trailing slash differs
		{"/admin$", "/admin/foo", false},
		{"/admin$", "/admin-other", false},

		// /$ — only root
		{"/$", "/", true},
		{"/$", "/foo", false},
		{"/$", "/index.html", false},

		// /foo/$ — only that exact dir
		{"/foo/$", "/foo/", true},
		{"/foo/$", "/foo/index.html", false},
	}
	for _, tc := range cases {
		if got := patternMatches(tc.pattern, tc.path); got != tc.want {
			t.Errorf("patternMatches(%q, %q) = %t, want %t", tc.pattern, tc.path, got, tc.want)
		}
	}
}

// TestPatternMatchesEndAnchorWithWildcard covers `*...$` patterns.
// /*.pdf$ matches files ending in .pdf, not /file.pdf?query.
func TestPatternMatchesEndAnchorWithWildcard(t *testing.T) {
	cases := []struct {
		pattern string
		path    string
		want    bool
	}{
		// /*.pdf$ — extension match
		{"/*.pdf$", "/file.pdf", true},
		{"/*.pdf$", "/dir/file.pdf", true},
		{"/*.pdf$", "/file.pdf?query", false}, // query suffix breaks $-anchor
		{"/*.pdf$", "/file.html", false},
		{"/*.pdf$", "/.pdf", true}, // "/" + "" + ".pdf" — minimal but valid

		// /private/*$ — trailing *$ matches anything under /private/ (including / itself)
		{"/private/*$", "/private/foo", true},
		{"/private/*$", "/private/", true},
		{"/private/*$", "/other/foo", false},

		// /api/*/secret$ — middle wildcard + end anchor
		{"/api/*/secret$", "/api/v1/secret", true},
		{"/api/*/secret$", "/api/v1/v2/secret", true},     // * is greedy, matches v1/v2
		{"/api/*/secret$", "/api/v1/secret/foo", false},   // suffix breaks
		{"/api/*/secret$", "/api/secret", false},          // missing middle segment "/"
	}
	for _, tc := range cases {
		if got := patternMatches(tc.pattern, tc.path); got != tc.want {
			t.Errorf("patternMatches(%q, %q) = %t, want %t", tc.pattern, tc.path, got, tc.want)
		}
	}
}

// TestPatternMatchesNoEndAnchorBehaviorUnchanged is the iter-3 regression
// guard. All patterns without `$` should behave EXACTLY as before iter-132.
func TestPatternMatchesNoEndAnchorBehaviorUnchanged(t *testing.T) {
	cases := []struct {
		pattern string
		path    string
		want    bool
	}{
		// /admin — prefix match (iter-3)
		{"/admin", "/admin", true},
		{"/admin", "/admin/foo", true}, // prefix match WITHOUT $
		{"/admin", "/admin-other", true},
		{"/admin", "/other", false},

		// /*.pdf — without $: matches anywhere
		{"/*.pdf", "/file.pdf", true},
		{"/*.pdf", "/file.pdf?query", true}, // suffix doesn't break without $

		// /private/* — wildcard match
		{"/private/*", "/private/foo", true},
		{"/private/*", "/private/", true},
	}
	for _, tc := range cases {
		if got := patternMatches(tc.pattern, tc.path); got != tc.want {
			t.Errorf("patternMatches(%q, %q) = %t, want %t", tc.pattern, tc.path, got, tc.want)
		}
	}
}

// TestParseRobotsEndAnchorIntegration: end-to-end via parseRobots.
// Ensures the directive-level parser propagates patterns containing `$` through
// the matcher correctly.
func TestParseRobotsEndAnchorIntegration(t *testing.T) {
	body := `User-agent: *
Disallow: /admin$
Allow: /admin/public/
Disallow: /*.pdf$
`
	rules := parseRobots(body)
	cases := []struct {
		path    string
		allowed bool
	}{
		{"/admin", false},          // /admin$ disallows exactly /admin
		{"/admin/foo", true},       // /admin without $ would match; with $, only /admin exactly
		{"/admin/public/", true},   // explicit Allow (iter-3 longer-prefix wins)
		{"/file.pdf", false},       // /*.pdf$ disallows
		{"/file.pdf?dl=1", true},   // query suffix breaks $-anchor
		{"/file.html", true},       // not a pdf
	}
	for _, tc := range cases {
		allowed, _ := decideRobots(rules, tc.path, "anybot")
		if allowed != tc.allowed {
			t.Errorf("decideRobots(path=%q) allowed=%t, want %t", tc.path, allowed, tc.allowed)
		}
	}
}
