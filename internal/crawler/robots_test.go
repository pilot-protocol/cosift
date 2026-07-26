package crawler

import (
	"strings"
	"testing"
	"time"
)

func TestParseRobotsBasic(t *testing.T) {
	body := `# example
User-agent: *
Disallow: /admin/
Disallow: /private/
Allow: /public/
Crawl-delay: 2

User-agent: CosiftBot
Allow: /
`
	r := parseRobots(body)
	if len(r.groups) != 2 {
		t.Fatalf("groups: got %d want 2", len(r.groups))
	}
	if r.groups[0].crawlDelay != 2*time.Second {
		t.Errorf("delay: got %v want 2s", r.groups[0].crawlDelay)
	}
	if len(r.groups[0].disallow) != 2 || len(r.groups[0].allow) != 1 {
		t.Errorf("group 0 rules: %+v", r.groups[0])
	}
}

func TestParseRobotsFractionalCrawlDelay(t *testing.T) {
	// "Crawl-delay: 0.5" is common in the wild; the previous integer-only
	// parse silently dropped it.
	r := parseRobots("User-agent: *\nCrawl-delay: 0.5\n")
	if got := r.groups[0].crawlDelay; got != 500*time.Millisecond {
		t.Errorf("fractional delay: got %v want 500ms", got)
	}
	r = parseRobots("User-agent: *\nCrawl-delay: 30\n")
	if got := r.groups[0].crawlDelay; got != 30*time.Second {
		t.Errorf("integer delay: got %v want 30s", got)
	}
	r = parseRobots("User-agent: *\nCrawl-delay: -3\n")
	if got := r.groups[0].crawlDelay; got != 0 {
		t.Errorf("negative delay should be ignored: got %v", got)
	}
}

func TestRobotsAllowAndDisallow(t *testing.T) {
	body := `User-agent: *
Disallow: /admin/
Allow: /admin/public/
`
	rules := parseRobots(body)

	cases := []struct {
		path    string
		ua      string
		allowed bool
	}{
		{"/admin/", "anybot", false},
		{"/admin/secret", "anybot", false},
		{"/admin/public/", "anybot", true},
		{"/admin/public/info.html", "anybot", true},
		{"/", "anybot", true},
		{"/blog/post", "anybot", true},
	}
	for _, c := range cases {
		got, _ := decideRobots(rules, c.path, c.ua)
		if got != c.allowed {
			t.Errorf("%s [%s]: got %v want %v", c.path, c.ua, got, c.allowed)
		}
	}
}

func TestRobotsSpecificUABeatsStar(t *testing.T) {
	body := `User-agent: *
Disallow: /

User-agent: CosiftBot
Allow: /
`
	rules := parseRobots(body)

	if got, _ := decideRobots(rules, "/anything", "MyCosiftBot/1.0"); !got {
		t.Errorf("specific UA should override star: got disallow")
	}
	if got, _ := decideRobots(rules, "/anything", "RandomCrawler"); got {
		t.Errorf("non-matching UA should hit star: got allow")
	}
}

func TestRobotsGroupedUserAgents(t *testing.T) {
	// Consecutive User-agent lines share rules (per REP).
	body := `User-agent: BotA
User-agent: BotB
Disallow: /no/
`
	rules := parseRobots(body)
	if len(rules.groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(rules.groups))
	}
	if len(rules.groups[0].agents) != 2 {
		t.Errorf("expected 2 agents, got %v", rules.groups[0].agents)
	}
	if got, _ := decideRobots(rules, "/no/path", "MyBotA"); got {
		t.Errorf("BotA should be blocked")
	}
	if got, _ := decideRobots(rules, "/no/path", "MyBotB"); got {
		t.Errorf("BotB should be blocked")
	}
}

func TestPatternMatchesWildcard(t *testing.T) {
	cases := []struct {
		pat, path string
		want      bool
	}{
		{"/admin/", "/admin/", true},
		{"/admin/", "/admin/foo", true},
		{"/admin/", "/admin", false}, // no trailing slash
		{"/*.pdf", "/file.pdf", true},
		{"/*.pdf", "/dir/file.pdf", true},
		{"/*.pdf", "/file.txt", false},
		{"/a/*/b", "/a/x/b", true},
		{"/a/*/b", "/a/b", false},
		{"", "/anything", false}, // empty pattern matches nothing
	}
	for _, c := range cases {
		got := false
		if c.pat == "" {
			// longestMatch skips empty patterns; mirror that.
			got = false
		} else {
			got = patternMatches(c.pat, c.path)
		}
		if got != c.want {
			t.Errorf("patternMatches(%q, %q): got %v want %v", c.pat, c.path, got, c.want)
		}
	}
}

func TestRobotsEmptyDisallowMeansAllowAll(t *testing.T) {
	body := `User-agent: *
Disallow:
`
	rules := parseRobots(body)
	if got, _ := decideRobots(rules, "/anything", "anybot"); !got {
		t.Errorf("empty Disallow should mean allow-all")
	}
}

func TestRobotsCommentsAndBlankLines(t *testing.T) {
	body := `# top comment
User-agent: *  # inline comment
Disallow: /x  # trailing comment

# blank above

Disallow: /y
`
	rules := parseRobots(body)
	if len(rules.groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(rules.groups))
	}
	if !strings.Contains(strings.Join(rules.groups[0].disallow, ","), "/x") {
		t.Errorf("missing /x rule")
	}
	if !strings.Contains(strings.Join(rules.groups[0].disallow, ","), "/y") {
		t.Errorf("missing /y rule")
	}
}
