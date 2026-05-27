package crawler

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/store"
)

func TestCrawlerMaxDepthForUsesOverride(t *testing.T) {
	// Iter 129: cfg.PerHostMaxDepth wins over cfg.MaxDepth.
	cfg := config.Crawler{
		MaxDepth: 3,
		PerHostMaxDepth: map[string]int{
			"docs.example.com": 10,
			"external.io":      0,
		},
	}
	c := &Crawler{cfg: cfg}

	if got := c.maxDepthFor("docs.example.com"); got != 10 {
		t.Errorf("override should win: got %d, want 10", got)
	}
	if got := c.maxDepthFor("external.io"); got != 0 {
		t.Errorf("zero override should win (limits to seeds only): got %d, want 0", got)
	}
	if got := c.maxDepthFor("other.com"); got != 3 {
		t.Errorf("default should apply for non-overridden host: got %d, want 3", got)
	}
}

func TestCrawlerMaxDepthForNilMap(t *testing.T) {
	// Empty config map → all hosts use default. Matches the iter-1 single-cap
	// behavior; iter-129 should be additive.
	cfg := config.Crawler{MaxDepth: 5}
	c := &Crawler{cfg: cfg}

	if got := c.maxDepthFor("anywhere.com"); got != 5 {
		t.Errorf("nil override map: got %d, want default 5", got)
	}
}

func TestCrawlerMaxDepthForOverrideCanExceedDefault(t *testing.T) {
	// An override can be LARGER than the global default — operators explicitly
	// opt in to deeper crawls on specific hosts. iter-129 semantics.
	cfg := config.Crawler{
		MaxDepth:        2,
		PerHostMaxDepth: map[string]int{"docs.example.com": 10},
	}
	c := &Crawler{cfg: cfg}

	if got := c.maxDepthFor("docs.example.com"); got != 10 {
		t.Errorf("override should be allowed to exceed default: got %d, want 10", got)
	}
}

func TestCrawlerEnqueueLinksDropsOverCappedChildren(t *testing.T) {
	// Integration: feed enqueueLinks 3 links across 3 hosts with different
	// caps. Only the one whose depth is under its host's cap should land in
	// the frontier.
	dir := filepath.Join(t.TempDir(), "data")
	s, err := store.Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	cfg := config.Default().Crawler
	cfg.RespectRobots = false
	cfg.MaxDepth = 5
	cfg.PerHostMaxDepth = map[string]int{
		"strict.example.com": 1, // cap 1: drop anything at depth > 1
		"deep.example.com":   10, // cap 10: allow up to depth 10
	}
	// IncludeDomains empty → c.allowedDomain accepts everything.
	c := New(cfg, s)

	// depth=3:
	//   strict.example.com (cap 1): 3 > 1 → DROP
	//   deep.example.com (cap 10): 3 <= 10 → KEEP
	//   other.com (default cap 5): 3 <= 5 → KEEP
	c.enqueueLinks(context.Background(),
		[]string{
			"https://strict.example.com/page",
			"https://deep.example.com/page",
			"https://other.com/page",
		},
		3,
	)

	// Inspect frontier — only deep.example.com and other.com should be queued.
	due, err := s.DueForRefresh(context.Background(), 0, 0, 100)
	if err != nil || len(due) == 0 {
		// DueForRefresh might not include never-fetched URLs. Try a different
		// path: count queued frontier rows.
		stats, statsErr := s.GetFrontierStats(context.Background())
		if statsErr != nil {
			t.Fatalf("frontier stats: %v", statsErr)
		}
		// Should have 2 queued (deep + other), not 3 (strict dropped).
		if stats.Queued != 2 {
			t.Errorf("want 2 queued (strict dropped), got %d", stats.Queued)
		}
	}
}

func TestCrawlerEnqueueLinksOverrideExceedsDefault(t *testing.T) {
	// Override can exceed default. depth=8, host=deep with cap=10 → kept.
	// Same depth on host=normal (default cap 5) → dropped.
	dir := filepath.Join(t.TempDir(), "data")
	s, err := store.Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	cfg := config.Default().Crawler
	cfg.RespectRobots = false
	cfg.MaxDepth = 5
	cfg.PerHostMaxDepth = map[string]int{"deep.example.com": 10}
	c := New(cfg, s)

	c.enqueueLinks(context.Background(),
		[]string{
			"https://deep.example.com/a",   // cap 10, depth 8 → KEEP
			"https://normal.example.com/b", // cap 5, depth 8 → DROP
		},
		8,
	)

	stats, err := s.GetFrontierStats(context.Background())
	if err != nil {
		t.Fatalf("frontier stats: %v", err)
	}
	if stats.Queued != 1 {
		t.Errorf("want 1 queued (only deep.example.com), got %d", stats.Queued)
	}
}
