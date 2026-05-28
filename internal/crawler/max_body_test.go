package crawler

import (
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
)

func TestCrawlerMaxBodyBytesForUsesOverride(t *testing.T) {
	// cfg.PerHostMaxBodyBytes wins over cfg.MaxBodyBytes for matching hosts.
	cfg := config.Crawler{
		MaxBodyBytes: 5 << 20, // 5 MB default
		PerHostMaxBodyBytes: map[string]int64{
			"papers.example.com": 50 << 20, // 50 MB
			"tiny.example.com":   1 << 20,  // 1 MB
		},
	}
	c := &Crawler{cfg: cfg}

	if got := c.maxBodyBytesFor("papers.example.com"); got != 50<<20 {
		t.Errorf("papers.example.com: want 50MB override, got %d", got)
	}
	if got := c.maxBodyBytesFor("tiny.example.com"); got != 1<<20 {
		t.Errorf("tiny.example.com: want 1MB override, got %d", got)
	}
	if got := c.maxBodyBytesFor("other.com"); got != 5<<20 {
		t.Errorf("other.com (no override): want default 5MB, got %d", got)
	}
}

func TestCrawlerMaxBodyBytesForNilMap(t *testing.T) {
	// Empty config → all hosts use default. semantic preserved.
	cfg := config.Crawler{MaxBodyBytes: 2 << 20}
	c := &Crawler{cfg: cfg}

	if got := c.maxBodyBytesFor("anywhere.com"); got != 2<<20 {
		t.Errorf("nil map: want default 2MB, got %d", got)
	}
}

func TestCrawlerMaxBodyBytesForBothZeroFallsTo5MB(t *testing.T) {
	// When both PerHostMaxBodyBytes[host] is unset/zero AND MaxBodyBytes is
	// zero, fall back to 5MB safe default (preserves fetch behavior:
	// `if limit <= 0 { limit = 5 << 20 }`).
	cfg := config.Crawler{} // both unset
	c := &Crawler{cfg: cfg}

	if got := c.maxBodyBytesFor("any.com"); got != 5<<20 {
		t.Errorf("both unset: want 5MB safe default, got %d", got)
	}
}

func TestCrawlerMaxBodyBytesForZeroOverrideIgnored(t *testing.T) {
	// An override of 0 is treated as "unset" — falls back to default.
	// Operators can't accidentally set body cap to 0 (which would mean
	// "read no body at all" — almost certainly a misconfiguration).
	cfg := config.Crawler{
		MaxBodyBytes:        10 << 20,
		PerHostMaxBodyBytes: map[string]int64{"weird.com": 0},
	}
	c := &Crawler{cfg: cfg}

	if got := c.maxBodyBytesFor("weird.com"); got != 10<<20 {
		t.Errorf("zero override should be ignored, falling to default; got %d, want 10MB", got)
	}
}

func TestCrawlerMaxBodyBytesForOverrideCanExceedDefault(t *testing.T) {
	// Override is the FULL cap — can be larger than default.
	cfg := config.Crawler{
		MaxBodyBytes:        1 << 20, // 1 MB default
		PerHostMaxBodyBytes: map[string]int64{"papers.example.com": 100 << 20},
	}
	c := &Crawler{cfg: cfg}

	if got := c.maxBodyBytesFor("papers.example.com"); got != 100<<20 {
		t.Errorf("override should exceed default; got %d, want 100MB", got)
	}
}
