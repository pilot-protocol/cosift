package crawler

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
)

// TestDynamicAllowlist verifies that AddAllowedDomain promotes a domain so
// allowedDomain accepts it (and its subdomains), alongside static IncludeDomains,
// and that promotions persist to the file + survive a reload.
func TestDynamicAllowlist(t *testing.T) {
	dynFile := filepath.Join(t.TempDir(), "dyn-domains.txt")
	t.Setenv("COSIFT_DYNAMIC_DOMAINS_FILE", dynFile)

	cfg := config.Default().Crawler
	cfg.RespectRobots = false
	cfg.IncludeDomains = []string{"arxiv.org"} // static allowlist (non-empty)
	c := newBare(cfg)

	// Static domain allowed; unknown domain rejected.
	if !c.allowedDomain("https://arxiv.org/abs/1234") {
		t.Error("static allowlisted arxiv.org should be allowed")
	}
	if c.allowedDomain("https://example.com/x") {
		t.Error("example.com should be rejected before promotion")
	}

	// Promote example.com.
	if err := c.AddAllowedDomain("example.com"); err != nil {
		t.Fatalf("AddAllowedDomain: %v", err)
	}
	if !c.allowedDomain("https://example.com/x") {
		t.Error("example.com should be allowed after promotion")
	}
	if !c.allowedDomain("https://blog.example.com/y") {
		t.Error("subdomain of promoted domain should be allowed (dot-boundary)")
	}
	// Adversarial: evilexample.com must NOT match promoted example.com.
	if c.allowedDomain("https://evilexample.com/z") {
		t.Error("evilexample.com must not match promoted example.com")
	}

	// Persisted to file.
	b, err := os.ReadFile(dynFile)
	if err != nil || len(b) == 0 {
		t.Fatalf("promotion not persisted: %v", err)
	}

	// A fresh crawler loading the same file inherits the promotion.
	c2 := newBare(cfg)
	if !c2.allowedDomain("https://example.com/x") {
		t.Error("reloaded crawler should inherit promoted domain from file")
	}
}
