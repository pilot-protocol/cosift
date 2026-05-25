package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/calinteodor/cosift/internal/config"
)

func TestSiteToHost(t *testing.T) {
	cases := []struct {
		in     string
		want   string
		hasErr bool
	}{
		{"https://docs.example.com", "docs.example.com", false},
		{"https://docs.example.com/page", "docs.example.com", false},
		{"http://example.com:8080/x", "example.com:8080", false},
		{"docs.example.com", "docs.example.com", false}, // bare host
		{"example.org", "example.org", false},
		{"", "", true},
		{"docs example.com", "", true},          // space → not a hostname
		{"docs.example.com/path", "", true},     // path without scheme → ambiguous; reject
		{"://broken-url", "", true},             // unparseable
	}
	for _, c := range cases {
		got, err := siteToHost(c.in)
		if (err != nil) != c.hasErr {
			t.Errorf("siteToHost(%q): err=%v hasErr=%v", c.in, err, c.hasErr)
			continue
		}
		if !c.hasErr && got != c.want {
			t.Errorf("siteToHost(%q) = %q want %q", c.in, got, c.want)
		}
	}
}

func TestRunInitWritesDefault(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cosift.json")

	if err := runInit(cfgPath, nil); err != nil {
		t.Fatalf("runInit: %v", err)
	}
	if _, err := os.Stat(cfgPath); err != nil {
		t.Fatalf("file not written: %v", err)
	}

	// Round-trip: read back via config.Load and confirm key fields match Default().
	got, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load back: %v", err)
	}
	def := config.Default()
	if got.DataDir != def.DataDir {
		t.Errorf("data_dir: got %q want %q", got.DataDir, def.DataDir)
	}
	if got.Server.Addr != def.Server.Addr {
		t.Errorf("server.addr: got %q want %q", got.Server.Addr, def.Server.Addr)
	}
	if got.Crawler.MaxConcurrent != def.Crawler.MaxConcurrent {
		t.Errorf("crawler.max_concurrent: got %d want %d", got.Crawler.MaxConcurrent, def.Crawler.MaxConcurrent)
	}
	if !got.Crawler.RespectRobots {
		t.Errorf("respect_robots should default true")
	}
}

func TestRunInitWithSite(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cosift.json")

	if err := runInit(cfgPath, []string{"-site", "https://docs.example.com"}); err != nil {
		t.Fatalf("runInit: %v", err)
	}
	// Parse JSON directly to verify include_domains.
	b, _ := os.ReadFile(cfgPath)
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	crawler := doc["crawler"].(map[string]any)
	domains := crawler["include_domains"].([]any)
	if len(domains) != 1 || domains[0] != "docs.example.com" {
		t.Errorf("include_domains: %v", domains)
	}
}

func TestRunInitRefusesOverwrite(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cosift.json")
	// Pre-create the file.
	if err := os.WriteFile(cfgPath, []byte(`{"data_dir":"existing"}`), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	err := runInit(cfgPath, nil)
	if err == nil {
		t.Fatalf("expected refusal, got nil")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("error should mention conflict: %v", err)
	}
	// And the file must NOT have been overwritten.
	b, _ := os.ReadFile(cfgPath)
	if !strings.Contains(string(b), "existing") {
		t.Errorf("file overwritten despite refusal! contents: %s", b)
	}
}

func TestRunInitForceOverwrites(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cosift.json")
	if err := os.WriteFile(cfgPath, []byte(`{"data_dir":"existing"}`), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := runInit(cfgPath, []string{"-force"}); err != nil {
		t.Fatalf("runInit -force: %v", err)
	}
	b, _ := os.ReadFile(cfgPath)
	if strings.Contains(string(b), "existing") {
		t.Errorf("-force did not overwrite: %s", b)
	}
	// The default config should be there.
	if !strings.Contains(string(b), "127.0.0.1:7777") {
		t.Errorf("expected default config after -force: %s", b)
	}
}

// TestRunInitConfigSurvivesDoctorDefaults verifies the iter-67 init output
// doesn't trigger any FAIL in iter-58's defaults cross-check. A config that
// init writes and doctor rejects would be a regression at the seam between
// the two iters.
func TestRunInitConfigSurvivesDoctorDefaults(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cosift.json")
	if err := runInit(cfgPath, []string{"-site", "https://docs.example.com"}); err != nil {
		t.Fatalf("runInit: %v", err)
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load back: %v", err)
	}
	// Init produces zero-valued defaults (no retriever/expand/research_strategy set)
	// → doctorDefaultsChecks returns the single "no defaults set" PASS row.
	checks := doctorDefaultsChecks(cfg.Defaults, true, true)
	for _, c := range checks {
		if c.Status == "FAIL" {
			t.Errorf("init output triggered doctor FAIL: %+v", c)
		}
	}
}

func TestRunInitBadSite(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cosift.json")

	err := runInit(cfgPath, []string{"-site", "docs example.com"})
	if err == nil {
		t.Fatalf("expected error on bad site")
	}
	// File must NOT be written when -site fails.
	if _, err := os.Stat(cfgPath); err == nil {
		t.Errorf("file written despite bad -site")
	}
}
