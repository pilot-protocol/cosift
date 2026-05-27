package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/eval"
	"github.com/pilot-protocol/cosift/internal/store"
)

// seedExportFilterStore puts 4 docs across 3 domains and 3 dates so each
// filter has something to keep and something to drop.
func seedExportFilterStore(t *testing.T) *config.Config {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	s, err := store.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	docs := []store.Document{
		{URL: "https://blog.example.com/a", Title: "Example A", Text: "body A",
			Source: "test", FetchedAt: time.Now(),
			PublishedAt: time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)},
		{URL: "https://example.com/b", Title: "Example B", Text: "body B",
			Source: "test", FetchedAt: time.Now(),
			PublishedAt: time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)},
		{URL: "https://otherdomain.org/c", Title: "Other C", Text: "body C",
			Source: "test", FetchedAt: time.Now(),
			PublishedAt: time.Date(2026, 4, 10, 0, 0, 0, 0, time.UTC)},
		{URL: "https://undated.test/d", Title: "Undated D", Text: "body D",
			Source: "test", FetchedAt: time.Now(),
			// PublishedAt deliberately zero.
		},
	}
	for i := range docs {
		if _, err := s.UpsertDocument(context.Background(), &docs[i]); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}
	cfg := config.Default()
	cfg.DataDir = dir
	return cfg
}

// readExportedURLs parses the json-format export and returns the URLs in order.
func readExportedURLs(t *testing.T, path string) []string {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var corpus eval.Corpus
	if err := json.Unmarshal(body, &corpus); err != nil {
		t.Fatalf("parse %s: %v\nbody=%q", path, err, body)
	}
	urls := make([]string, len(corpus.Docs))
	for i, d := range corpus.Docs {
		urls[i] = d.URL
	}
	return urls
}

func TestRunExportNoFilters(t *testing.T) {
	// Without filters, all 4 docs are exported (regression check for iter-103).
	cfg := seedExportFilterStore(t)
	out := filepath.Join(t.TempDir(), "out.json")
	if err := runExport(context.Background(), cfg, []string{"-output", out}); err != nil {
		t.Fatalf("export: %v", err)
	}
	if got := readExportedURLs(t, out); len(got) != 4 {
		t.Errorf("want 4 docs without filters, got %d: %+v", len(got), got)
	}
}

func TestRunExportIncludeDomain(t *testing.T) {
	// example.com matches example.com AND blog.example.com (suffix on dot boundary).
	cfg := seedExportFilterStore(t)
	out := filepath.Join(t.TempDir(), "out.json")
	if err := runExport(context.Background(), cfg, []string{
		"-output", out, "-include-domains", "example.com",
	}); err != nil {
		t.Fatalf("export: %v", err)
	}
	urls := readExportedURLs(t, out)
	if len(urls) != 2 {
		t.Fatalf("want 2 example.com docs, got %d: %+v", len(urls), urls)
	}
	for _, u := range urls {
		if !strings.Contains(u, "example.com") {
			t.Errorf("non-example.com URL leaked through: %s", u)
		}
	}
}

func TestRunExportExcludeDomain(t *testing.T) {
	cfg := seedExportFilterStore(t)
	out := filepath.Join(t.TempDir(), "out.json")
	if err := runExport(context.Background(), cfg, []string{
		"-output", out, "-exclude-domains", "example.com",
	}); err != nil {
		t.Fatalf("export: %v", err)
	}
	urls := readExportedURLs(t, out)
	for _, u := range urls {
		if strings.Contains(u, "example.com") {
			t.Errorf("excluded domain leaked through: %s", u)
		}
	}
	// otherdomain.org + undated.test = 2 docs.
	if len(urls) != 2 {
		t.Errorf("want 2 non-example docs, got %d: %+v", len(urls), urls)
	}
}

func TestRunExportSinceDateFilter(t *testing.T) {
	cfg := seedExportFilterStore(t)
	out := filepath.Join(t.TempDir(), "out.json")
	if err := runExport(context.Background(), cfg, []string{
		"-output", out, "-since", "2026-01-01",
	}); err != nil {
		t.Fatalf("export: %v", err)
	}
	urls := readExportedURLs(t, out)
	// blog.example.com/a (2025-06-01) dropped; example.com/b (2026-02-15) kept;
	// otherdomain.org/c (2026-04-10) kept; undated.test/d (zero) dropped per
	// iter-77 semantics.
	if len(urls) != 2 {
		t.Fatalf("want 2 post-2026 docs, got %d: %+v", len(urls), urls)
	}
	for _, u := range urls {
		if strings.Contains(u, "/a") || strings.Contains(u, "/d") {
			t.Errorf("pre-2026 or undated doc leaked through: %s", u)
		}
	}
}

func TestRunExportUntilDateFilter(t *testing.T) {
	cfg := seedExportFilterStore(t)
	out := filepath.Join(t.TempDir(), "out.json")
	if err := runExport(context.Background(), cfg, []string{
		"-output", out, "-until", "2026-03-01",
	}); err != nil {
		t.Fatalf("export: %v", err)
	}
	urls := readExportedURLs(t, out)
	// a (2025-06-01) kept; b (2026-02-15) kept; c (2026-04-10) dropped; d (undated) dropped.
	if len(urls) != 2 {
		t.Fatalf("want 2 pre-march-2026 docs, got %d: %+v", len(urls), urls)
	}
}

func TestRunExportFiltersCompose(t *testing.T) {
	// example.com + since=2026 → only example.com/b.
	cfg := seedExportFilterStore(t)
	out := filepath.Join(t.TempDir(), "out.json")
	if err := runExport(context.Background(), cfg, []string{
		"-output", out,
		"-include-domains", "example.com",
		"-since", "2026-01-01",
	}); err != nil {
		t.Fatalf("export: %v", err)
	}
	urls := readExportedURLs(t, out)
	if len(urls) != 1 {
		t.Fatalf("want exactly example.com/b: got %d: %+v", len(urls), urls)
	}
	if urls[0] != "https://example.com/b" {
		t.Errorf("composed filter wrong doc: %s", urls[0])
	}
}

func TestRunExportLimitAppliesAfterFilters(t *testing.T) {
	// Filter to example.com (2 docs) then -limit 1 → exactly 1 doc.
	cfg := seedExportFilterStore(t)
	out := filepath.Join(t.TempDir(), "out.json")
	if err := runExport(context.Background(), cfg, []string{
		"-output", out,
		"-include-domains", "example.com",
		"-limit", "1",
	}); err != nil {
		t.Fatalf("export: %v", err)
	}
	urls := readExportedURLs(t, out)
	if len(urls) != 1 {
		t.Fatalf("limit-after-filter should produce 1 doc, got %d: %+v", len(urls), urls)
	}
	if !strings.Contains(urls[0], "example.com") {
		t.Errorf("limit applied before filter? got %s", urls[0])
	}
}

func TestRunExportInvalidDate(t *testing.T) {
	cfg := seedExportFilterStore(t)
	err := runExport(context.Background(), cfg, []string{"-since", "yesterday"})
	if err == nil {
		t.Fatal("invalid date should error")
	}
	if !strings.Contains(err.Error(), "yesterday") {
		t.Errorf("error should name the bad value: %v", err)
	}
}

func TestMatchesDomainPattern(t *testing.T) {
	// iter-79 semantics: suffix on dot boundary.
	cases := []struct {
		host, pattern string
		want          bool
	}{
		{"example.com", "example.com", true},
		{"blog.example.com", "example.com", true},
		{"deep.blog.example.com", "example.com", true},
		{"evilexample.com", "example.com", false},
		{"example.com.evil.io", "example.com", false},
		{"other.org", "example.com", false},
	}
	for _, c := range cases {
		got := matchesDomainPattern(c.host, []string{c.pattern})
		if got != c.want {
			t.Errorf("matchesDomainPattern(%q, %q) = %t, want %t", c.host, c.pattern, got, c.want)
		}
	}
}
