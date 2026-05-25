package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/calinteodor/cosift/internal/config"
	"github.com/calinteodor/cosift/internal/eval"
	"github.com/calinteodor/cosift/internal/store"
)

// seedExportTestStore puts two docs in a fresh store and returns a Config
// pointing at it. Two docs is enough to verify per-doc separators.
func seedExportTestStore(t *testing.T) *config.Config {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	s, err := store.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	docs := []store.Document{
		{URL: "https://x/go", Title: "Go concurrency", Text: "Go has goroutines.", Source: "test", FetchedAt: time.Now()},
		{URL: "https://x/rust", Title: "Rust ownership", Text: "Rust has lifetimes.", Source: "test", FetchedAt: time.Now()},
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

func TestRunExportFormatJSONDefault(t *testing.T) {
	// Default format json preserves the iter-1 wire shape (eval.Corpus pretty-printed).
	cfg := seedExportTestStore(t)
	out := filepath.Join(t.TempDir(), "out.json")
	if err := runExport(context.Background(), cfg, []string{"-output", out}); err != nil {
		t.Fatalf("export: %v", err)
	}
	body, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var corpus eval.Corpus
	if err := json.Unmarshal(body, &corpus); err != nil {
		t.Fatalf("parse json: %v\nbody=%q", err, body)
	}
	if len(corpus.Docs) != 2 {
		t.Errorf("want 2 docs in corpus, got %d", len(corpus.Docs))
	}
	// Pretty-printed → contains newlines + indent.
	if !strings.Contains(string(body), "\n  ") {
		t.Errorf("default json should be pretty-printed (iter-1 shape): %q", body)
	}
}

func TestRunExportFormatJSONL(t *testing.T) {
	cfg := seedExportTestStore(t)
	out := filepath.Join(t.TempDir(), "out.jsonl")
	if err := runExport(context.Background(), cfg, []string{"-output", out, "-format", "jsonl"}); err != nil {
		t.Fatalf("export: %v", err)
	}
	body, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(body), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("jsonl should have one line per doc; got %d lines: %q", len(lines), body)
	}
	// Each line is a single CorpusDoc object (no outer array).
	for i, line := range lines {
		var d eval.CorpusDoc
		if err := json.Unmarshal([]byte(line), &d); err != nil {
			t.Errorf("line %d not a single doc object: %v\n%q", i, err, line)
		}
		if d.URL == "" || d.Title == "" {
			t.Errorf("line %d missing fields: %+v", i, d)
		}
	}
}

func TestRunExportFormatText(t *testing.T) {
	cfg := seedExportTestStore(t)
	out := filepath.Join(t.TempDir(), "out.txt")
	if err := runExport(context.Background(), cfg, []string{"-output", out, "-format", "text"}); err != nil {
		t.Fatalf("export: %v", err)
	}
	body, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	s := string(body)
	for _, want := range []string{
		"Title: Go concurrency",
		"URL: https://x/go",
		"Go has goroutines.",
		"---",
		"Title: Rust ownership",
		"URL: https://x/rust",
		"Rust has lifetimes.",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("text export missing %q: %q", want, s)
		}
	}
	// First doc should NOT be preceded by `---` (separator is between docs only).
	if strings.HasPrefix(s, "---") {
		t.Errorf("text export shouldn't start with separator: %q", s)
	}
}

func TestRunExportFormatMarkdown(t *testing.T) {
	cfg := seedExportTestStore(t)
	out := filepath.Join(t.TempDir(), "out.md")
	if err := runExport(context.Background(), cfg, []string{"-output", out, "-format", "md"}); err != nil {
		t.Fatalf("export: %v", err)
	}
	body, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	s := string(body)
	for _, want := range []string{
		"# Go concurrency",
		"_https://x/go_",
		"Go has goroutines.",
		"# Rust ownership",
		"_https://x/rust_",
		"Rust has lifetimes.",
		"---",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("md export missing %q: %q", want, s)
		}
	}
	if !strings.HasPrefix(s, "# ") {
		t.Errorf("md export should start with a heading: %q", s)
	}
}

func TestRunExportFormatInvalid(t *testing.T) {
	cfg := seedExportTestStore(t)
	if err := runExport(context.Background(), cfg, []string{"-format", "yaml"}); err == nil {
		t.Errorf("invalid format should error")
	}
}

func TestRunExportDefaultOutputExtension(t *testing.T) {
	// When -output isn't set, the default filename should use the format's extension.
	// Switch CWD to a temp dir so the default file doesn't pollute the repo.
	cfg := seedExportTestStore(t)
	prevWd, _ := os.Getwd()
	tmp := t.TempDir()
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(prevWd) })

	for format, ext := range map[string]string{
		"json":  "json",
		"jsonl": "jsonl",
		"text":  "txt",
		"md":    "md",
	} {
		t.Run(format, func(t *testing.T) {
			if err := runExport(context.Background(), cfg, []string{"-format", format}); err != nil {
				t.Fatalf("export %s: %v", format, err)
			}
			expected := filepath.Join(tmp, "corpus-export."+ext)
			if _, err := os.Stat(expected); err != nil {
				t.Errorf("expected default output at %s: %v", expected, err)
			}
		})
	}
}

func TestValidateExportFormat(t *testing.T) {
	for _, ok := range []string{"json", "jsonl", "text", "md"} {
		if err := validateExportFormat(ok); err != nil {
			t.Errorf("validateExportFormat(%q): %v", ok, err)
		}
	}
	for _, bad := range []string{"", "yaml", "markdown", "JSON", "csv"} {
		if err := validateExportFormat(bad); err == nil {
			t.Errorf("validateExportFormat(%q) should reject", bad)
		}
	}
}
