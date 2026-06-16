package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

// TestRunPurgeDomain verifies the host-suffix sweep soft-deletes exactly the
// matching-TLD docs (dot-boundary) and leaves the rest, and that the default
// dry run deletes nothing.
func TestRunPurgeDomain(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	idx := index.NewPebbleBM25(ps)

	docs := []struct{ url, title, text string }{
		{"https://spam1.cfd/a", "Spam A", "junk body one"},
		{"https://x.spam.cfd/b", "Spam B", "junk body two"},  // subdomain of a .cfd host
		{"https://gamble.sbs/c", "Bet C", "junk body three"}, // .sbs
		{"https://good.com/d", "Good D", "real useful content"},
		{"https://docs.example.org/e", "Docs E", "reference material"},
		{"https://notcfd.com/f", "Edge F", "ends in cfd-ish but is .com"}, // must NOT match
	}
	for _, d := range docs {
		id, err := ps.UpsertDocument(ctx, &store.Document{URL: d.url, Title: d.title, Text: d.text, FetchedAt: time.Now()})
		if err != nil {
			t.Fatalf("UpsertDocument %s: %v", d.url, err)
		}
		if err := idx.IndexDocument(ctx, id, d.title, d.text); err != nil {
			t.Fatalf("IndexDocument %s: %v", d.url, err)
		}
	}
	ps.Close() // release the write lock so runPurgeDomain can open the dir

	// Dry run: must delete nothing.
	if err := runPurgeDomain(ctx, []string{"-dir", dir, "-suffix", "cfd,sbs"}); err != nil {
		t.Fatalf("dry run: %v", err)
	}
	assertDocs(t, dir, map[string]bool{
		"https://spam1.cfd/a": true, "https://gamble.sbs/c": true, "https://good.com/d": true,
	})

	// Apply: purge *.cfd and *.sbs.
	if err := runPurgeDomain(ctx, []string{"-dir", dir, "-suffix", "cfd,sbs", "-apply"}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	assertDocs(t, dir, map[string]bool{
		"https://spam1.cfd/a":        false, // purged
		"https://x.spam.cfd/b":       false, // purged (subdomain)
		"https://gamble.sbs/c":       false, // purged
		"https://good.com/d":         true,  // kept
		"https://docs.example.org/e": true,  // kept
		"https://notcfd.com/f":       true,  // kept (dot-boundary: not a .cfd)
	})
}

// assertDocs opens the store read-only and checks presence/absence per URL.
func assertDocs(t *testing.T, dir string, want map[string]bool) {
	t.Helper()
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ps.Close()
	for u, shouldExist := range want {
		d, _ := ps.GetDocByURL(context.Background(), u)
		if shouldExist && d == nil {
			t.Errorf("%s: expected present, got deleted", u)
		}
		if !shouldExist && d != nil {
			t.Errorf("%s: expected purged, still present", u)
		}
	}
}

func TestRunPurgeDomainRequiresSuffix(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	ps.Close()
	if err := runPurgeDomain(context.Background(), []string{"-dir", dir}); err == nil {
		t.Error("expected error when -suffix is empty")
	}
	if err := runPurgeDomain(context.Background(), []string{"-dir", dir, "-suffix", "cfd", "-apply", "-readonly"}); err == nil {
		t.Error("expected error when -apply combined with -readonly")
	}
}
