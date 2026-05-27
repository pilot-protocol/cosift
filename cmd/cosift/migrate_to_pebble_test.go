package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

// TestMigrateToPebble — round-trip: populate a SQLite store, migrate to
// Pebble, run a sample query through PebbleBM25 against the migrated
// data, verify the same top hit. Iter 204.
func TestMigrateToPebble(t *testing.T) {
	ctx := context.Background()

	// 1. Set up a SQLite source store with 3 docs.
	srcDir := t.TempDir()
	src, err := store.Open(srcDir)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer src.Close()
	sqliteIdx := index.NewBM25(src)
	corpus := []struct{ url, title, text string }{
		{"https://x/raft", "Raft consensus", "Raft is a distributed consensus algorithm."},
		{"https://x/paxos", "Paxos algorithm", "Paxos is the classical distributed consensus algorithm."},
		{"https://x/cooking", "Cooking pasta", "Boil water, salt, drop pasta, drain when al dente."},
	}
	for _, d := range corpus {
		id, err := src.UpsertDocument(ctx, &store.Document{
			URL: d.url, Title: d.title, Text: d.text, FetchedAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("upsert: %v", err)
		}
		if err := sqliteIdx.IndexDocument(ctx, id, d.title, d.text); err != nil {
			t.Fatalf("index: %v", err)
		}
	}

	// 2. Run migrate-to-pebble.
	dstDir := filepath.Join(t.TempDir(), "pebble")
	cfg := &config.Config{DataDir: srcDir}
	if err := runMigrateToPebble(ctx, cfg, []string{"-output", dstDir, "-progress", "0"}); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// 3. Open the Pebble store + query it via PebbleBM25.
	dst, err := store.OpenPebble(dstDir)
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	defer dst.Close()
	stats, err := dst.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Documents != int64(len(corpus)) {
		t.Errorf("documents count: want %d, got %d", len(corpus), stats.Documents)
	}

	pidx := index.NewPebbleBM25(dst)
	hits, err := pidx.Search(ctx, "raft consensus", 3)
	if err != nil {
		t.Fatalf("post-migration search: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("post-migration search returned 0 hits")
	}
	if hits[0].URL != "https://x/raft" {
		t.Errorf("top hit: want https://x/raft, got %s", hits[0].URL)
	}

	// Cross-check a different query topic to make sure the post-migration
	// index actually distinguishes content (not just lucky on one query).
	hits, _ = pidx.Search(ctx, "pasta water", 3)
	if len(hits) == 0 || hits[0].URL != "https://x/cooking" {
		t.Errorf("cooking query: %+v", hits)
	}
}

// TestMigrateRefusesNonEmptyOutput — destination dir with existing data
// fails cleanly. Prevents a mistyped path from clobbering a live store.
func TestMigrateRefusesNonEmptyOutput(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	if _, err := store.Open(srcDir); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	dstDir := t.TempDir()
	// Create a sentinel file so the dir is non-empty.
	if err := os.WriteFile(filepath.Join(dstDir, "sentinel"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}
	cfg := &config.Config{DataDir: srcDir}
	err := runMigrateToPebble(ctx, cfg, []string{"-output", dstDir})
	if err == nil {
		t.Errorf("expected error on non-empty -output, got nil")
	}
	if !strings.Contains(err.Error(), "non-empty") {
		t.Errorf("expected 'non-empty' in error message, got: %v", err)
	}
}
