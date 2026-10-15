package main

import (
	"context"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/store"
)

func captureBackfillStderr(t *testing.T, f func()) string {
	t.Helper()
	orig := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
	defer func() { os.Stderr = orig }()
	f()
	w.Close()
	out, _ := io.ReadAll(r)
	return string(out)
}

func seedBackfillStore(t *testing.T, texts ...string) string {
	t.Helper()
	dir := t.TempDir()
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range texts {
		sum := sha256.Sum256([]byte(text))
		if _, err := ps.UpsertDocument(context.Background(), &store.Document{URL: "https://example.com/" + text, Title: text, Text: text, ContentSHA: sum[:]}); err != nil {
			t.Fatal(err)
		}
	}
	if err := ps.DB().DeleteRange([]byte{'C'}, []byte{'D'}, nil); err != nil {
		t.Fatal(err)
	}
	ps.Close()
	return dir
}

func TestBackfillContentSHALeavesACompleteIndexAloneUnlessRescanned(t *testing.T) {
	dir := seedBackfillStore(t, "one", "two")
	ctx := context.Background()
	first := captureBackfillStderr(t, func() {
		if err := runBackfillContentSHA(ctx, []string{"-dir", dir}); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(first, "scanned 2 documents, indexed 2") {
		t.Fatalf("first pass output %q", first)
	}
	second := captureBackfillStderr(t, func() {
		if err := runBackfillContentSHA(ctx, []string{"-dir", dir}); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(second, "already complete") || !strings.Contains(second, "-rescan") || strings.Contains(second, "scanned") {
		t.Fatalf("second pass output %q", second)
	}
	rescan := captureBackfillStderr(t, func() {
		if err := runBackfillContentSHA(ctx, []string{"-dir", dir, "-rescan"}); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(rescan, "scanned 2 documents, indexed 2") {
		t.Fatalf("rescan output %q", rescan)
	}
}

func TestBackfillContentSHAReportsHowToResumeAfterAnInterruption(t *testing.T) {
	dir := seedBackfillStore(t, "one")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := runBackfillContentSHA(ctx, []string{"-dir", dir})
	if err == nil || !strings.Contains(err.Error(), "rerun to resume") || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("interrupted backfill: %v", err)
	}
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	if done, _ := ps.ContentIndexComplete(context.Background()); done {
		t.Fatal("an interrupted command marked the index complete")
	}
}

func TestBackfillContentSHAFailsWhenTheStoreCannotBeOpened(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := runBackfillContentSHA(context.Background(), []string{"-dir", filepath.Join(blocker, "pebble")})
	if err == nil || !strings.Contains(err.Error(), "open store") {
		t.Fatalf("unopenable store: %v", err)
	}
}
