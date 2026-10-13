package main

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/store"
)

func TestBackfillContentSHACommand(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("body"))
	if _, err := ps.UpsertDocument(ctx, &store.Document{URL: "https://example.com/a", Title: "A", Text: "body", ContentSHA: sum[:]}); err != nil {
		t.Fatal(err)
	}
	if err := ps.DB().DeleteRange([]byte{'C'}, []byte{'D'}, nil); err != nil {
		t.Fatal(err)
	}
	ps.Close()

	if err := runBackfillContentSHA(ctx, nil); err == nil {
		t.Fatal("missing -dir accepted")
	}
	if err := runBackfillContentSHA(ctx, []string{"-dir", dir}); err != nil {
		t.Fatal(err)
	}
	ps, err = store.OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	if done, _ := ps.ContentIndexComplete(ctx); !done {
		t.Fatal("index not marked complete")
	}
	if live, _ := ps.HasLiveContent(ctx, sum[:], 0); !live {
		t.Fatal("existing document not indexed")
	}
}

func TestBackfillProgressLineShowsPercentAndETA(t *testing.T) {
	line := backfillProgressLine(store.ContentSHABackfill{Scanned: 250, Indexed: 240, LastID: 250, MaxID: 1000}, 25*time.Second)
	for _, want := range []string{"25.0%", "250 scanned", "ETA 1m15s"} {
		if !strings.Contains(line, want) {
			t.Fatalf("%q missing %q", line, want)
		}
	}
	if done := backfillProgressLine(store.ContentSHABackfill{LastID: 1000, MaxID: 1000}, time.Second); !strings.Contains(done, "100.0%") || !strings.Contains(done, "ETA 0s") {
		t.Fatalf("finished line %q", done)
	}
}
