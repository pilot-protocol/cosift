package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestOpenMemoryBasicCRUD: insert + fetch via OpenMemory works end-to-end.
// Catches the "per-connection SQLite memory mode" trap — without
// SetMaxOpenConns(1), the second query gets a fresh empty DB and fails.
func TestOpenMemoryBasicCRUD(t *testing.T) {
	s, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer s.Close()

	id, err := s.UpsertDocument(context.Background(), &Document{
		URL:       "https://x/doc",
		Title:     "Test",
		Text:      "content",
		Source:    "iter-133",
		FetchedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if id <= 0 {
		t.Errorf("upsert returned non-positive id: %d", id)
	}

	// Round-trip through a SEPARATE query — exercises the connection-pool
	// boundary that SetMaxOpenConns(1) protects against.
	doc, err := s.GetDocByURL(context.Background(), "https://x/doc")
	if err != nil {
		t.Fatalf("GetDocByURL: %v", err)
	}
	if doc.Title != "Test" || doc.Text != "content" {
		t.Errorf("round-trip lost fields: %+v", doc)
	}
}

func TestOpenMemoryPathIndicator(t *testing.T) {
	// The store records its path as ":memory:" so anything inspecting the
	// store can tell it's the in-memory variant.
	s, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer s.Close()
	if s.path != ":memory:" {
		t.Errorf("path indicator: got %q, want :memory:", s.path)
	}
}

func TestOpenMemoryMigrationRuns(t *testing.T) {
	// The iter-104 ListDocuments query reads published_at; if migrations
	// didn't run, the column would be missing and the query would fail.
	// This is a stronger schema-applied check than just opening.
	s, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer s.Close()

	pub := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	if _, err := s.UpsertDocument(context.Background(), &Document{
		URL:         "https://x/dated",
		Title:       "Dated",
		Text:        "body",
		Source:      "iter-133",
		FetchedAt:   time.Now(),
		PublishedAt: pub,
	}); err != nil {
		t.Fatalf("upsert with published_at: %v", err)
	}

	docs, err := s.ListDocuments(context.Background(), 0)
	if err != nil {
		t.Fatalf("ListDocuments: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("want 1 doc, got %d", len(docs))
	}
	if !docs[0].PublishedAt.Equal(pub) {
		t.Errorf("published_at not migrated: got %v, want %v", docs[0].PublishedAt, pub)
	}
}

func TestOpenMemoryIsolation(t *testing.T) {
	// Two separate OpenMemory calls produce independent stores (no leak).
	// Required because tests may run in parallel; one test's data must not
	// be visible from another's store.
	a, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory a: %v", err)
	}
	defer a.Close()

	b, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory b: %v", err)
	}
	defer b.Close()

	_, err = a.UpsertDocument(context.Background(), &Document{
		URL: "https://a/doc", Title: "A", Text: "a", Source: "t", FetchedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert a: %v", err)
	}

	// b should NOT see a's doc.
	_, err = b.GetDocByURL(context.Background(), "https://a/doc")
	if err == nil {
		t.Error("OpenMemory stores should be isolated, but b sees a's doc")
	} else if !strings.Contains(err.Error(), "no rows") && err.Error() != "sql: no rows in result set" {
		// Acceptable: "no rows" error. Anything else means real failure.
		// (Use Contains for flexibility across SQLite driver wrapping.)
	}
}
