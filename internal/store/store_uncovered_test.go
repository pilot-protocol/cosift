package store

import (
	"context"
	"testing"
	"time"
)

func TestStats(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Empty store.
	st, err := s.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats empty: %v", err)
	}
	if st.Documents != 0 || st.Terms != 0 {
		t.Errorf("empty stats: got %+v", st)
	}

	// One doc → Documents=1, Terms still 0 (we don't index terms in this path).
	_, _ = s.UpsertDocument(ctx, &Document{URL: "u1", Source: "t", FetchedAt: time.Now()})
	st, err = s.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.Documents != 1 {
		t.Errorf("Documents: got %d want 1", st.Documents)
	}
}

func TestGetDocTexts(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Empty input → empty map, no error.
	out, err := s.GetDocTexts(ctx, nil, 0)
	if err != nil {
		t.Fatalf("empty: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected empty result, got %d", len(out))
	}

	// Insert two docs with text.
	_, _ = s.UpsertDocument(ctx, &Document{URL: "u1", Text: "hello world this is body one", Source: "t", FetchedAt: time.Now()})
	_, _ = s.UpsertDocument(ctx, &Document{URL: "u2", Text: "shorter", Source: "t", FetchedAt: time.Now()})

	got, err := s.GetDocTexts(ctx, []string{"u1", "u2", "nonexistent"}, 0)
	if err != nil {
		t.Fatalf("GetDocTexts: %v", err)
	}
	if got["u1"] != "hello world this is body one" {
		t.Errorf("u1: %q", got["u1"])
	}
	if got["u2"] != "shorter" {
		t.Errorf("u2: %q", got["u2"])
	}
	if _, ok := got["nonexistent"]; ok {
		t.Errorf("nonexistent should be absent")
	}
}

func TestGetDocTextsMaxLen(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	_, _ = s.UpsertDocument(ctx, &Document{URL: "u", Text: "abcdefghijklmnopqrstuvwxyz", Source: "t", FetchedAt: time.Now()})
	got, err := s.GetDocTexts(ctx, []string{"u"}, 10)
	if err != nil {
		t.Fatalf("GetDocTexts: %v", err)
	}
	if len(got["u"]) != 10 {
		t.Errorf("maxLen=10 should truncate, got len=%d: %q", len(got["u"]), got["u"])
	}
}

func TestListDocSitemapEntries(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().Unix()

	_, _ = s.UpsertDocument(ctx, &Document{URL: "u1", Source: "t", FetchedAt: time.Now(), LastChangedAt: now})
	_, _ = s.UpsertDocument(ctx, &Document{URL: "u2", Source: "t", FetchedAt: time.Now()})

	entries, err := s.ListDocSitemapEntries(ctx, 100)
	if err != nil {
		t.Fatalf("ListDocSitemapEntries: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries: got %d want 2", len(entries))
	}
	// Ordering is by id ASC: u1 first, u2 second.
	if entries[0].URL != "u1" || entries[1].URL != "u2" {
		t.Errorf("order: %+v", entries)
	}
	if entries[0].LastChangedAt.IsZero() {
		t.Errorf("u1 should have LastChangedAt set")
	}
	if !entries[1].LastChangedAt.IsZero() {
		t.Errorf("u2 should have zero LastChangedAt")
	}
}

func TestListDocSitemapEntriesNoLimit(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_, _ = s.UpsertDocument(ctx, &Document{URL: "u", Source: "t", FetchedAt: time.Now()})

	entries, err := s.ListDocSitemapEntries(ctx, 0)
	if err != nil {
		t.Fatalf("ListDocSitemapEntries: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("limit=0 (no limit): got %d want 1", len(entries))
	}
}

func TestCountByDomain(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Three docs across two domains.
	_, _ = s.UpsertDocument(ctx, &Document{URL: "https://a.com/1", Domain: "a.com", Source: "t", FetchedAt: time.Now()})
	_, _ = s.UpsertDocument(ctx, &Document{URL: "https://a.com/2", Domain: "a.com", Source: "t", FetchedAt: time.Now()})
	_, _ = s.UpsertDocument(ctx, &Document{URL: "https://b.com/1", Domain: "b.com", Source: "t", FetchedAt: time.Now()})

	counts, err := s.CountByDomain(ctx, 10)
	if err != nil {
		t.Fatalf("CountByDomain: %v", err)
	}
	if counts["a.com"] != 2 {
		t.Errorf("a.com: got %d want 2", counts["a.com"])
	}
	if counts["b.com"] != 1 {
		t.Errorf("b.com: got %d want 1", counts["b.com"])
	}
}

func TestCountByDomainDefaultTopN(t *testing.T) {
	s := newTestStore(t)
	// topN<=0 should use default of 20.
	if _, err := s.CountByDomain(context.Background(), 0); err != nil {
		t.Errorf("CountByDomain default topN: %v", err)
	}
}

func TestCountPassagesAllModelsAndParaphrases(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	id, _ := s.UpsertDocument(ctx, &Document{URL: "u", Source: "t", FetchedAt: time.Now()})

	_ = s.UpsertPassage(ctx, &Passage{DocID: id, Offset: 0, Model: "a", Embedding: []float32{1, 0}})
	_ = s.UpsertPassage(ctx, &Passage{DocID: id, Offset: 10, Model: "b", Embedding: []float32{0, 1}})

	n, err := s.CountPassagesAllModels(ctx)
	if err != nil {
		t.Fatalf("CountPassagesAllModels: %v", err)
	}
	if n != 2 {
		t.Errorf("got %d want 2", n)
	}

	pp, err := s.CountParaphrases(ctx)
	if err != nil {
		t.Fatalf("CountParaphrases empty: %v", err)
	}
	if pp != 0 {
		t.Errorf("empty paraphrases: got %d", pp)
	}
	_ = s.SaveParaphrases(ctx, "m", "q", []string{"p1"})
	_ = s.SaveParaphrases(ctx, "m", "q2", []string{"p2"})
	pp, _ = s.CountParaphrases(ctx)
	if pp != 2 {
		t.Errorf("after save: got %d want 2", pp)
	}
}

func TestCountHyDE(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	n, err := s.CountHyDE(ctx)
	if err != nil {
		t.Fatalf("empty: %v", err)
	}
	if n != 0 {
		t.Errorf("empty hyde: got %d", n)
	}
	_ = s.SaveHyDE(ctx, "m", "q", "passage")
	n, _ = s.CountHyDE(ctx)
	if n != 1 {
		t.Errorf("after save: got %d want 1", n)
	}
}

func TestCountDocsWithPublishedAt(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	pub := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	_, _ = s.UpsertDocument(ctx, &Document{URL: "with", Source: "t", FetchedAt: time.Now(), PublishedAt: pub})
	_, _ = s.UpsertDocument(ctx, &Document{URL: "without", Source: "t", FetchedAt: time.Now()})

	n, err := s.CountDocsWithPublishedAt(ctx)
	if err != nil {
		t.Fatalf("CountDocsWithPublishedAt: %v", err)
	}
	if n != 1 {
		t.Errorf("got %d want 1", n)
	}
}

func TestListOutcomes(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	got, err := s.ListOutcomes(ctx, 0)
	if err != nil {
		t.Fatalf("empty: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("empty outcomes: got %d", len(got))
	}

	for i := 0; i < 3; i++ {
		_ = s.RecordOutcome(ctx, &Outcome{
			Query: "q", URL: "u", Score: float64(i), Useful: i%2 == 0,
			Source: "test", RecordedAt: time.Now(),
		})
	}

	got, err = s.ListOutcomes(ctx, 0)
	if err != nil {
		t.Fatalf("ListOutcomes: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("listed: got %d want 3", len(got))
	}

	got, _ = s.ListOutcomes(ctx, 1)
	if len(got) != 1 {
		t.Errorf("limit=1: got %d", len(got))
	}
}

func TestVacuum(t *testing.T) {
	s := newTestStore(t)
	if err := s.Vacuum(context.Background()); err != nil {
		t.Errorf("Vacuum: %v", err)
	}
}

func TestCountQueuedPerHost(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Empty input.
	m, err := s.CountQueuedPerHost(ctx, nil)
	if err != nil {
		t.Fatalf("empty: %v", err)
	}
	if len(m) != 0 {
		t.Errorf("empty hosts: got %d", len(m))
	}

	// Two URLs from same host queued, one from another.
	_ = s.PushFrontier(ctx, "https://a.com/x", 0, 1)
	_ = s.PushFrontier(ctx, "https://a.com/y", 0, 1)
	_ = s.PushFrontier(ctx, "https://b.com/z", 0, 1)

	m, err = s.CountQueuedPerHost(ctx, []string{"a.com", "b.com", "absent.com"})
	if err != nil {
		t.Fatalf("CountQueuedPerHost: %v", err)
	}
	if m["a.com"] != 2 {
		t.Errorf("a.com: got %d want 2", m["a.com"])
	}
	if m["b.com"] != 1 {
		t.Errorf("b.com: got %d want 1", m["b.com"])
	}
	if _, ok := m["absent.com"]; ok {
		t.Errorf("absent host should not appear")
	}
}

func TestRecrawlURLExisting(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	_ = s.PushFrontier(ctx, "https://x/y", 0, 1.0)
	// Fail it so attempts > 0 and status = 'error'.
	_ = s.FailFrontier(ctx, "https://x/y", "boom")

	if err := s.RecrawlURL(ctx, "https://x/y"); err != nil {
		t.Fatalf("RecrawlURL: %v", err)
	}

	// Verify status is back to queued.
	var status string
	var attempts int
	_ = s.DB().QueryRowContext(ctx, `SELECT status, attempts FROM frontier WHERE url=?;`, "https://x/y").Scan(&status, &attempts)
	if status != "queued" {
		t.Errorf("status: got %q want queued", status)
	}
	if attempts != 0 {
		t.Errorf("attempts: got %d want 0", attempts)
	}
}

func TestRecrawlURLNotPresent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// URL not in frontier — should insert.
	if err := s.RecrawlURL(ctx, "https://new/url"); err != nil {
		t.Fatalf("RecrawlURL: %v", err)
	}
	var status string
	if err := s.DB().QueryRowContext(ctx, `SELECT status FROM frontier WHERE url=?;`, "https://new/url").Scan(&status); err != nil {
		t.Fatalf("query: %v", err)
	}
	if status != "queued" {
		t.Errorf("status: got %q want queued", status)
	}
}
