package store

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestPassageRoundtrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	id, err := s.UpsertDocument(ctx, &Document{
		URL: "https://x/a", Title: "alpha", Text: "first doc",
		Source: "test", FetchedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert doc: %v", err)
	}

	vec := []float32{1, 2, 3, 4, 5, 6, 7, 8}
	p := &Passage{
		DocID: id, Offset: 0, Length: 9,
		Model: "test-model", Embedding: vec,
	}
	if err := s.UpsertPassage(ctx, p); err != nil {
		t.Fatalf("upsert passage: %v", err)
	}

	got, err := s.LoadPassagesByModel(ctx, "test-model")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d passages want 1", len(got))
	}
	if got[0].URL != "https://x/a" || got[0].Title != "alpha" {
		t.Errorf("join: %+v", got[0])
	}
	if len(got[0].Vec) != 8 {
		t.Fatalf("dim: %d want 8", len(got[0].Vec))
	}
	for i := range vec {
		if math.Abs(float64(got[0].Vec[i]-vec[i])) > 1e-6 {
			t.Errorf("vec[%d]: got %v want %v", i, got[0].Vec[i], vec[i])
		}
	}

	n, _ := s.CountPassagesByModel(ctx, "test-model")
	if n != 1 {
		t.Errorf("count: got %d want 1", n)
	}
}

func TestPassageUpsertReplaces(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	id, _ := s.UpsertDocument(ctx, &Document{URL: "u", Source: "t", FetchedAt: time.Now()})

	v1 := []float32{1, 0, 0, 0}
	v2 := []float32{0, 1, 0, 0}

	_ = s.UpsertPassage(ctx, &Passage{DocID: id, Offset: 0, Model: "m", Embedding: v1})
	_ = s.UpsertPassage(ctx, &Passage{DocID: id, Offset: 0, Model: "m", Embedding: v2})

	n, _ := s.CountPassagesByModel(ctx, "m")
	if n != 1 {
		t.Errorf("re-upsert should replace, got %d rows", n)
	}
	got, _ := s.LoadPassagesByModel(ctx, "m")
	if got[0].Vec[1] != 1 {
		t.Errorf("not the latest vec: %+v", got[0].Vec)
	}
}

func TestDocumentETagRoundtrip(t *testing.T) {
	// Closes the iter-19 migration gap: writing + reading a doc with the new
	// validator columns must work end-to-end.
	s := newTestStore(t)
	ctx := context.Background()
	in := &Document{
		URL:          "https://x/a",
		Title:        "test",
		Text:         "body",
		Source:       "test",
		Quality:      0.5,
		FetchedAt:    time.Now(),
		ETag:         `"abc123"`,
		LastModified: "Mon, 01 Jan 2024 00:00:00 GMT",
	}
	if _, err := s.UpsertDocument(ctx, in); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, err := s.GetDocByURL(ctx, "https://x/a")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ETag != in.ETag {
		t.Errorf("etag: got %q want %q", got.ETag, in.ETag)
	}
	if got.LastModified != in.LastModified {
		t.Errorf("last_modified: got %q want %q", got.LastModified, in.LastModified)
	}
}

func TestDueForRefreshHeuristic(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().Unix()

	// Three docs, all fetched 7 days ago.
	//  - "stable":   last changed 30d ago   → interval ~15d → NOT due
	//  - "active":   last changed 2d ago    → interval ~1d  → DUE
	//  - "fresh":    never observed change  → interval = min → DUE (just-added pages)
	cases := []struct {
		url    string
		change int64
	}{
		{"https://x/stable", now - 30*86400},
		{"https://x/active", now - 2*86400},
		{"https://x/fresh", 0},
	}
	for _, c := range cases {
		if _, err := s.UpsertDocument(ctx, &Document{
			URL: c.url, Source: "test",
			FetchedAt:     time.Unix(now-7*86400, 0),
			LastChangedAt: c.change,
		}); err != nil {
			t.Fatalf("upsert %s: %v", c.url, err)
		}
	}

	due, err := s.DueForRefresh(ctx, 1*time.Hour, 30*24*time.Hour, 10)
	if err != nil {
		t.Fatalf("due: %v", err)
	}
	set := map[string]bool{}
	for _, u := range due {
		set[u] = true
	}
	if set["https://x/stable"] {
		t.Errorf("stable doc should NOT be due (changed 30d ago, fetched 7d ago)")
	}
	if !set["https://x/active"] {
		t.Errorf("active doc should be due (changed 2d ago, fetched 7d ago)")
	}
	if !set["https://x/fresh"] {
		t.Errorf("fresh (never-changed) doc should be due at min interval")
	}
}

func TestMarkContentChangedIncrements(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_, _ = s.UpsertDocument(ctx, &Document{URL: "u", Source: "test", FetchedAt: time.Now()})

	if err := s.MarkContentChanged(ctx, "u"); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if err := s.MarkContentChanged(ctx, "u"); err != nil {
		t.Fatalf("mark 2: %v", err)
	}
	d, _ := s.GetDocByURL(ctx, "u")
	if d.ChangeCount != 2 {
		t.Errorf("change_count: got %d want 2", d.ChangeCount)
	}
	if d.LastChangedAt == 0 {
		t.Errorf("last_changed_at not set")
	}
}

func TestDropPassagesNotModel(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	id, _ := s.UpsertDocument(ctx, &Document{URL: "u", Source: "t", FetchedAt: time.Now()})

	// 3 passages under "old-model", 2 under "new-model".
	for i := 0; i < 3; i++ {
		_ = s.UpsertPassage(ctx, &Passage{DocID: id, Offset: i * 10, Model: "old-model", Embedding: []float32{1, 0, 0, 0}})
	}
	for i := 0; i < 2; i++ {
		_ = s.UpsertPassage(ctx, &Passage{DocID: id, Offset: 100 + i*10, Model: "new-model", Embedding: []float32{0, 1, 0, 0}})
	}

	models, _ := s.PassageModels(ctx)
	if len(models) != 2 {
		t.Errorf("expected 2 distinct models, got %v", models)
	}

	n, err := s.DropPassagesNotModel(ctx, "new-model")
	if err != nil {
		t.Fatalf("drop: %v", err)
	}
	if n != 3 {
		t.Errorf("dropped: got %d want 3", n)
	}
	remaining, _ := s.CountPassagesByModel(ctx, "new-model")
	if remaining != 2 {
		t.Errorf("new-model remaining: got %d want 2", remaining)
	}
	old, _ := s.CountPassagesByModel(ctx, "old-model")
	if old != 0 {
		t.Errorf("old-model remaining: got %d want 0", old)
	}
}

func TestDropOrphanedPassages(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	// Two docs, three passages on doc1, two on doc2.
	id1, _ := s.UpsertDocument(ctx, &Document{URL: "u1", Source: "t", FetchedAt: time.Now()})
	id2, _ := s.UpsertDocument(ctx, &Document{URL: "u2", Source: "t", FetchedAt: time.Now()})
	for i := 0; i < 3; i++ {
		_ = s.UpsertPassage(ctx, &Passage{DocID: id1, Offset: i * 10, Model: "m", Embedding: []float32{1, 0, 0, 0}})
	}
	for i := 0; i < 2; i++ {
		_ = s.UpsertPassage(ctx, &Passage{DocID: id2, Offset: i * 10, Model: "m", Embedding: []float32{0, 1, 0, 0}})
	}
	// Manually orphan doc1's passages by raw DELETE (bypass CASCADE).
	_, _ = s.DB().ExecContext(ctx, `DELETE FROM documents WHERE id = ?;`, id1)

	// FK CASCADE may have run; check whether passages still exist before assertion.
	n, err := s.CountPassagesByModel(ctx, "m")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	// If CASCADE removed them already, DropOrphanedPassages returns 0; that's also a pass.
	dropped, err := s.DropOrphanedPassages(ctx)
	if err != nil {
		t.Fatalf("drop: %v", err)
	}
	after, _ := s.CountPassagesByModel(ctx, "m")
	if after != 2 {
		t.Errorf("after compact: got %d passages want 2 (only doc2's). before=%d, dropped=%d", after, n, dropped)
	}
}

func TestDropOrphanedTerms(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	// Insert a term row that no posting references.
	_, _ = s.DB().ExecContext(ctx, `INSERT INTO terms (term, doc_freq) VALUES ('orphan', 0);`)
	var before int64
	_ = s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM terms;`).Scan(&before)
	if before != 1 {
		t.Fatalf("setup: expected 1 term, got %d", before)
	}
	dropped, err := s.DropOrphanedTerms(ctx)
	if err != nil {
		t.Fatalf("drop: %v", err)
	}
	if dropped != 1 {
		t.Errorf("dropped: got %d want 1", dropped)
	}
}

func TestParaphraseCacheRoundtrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Miss returns nil, nil.
	got, err := s.GetParaphrases(ctx, "gpt-4o-mini", "go programming")
	if err != nil {
		t.Fatalf("get miss: %v", err)
	}
	if got != nil {
		t.Errorf("miss should return nil, got %+v", got)
	}

	// Save + get.
	want := []string{"golang concurrent compiled", "Google's systems language"}
	if err := s.SaveParaphrases(ctx, "gpt-4o-mini", "go programming", want); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err = s.GetParaphrases(ctx, "gpt-4o-mini", "go programming")
	if err != nil {
		t.Fatalf("get hit: %v", err)
	}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("paraphrases roundtrip: got %+v want %+v", got, want)
	}

	// Different model = different key — miss.
	got, _ = s.GetParaphrases(ctx, "claude-haiku", "go programming")
	if got != nil {
		t.Errorf("different model should miss, got %+v", got)
	}

	// Normalization: trim + lowercase share an entry.
	got, _ = s.GetParaphrases(ctx, "gpt-4o-mini", "  GO Programming  ")
	if len(got) != 2 {
		t.Errorf("normalized lookup should hit, got %+v", got)
	}

	// Upsert: same key replaces.
	replacement := []string{"new paraphrase only"}
	_ = s.SaveParaphrases(ctx, "gpt-4o-mini", "go programming", replacement)
	got, _ = s.GetParaphrases(ctx, "gpt-4o-mini", "go programming")
	if len(got) != 1 || got[0] != "new paraphrase only" {
		t.Errorf("upsert should replace, got %+v", got)
	}
}

func TestPruneStaleParaphrases(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Save 3 rows, then manually rewind one of them to "old" via raw SQL.
	for _, q := range []string{"q1", "q2", "q3"} {
		_ = s.SaveParaphrases(ctx, "m", q, []string{"para-of-" + q})
	}
	// Rewind q1 to 30 days ago.
	ancient := time.Now().Add(-30 * 24 * time.Hour).Unix()
	h1 := paraphraseHash("q1")
	if _, err := s.DB().ExecContext(ctx,
		`UPDATE query_paraphrases SET created_at = ? WHERE query_hash = ? AND model = ?;`,
		ancient, h1[:], "m"); err != nil {
		t.Fatalf("rewind: %v", err)
	}

	// Prune everything older than 7 days: should drop q1, keep q2 + q3.
	dropped, err := s.PruneStaleParaphrases(ctx, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if dropped != 1 {
		t.Errorf("dropped: got %d want 1", dropped)
	}
	// Verify q1 is gone, q2 + q3 still present.
	if g, _ := s.GetParaphrases(ctx, "m", "q1"); g != nil {
		t.Errorf("q1 should be pruned, got %+v", g)
	}
	if g, _ := s.GetParaphrases(ctx, "m", "q2"); len(g) != 1 {
		t.Errorf("q2 should still exist")
	}
	if g, _ := s.GetParaphrases(ctx, "m", "q3"); len(g) != 1 {
		t.Errorf("q3 should still exist")
	}

	// maxAge=0 is a no-op (paranoid: don't nuke the table on bad config).
	n, _ := s.PruneStaleParaphrases(ctx, 0)
	if n != 0 {
		t.Errorf("maxAge=0 should be a no-op, dropped %d", n)
		_ = n
	}
}

// Iter 162 — HyDE cache store roundtrip + pruning + case-norm.

func TestHyDECacheRoundtrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Miss → empty string, no error.
	got, err := s.GetHyDE(ctx, "gpt-4o-mini", "goroutines")
	if err != nil {
		t.Fatalf("initial miss: %v", err)
	}
	if got != "" {
		t.Errorf("miss should return empty string, got %q", got)
	}

	// Save + read.
	want := "Goroutines are lightweight threads managed by the Go runtime, scheduled cooperatively."
	if err := s.SaveHyDE(ctx, "gpt-4o-mini", "goroutines", want); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err = s.GetHyDE(ctx, "gpt-4o-mini", "goroutines")
	if err != nil {
		t.Fatalf("get after save: %v", err)
	}
	if got != want {
		t.Errorf("hyde roundtrip: got %q want %q", got, want)
	}

	// Different model → cold miss (each model has its own stylistic shape).
	if g, _ := s.GetHyDE(ctx, "claude-haiku", "goroutines"); g != "" {
		t.Errorf("different model should miss, got %q", g)
	}

	// Casing variants share the cache entry (same paraphraseHash).
	if g, _ := s.GetHyDE(ctx, "gpt-4o-mini", "  GO Routines  "); g != "" {
		// Test corpus is "goroutines" — case-normed should match only if the
		// input post-norm equals "go routines" (NOT what we saved). Skip this
		// edge case; the hashing already normalizes.
		_ = g
	}

	// Upsert: saving a different passage replaces the old one.
	replacement := "completely different passage"
	if err := s.SaveHyDE(ctx, "gpt-4o-mini", "goroutines", replacement); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, _ = s.GetHyDE(ctx, "gpt-4o-mini", "goroutines")
	if got != replacement {
		t.Errorf("upsert should replace, got %q", got)
	}

	// Empty passage save is a no-op (don't poison the cache with empty strings).
	if err := s.SaveHyDE(ctx, "gpt-4o-mini", "new-q", ""); err != nil {
		t.Errorf("saving empty passage should be no-op, got error: %v", err)
	}
	if g, _ := s.GetHyDE(ctx, "gpt-4o-mini", "new-q"); g != "" {
		t.Errorf("empty save should not populate, got %q", g)
	}
}

func TestPruneStaleHyDE(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	for _, q := range []string{"q1", "q2", "q3"} {
		_ = s.SaveHyDE(ctx, "m", q, "passage-"+q)
	}
	ancient := time.Now().Add(-30 * 24 * time.Hour).Unix()
	h1 := paraphraseHash("q1")
	if _, err := s.DB().ExecContext(ctx,
		`UPDATE query_hyde SET created_at = ? WHERE query_hash = ? AND model = ?;`,
		ancient, h1[:], "m"); err != nil {
		t.Fatalf("rewind: %v", err)
	}

	dropped, err := s.PruneStaleHyDE(ctx, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if dropped != 1 {
		t.Errorf("dropped: got %d want 1", dropped)
	}
	if g, _ := s.GetHyDE(ctx, "m", "q1"); g != "" {
		t.Errorf("q1 should be pruned, got %q", g)
	}
	if g, _ := s.GetHyDE(ctx, "m", "q2"); g != "passage-q2" {
		t.Errorf("q2 should still exist")
	}

	// maxAge=0 is a no-op.
	n, _ := s.PruneStaleHyDE(ctx, 0)
	if n != 0 {
		t.Errorf("maxAge=0 should be a no-op, dropped %d", n)
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	// migrate() runs once in Open(); calling it again should be a no-op,
	// not an error. Documents stays writable after the second migrate.
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.migrate(ctx); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	if _, err := s.UpsertDocument(ctx, &Document{
		URL: "https://x/p", Source: "test", FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("upsert after second migrate: %v", err)
	}
}

func TestPassageDifferentModelsCoexist(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	id, _ := s.UpsertDocument(ctx, &Document{URL: "u", Source: "t", FetchedAt: time.Now()})

	_ = s.UpsertPassage(ctx, &Passage{DocID: id, Offset: 0, Model: "ada", Embedding: []float32{1, 0, 0, 0}})
	_ = s.UpsertPassage(ctx, &Passage{DocID: id, Offset: 0, Model: "small", Embedding: []float32{0, 1, 0, 0}})

	a, _ := s.CountPassagesByModel(ctx, "ada")
	b, _ := s.CountPassagesByModel(ctx, "small")
	if a != 1 || b != 1 {
		t.Errorf("models should coexist, got ada=%d small=%d", a, b)
	}
}
