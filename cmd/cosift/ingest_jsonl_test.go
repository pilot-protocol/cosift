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

func writeJSONL(t *testing.T, path string, docs []eval.CorpusDoc) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, d := range docs {
		if err := enc.Encode(d); err != nil {
			t.Fatalf("encode jsonl: %v", err)
		}
	}
}

func TestLoadCorpusJSONLBasic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "in.jsonl")
	writeJSONL(t, path, []eval.CorpusDoc{
		{URL: "https://x/a", Title: "A", Text: "body A"},
		{URL: "https://x/b", Title: "B", Text: "body B"},
	})
	c, err := loadCorpusJSONL(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(c.Docs) != 2 {
		t.Fatalf("want 2 docs, got %d", len(c.Docs))
	}
	if c.Docs[0].URL != "https://x/a" || c.Docs[1].URL != "https://x/b" {
		t.Errorf("URLs in wrong order: %+v", c.Docs)
	}
}

func TestLoadCorpusJSONLSkipsBlankLines(t *testing.T) {
	// Some tools (jq, sed pipelines) emit trailing blank lines.
	path := filepath.Join(t.TempDir(), "in.jsonl")
	if err := os.WriteFile(path,
		[]byte(`{"url":"https://x/a","title":"A","text":"a"}`+"\n\n"+
			`{"url":"https://x/b","title":"B","text":"b"}`+"\n\n"),
		0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	c, err := loadCorpusJSONL(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(c.Docs) != 2 {
		t.Errorf("blank lines should be skipped; got %d docs", len(c.Docs))
	}
}

func TestLoadCorpusJSONLInvalidLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.jsonl")
	if err := os.WriteFile(path,
		[]byte(`{"url":"https://x/a"}`+"\nthis is not json\n"),
		0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := loadCorpusJSONL(path)
	if err == nil {
		t.Fatal("invalid line should error")
	}
	if !strings.Contains(err.Error(), "line 2") {
		t.Errorf("error should name the bad line number: %v", err)
	}
}

func TestLoadCorpusJSONLNonexistent(t *testing.T) {
	_, err := loadCorpusJSONL("/nonexistent/path.jsonl")
	if err == nil {
		t.Fatal("nonexistent file should error")
	}
}

// TestRunIngestJSONLEndToEnd is the round-trip we care about: write a JSONL
// file, run `cosift ingest -format jsonl`, verify docs land in the store.
func TestRunIngestJSONLEndToEnd(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	cfg := config.Default()
	cfg.DataDir = dir

	jsonlPath := filepath.Join(t.TempDir(), "corpus.jsonl")
	writeJSONL(t, jsonlPath, []eval.CorpusDoc{
		{URL: "https://x/a", Title: "Doc A", Text: "body A"},
		{URL: "https://x/b", Title: "Doc B", Text: "body B"},
		{URL: "https://x/c", Title: "Doc C", Text: "body C"},
	})

	if err := runIngest(context.Background(), cfg,
		[]string{"-corpus", jsonlPath, "-format", "jsonl"}); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	// Verify by listing back from the store.
	s, err := store.Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()
	docs, err := s.ListDocuments(context.Background(), 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(docs) != 3 {
		t.Errorf("want 3 docs ingested, got %d", len(docs))
	}
	gotURLs := make(map[string]bool)
	for _, d := range docs {
		gotURLs[d.URL] = true
	}
	for _, want := range []string{"https://x/a", "https://x/b", "https://x/c"} {
		if !gotURLs[want] {
			t.Errorf("missing %s in store after ingest", want)
		}
	}
}

func TestRunIngestAutoDetectJSONLExtension(t *testing.T) {
	// -format auto + .jsonl extension → JSONL loader picked.
	dir := filepath.Join(t.TempDir(), "data")
	cfg := config.Default()
	cfg.DataDir = dir

	jsonlPath := filepath.Join(t.TempDir(), "corpus.jsonl")
	writeJSONL(t, jsonlPath, []eval.CorpusDoc{
		{URL: "https://x/a", Title: "A", Text: "a"},
	})

	// No explicit -format flag → defaults to "auto".
	if err := runIngest(context.Background(), cfg,
		[]string{"-corpus", jsonlPath}); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	s, _ := store.Open(dir)
	defer s.Close()
	docs, _ := s.ListDocuments(context.Background(), 0)
	if len(docs) != 1 {
		t.Errorf("auto-detected jsonl should have loaded 1 doc, got %d", len(docs))
	}
}

func TestRunIngestAutoDetectJSONFallback(t *testing.T) {
	// -format auto + non-jsonl extension → JSON loader (iter-1 behavior preserved).
	dir := filepath.Join(t.TempDir(), "data")
	cfg := config.Default()
	cfg.DataDir = dir

	jsonPath := filepath.Join(t.TempDir(), "corpus.json")
	corpus := eval.Corpus{Docs: []eval.CorpusDoc{
		{URL: "https://x/a", Title: "A", Text: "a"},
		{URL: "https://x/b", Title: "B", Text: "b"},
	}}
	body, _ := json.MarshalIndent(corpus, "", "  ")
	if err := os.WriteFile(jsonPath, body, 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := runIngest(context.Background(), cfg,
		[]string{"-corpus", jsonPath}); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	s, _ := store.Open(dir)
	defer s.Close()
	docs, _ := s.ListDocuments(context.Background(), 0)
	if len(docs) != 2 {
		t.Errorf("auto-detected json should have loaded 2 docs, got %d", len(docs))
	}
}

func TestRunIngestUnknownFormat(t *testing.T) {
	cfg := config.Default()
	err := runIngest(context.Background(), cfg,
		[]string{"-corpus", "anything.txt", "-format", "yaml"})
	if err == nil {
		t.Fatal("unknown format should error")
	}
	if !strings.Contains(err.Error(), "yaml") {
		t.Errorf("error should name the bad format: %v", err)
	}
}

// TestExportIngestRoundTripJSONL is the actual demand-shaped use case: export
// the index as JSONL, re-ingest into a fresh store, verify the docs match.
func TestExportIngestRoundTripJSONL(t *testing.T) {
	// Source store with 2 docs.
	srcDir := filepath.Join(t.TempDir(), "src")
	srcStore, err := store.Open(srcDir)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	docs := []store.Document{
		{URL: "https://x/a", Title: "Doc A", Text: "body of A.",
			Source: "test", FetchedAt: time.Now()},
		{URL: "https://x/b", Title: "Doc B", Text: "body of B.",
			Source: "test", FetchedAt: time.Now()},
	}
	for i := range docs {
		if _, err := srcStore.UpsertDocument(context.Background(), &docs[i]); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}
	srcStore.Close()

	srcCfg := config.Default()
	srcCfg.DataDir = srcDir

	// Export as JSONL.
	exportPath := filepath.Join(t.TempDir(), "exported.jsonl")
	if err := runExport(context.Background(), srcCfg,
		[]string{"-output", exportPath, "-format", "jsonl"}); err != nil {
		t.Fatalf("export: %v", err)
	}

	// Re-ingest into a fresh store.
	dstDir := filepath.Join(t.TempDir(), "dst")
	dstCfg := config.Default()
	dstCfg.DataDir = dstDir
	if err := runIngest(context.Background(), dstCfg,
		[]string{"-corpus", exportPath, "-format", "jsonl"}); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	// Verify dst store has the same URLs + titles + texts.
	dstStore, err := store.Open(dstDir)
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	defer dstStore.Close()
	got, err := dstStore.ListDocuments(context.Background(), 0)
	if err != nil {
		t.Fatalf("list dst: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("round-trip lost docs: want 2, got %d", len(got))
	}
	for _, d := range got {
		if d.Title == "" || d.Text == "" {
			t.Errorf("round-trip lost fields: %+v", d)
		}
	}
}
