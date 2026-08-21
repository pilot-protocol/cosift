package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// runEval -backend pebble — the golden set gating the production BM25 path,
// including distractor ingest into the Pebble index.
func TestRunEvalPebbleBackend(t *testing.T) {
	tmp := t.TempDir()
	corpusPath := filepath.Join(tmp, "corpus.json")
	queriesPath := filepath.Join(tmp, "queries.json")
	mustWriteJSON(t, corpusPath, map[string]any{
		"docs": []map[string]any{
			{"url": "https://x.example/a", "title": "raft consensus", "text": "raft is a distributed consensus algorithm"},
			{"url": "https://x.example/b", "title": "pasta recipe", "text": "boil water with salt then drop pasta"},
		},
	})
	mustWriteJSON(t, queriesPath, map[string]any{
		"name": "pebble-local",
		"queries": []map[string]any{
			{"text": "raft consensus", "relevant": []string{"https://x.example/a"}},
			{"text": "pasta", "relevant": []string{"https://x.example/b"}},
		},
	})
	out := captureStdoutCosift(t, func() {
		err := runEval(context.Background(), []string{
			"-retriever", "bm25",
			"-backend", "pebble",
			"-distractors", "3",
			"-corpus", corpusPath,
			"-queries", queriesPath,
			"-embed-cache", "",
		})
		if err != nil {
			t.Errorf("runEval pebble backend: %v", err)
		}
	})
	if !strings.Contains(out, "pebble-local") {
		t.Errorf("missing summary name: %s", out)
	}
}

func TestRunEvalUnknownBackend(t *testing.T) {
	tmp := t.TempDir()
	corpusPath := filepath.Join(tmp, "c.json")
	queriesPath := filepath.Join(tmp, "q.json")
	mustWriteJSON(t, corpusPath, map[string]any{"docs": []any{}})
	mustWriteJSON(t, queriesPath, map[string]any{
		"queries": []map[string]any{{"text": "x", "relevant": []string{}}},
	})
	err := runEval(context.Background(), []string{
		"-backend", "nonesuch",
		"-corpus", corpusPath,
		"-queries", queriesPath,
	})
	if err == nil || !strings.Contains(err.Error(), "unknown backend") {
		t.Errorf("got %v", err)
	}
}

func TestRunEvalPebbleBackendRequiresBM25(t *testing.T) {
	tmp := t.TempDir()
	corpusPath := filepath.Join(tmp, "c.json")
	queriesPath := filepath.Join(tmp, "q.json")
	mustWriteJSON(t, corpusPath, map[string]any{"docs": []any{}})
	mustWriteJSON(t, queriesPath, map[string]any{
		"queries": []map[string]any{{"text": "x", "relevant": []string{}}},
	})
	err := runEval(context.Background(), []string{
		"-backend", "pebble",
		"-retriever", "dense",
		"-corpus", corpusPath,
		"-queries", queriesPath,
	})
	if err == nil || !strings.Contains(err.Error(), "only supports -retriever bm25") {
		t.Errorf("got %v", err)
	}
}
