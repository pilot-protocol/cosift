package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
)

// TestRunDoctorLocalOnly exercises runDoctor's no-server path: writability,
// sqlite open, pebble SKIP (no dir), config recognition, env summary,
// defaults checks.
func TestRunDoctorLocalOnly(t *testing.T) {
	tmp := t.TempDir()
	cfg := config.Default()
	cfg.DataDir = filepath.Join(tmp, "data")

	// Unset embed/chat keys so the doctor reports MISSING for those.
	t.Setenv("COSIFT_EMBED_API_KEY", "")
	t.Setenv("COSIFT_CHAT_API_KEY", "")
	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("OPENAI", "")
	// Clear any path-2 overrides so the COSIFT_* env line prints "no overrides".
	for _, e := range []string{
		"COSIFT_PEBBLE_CACHE_MB", "COSIFT_PEBBLE_MEMTABLE_MB", "COSIFT_PEBBLE_MEMTABLES",
		"COSIFT_PEBBLE_SYNC", "COSIFT_BM25_K1", "COSIFT_BM25_B",
		"COSIFT_BM25_TOPK_POOL_FACTOR", "COSIFT_BM25_DISABLE_TOPK_POOL",
		"COSIFT_HYDE_CACHE_SIZE", "COSIFT_PARA_CACHE_SIZE", "COSIFT_LOAD_HNSW",
	} {
		t.Setenv(e, "")
	}

	stdout := captureStdoutCosift(t, func() {
		if err := runDoctor(context.Background(), cfg, nil); err != nil {
			t.Errorf("runDoctor (local): %v", err)
		}
	})
	for _, want := range []string{"data_dir writable", "sqlite open + schema", "config"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("missing %q in doctor output:\n%s", want, stdout)
		}
	}
}

func TestRunDoctorJSON(t *testing.T) {
	tmp := t.TempDir()
	cfg := config.Default()
	cfg.DataDir = filepath.Join(tmp, "data")

	t.Setenv("OPENAI_API_KEY", "")
	stdout := captureStdoutCosift(t, func() {
		if err := runDoctor(context.Background(), cfg, []string{"-json"}); err != nil {
			t.Errorf("runDoctor (-json): %v", err)
		}
	})
	if !strings.HasPrefix(strings.TrimSpace(stdout), "[") &&
		!strings.HasPrefix(strings.TrimSpace(stdout), "{") {
		t.Errorf("doctor -json should emit JSON; got:\n%s", stdout)
	}
}
