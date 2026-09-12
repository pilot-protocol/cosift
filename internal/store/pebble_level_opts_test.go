package store

import (
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestLevelOptsUnsetMatchPebbleDefaults(t *testing.T) {
	for _, k := range []string{"COSIFT_PEBBLE_TARGET_FILE_MB", "COSIFT_PEBBLE_LBASE_MB", "COSIFT_PEBBLE_L0_COMPACTION_FILES", "COSIFT_PEBBLE_L0_STOP_WRITES"} {
		t.Setenv(k, "")
	}
	got := &pebble.Options{}
	applyLevelOptsFromEnv(got)
	if got.Levels != nil || got.LBaseMaxBytes != 0 || got.L0CompactionFileThreshold != 0 || got.L0StopWritesThreshold != 0 {
		t.Fatalf("unset env must not touch options: %+v", got)
	}
	got.EnsureDefaults()
	want := (&pebble.Options{}).EnsureDefaults()
	for i := 0; i < 7; i++ {
		if g, w := got.Level(i).TargetFileSize, want.Level(i).TargetFileSize; g != w {
			t.Errorf("L%d TargetFileSize: got %d want %d", i, g, w)
		}
	}
	if got.LBaseMaxBytes != want.LBaseMaxBytes || got.L0CompactionFileThreshold != want.L0CompactionFileThreshold || got.L0StopWritesThreshold != want.L0StopWritesThreshold {
		t.Errorf("defaults drifted: got lbase=%d l0files=%d l0stop=%d want %d %d %d",
			got.LBaseMaxBytes, got.L0CompactionFileThreshold, got.L0StopWritesThreshold,
			want.LBaseMaxBytes, want.L0CompactionFileThreshold, want.L0StopWritesThreshold)
	}
}

func TestLevelOptsFromEnv(t *testing.T) {
	t.Setenv("COSIFT_PEBBLE_TARGET_FILE_MB", "16")
	t.Setenv("COSIFT_PEBBLE_LBASE_MB", "512")
	t.Setenv("COSIFT_PEBBLE_L0_COMPACTION_FILES", "50")
	t.Setenv("COSIFT_PEBBLE_L0_STOP_WRITES", "24")
	opts := &pebble.Options{}
	applyLevelOptsFromEnv(opts)
	opts.EnsureDefaults()
	for i := 0; i < 7; i++ {
		if g, w := opts.Level(i).TargetFileSize, int64(16)<<20<<i; g != w {
			t.Errorf("L%d TargetFileSize: got %d want %d", i, g, w)
		}
	}
	if opts.LBaseMaxBytes != 512<<20 {
		t.Errorf("LBaseMaxBytes: got %d", opts.LBaseMaxBytes)
	}
	if opts.L0CompactionFileThreshold != 50 {
		t.Errorf("L0CompactionFileThreshold: got %d", opts.L0CompactionFileThreshold)
	}
	if opts.L0StopWritesThreshold != 24 {
		t.Errorf("L0StopWritesThreshold: got %d", opts.L0StopWritesThreshold)
	}
}

func TestLevelOptsPartialEnv(t *testing.T) {
	t.Setenv("COSIFT_PEBBLE_TARGET_FILE_MB", "")
	t.Setenv("COSIFT_PEBBLE_LBASE_MB", "256")
	t.Setenv("COSIFT_PEBBLE_L0_COMPACTION_FILES", "0")
	t.Setenv("COSIFT_PEBBLE_L0_STOP_WRITES", "bogus")
	opts := &pebble.Options{}
	applyLevelOptsFromEnv(opts)
	opts.EnsureDefaults()
	want := (&pebble.Options{}).EnsureDefaults()
	if opts.LBaseMaxBytes != 256<<20 {
		t.Errorf("LBaseMaxBytes: got %d", opts.LBaseMaxBytes)
	}
	if opts.Level(0).TargetFileSize != want.Level(0).TargetFileSize || opts.L0CompactionFileThreshold != want.L0CompactionFileThreshold || opts.L0StopWritesThreshold != want.L0StopWritesThreshold {
		t.Errorf("unset/invalid vars must keep defaults: %+v", opts)
	}
}

func TestOpenPebbleWithLevelOpts(t *testing.T) {
	t.Setenv("COSIFT_PEBBLE_TARGET_FILE_MB", "4")
	t.Setenv("COSIFT_PEBBLE_LBASE_MB", "128")
	t.Setenv("COSIFT_PEBBLE_L0_COMPACTION_FILES", "20")
	t.Setenv("COSIFT_PEBBLE_L0_STOP_WRITES", "16")
	p, err := OpenPebble(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	defer p.Close()
}
