package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommunityModerationLimit(t *testing.T) {
	ptr := func(n int) *int { return &n }
	for _, tc := range []struct {
		name string
		set  *int
		want int
		bad  bool
	}{
		{"unset is the default", nil, 1000, false},
		{"zero halts", ptr(0), 0, false},
		{"the maximum", ptr(1000), 1000, false},
		{"a lower ceiling", ptr(25), 25, false},
		{"above the maximum", ptr(1001), 0, true},
		{"negative", ptr(-1), 0, true},
	} {
		got, err := Work{CommunityModerationsPerDay: tc.set}.CommunityModerationLimit()
		if tc.bad {
			if err == nil || !strings.Contains(err.Error(), "work.community_moderations_per_day") {
				t.Errorf("%s: %d %v, want an error naming the key", tc.name, got, err)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("%s: %d %v, want %d", tc.name, got, err, tc.want)
		}
	}
}

func TestLoadReadsTheWorkBlock(t *testing.T) {
	unsetEnvWithRestore(t, "PORT", "COSIFT_LISTEN", "COSIFT_DATA_DIR")
	path := filepath.Join(t.TempDir(), "cosift.json")
	if err := os.WriteFile(path, []byte(`{"work":{"community_moderations_per_day":0}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := cfg.Work.CommunityModerationLimit(); err != nil || got != 0 {
		t.Fatalf("a configured 0 must halt, not fall back to the default: %d %v", got, err)
	}
}
