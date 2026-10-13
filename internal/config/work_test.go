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

func TestWorkSettingsDefaultsAndRanges(t *testing.T) {
	def, err := Work{}.Settings()
	if err != nil {
		t.Fatal(err)
	}
	want := WorkSettings{
		LeaseTTLS: 600, MaxLeasesPerWorker: 32, MaxLeasesTotal: 2048, DistinctLeasesPerURL: 3, HostSpacingMS: 2000,
		QuorumWindowS: 1800, SimhashMax: 6, AttestationTTLH: 168, WorkModerationsPerDay: 1000, CommunityModerationsPerDay: 1000,
		MaxAttempts: 6, ModerationAttemptsMax: 5, PoolMax: 20000, ErroredScanIntervalS: 60, ErroredScanBatch: 500,
		RefetchConcurrency: 2, ModerationConcurrency: 2, ModerationQueueMax: 256, SweepIntervalS: 30,
	}
	if def != want {
		t.Fatalf("defaults:\n got %+v\nwant %+v", def, want)
	}
	ptr := func(n int) *int { return &n }
	for _, tc := range []struct {
		key      string
		set      func(*Work, *int)
		min, max int
	}{
		{"lease_ttl_s", func(w *Work, v *int) { w.LeaseTTLS = v }, 60, 3600},
		{"max_leases_per_worker", func(w *Work, v *int) { w.MaxLeasesPerWorker = v }, 1, 32},
		{"max_leases_total", func(w *Work, v *int) { w.MaxLeasesTotal = v }, 32, 8192},
		{"distinct_leases_per_url", func(w *Work, v *int) { w.DistinctLeasesPerURL = v }, 2, 3},
		{"host_spacing_ms", func(w *Work, v *int) { w.HostSpacingMS = v }, 0, 60000},
		{"quorum_window_s", func(w *Work, v *int) { w.QuorumWindowS = v }, 60, 86400},
		{"simhash_max", func(w *Work, v *int) { w.SimhashMax = v }, 0, 14},
		{"attestation_ttl_h", func(w *Work, v *int) { w.AttestationTTLH = v }, 24, 168},
		{"work_moderations_per_day", func(w *Work, v *int) { w.WorkModerationsPerDay = v }, 0, 1000},
		{"max_attempts", func(w *Work, v *int) { w.MaxAttempts = v }, 1, 20},
		{"moderation_attempts_max", func(w *Work, v *int) { w.ModerationAttemptsMax = v }, 1, 10},
		{"pool_max", func(w *Work, v *int) { w.PoolMax = v }, 100, 200000},
		{"errored_scan_interval_s", func(w *Work, v *int) { w.ErroredScanIntervalS = v }, 5, 3600},
		{"errored_scan_batch", func(w *Work, v *int) { w.ErroredScanBatch = v }, 10, 5000},
		{"refetch_concurrency", func(w *Work, v *int) { w.RefetchConcurrency = v }, 1, 8},
		{"moderation_concurrency", func(w *Work, v *int) { w.ModerationConcurrency = v }, 1, 4},
		{"moderation_queue_max", func(w *Work, v *int) { w.ModerationQueueMax = v }, 8, 2048},
		{"sweep_interval_s", func(w *Work, v *int) { w.SweepIntervalS = v }, 5, 600},
	} {
		for _, edge := range []int{tc.min, tc.max} {
			var w Work
			tc.set(&w, ptr(edge))
			if _, err := w.Settings(); err != nil {
				t.Errorf("%s=%d: %v", tc.key, edge, err)
			}
		}
		for _, edge := range []int{tc.min - 1, tc.max + 1} {
			var w Work
			tc.set(&w, ptr(edge))
			if _, err := w.Settings(); err == nil || !strings.Contains(err.Error(), "work."+tc.key) {
				t.Errorf("%s=%d: %v, want an error naming the key", tc.key, edge, err)
			}
		}
	}
}

func TestWorkSettingsKeepAConfiguredZero(t *testing.T) {
	zero := 0
	s, err := Work{WorkModerationsPerDay: &zero, HostSpacingMS: &zero, SimhashMax: &zero}.Settings()
	if err != nil || s.WorkModerationsPerDay != 0 || s.HostSpacingMS != 0 || s.SimhashMax != 0 {
		t.Fatalf("a configured 0 must stay 0: %+v %v", s, err)
	}
}

func TestWorkFlags(t *testing.T) {
	for _, tc := range []struct {
		env, val string
		fn       func() (bool, error)
		want     bool
		bad      bool
	}{
		{"COSIFT_WORK_ENABLED", "", WorkEnabled, false, false},
		{"COSIFT_WORK_ENABLED", "0", WorkEnabled, false, false},
		{"COSIFT_WORK_ENABLED", "1", WorkEnabled, true, false},
		{"COSIFT_WORK_ENABLED", "true", WorkEnabled, false, true},
		{"COSIFT_WORK_FRONTIER", "", WorkFrontier, true, false},
		{"COSIFT_WORK_FRONTIER", "1", WorkFrontier, true, false},
		{"COSIFT_WORK_FRONTIER", "0", WorkFrontier, false, false},
		{"COSIFT_WORK_FRONTIER", "off", WorkFrontier, false, true},
	} {
		t.Setenv(tc.env, tc.val)
		got, err := tc.fn()
		if tc.bad != (err != nil) || (!tc.bad && got != tc.want) {
			t.Errorf("%s=%q: %v %v", tc.env, tc.val, got, err)
		}
	}
}
