package config

import (
	"fmt"
	"os"
)

// MaxCommunityModerationsPerDay is the ceiling's own ceiling: a typo can tighten it, never loosen it.
const MaxCommunityModerationsPerDay = 1000

// MaxWorkModerationsPerDay bounds work_moderations_per_day the same way.
const MaxWorkModerationsPerDay = 1000

// Work holds the community and fetch-work settings; an unset key takes its default.
type Work struct {
	// CommunityModerationsPerDay caps the model calls of POST /v1/community/moderate per UTC day.
	// Unset is MaxCommunityModerationsPerDay and 0 halts the route.
	CommunityModerationsPerDay *int `json:"community_moderations_per_day,omitempty"`

	LeaseTTLS             *int `json:"lease_ttl_s,omitempty"`
	MaxLeasesPerWorker    *int `json:"max_leases_per_worker,omitempty"`
	MaxLeasesTotal        *int `json:"max_leases_total,omitempty"`
	DistinctLeasesPerURL  *int `json:"distinct_leases_per_url,omitempty"`
	HostSpacingMS         *int `json:"host_spacing_ms,omitempty"`
	QuorumWindowS         *int `json:"quorum_window_s,omitempty"`
	SimhashMax            *int `json:"simhash_max,omitempty"`
	AttestationTTLH       *int `json:"attestation_ttl_h,omitempty"`
	WorkModerationsPerDay *int `json:"work_moderations_per_day,omitempty"`
	MaxAttempts           *int `json:"max_attempts,omitempty"`
	ModerationAttemptsMax *int `json:"moderation_attempts_max,omitempty"`
	PoolMax               *int `json:"pool_max,omitempty"`
	ErroredScanIntervalS  *int `json:"errored_scan_interval_s,omitempty"`
	ErroredScanBatch      *int `json:"errored_scan_batch,omitempty"`
	RefetchConcurrency    *int `json:"refetch_concurrency,omitempty"`
	ModerationConcurrency *int `json:"moderation_concurrency,omitempty"`
	ModerationQueueMax    *int `json:"moderation_queue_max,omitempty"`
	SweepIntervalS        *int `json:"sweep_interval_s,omitempty"`
}

// WorkSettings is the work block with every default applied and every range checked.
type WorkSettings struct {
	LeaseTTLS                  int
	MaxLeasesPerWorker         int
	MaxLeasesTotal             int
	DistinctLeasesPerURL       int
	HostSpacingMS              int
	QuorumWindowS              int
	SimhashMax                 int
	AttestationTTLH            int
	WorkModerationsPerDay      int
	CommunityModerationsPerDay int
	MaxAttempts                int
	ModerationAttemptsMax      int
	PoolMax                    int
	ErroredScanIntervalS       int
	ErroredScanBatch           int
	RefetchConcurrency         int
	ModerationConcurrency      int
	ModerationQueueMax         int
	SweepIntervalS             int
}

// CommunityModerationLimit is the daily ceiling, or an error naming the key.
func (w Work) CommunityModerationLimit() (int, error) {
	if w.CommunityModerationsPerDay == nil {
		return MaxCommunityModerationsPerDay, nil
	}
	n := *w.CommunityModerationsPerDay
	if n < 0 || n > MaxCommunityModerationsPerDay {
		return 0, fmt.Errorf("work.community_moderations_per_day: must be 0-%d", MaxCommunityModerationsPerDay)
	}
	return n, nil
}

// Settings resolves the block, or fails naming the first key outside its range.
func (w Work) Settings() (WorkSettings, error) {
	var s WorkSettings
	for _, k := range []struct {
		name     string
		set      *int
		dst      *int
		def      int
		min, max int
	}{
		{"lease_ttl_s", w.LeaseTTLS, &s.LeaseTTLS, 600, 60, 3600},
		{"max_leases_per_worker", w.MaxLeasesPerWorker, &s.MaxLeasesPerWorker, 32, 1, 32},
		{"max_leases_total", w.MaxLeasesTotal, &s.MaxLeasesTotal, 2048, 32, 8192},
		{"distinct_leases_per_url", w.DistinctLeasesPerURL, &s.DistinctLeasesPerURL, 3, 2, 3},
		{"host_spacing_ms", w.HostSpacingMS, &s.HostSpacingMS, 2000, 0, 60000},
		{"quorum_window_s", w.QuorumWindowS, &s.QuorumWindowS, 1800, 60, 86400},
		{"simhash_max", w.SimhashMax, &s.SimhashMax, 6, 0, 14},
		{"attestation_ttl_h", w.AttestationTTLH, &s.AttestationTTLH, 168, 24, 168},
		{"work_moderations_per_day", w.WorkModerationsPerDay, &s.WorkModerationsPerDay, MaxWorkModerationsPerDay, 0, MaxWorkModerationsPerDay},
		{"community_moderations_per_day", w.CommunityModerationsPerDay, &s.CommunityModerationsPerDay, MaxCommunityModerationsPerDay, 0, MaxCommunityModerationsPerDay},
		{"max_attempts", w.MaxAttempts, &s.MaxAttempts, 6, 1, 20},
		{"moderation_attempts_max", w.ModerationAttemptsMax, &s.ModerationAttemptsMax, 5, 1, 10},
		{"pool_max", w.PoolMax, &s.PoolMax, 20000, 100, 200000},
		{"errored_scan_interval_s", w.ErroredScanIntervalS, &s.ErroredScanIntervalS, 60, 5, 3600},
		{"errored_scan_batch", w.ErroredScanBatch, &s.ErroredScanBatch, 500, 10, 5000},
		{"refetch_concurrency", w.RefetchConcurrency, &s.RefetchConcurrency, 2, 1, 8},
		{"moderation_concurrency", w.ModerationConcurrency, &s.ModerationConcurrency, 2, 1, 4},
		{"moderation_queue_max", w.ModerationQueueMax, &s.ModerationQueueMax, 256, 8, 2048},
		{"sweep_interval_s", w.SweepIntervalS, &s.SweepIntervalS, 30, 5, 600},
	} {
		v := k.def
		if k.set != nil {
			v = *k.set
		}
		if v < k.min || v > k.max {
			return WorkSettings{}, fmt.Errorf("work.%s: must be %d-%d", k.name, k.min, k.max)
		}
		*k.dst = v
	}
	return s, nil
}

// WorkEnabled reports COSIFT_WORK_ENABLED: unset or 0 is off, 1 is on, anything else is an error.
func WorkEnabled() (bool, error) { return envFlag("COSIFT_WORK_ENABLED", false) }

// WorkFrontier reports COSIFT_WORK_FRONTIER: unset or 1 is on, 0 is off, anything else is an error.
func WorkFrontier() (bool, error) { return envFlag("COSIFT_WORK_FRONTIER", true) }

func envFlag(name string, def bool) (bool, error) {
	switch v := os.Getenv(name); v {
	case "":
		return def, nil
	case "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, fmt.Errorf("%s: must be 0 or 1", name)
	}
}
