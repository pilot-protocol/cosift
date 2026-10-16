package work

import (
	"fmt"
	"io"
	"sort"
)

// PoolStats counts pool entries by class.
type PoolStats struct {
	Total    int `json:"total"`
	Pushed   int `json:"pushed"`
	Errored  int `json:"errored"`
	Frontier int `json:"frontier"`
}

// Stats is the work block of /stats.
type Stats struct {
	Enabled           bool      `json:"enabled"`
	ExtractVersion    int       `json:"extract_version"`
	Pool              PoolStats `json:"pool"`
	Leased            int       `json:"leased"`
	Awaiting          int       `json:"awaiting"`
	Verified          int       `json:"verified"`
	Mismatch          int       `json:"mismatch"`
	Rejected          int       `json:"rejected"`
	Expired           int       `json:"expired"`
	Deferred          int       `json:"deferred"`
	Parked            int       `json:"parked"`
	ModerationsToday  int       `json:"moderations_today"`
	ModerationCeiling bool      `json:"moderation_ceiling"`
	SettledSeq        uint64    `json:"settled_seq"`
}

// Stats counts the pool, the leases and the records from memory and one bounded scan of the records.
func (s *Service) Stats() Stats {
	now := s.now()
	out := Stats{Enabled: true, ExtractVersion: extractVersion}
	s.modMu.Lock()
	if c, err := s.today(now); err == nil {
		out.ModerationsToday, out.ModerationCeiling = c.N, c.N >= s.cfg.WorkModerationsPerDay
	}
	s.modMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.pool {
		switch e.class {
		case ClassPushed:
			out.Pool.Pushed++
		case ClassErrored:
			out.Pool.Errored++
		case ClassFrontier:
			out.Pool.Frontier++
		}
	}
	out.Pool.Total = len(s.pool)
	out.Leased, out.SettledSeq = len(s.leases), s.lastSeq
	for _, until := range s.parked {
		if now.Before(until) {
			out.Parked++
		}
	}
	ttl := s.exclusionTTL()
	_ = s.forEachRecord(func(_ string, r *Record) bool {
		if now.Sub(r.FirstAt) >= ttl {
			return true
		}
		switch r.State {
		case StateAwaiting:
			out.Awaiting++
		case StateDone:
			out.Verified++
		case StateMismatch:
			out.Mismatch++
		case StateRejected:
			out.Rejected++
		case StateExpired:
			out.Expired++
		case StateDeferred:
			out.Deferred++
		}
		return true
	})
	return out
}

// WriteMetrics writes the work series in Prometheus text format; every label value is from a fixed set.
func (s *Service) WriteMetrics(w io.Writer) {
	series := func(name, help, label string, values []string, snap map[string]int64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
		for _, v := range values {
			fmt.Fprintf(w, "%s{%s=%q} %d\n", name, label, v, snap[v])
		}
	}
	series("cosift_work_leases_total", "Leases granted by source.", "source", []string{"pushed", "errored", "frontier"}, s.metrics.leases.snapshot())
	series("cosift_work_submissions_total", "Submissions by verdict.", "verdict", []string{"awaiting_quorum", "verified", "mismatch", "deferred", "rejected", "released"}, s.metrics.submissions.snapshot())
	series("cosift_work_settled_total", "Settled events by verdict.", "verdict", []string{"verified", "mismatch", "rejected", "expired"}, s.metrics.settled.snapshot())
	series("cosift_work_moderations_total", "Moderation runs by result.", "result", []string{"allow", "reject", "error", "ceiling", "index_error"}, s.metrics.moderations.snapshot())
	series("cosift_work_refetch_total", "Server re-fetches by result.", "result", []string{"match", "mismatch", "fail", "busy"}, s.metrics.refetch.snapshot())
	st := s.Stats()
	classes := map[string]int{"pushed": st.Pool.Pushed, "errored": st.Pool.Errored, "frontier": st.Pool.Frontier}
	names := make([]string, 0, len(classes))
	for c := range classes {
		names = append(names, c)
	}
	sort.Strings(names)
	fmt.Fprint(w, "# HELP cosift_work_pool Pool entries by class.\n# TYPE cosift_work_pool gauge\n")
	for _, c := range names {
		fmt.Fprintf(w, "cosift_work_pool{class=%q} %d\n", c, classes[c])
	}
}
