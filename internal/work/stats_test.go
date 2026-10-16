package work

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestStatsCountPoolLeasesAndRecords(t *testing.T) {
	h := newHarness(t)
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	h.add(ClassPushed, "https://p.example.org/1")
	h.add(ClassErrored, pageURL, "https://e.example.org/2")
	h.add(ClassFrontier, "https://f.example.org/3")
	for _, text := range []string{a, b} {
		w, n := h.nextPair()
		if _, err := h.s.Submit(request(h.grant(pageURL, w, n), w, n, pageURL, text)); err != nil {
			t.Fatal(err)
		}
	}
	h.moderate()
	st := h.s.Stats()
	if !st.Enabled || st.ExtractVersion != 1 || st.Pool.Total != 3 || st.Pool.Pushed != 1 || st.Pool.Errored != 1 || st.Pool.Frontier != 1 {
		t.Fatalf("%+v", st)
	}
	if st.Verified != 1 || st.Awaiting != 0 || st.Leased != 0 || st.SettledSeq != 1 || st.ModerationsToday != 1 || st.ModerationCeiling {
		t.Fatalf("%+v", st)
	}
	w, n := h.nextPair()
	h.claimFor(w, n)
	if got := h.s.Stats().Leased; got != 1 {
		t.Fatalf("leased %d", got)
	}
	raw, _ := json.Marshal(map[string]any{"work": st})
	for _, key := range []string{`"enabled"`, `"extract_version"`, `"pool"`, `"leased"`, `"awaiting"`, `"verified"`, `"mismatch"`, `"rejected"`, `"expired"`, `"deferred"`, `"parked"`, `"moderations_today"`, `"moderation_ceiling"`, `"settled_seq"`} {
		if !strings.Contains(string(raw), key) {
			t.Errorf("no %s in %s", key, raw)
		}
	}
}

func TestStatsCountRecordsByStateWithinTheTTL(t *testing.T) {
	h := newHarness(t)
	h.add(ClassPushed, pageURL)
	a, _, _ := fixture(t, "rotated-ad-and-timestamp")
	h.attest(a)
	h.failFetch(pageURL)
	if st := h.s.Stats(); st.Awaiting != 1 {
		t.Fatalf("%+v", st)
	}
	h.advance(169 * time.Hour)
	h.sweep()
	if st := h.s.Stats(); st.Awaiting != 0 || st.Expired != 0 {
		t.Fatalf("a record past the TTL is not counted: %+v", st)
	}
	if _, err := h.s.Purge(context.Background(), pageURL, true); err != nil {
		t.Fatal(err)
	}
	if st := h.s.Stats(); st.Parked != 1 {
		t.Fatalf("%+v", st)
	}
}

func TestMetricsUseOnlyBoundedLabels(t *testing.T) {
	h := newHarness(t)
	h.add(ClassErrored, pageURL)
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	h.quorum(a, b)
	h.moderate()
	var buf bytes.Buffer
	h.s.WriteMetrics(&buf)
	out := buf.String()
	for _, want := range []string{
		`cosift_work_leases_total{source="errored"} 2`, `cosift_work_submissions_total{verdict="verified"} 1`,
		`cosift_work_submissions_total{verdict="awaiting_quorum"} 1`, `cosift_work_settled_total{verdict="verified"} 1`,
		`cosift_work_moderations_total{result="allow"} 1`, `cosift_work_refetch_total{result="fail"} 0`, `cosift_work_pool{class="errored"} 0`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "example.org") || strings.Contains(line, hexID(1)) {
			t.Errorf("a series carries a value: %s", line)
		}
	}
}
