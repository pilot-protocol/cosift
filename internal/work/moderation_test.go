package work

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pilot-protocol/cosift/internal/config"
)

// quorum puts two agreeing attestations on the entry at pageURL and returns the two workers.
func (h *harness) quorum(a, b string) (string, string) {
	h.t.Helper()
	_, w1, _, err := h.attest(a)
	if err != nil {
		h.t.Fatal(err)
	}
	_, w2, _, err := h.attest(b)
	if err != nil {
		h.t.Fatal(err)
	}
	return w1, w2
}

var errNoModel = errors.New("model down")

func (h *harness) moderate() { h.s.runModerations(context.Background()) }

func (h *harness) events(verdict string) []Event {
	var out []Event
	for _, ev := range h.settled() {
		if ev.Verdict == verdict {
			out = append(out, ev)
		}
	}
	return out
}

func (h *harness) textCount() int {
	n := 0
	it, _ := h.db.NewIter(&pebble.IterOptions{LowerBound: prefixText, UpperBound: upperBound(prefixText)})
	defer it.Close()
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	return n
}

func TestAllowedPageIsIndexedAndTheGroupIsSettledVerified(t *testing.T) {
	h := newHarness(t)
	h.add(ClassErrored, pageURL)
	h.f.rows[pageURL] = "errored"
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	w1, w2 := h.quorum(a, b)
	h.moderate()
	if len(h.f.indexed) != 1 {
		t.Fatalf("indexed %d", len(h.f.indexed))
	}
	d := h.f.indexed[0]
	if d.Text != b || d.URL != pageURL || d.LeaseURL != pageURL || d.VerifiedBy != "quorum" || d.Title != "A page" || d.Lang != "en" {
		t.Fatalf("%+v", d)
	}
	rec := h.record(pageURL)
	if rec.State != StateDone || !rec.ContentNovel || h.textCount() != 0 || len(h.s.texts) != 0 || h.f.rows[pageURL] != "done" {
		t.Fatalf("%+v texts %d row %s", rec, h.textCount(), h.f.rows[pageURL])
	}
	evs := h.events("verified")
	if len(evs) != 1 || fmt.Sprint(evs[0].Workers) != fmt.Sprint([]string{w1, w2}) || !evs[0].ContentNovel || evs[0].URLSha != urlSHA(pageURL) {
		t.Fatalf("%+v", evs)
	}
	h.moderate()
	if len(h.f.indexed) != 1 || len(h.events("verified")) != 1 {
		t.Fatal("a finished record was moderated again")
	}
}

func TestContentThatIsNotNewPaysNothing(t *testing.T) {
	h := newHarness(t)
	h.f.novel = false
	h.add(ClassErrored, pageURL)
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	h.quorum(a, b)
	h.moderate()
	evs := h.events("verified")
	if len(evs) != 1 || evs[0].ContentNovel {
		t.Fatalf("%+v", evs)
	}
}

func TestARejectedPageSettlesEveryAttestingWorkerRejected(t *testing.T) {
	for _, v := range []Verdict{{"reject", "spam"}, {"uncertain", "unverified"}} {
		h := newHarness(t)
		h.f.verdict = v
		h.add(ClassErrored, pageURL)
		a, forged, _ := fixture(t, "injected-paragraph-short-page")
		h.attest(forged)
		h.quorum(a, a)
		h.moderate()
		rec := h.record(pageURL)
		if rec.State != StateRejected || rec.RejectReason != v.Category || h.textCount() != 0 || len(h.f.indexed) != 0 {
			t.Fatalf("%s: %+v", v.Decision, rec)
		}
		evs := h.events("rejected")
		if len(evs) != 1 || len(evs[0].Workers) != 3 || evs[0].ContentNovel {
			t.Fatalf("%+v", evs)
		}
		if len(h.events("verified")) != 0 {
			t.Fatal("a rejected page paid")
		}
	}
}

func TestAModelFailureBacksOffAndTheFifthRejects(t *testing.T) {
	h := newHarness(t)
	h.f.modErr = errors.New("model down")
	h.add(ClassErrored, pageURL)
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	h.quorum(a, b)
	for i, wait := range []time.Duration{10 * time.Minute, 20 * time.Minute, 40 * time.Minute, 80 * time.Minute} {
		h.moderate()
		rec := h.record(pageURL)
		if rec.State != StateModerating || rec.ModerationFailures != i+1 || !rec.RetryAt.Equal(h.now().Add(wait)) {
			t.Fatalf("failure %d: %+v", i+1, rec)
		}
		h.advance(wait - time.Second)
		h.moderate()
		if h.record(pageURL).ModerationFailures != i+1 {
			t.Fatalf("failure %d: retried before its back-off", i+1)
		}
		h.advance(time.Second)
	}
	h.moderate()
	rec := h.record(pageURL)
	if rec.State != StateRejected || rec.RejectReason != "moderation_failed" || h.textCount() != 0 {
		t.Fatalf("%+v", rec)
	}
	if evs := h.events("rejected"); len(evs) != 1 || len(evs[0].Workers) != 2 || evs[0].ContentNovel {
		t.Fatalf("%+v", evs)
	}
	if h.f.modelCalls != 5 {
		t.Fatalf("each try spends a unit of the ceiling: %d model calls", h.f.modelCalls)
	}
	if c, _ := h.s.today(h.now()); c.N != 5 {
		t.Fatalf("%+v", c)
	}
}

func TestTheCeilingDefersModerationAndStopsClaims(t *testing.T) {
	h := newHarness(t, func(c *config.WorkSettings) { c.WorkModerationsPerDay = 1 })
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	urls := []string{"https://one.example.org/p", "https://two.example.org/p"}
	h.add(ClassErrored, urls...)
	for _, u := range urls {
		for i, text := range []string{a, b} {
			w, n := h.nextPair()
			res, err := h.s.Claim(w, n, 2)
			if err != nil || len(res.Leases) == 0 {
				t.Fatalf("claim: %+v %v", res, err)
			}
			var l LeaseGrant
			for _, g := range res.Leases {
				if g.URL == u {
					l = g
				}
			}
			if l.ID == "" {
				t.Fatalf("no lease on %s (attestation %d)", u, i)
			}
			if _, err := h.s.Submit(request(l, w, n, u, text)); err != nil {
				t.Fatal(err)
			}
		}
	}
	h.moderate()
	states := map[State]int{}
	for _, u := range urls {
		states[h.record(u).State]++
	}
	if states[StateDone] != 1 || states[StateDeferred] != 1 || len(h.f.indexed) != 1 {
		t.Fatalf("%v indexed %d", states, len(h.f.indexed))
	}
	res := claimN(t, h, hexID(800), hexID(801), 1)
	if len(res.Leases) != 0 || !res.ModerationCeiling {
		t.Fatalf("%+v", res)
	}
	if settled, _ := h.s.Settled(0, 10); !settled.ModerationCeiling {
		t.Fatal("settled does not report the ceiling")
	}
	h.advance(time.Hour)
	h.moderate()
	if len(h.f.indexed) != 1 {
		t.Fatal("a deferred page was retried before 00:00 UTC")
	}
	h.advance(nextMidnight(h.now()).Sub(h.now()))
	h.moderate()
	if len(h.f.indexed) != 2 {
		t.Fatalf("the deferred page was not retried after midnight: %d", len(h.f.indexed))
	}
	if len(h.events("verified")) != 2 {
		t.Fatal("both pages settle verified")
	}
}

func TestAnIndexFailureKeepsThePageForARetry(t *testing.T) {
	h := newHarness(t)
	h.f.indexErr = errors.New("disk")
	h.add(ClassErrored, pageURL)
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	h.quorum(a, b)
	h.moderate()
	rec := h.record(pageURL)
	if rec.State != StateModerating || rec.ModerationFailures != 0 || !rec.RetryAt.Equal(h.now().Add(time.Minute)) || h.textCount() != 1 {
		t.Fatalf("%+v", rec)
	}
	h.f.indexErr = nil
	h.advance(time.Minute)
	h.moderate()
	if h.record(pageURL).State != StateDone || len(h.events("verified")) != 1 {
		t.Fatal("not indexed on retry")
	}
}

func TestALateAgreeingWorkerIsPaidWithTheOriginalNovelty(t *testing.T) {
	for _, novel := range []bool{true, false} {
		h := newHarness(t)
		h.f.novel = novel
		h.add(ClassErrored, pageURL)
		a, b, _ := fixture(t, "rotated-ad-and-timestamp")
		w1, n1 := h.nextPair()
		w2, n2 := h.nextPair()
		w3, n3 := h.nextPair()
		l1, _ := h.claimFor(w1, n1)
		l2, _ := h.claimFor(w2, n2)
		l3, _ := h.claimFor(w3, n3)
		h.s.Submit(request(l1, w1, n1, pageURL, a))
		h.s.Submit(request(l2, w2, n2, pageURL, b))
		h.moderate()
		res, err := h.s.Submit(request(l3, w3, n3, pageURL, a))
		if err != nil || res.Verdict != "verified" || !res.Indexed || len(res.Agreeing) != 3 {
			t.Fatalf("%+v %v", res, err)
		}
		evs := h.events("verified")
		if len(evs) != 2 || fmt.Sprint(evs[1].Workers) != fmt.Sprint([]string{w3}) || evs[1].ContentNovel != novel {
			t.Fatalf("novel=%v: %+v", novel, evs)
		}
	}
}

func TestALateDissenterAfterIndexingIsAMismatch(t *testing.T) {
	h := newHarness(t)
	h.add(ClassErrored, pageURL)
	a, forged, _ := fixture(t, "injected-paragraph-short-page")
	w1, n1 := h.nextPair()
	w2, n2 := h.nextPair()
	w3, n3 := h.nextPair()
	l1, _ := h.claimFor(w1, n1)
	l2, _ := h.claimFor(w2, n2)
	l3, _ := h.claimFor(w3, n3)
	h.s.Submit(request(l1, w1, n1, pageURL, a))
	h.s.Submit(request(l2, w2, n2, pageURL, a))
	h.moderate()
	res, err := h.s.Submit(request(l3, w3, n3, pageURL, forged))
	if err != nil || res.Verdict != "mismatch" {
		t.Fatalf("%+v %v", res, err)
	}
	if evs := h.events("mismatch"); len(evs) != 1 || fmt.Sprint(evs[0].Workers) != fmt.Sprint([]string{w3}) {
		t.Fatalf("%+v", evs)
	}
	if len(h.f.indexed) != 1 {
		t.Fatal("a dissenter changed what was indexed")
	}
}

func TestAfterARejectionALateWorkerIsToldRejected(t *testing.T) {
	h := newHarness(t)
	h.f.verdict = Verdict{"reject", "malware"}
	h.add(ClassErrored, pageURL)
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	w1, n1 := h.nextPair()
	w2, n2 := h.nextPair()
	w3, n3 := h.nextPair()
	l1, _ := h.claimFor(w1, n1)
	l2, _ := h.claimFor(w2, n2)
	l3, _ := h.claimFor(w3, n3)
	h.s.Submit(request(l1, w1, n1, pageURL, a))
	h.s.Submit(request(l2, w2, n2, pageURL, b))
	h.moderate()
	res, err := h.s.Submit(request(l3, w3, n3, pageURL, a))
	if err != nil || res.Verdict != "rejected" || res.Reason != "malware" {
		t.Fatalf("%+v %v", res, err)
	}
}
