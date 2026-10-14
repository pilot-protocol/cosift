package work

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/crawler"
)

func TestAFailedServerFetchBacksOffAnHourDoublingToADay(t *testing.T) {
	h := newHarness(t)
	h.add(ClassPushed, pageURL)
	a, _, _ := fixture(t, "rotated-ad-and-timestamp")
	h.attest(a)
	h.advance(31 * time.Minute)
	h.failFetch(pageURL)
	for i, want := range []time.Duration{time.Hour, 2 * time.Hour, 4 * time.Hour, 8 * time.Hour, 16 * time.Hour, 24 * time.Hour, 24 * time.Hour} {
		h.sweep()
		rec := h.record(pageURL)
		if rec.State != StateAwaiting || rec.RefetchFailures != i+1 || !rec.RefetchAt.Equal(h.now().Add(want)) {
			t.Fatalf("failure %d: %+v", i+1, rec)
		}
		h.advance(want - time.Second)
		h.sweep()
		if h.f.fetches[pageURL] != i+1 {
			t.Fatalf("failure %d: fetched again inside the back-off", i+1)
		}
		h.advance(time.Second)
	}
}

func TestACopyThatIsNotARealPageIsAFailedFetch(t *testing.T) {
	a, _, _ := fixture(t, "rotated-ad-and-timestamp")
	for name, page := range map[string]Page{
		"bot check":       {FinalURL: pageURL, Title: "Just a moment...", Text: a},
		"short":           {FinalURL: pageURL, Title: "T", Text: "too short to be a page"},
		"no token":        {FinalURL: pageURL, Title: "T", Text: strings.Repeat("-- ", 40)},
		"off policy":      {FinalURL: "https://blocked.example.org/x", Title: "T", Text: a},
		"unusable target": {FinalURL: "ftp://example.org/x", Title: "T", Text: a},
	} {
		h := newHarness(t)
		h.f.blockedFor["blocked.example.org"] = true
		h.add(ClassPushed, pageURL)
		h.attest(a)
		h.advance(31 * time.Minute)
		h.f.pages[pageURL] = page
		h.sweep()
		if rec := h.record(pageURL); rec.State != StateAwaiting || rec.RefetchFailures != 1 {
			t.Errorf("%s: %+v", name, rec)
		}
	}
}

func TestTheServersCopyIsIndexedAndTheMatchingGroupPaid(t *testing.T) {
	h := newHarness(t)
	h.add(ClassPushed, pageURL)
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	w1, w2 := h.quorum(a, b)
	server := serverCopy(t, a)
	h.f.pages[pageURL] = Page{FinalURL: "https://EXAMPLE.org/blocked-page#top", Title: "Tidal", Text: server, Lang: "de"}
	h.sweep()
	if len(h.f.indexed) != 1 {
		t.Fatalf("indexed %d", len(h.f.indexed))
	}
	d := h.f.indexed[0]
	if d.Text != server || d.VerifiedBy != "server" || d.URL != "https://example.org/blocked-page" || d.LeaseURL != pageURL || d.Title != "Tidal" || d.Lang != "de" {
		t.Fatalf("%+v", d)
	}
	evs := h.events("verified")
	if len(evs) != 1 || fmt.Sprint(evs[0].Workers) != fmt.Sprint([]string{w1, w2}) {
		t.Fatalf("%+v", evs)
	}
	if rec := h.record(pageURL); rec.VerifiedBy != "server" || rec.ContentHash != sha(server) {
		t.Fatalf("%+v", rec)
	}
}

func TestACollidingPairWithTextThatDiffersFromTheServersIsAMismatch(t *testing.T) {
	h := newHarness(t)
	h.add(ClassPushed, pageURL)
	honest, forged, dist := fixture(t, "injected-paragraph-short-page")
	if dist <= h.cfg.SimhashMax {
		t.Fatal("fixture")
	}
	w1, w2 := h.quorum(forged, forged)
	h.f.pages[pageURL] = Page{FinalURL: pageURL, Title: "A page", Text: honest, Lang: "en"}
	h.sweep()
	rec := h.record(pageURL)
	if rec.State != StateMismatch || rec.Copy != CopyDone || rec.VerifiedBy != "" || h.textCount() != 0 {
		t.Fatalf("%+v", rec)
	}
	if len(h.f.indexed) != 1 || h.f.indexed[0].Text != honest || h.f.indexed[0].VerifiedBy != "server" {
		t.Fatalf("the server's copy is what is indexed: %+v", h.f.indexed)
	}
	if evs := h.events("mismatch"); len(evs) != 1 || fmt.Sprint(evs[0].Workers) != fmt.Sprint([]string{w1, w2}) {
		t.Fatalf("%+v", evs)
	}
	if len(h.events("verified")) != 0 || len(h.events("rejected")) != 0 {
		t.Fatal("a forged quorum was credited")
	}
}

func TestTamperedTextNeverBecomesTheVerifiedGroup(t *testing.T) {
	h := newHarness(t)
	h.add(ClassPushed, pageURL)
	honest, tampered, _ := fixture(t, "entity-swap-short-page")
	_, wHonest, _, _ := h.attest(honest)
	_, wForger, _, _ := h.attest(tampered)
	h.advance(31 * time.Minute)
	h.f.pages[pageURL] = Page{FinalURL: pageURL, Title: "A page", Text: honest}
	h.sweep()
	rec := h.record(pageURL)
	if rec.State != StateDone || rec.VerifiedBy != "server" {
		t.Fatalf("%+v", rec)
	}
	if g := rec.Groups[rec.VerifiedGroup]; len(g.Members) != 1 || g.Members[0].Worker != wHonest {
		t.Fatalf("%+v", g)
	}
	if evs := h.events("mismatch"); len(evs) != 1 || fmt.Sprint(evs[0].Workers) != fmt.Sprint([]string{wForger}) {
		t.Fatalf("%+v", evs)
	}
	if h.f.indexed[0].Text != honest {
		t.Fatal("tampered text was indexed")
	}
}

func TestTheServersCopyPicksTheNearestGroup(t *testing.T) {
	h := newHarness(t)
	h.add(ClassPushed, pageURL)
	h.s.cfg.SimhashMax = 14
	base, _, _ := fixture(t, "rotated-ad-and-timestamp")
	sa, _ := crawler.Simhash(base)
	near := sa ^ 0b1
	far := sa ^ 0b1111111100
	rec := &Record{Groups: []Group{{Anchor: crawler.SimhashHex(far), Members: []Attestation{{Worker: "w1"}, {Worker: "w2"}, {Worker: "w3"}}}, {Anchor: crawler.SimhashHex(near)}}}
	if gi := h.s.closestGroup(rec, sa); gi != 1 {
		t.Fatalf("group %d, want the nearer one", gi)
	}
	if gi := h.s.closestGroup(rec, sa^(1<<63|1<<62|1<<61|1<<60|1<<59|1<<58|1<<57|1<<56|1<<55|1<<54|1<<53|1<<52|1<<51|1<<50|1<<49)); gi != -1 {
		t.Fatalf("group %d, want none beyond the bound", gi)
	}
	tie := &Record{Groups: []Group{{Anchor: crawler.SimhashHex(sa ^ 1), Members: []Attestation{{}}}, {Anchor: crawler.SimhashHex(sa ^ 2), Members: []Attestation{{}, {}}}}}
	if gi := h.s.closestGroup(tie, sa); gi != 1 {
		t.Fatalf("a tie goes to the larger group: %d", gi)
	}
}

func TestAnAwaitingRecordExpiresAtTheTTLAndIsDeletedAfter(t *testing.T) {
	h := newHarness(t)
	h.add(ClassPushed, pageURL)
	a, _, _ := fixture(t, "rotated-ad-and-timestamp")
	_, w, _, _ := h.attest(a)
	h.failFetch(pageURL)
	h.advance(168*time.Hour - time.Minute)
	h.sweep()
	if h.record(pageURL).State == StateExpired {
		t.Fatal("expired early")
	}
	h.advance(time.Minute)
	h.sweep()
	rec := h.record(pageURL)
	if rec.State != StateExpired {
		t.Fatalf("%+v", rec)
	}
	evs := h.events("expired")
	if len(evs) != 1 || fmt.Sprint(evs[0].Workers) != fmt.Sprint([]string{w}) || evs[0].ContentNovel {
		t.Fatalf("%+v", evs)
	}
	h.sweep()
	if h.record(pageURL) != nil {
		t.Fatal("the expired record stays past its sweep")
	}
}

func TestModeratingAndDeferredRecordsExpireAndLoseTheirText(t *testing.T) {
	for _, state := range []string{"moderating", "deferred"} {
		h := newHarness(t)
		h.f.modErr = errNoModel
		h.add(ClassErrored, pageURL)
		a, b, _ := fixture(t, "rotated-ad-and-timestamp")
		h.quorum(a, b)
		if state == "deferred" {
			h.s.cfg.WorkModerationsPerDay = 0
			rec := h.record(pageURL)
			rec.State = StateDeferred
			tx := h.s.begin()
			tx.putRecord(pageURL, rec)
			tx.commit()
		}
		if h.textCount() != 1 {
			t.Fatal("no queued text")
		}
		h.advance(169 * time.Hour)
		h.sweep()
		if rec := h.record(pageURL); rec.State != StateExpired || h.textCount() != 0 || len(h.s.texts) != 0 {
			t.Fatalf("%s: %+v", state, rec)
		}
		if len(h.events("expired")) != 1 {
			t.Fatalf("%s: no expired event", state)
		}
	}
}

func TestAMismatchWhoseCopyIsStillPendingIsDeletedWithItsTextAtTheTTL(t *testing.T) {
	h := newHarness(t)
	h.f.modErr = errNoModel
	h.add(ClassPushed, pageURL)
	honest, forged, _ := fixture(t, "injected-paragraph-short-page")
	h.quorum(forged, forged)
	h.f.pages[pageURL] = Page{FinalURL: pageURL, Title: "A page", Text: honest}
	h.sweep()
	rec := h.record(pageURL)
	if rec.State != StateMismatch || rec.Copy != CopyModerating || h.textCount() != 1 {
		t.Fatalf("%+v", rec)
	}
	issued := h.s.lastSeq
	h.advance(169 * time.Hour)
	h.sweep()
	if h.record(pageURL) != nil || h.textCount() != 0 {
		t.Fatal("the pending copy outlived the TTL")
	}
	if h.s.lastSeq != issued || len(h.events("expired")) != 0 {
		t.Fatal("the mismatch event was issued already; nothing more is settled")
	}
}

func TestTheCopyOfAMismatchIsModeratedUnderCopyWithoutCredit(t *testing.T) {
	for _, v := range []Verdict{{"allow", "safe"}, {"reject", "spam"}} {
		h := newHarness(t)
		h.f.verdict = v
		h.add(ClassPushed, pageURL)
		honest, forged, _ := fixture(t, "injected-paragraph-short-page")
		h.quorum(forged, forged)
		h.f.pages[pageURL] = Page{FinalURL: pageURL, Title: "A page", Text: honest}
		h.sweep()
		rec := h.record(pageURL)
		want := map[string]CopyState{"allow": CopyDone, "reject": CopyRejected}[v.Decision]
		if rec.State != StateMismatch || rec.Copy != want || h.textCount() != 0 {
			t.Fatalf("%s: %+v", v.Decision, rec)
		}
		if len(h.settled()) != 1 || h.settled()[0].Verdict != "mismatch" {
			t.Fatalf("%s: %+v", v.Decision, h.settled())
		}
	}
}

func TestRecordsAndEventsSurviveARestartButLeasesAndThePoolDoNot(t *testing.T) {
	h := newHarness(t)
	h.add(ClassErrored, pageURL)
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	h.quorum(a, b)
	h.moderate()
	last := h.settled()[0].Seq
	h.add(ClassPushed, "https://other.example.org/p")
	req, _ := leased(t, h)
	h.db.Close()
	h.open()
	if h.record(pageURL).State != StateDone {
		t.Fatal("the record was lost")
	}
	if evs := h.settled(); len(evs) != 1 || evs[0].Seq != last {
		t.Fatalf("%+v", evs)
	}
	if len(h.s.pool) != 0 || len(h.s.leases) != 0 {
		t.Fatal("the pool and the leases are memory only")
	}
	_, err := h.s.Submit(req)
	wantError(t, err, 409, "lease_expired")
	h.add(ClassErrored, "https://another.example.org/p")
	w, n := h.nextPair()
	l, _ := h.claimFor(w, n)
	h.f.indexed = nil
	h.s.Submit(request(l, w, n, l.URL, a))
	w, n = h.nextPair()
	l, _ = h.claimFor(w, n)
	h.s.Submit(request(l, w, n, l.URL, b))
	h.moderate()
	if evs := h.settled(); len(evs) != 2 || evs[1].Seq != last+1 {
		t.Fatalf("a sequence number was reused: %+v", evs)
	}
}

func TestSettledAnswersAnEmptyListWhenTheCursorIsAheadOfTheEngine(t *testing.T) {
	h := newHarness(t)
	h.add(ClassErrored, pageURL)
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	h.quorum(a, b)
	h.moderate()
	res, err := h.s.Settled(0, 100)
	if err != nil || len(res.Events) != 1 || res.Next != res.Events[0].Seq || res.LastSeq != res.Next {
		t.Fatalf("%+v %v", res, err)
	}
	res, err = h.s.Settled(res.LastSeq, 100)
	if err != nil || len(res.Events) != 0 || res.Next != res.LastSeq {
		t.Fatalf("%+v %v", res, err)
	}
	res, err = h.s.Settled(res.LastSeq+50, 100)
	if err != nil || len(res.Events) != 0 || res.Next != res.LastSeq+50 || res.Events == nil {
		t.Fatalf("a restore to an older snapshot: %+v %v", res, err)
	}
}

func TestEventsOlderThanTheTTLAreSwept(t *testing.T) {
	h := newHarness(t)
	h.add(ClassErrored, pageURL)
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	h.quorum(a, b)
	h.moderate()
	h.advance(167 * time.Hour)
	h.sweep()
	if len(h.settled()) != 1 {
		t.Fatal("swept early")
	}
	h.advance(2 * time.Hour)
	h.sweep()
	if h.record(pageURL) != nil && len(h.settled()) != 0 {
		h.sweep()
	}
	if len(h.settled()) != 0 {
		t.Fatalf("%+v", h.settled())
	}
}

func TestTheModerationQueueIsBounded(t *testing.T) {
	h := newHarness(t, func(c *config.WorkSettings) { c.ModerationQueueMax = 8 })
	h.f.modErr = errNoModel
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	verified := 0
	for i := 0; i < 20; i++ {
		u := fmt.Sprintf("https://h%d.example.org/p", i)
		h.add(ClassErrored, u)
		for _, text := range []string{a, b} {
			w, n := h.nextPair()
			l := h.grant(u, w, n)
			res, err := h.s.Submit(request(l, w, n, u, text))
			if err != nil {
				t.Fatal(err)
			}
			if res.Verdict == "verified" {
				verified++
			}
		}
		if h.textCount() > 8 || len(h.s.texts) > 8 {
			t.Fatalf("after %d pages the queue holds %d texts", i+1, h.textCount())
		}
	}
	if verified != 8 {
		t.Fatalf("verified %d, want 8", verified)
	}
}

// grant leases the URL to a pair directly, past the pool order.
func (h *harness) grant(u, w, n string) LeaseGrant {
	h.t.Helper()
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	e := h.s.pool[u]
	l := &lease{id: randomID(), url: u, worker: w, net: n, class: e.class, granted: h.now(), expiry: h.now().Add(10 * time.Minute)}
	h.s.leases[l.id] = l
	e.leases[l.id] = l
	h.s.byWorker[w]++
	return LeaseGrant{ID: l.id, URL: u, Source: e.class}
}
