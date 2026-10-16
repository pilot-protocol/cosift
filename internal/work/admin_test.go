package work

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestPurgeWithoutApplyChangesNothing(t *testing.T) {
	h := newHarness(t)
	h.add(ClassErrored, pageURL)
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	h.quorum(a, b)
	h.f.attested[pageURL] = true
	before := h.record(pageURL)
	res, err := h.s.Purge(context.Background(), pageURL, false)
	if err != nil || res.DocumentsDeleted != 1 || res.RecordsDeleted != 1 || res.TextsDeleted != 1 || res.WorkersSettled != 2 || !res.ParkedUntil.Equal(h.now().Add(168*time.Hour)) {
		t.Fatalf("%+v %v", res, err)
	}
	after := h.record(pageURL)
	if after == nil || after.State != before.State || len(h.f.purged) != 0 || h.textCount() != 1 || len(h.settled()) != 0 || len(h.s.texts) != 1 {
		t.Fatal("a dry run changed state")
	}
	if h.s.isParked(urlSHA(pageURL)) {
		t.Fatal("a dry run parked the URL")
	}
}

func TestPurgeRemovesTheForgeryParksItAndSettlesTheAttesters(t *testing.T) {
	h := newHarness(t)
	h.add(ClassPushed, pageURL)
	honest, forged, _ := fixture(t, "injected-paragraph-short-page")
	var ws, ns [4]string
	var ls [4]LeaseGrant
	for i := range ls {
		ws[i], ns[i] = h.nextPair()
		ls[i] = h.grant(pageURL, ws[i], ns[i])
	}
	for i, text := range []string{forged, forged, honest} {
		if _, err := h.s.Submit(request(ls[i], ws[i], ns[i], pageURL, text)); err != nil {
			t.Fatal(err)
		}
	}
	w1, w2, w3 := ws[0], ws[1], ws[2]
	h.f.pages[pageURL] = Page{FinalURL: "https://example.org/moved", Title: "A page", Text: forged}
	h.sweep()
	rec := h.record(pageURL)
	if rec.State != StateDone || rec.FinalURL != "https://example.org/moved" {
		t.Fatalf("setup: %+v", rec)
	}
	h.f.attested["https://example.org/moved"] = true
	h.f.crawled[pageURL] = true
	req := request(ls[3], ws[3], ns[3], pageURL, honest)
	res, err := h.s.Purge(context.Background(), pageURL, true)
	if err != nil {
		t.Fatal(err)
	}
	if res.DocumentsDeleted != 1 || res.DocumentsKept != 1 || res.RecordsDeleted != 1 || res.TextsDeleted != 0 || res.WorkersSettled != 3 {
		t.Fatalf("%+v", res)
	}
	if !h.f.purged["https://example.org/moved"] || h.record(pageURL) != nil || h.textCount() != 0 {
		t.Fatal("the document, the record or the text remains")
	}
	evs := h.events("rejected")
	if len(evs) != 1 || fmt.Sprint(evs[0].Workers) != fmt.Sprint([]string{w1, w2, w3}) || evs[0].ContentNovel || evs[0].URLSha != urlSHA(pageURL) {
		t.Fatalf("%+v", evs)
	}
	if !h.s.isParked(urlSHA(pageURL)) {
		t.Fatal("the park is not stored")
	}
	if res := h.s.consider(context.Background(), pageURL, ClassPushed); res != fedDuplicate {
		t.Fatalf("a purged URL was fed again: %v", res)
	}
	if _, err := h.s.Submit(req); err == nil {
		t.Fatal("a lease on a purged URL still submits")
	} else {
		wantError(t, err, 409, "lease_expired")
	}
	if _, ok := h.claimFor(hexID(700), hexID(701)); ok {
		t.Fatal("a purged URL was leased")
	}
	h.db.Close()
	h.open()
	if !h.s.isParked(urlSHA(pageURL)) {
		t.Fatal("the park does not survive a restart")
	}
	if res := h.s.consider(context.Background(), pageURL, ClassPushed); res != fedDuplicate {
		t.Fatalf("a parked URL was fed after a restart: %v", res)
	}
	again, err := h.s.Purge(context.Background(), pageURL, true)
	if err != nil || again.RecordsDeleted != 0 || again.WorkersSettled != 0 || again.TextsDeleted != 0 || len(h.events("rejected")) != 1 {
		t.Fatalf("%+v %v", again, err)
	}
}

func TestPurgeRefusesAnUnusableURL(t *testing.T) {
	h := newHarness(t)
	_, err := h.s.Purge(context.Background(), "http://127.0.0.1/x", true)
	wantError(t, err, 400, "invalid_url")
}

func TestPurgeEndsAtTheTTL(t *testing.T) {
	h := newHarness(t)
	h.add(ClassPushed, pageURL)
	if _, err := h.s.Purge(context.Background(), pageURL, true); err != nil {
		t.Fatal(err)
	}
	h.advance(168*time.Hour + time.Minute)
	h.sweep()
	if h.s.isParked(urlSHA(pageURL)) {
		t.Fatal("park kept")
	}
	if res := h.s.consider(context.Background(), pageURL, ClassPushed); res != fedAdded {
		t.Fatalf("after the park: %v", res)
	}
	if n := countPrefix(h, prefixPark); n != 0 {
		t.Fatalf("%d parks stored", n)
	}
}

func countPrefix(h *harness, prefix []byte) int {
	it, _ := h.db.NewIter(nil)
	defer it.Close()
	n := 0
	for ok := it.SeekGE(prefix); ok && len(it.Key()) >= len(prefix) && string(it.Key()[:len(prefix)]) == string(prefix); ok = it.Next() {
		n++
	}
	return n
}

func TestVoidDropsTheWorkersAttestationsAndRegroups(t *testing.T) {
	h := newHarness(t)
	h.add(ClassErrored, "https://a.example.org/p")
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	_, forged, _ := fixture(t, "injected-paragraph-short-page")
	// two attestations of one text and one forged: no quorum without the forgery
	_, bad, _, _ := h.attest(forged)
	_, w1, _, _ := h.attest(a)
	url := "https://a.example.org/p"
	rec := h.record(url)
	if len(rec.Groups) != 2 {
		t.Fatalf("%+v", rec.Groups)
	}
	res, err := h.s.Void(context.Background(), bad, false)
	if err != nil || res.RecordsChanged != 1 || res.AttestationsDropped != 1 || res.RecordsDeleted != 0 || res.URLsParked != 1 {
		t.Fatalf("%+v %v", res, err)
	}
	if len(h.record(url).Groups) != 2 {
		t.Fatal("a dry run changed the record")
	}
	res, err = h.s.Void(context.Background(), bad, true)
	if err != nil || res.RecordsChanged != 1 {
		t.Fatalf("%+v %v", res, err)
	}
	rec = h.record(url)
	if len(rec.Groups) != 1 || rec.Groups[0].Members[0].Worker != w1 {
		t.Fatalf("%+v", rec.Groups)
	}
	if !h.s.isParked(urlSHA(url)) {
		t.Fatal("a URL left with a lone attestation must be parked")
	}
	if _, ok := h.claimFor(hexID(800), hexID(801)); ok {
		t.Fatal("a parked URL was leased")
	}
	if len(h.settled()) != 0 {
		t.Fatal("void emits no event")
	}
	again, _ := h.s.Void(context.Background(), bad, true)
	if again != (VoidResult{}) {
		t.Fatalf("a second run changed something: %+v", again)
	}
	_ = b
}

func TestVoidDeletesARecordLeftEmpty(t *testing.T) {
	h := newHarness(t)
	h.add(ClassErrored, pageURL)
	a, _, _ := fixture(t, "rotated-ad-and-timestamp")
	_, w, _, _ := h.attest(a)
	res, err := h.s.Void(context.Background(), w, true)
	if err != nil || res.RecordsDeleted != 1 || res.URLsParked != 0 || h.record(pageURL) != nil {
		t.Fatalf("%+v %v", res, err)
	}
	if _, ok := h.claimFor(hexID(800), hexID(801)); !ok {
		t.Fatal("the URL should be as if nobody had attested it")
	}
	if _, ok := h.claimFor(w, hexID(5000)); !ok {
		t.Fatal("the voided worker is not barred from the pool by the engine")
	}
}

func TestVoidKeepsAGroupOfTwoAndLeavesOtherRecordsAlone(t *testing.T) {
	h := newHarness(t)
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	h.add(ClassPushed, pageURL, "https://other.example.org/p")
	var ws, ns [3]string
	var ls [3]LeaseGrant
	for i := range ls {
		ws[i], ns[i] = h.nextPair()
		ls[i] = h.grant(pageURL, ws[i], ns[i])
	}
	for i, text := range []string{a, b, a} {
		if _, err := h.s.Submit(request(ls[i], ws[i], ns[i], pageURL, text)); err != nil {
			t.Fatal(err)
		}
	}
	w, n := h.nextPair()
	other := h.grant("https://other.example.org/p", w, n)
	if _, err := h.s.Submit(request(other, w, n, other.URL, a)); err != nil {
		t.Fatal(err)
	}
	before := h.record("https://other.example.org/p")
	res, err := h.s.Void(context.Background(), ws[2], true)
	if err != nil || res != (VoidResult{RecordsChanged: 1, AttestationsDropped: 1}) {
		t.Fatalf("%+v %v", res, err)
	}
	rec := h.record(pageURL)
	if len(rec.Groups) != 1 || len(rec.Groups[0].Members) != 2 || rec.RefetchAt.After(h.now()) {
		t.Fatalf("the pair and its server fetch stay: %+v", rec)
	}
	if h.s.isParked(urlSHA(pageURL)) {
		t.Fatal("a URL that still has a group of two is not parked")
	}
	if after := h.record("https://other.example.org/p"); fmt.Sprint(after.Groups) != fmt.Sprint(before.Groups) {
		t.Fatal("another record changed")
	}
}

func TestVoidingABrokenQuorumOnAPushedURLNoLongerTriggersTheServerFetch(t *testing.T) {
	h := newHarness(t)
	h.add(ClassPushed, pageURL)
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	w1, _ := h.quorum(a, b)
	if rec := h.record(pageURL); rec.RefetchAt.After(h.now()) {
		t.Fatal("setup: the fetch should be due")
	}
	res, err := h.s.Void(context.Background(), w1, true)
	if err != nil || res.URLsParked != 1 || res.AttestationsDropped != 1 {
		t.Fatalf("%+v %v", res, err)
	}
	rec := h.record(pageURL)
	if !rec.RefetchAt.Equal(rec.WindowUntil) || rec.RefetchAt.Before(h.now()) {
		t.Fatalf("%+v", rec)
	}
	h.page(pageURL, a)
	h.sweep()
	if h.f.fetches[pageURL] != 0 {
		t.Fatal("the server fetched before the window ended")
	}
	h.advance(31 * time.Minute)
	h.sweep()
	if h.record(pageURL).State != StateDone {
		t.Fatalf("the server's own fetch must still verify what is left: %+v", h.record(pageURL))
	}
}

func TestVoidTouchesOnlyAwaitingRecords(t *testing.T) {
	h := newHarness(t)
	h.add(ClassErrored, pageURL)
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	w1, _ := h.quorum(a, b)
	h.moderate()
	events := h.s.lastSeq
	res, err := h.s.Void(context.Background(), w1, true)
	if err != nil || res != (VoidResult{}) || h.record(pageURL).State != StateDone || h.s.lastSeq != events {
		t.Fatalf("%+v %v", res, err)
	}
	_, err = h.s.Void(context.Background(), "nothex", true)
	wantInvalid(t, err, "worker", "pattern")
}
