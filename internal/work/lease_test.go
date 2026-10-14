package work

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/crawler"
)

func TestThreeDistinctPairsReportingParkAnEntry(t *testing.T) {
	h := newHarness(t)
	h.push(pageURL)
	for i := 1; i <= 3; i++ {
		l, ok := h.claimFor(hexID(i), hexID(100+i))
		if !ok {
			t.Fatalf("pair %d got no lease", i)
		}
		res, err := report(h, l, hexID(i), hexID(100+i), 403)
		if err != nil || res.Verdict != "released" {
			t.Fatalf("%+v %v", res, err)
		}
	}
	if _, ok := h.claimFor(hexID(4), hexID(104)); ok {
		t.Fatal("an entry reported by three pairs was leased")
	}
}

func TestOnePairReportingEveryLeaseCannotParkAnEntry(t *testing.T) {
	h := newHarness(t)
	h.push("https://a.example.org/1", "https://b.example.org/2", "https://c.example.org/3", "https://d.example.org/4")
	w, n := hexID(1), hexID(101)
	for i := 0; i < 4; i++ {
		l, ok := h.claimFor(w, n)
		if !ok {
			t.Fatalf("report %d: no lease", i)
		}
		if _, err := report(h, l, w, n, 403); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := h.claimFor(w, n); ok {
		t.Fatal("the pair was leased a URL it reported")
	}
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	for u, e := range h.s.pool {
		if len(e.failed) != 1 || len(e.reported) != 1 || !e.parkedUntil.IsZero() {
			t.Errorf("%s: failed %d reported %d parked %v", u, len(e.failed), len(e.reported), e.parkedUntil)
		}
	}
}

func wantError(t *testing.T, err error, status int, code string) *Error {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Status != status || e.Code != code {
		t.Fatalf("want %d %s, got %v", status, code, err)
	}
	return e
}

// leased claims a lease on pageURL for a fresh pair and returns a valid request for it.
func leased(t *testing.T, h *harness) (SubmitRequest, LeaseGrant) {
	t.Helper()
	w, n := h.nextPair()
	l, ok := h.claimFor(w, n)
	if !ok {
		t.Fatal("no lease")
	}
	return request(l, w, n, l.URL, pageText), l
}

func TestSubmitValidatesEveryMember(t *testing.T) {
	str := func(s string) *string { return &s }
	num := func(n int) *int { return &n }
	long := strings.Repeat("word ", maxTextBytes/5+1)
	cases := []struct {
		name   string
		mut    func(*SubmitRequest)
		status int
		code   string
		field  string
		rule   string
	}{
		{"lease id", func(r *SubmitRequest) { r.LeaseID = "short" }, 422, "invalid_field", "lease_id", "pattern"},
		{"worker", func(r *SubmitRequest) { r.Worker = strings.ToUpper(r.Worker) + "Z" }, 422, "invalid_field", "worker", "pattern"},
		{"net", func(r *SubmitRequest) { r.Net = "x" }, 422, "invalid_field", "net", "pattern"},
		{"no status", func(r *SubmitRequest) { r.HTTPStatus = nil }, 422, "invalid_field", "http_status", "required"},
		{"status low", func(r *SubmitRequest) { r.HTTPStatus = num(99) }, 422, "invalid_field", "http_status", "range"},
		{"status high", func(r *SubmitRequest) { r.HTTPStatus = num(600) }, 422, "invalid_field", "http_status", "range"},
		{"no final url", func(r *SubmitRequest) { r.FinalURL = nil }, 422, "invalid_field", "final_url", "required"},
		{"no title", func(r *SubmitRequest) { r.Title = nil }, 422, "invalid_field", "title", "required"},
		{"no text", func(r *SubmitRequest) { r.Text = nil }, 422, "invalid_field", "text", "required"},
		{"no lang", func(r *SubmitRequest) { r.Lang = nil }, 422, "invalid_field", "lang", "required"},
		{"no content hash", func(r *SubmitRequest) { r.ContentHash = nil }, 422, "invalid_field", "content_hash", "required"},
		{"no simhash", func(r *SubmitRequest) { r.Simhash = nil }, 422, "invalid_field", "simhash", "required"},
		{"no version", func(r *SubmitRequest) { r.ExtractVersion = nil }, 422, "invalid_field", "extract_version", "required"},
		{"stale version", func(r *SubmitRequest) { r.ExtractVersion = num(crawler.ExtractVersion + 1) }, 409, "extract_version_mismatch", "", ""},
		{"title too long", func(r *SubmitRequest) { r.Title = str(strings.Repeat("t", 1001)) }, 422, "invalid_field", "title", "length"},
		{"title not utf8", func(r *SubmitRequest) { r.Title = str("bad\xff") }, 422, "invalid_field", "title", "char_class"},
		{"text too short", func(r *SubmitRequest) { r.Text = str(strings.Repeat("a b ", 19)) }, 422, "invalid_field", "text", "length"},
		{"text too long", func(r *SubmitRequest) { r.Text = str(long) }, 422, "invalid_field", "text", "length"},
		{"text not utf8", func(r *SubmitRequest) { r.Text = str(pageText + "\xff") }, 422, "invalid_field", "text", "char_class"},
		{"text with NUL", func(r *SubmitRequest) { r.Text = str(pageText + "\x00") }, 422, "invalid_field", "text", "char_class"},
		{"text without a token", func(r *SubmitRequest) { r.Text = str(strings.Repeat("... --- ", 20)) }, 422, "invalid_field", "text", "required"},
		{"lang", func(r *SubmitRequest) { r.Lang = str("english-language") }, 422, "invalid_field", "lang", "pattern"},
		{"hash pattern", func(r *SubmitRequest) { r.ContentHash = str(strings.ToUpper(sha(pageText))) }, 422, "invalid_field", "content_hash", "pattern"},
		{"simhash pattern", func(r *SubmitRequest) { r.Simhash = str("abc") }, 422, "invalid_field", "simhash", "pattern"},
		{"hash mismatch", func(r *SubmitRequest) { r.ContentHash = str(sha("another text")) }, 422, "invalid_field", "content_hash", "mismatch"},
		{"simhash mismatch", func(r *SubmitRequest) { r.Simhash = str("0000000000000000") }, 422, "invalid_field", "simhash", "mismatch"},
		{"final url", func(r *SubmitRequest) { r.FinalURL = str("ftp://example.org/x") }, 422, "invalid_field", "final_url", "url"},
		{"final url private", func(r *SubmitRequest) { r.FinalURL = str("http://127.0.0.1/x") }, 422, "invalid_field", "final_url", "url"},
		{"final url off host", func(r *SubmitRequest) { r.FinalURL = str("https://elsewhere.example.net/x") }, 422, "invalid_field", "final_url", "off_host"},
		{"final url suffix host", func(r *SubmitRequest) { r.FinalURL = str("https://notexample.org/x") }, 422, "invalid_field", "final_url", "off_host"},
		{"artifact", func(r *SubmitRequest) { r.Artifact = &Artifact{Model: "m"} }, 422, "invalid_field", "artifact", "artifact_invalid"},
		{"artifact chunks without a model", func(r *SubmitRequest) {
			r.Artifact = &Artifact{Chunks: []crawler.IndexedChunk{{Text: "Tidal", Embedding: []float32{1}}}}
		}, 422, "invalid_field", "artifact", "artifact_invalid"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t)
			h.push(pageURL)
			req, _ := leased(t, h)
			c.mut(&req)
			_, err := h.s.Submit(req)
			e := wantError(t, err, c.status, c.code)
			if e.Field != c.field || e.Rule != c.rule {
				t.Fatalf("%+v", e)
			}
			if c.code == "extract_version_mismatch" && e.ExtractVersion != crawler.ExtractVersion {
				t.Fatalf("the engine's version is missing: %+v", e)
			}
		})
	}
}

func TestAnArtifactWithNeitherModelNorChunksIsValid(t *testing.T) {
	for name, a := range map[string]*Artifact{"empty": {}, "empty chunks": {Chunks: []crawler.IndexedChunk{}}} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.push(pageURL)
			req, _ := leased(t, h)
			req.Artifact = a
			if res, err := h.s.Submit(req); err != nil || res.Verdict != "awaiting_quorum" {
				t.Fatalf("%+v %v", res, err)
			}
		})
	}
}

func TestSubmitHoldsTheFinalURLToThePolicyAndTheSameSite(t *testing.T) {
	for _, tc := range []struct{ final, rule string }{
		{"https://example.org/other?x=1", ""},
		{"https://www.example.org/b", ""},
		{"http://EXAMPLE.org/c", ""},
		{"https://news.example.org/c", "off_host"},
		{"https://a.www.example.org/c", "off_host"},
		{"https://example.org/policy-blocked", "policy"},
	} {
		h := newHarness(t)
		h.push("https://www.example.org/blocked-page")
		h.f.blockedFor["example.org"] = tc.rule == "policy"
		req, _ := leased(t, h)
		req.FinalURL = &tc.final
		_, err := h.s.Submit(req)
		if tc.rule == "" && err != nil {
			t.Errorf("%s: %v", tc.final, err)
		}
		if tc.rule != "" {
			if e := wantError(t, err, 422, "invalid_field"); e.Rule != tc.rule || e.Field != "final_url" {
				t.Errorf("%s: %+v", tc.final, e)
			}
		}
	}
}

func TestSubmitOwnershipAndExpiry(t *testing.T) {
	h := newHarness(t)
	h.push(pageURL)
	req, l := leased(t, h)
	stranger := req
	stranger.Worker = hexID(999)
	_, err := h.s.Submit(stranger)
	wantError(t, err, 409, "lease_not_owner")
	moved := req
	moved.Net = hexID(998)
	_, err = h.s.Submit(moved)
	wantError(t, err, 409, "lease_not_owner")
	unknown := req
	unknown.LeaseID = hexID(5)
	_, err = h.s.Submit(unknown)
	wantError(t, err, 409, "lease_expired")
	h.advance(10*time.Minute + time.Second)
	_, err = h.s.Submit(req)
	wantError(t, err, 409, "lease_expired")
	if _, ok := h.claimFor(hexID(500), hexID(501)); !ok {
		t.Fatal("the lapsed lease should have returned to the pool")
	}
	_ = l
}

func TestAFailureReportRejectsAttestationMembers(t *testing.T) {
	for _, field := range []string{"title", "text", "lang", "content_hash", "simhash", "extract_version", "artifact"} {
		h := newHarness(t)
		h.push(pageURL)
		full, _ := leased(t, h)
		status := 503
		r := SubmitRequest{LeaseID: full.LeaseID, Worker: full.Worker, Net: full.Net, HTTPStatus: &status}
		switch field {
		case "title":
			r.Title = full.Title
		case "text":
			r.Text = full.Text
		case "lang":
			r.Lang = full.Lang
		case "content_hash":
			r.ContentHash = full.ContentHash
		case "simhash":
			r.Simhash = full.Simhash
		case "extract_version":
			r.ExtractVersion = full.ExtractVersion
		case "artifact":
			r.Artifact = &Artifact{}
		}
		_, err := h.s.Submit(r)
		if e := wantError(t, err, 422, "invalid_field"); e.Field != field || e.Rule != "field_not_allowed" {
			t.Errorf("%s: %+v", field, e)
		}
	}
	h := newHarness(t)
	h.push(pageURL)
	full, _ := leased(t, h)
	status := 503
	url := "https://example.org/moved"
	res, err := h.s.Submit(SubmitRequest{LeaseID: full.LeaseID, Worker: full.Worker, Net: full.Net, HTTPStatus: &status, FinalURL: &url})
	if err != nil || res.Verdict != "released" || res.Agreeing == nil || len(res.Agreeing) != 0 {
		t.Fatalf("%+v %v", res, err)
	}
	again, err := h.s.Submit(SubmitRequest{LeaseID: full.LeaseID, Worker: full.Worker, Net: full.Net, HTTPStatus: &status})
	if err != nil || again.Verdict != "released" {
		t.Fatalf("a repeated report: %+v %v", again, err)
	}
	_, err = h.s.Submit(full)
	wantError(t, err, 409, "lease_expired")
}

func TestARepeatedSubmissionIsIdempotentAndADifferentOneIsRefused(t *testing.T) {
	h := newHarness(t)
	h.add(ClassErrored, pageURL)
	req, _ := leased(t, h)
	first, err := h.s.Submit(req)
	if err != nil || first.Verdict != "awaiting_quorum" || first.WindowExpiresAt == nil || !first.WindowExpiresAt.Equal(h.now().Add(30*time.Minute)) {
		t.Fatalf("%+v %v", first, err)
	}
	again, err := h.s.Submit(req)
	if err != nil || again.Verdict != "awaiting_quorum" {
		t.Fatalf("%+v %v", again, err)
	}
	if rec := h.record(pageURL); len(rec.Groups) != 1 || len(rec.Groups[0].Members) != 1 {
		t.Fatalf("a repeat was counted as a second attestation: %+v", rec.Groups)
	}
	changed := request(LeaseGrant{ID: req.LeaseID}, req.Worker, req.Net, pageURL, pageText+" and a change")
	_, err = h.s.Submit(changed)
	wantError(t, err, 409, "lease_expired")
}

func TestAnErroredQuorumVerifiesAtOnce(t *testing.T) {
	h := newHarness(t)
	h.add(ClassErrored, pageURL)
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	res, w1, _, err := h.attest(a)
	if err != nil || res.Verdict != "awaiting_quorum" {
		t.Fatalf("%+v %v", res, err)
	}
	res, w2, _, err := h.attest(b)
	if err != nil || res.Verdict != "verified" || res.Indexed || fmt.Sprint(res.Agreeing) != fmt.Sprint([]string{w1, w2}) {
		t.Fatalf("%+v %v", res, err)
	}
	agreed := agreedOf(a, b)
	if agreed == b {
		t.Fatal("the fixture pair must differ in text the two did not both see")
	}
	rec := h.record(pageURL)
	if rec.State != StateModerating || rec.VerifiedBy != "quorum" || rec.ContentHash != sha(agreed) || rec.FinalURL != pageURL {
		t.Fatalf("%+v", rec)
	}
	text, _ := h.s.getText(urlSHA(pageURL))
	if text == nil || text.Text != agreed {
		t.Fatalf("the text both saw is the one to index: %+v", text)
	}
	if len(h.f.indexed) != 0 || len(h.settled()) != 0 {
		t.Fatal("verification is not indexing or settling")
	}
}

func TestAnErroredQuorumIsIndexedUnderTheLeaseURL(t *testing.T) {
	h := newHarness(t)
	h.add(ClassErrored, pageURL)
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	var res SubmitResult
	for _, text := range []string{a, b} {
		w, n := h.nextPair()
		l, _ := h.claimFor(w, n)
		var err error
		if res, err = h.s.Submit(request(l, w, n, "https://example.org/another-page", text)); err != nil {
			t.Fatal(err)
		}
	}
	if res.Verdict != "verified" {
		t.Fatalf("%+v", res)
	}
	text, _ := h.s.getText(urlSHA(pageURL))
	if text == nil {
		t.Fatal("no text to index")
	}
	if rec := h.record(pageURL); rec.FinalURL != pageURL || text.FinalURL != pageURL {
		t.Fatalf("a quorum names no URL of its own: record %q, text %q", rec.FinalURL, text.FinalURL)
	}
}

func TestADissentingGroupIsSettledAMismatch(t *testing.T) {
	h := newHarness(t)
	h.add(ClassErrored, pageURL)
	a, forged, dist := fixture(t, "injected-paragraph-short-page")
	if dist <= h.cfg.SimhashMax {
		t.Fatalf("fixture distance %d", dist)
	}
	if res, _, _, err := h.attest(a); err != nil || res.Verdict != "awaiting_quorum" {
		t.Fatalf("%+v %v", res, err)
	}
	_, forger, _, err := h.attest(forged)
	if err != nil {
		t.Fatal(err)
	}
	res, _, _, err := h.attest(a)
	if err != nil || res.Verdict != "verified" || len(res.Agreeing) != 2 {
		t.Fatalf("%+v %v", res, err)
	}
	evs := h.settled()
	if len(evs) != 1 || evs[0].Verdict != "mismatch" || fmt.Sprint(evs[0].Workers) != fmt.Sprint([]string{forger}) || evs[0].ContentNovel {
		t.Fatalf("%+v", evs)
	}
	if rec := h.record(pageURL); rec.VerifiedGroup != 0 || len(rec.Groups) != 2 {
		t.Fatalf("%+v", rec.Groups)
	}
}

func TestAPushedOrFrontierQuorumWaitsForTheServer(t *testing.T) {
	for _, class := range []Class{ClassPushed, ClassFrontier} {
		t.Run(string(class), func(t *testing.T) {
			h := newHarness(t)
			h.add(class, pageURL)
			a, b, _ := fixture(t, "rotated-ad-and-timestamp")
			if res, _, _, err := h.attest(a); err != nil || res.Verdict != "awaiting_quorum" {
				t.Fatalf("%+v %v", res, err)
			}
			if rec := h.record(pageURL); !rec.RefetchAt.After(h.now()) {
				t.Fatal("one attestation must wait for the window")
			}
			res, _, _, err := h.attest(b)
			if err != nil || res.Verdict != "awaiting_quorum" || len(res.Agreeing) != 0 || res.Indexed {
				t.Fatalf("a quorum never skips the server: %+v %v", res, err)
			}
			rec := h.record(pageURL)
			if rec.State != StateAwaiting || rec.VerifiedBy != "" || rec.RefetchAt.After(h.now()) {
				t.Fatalf("the re-fetch is due at once: %+v", rec)
			}
			if text, _ := h.s.getText(urlSHA(pageURL)); text != nil || len(h.f.indexed) != 0 || len(h.s.texts) != 0 {
				t.Fatal("nothing is queued or indexed before the server's copy")
			}
			if _, ok := h.claimFor(hexID(900), hexID(901)); ok {
				t.Fatal("a quorate entry is not leased further")
			}
		})
	}
}

func TestAQuorumNeedsRoomInTheModerationQueue(t *testing.T) {
	h := newHarness(t, func(c *config.WorkSettings) { c.ModerationQueueMax = 1 })
	h.s.texts["taken"] = struct{}{}
	h.add(ClassErrored, pageURL)
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	h.attest(a)
	res, _, _, err := h.attest(b)
	if err != nil || res.Verdict != "awaiting_quorum" {
		t.Fatalf("%+v %v", res, err)
	}
	if rec := h.record(pageURL); rec.State != StateAwaiting || len(rec.Groups[0].Members) != 2 {
		t.Fatalf("the attestation is kept, the record stays awaiting: %+v", rec)
	}
}

func TestAnErroredQuorumHeldByAFullQueueIsLeasedAgainAfterARestart(t *testing.T) {
	h := newHarness(t, func(c *config.WorkSettings) { c.ModerationQueueMax = 1 })
	h.s.texts["taken"] = struct{}{}
	h.add(ClassErrored, pageURL)
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	h.attest(a)
	if res, _, _, err := h.attest(b); err != nil || res.Verdict != "awaiting_quorum" {
		t.Fatalf("%+v %v", res, err)
	}
	if _, ok := h.claimFor(h.nextPair()); !ok {
		t.Fatal("the held errored quorum should be leasable for a third attestation")
	}
	h.db.Close()
	h.open()
	h.f.errored = []FrontierRow{{URL: pageURL, LastError: "http 403"}}
	h.s.Feed(context.Background())
	res, _, _, err := h.attest(a)
	if err != nil || res.Verdict != "verified" {
		t.Fatalf("after a restart: %+v %v", res, err)
	}
}

func TestAQuorumAtTheCeilingIsDeferred(t *testing.T) {
	h := newHarness(t, func(c *config.WorkSettings) { c.WorkModerationsPerDay = 1 })
	h.add(ClassErrored, pageURL)
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	w1, n1 := h.nextPair()
	w2, n2 := h.nextPair()
	l1, _ := h.claimFor(w1, n1)
	l2, _ := h.claimFor(w2, n2)
	if err := h.s.reserve(); err != nil {
		t.Fatal(err)
	}
	if _, err := h.s.Submit(request(l1, w1, n1, pageURL, a)); err != nil {
		t.Fatal(err)
	}
	res, err := h.s.Submit(request(l2, w2, n2, pageURL, b))
	if err != nil || res.Verdict != "deferred" || res.Reason != "moderation_ceiling" || len(res.Agreeing) != 0 {
		t.Fatalf("%+v %v", res, err)
	}
	rec := h.record(pageURL)
	if rec.State != StateDeferred || !rec.RetryAt.Equal(nextMidnight(h.now())) {
		t.Fatalf("%+v", rec)
	}
}

func TestALateAgreeingAttestationJoinsTheVerifiedGroup(t *testing.T) {
	h := newHarness(t)
	h.add(ClassErrored, pageURL)
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	w1, n1 := h.nextPair()
	w2, n2 := h.nextPair()
	w3, n3 := h.nextPair()
	l1, _ := h.claimFor(w1, n1)
	l2, _ := h.claimFor(w2, n2)
	l3, _ := h.claimFor(w3, n3)
	if _, err := h.s.Submit(request(l1, w1, n1, pageURL, a)); err != nil {
		t.Fatal(err)
	}
	if res, err := h.s.Submit(request(l2, w2, n2, pageURL, b)); err != nil || res.Verdict != "verified" {
		t.Fatalf("%+v %v", res, err)
	}
	res, err := h.s.Submit(request(l3, w3, n3, pageURL, a))
	if err != nil || res.Verdict != "verified" || res.Indexed || len(res.Agreeing) != 3 {
		t.Fatalf("%+v %v", res, err)
	}
	if rec := h.record(pageURL); len(rec.Groups[0].Members) != 3 {
		t.Fatalf("%+v", rec.Groups)
	}
}

func TestMatchingNeedsADistinctWorkerAndNetworkWithinTheBound(t *testing.T) {
	h := newHarness(t)
	a, near, _ := fixture(t, "rotated-ad-and-timestamp")
	_, far, _ := fixture(t, "injected-paragraph-short-page")
	sum := func(text string) string { v, _ := crawler.Simhash(text); return crawler.SimhashHex(v) }
	g := Group{Anchor: sum(a), Members: []Attestation{{Worker: "w1", Net: "n1", Simhash: sum(a)}}}
	for _, tc := range []struct {
		name string
		a    Attestation
		want bool
	}{
		{"distinct and near", Attestation{Worker: "w2", Net: "n2", Simhash: sum(near)}, true},
		{"same worker", Attestation{Worker: "w1", Net: "n2", Simhash: sum(near)}, false},
		{"same network", Attestation{Worker: "w2", Net: "n1", Simhash: sum(near)}, false},
		{"far", Attestation{Worker: "w2", Net: "n2", Simhash: sum(far)}, false},
		{"unparseable", Attestation{Worker: "w2", Net: "n2", Simhash: "zz"}, false},
	} {
		if got := h.s.matches(g, tc.a); got != tc.want {
			t.Errorf("%s: %v", tc.name, got)
		}
	}
	h.s.cfg.SimhashMax = 0
	if h.s.matches(g, Attestation{Worker: "w2", Net: "n2", Simhash: sum(near)}) {
		t.Error("a bound of 0 needs equal hashes")
	}
}

func TestTheHammingBoundIsInclusive(t *testing.T) {
	h := newHarness(t)
	max := h.cfg.SimhashMax
	anchor := uint64(0x2d1a482a4439cff0)
	at := func(bits int) Attestation {
		return Attestation{Worker: "w2", Net: "n2", Simhash: crawler.SimhashHex(anchor ^ (1<<uint(bits) - 1))}
	}
	g := Group{Anchor: crawler.SimhashHex(anchor), Members: []Attestation{{Worker: "w1", Net: "n1"}}}
	if !h.s.matches(g, at(max)) || h.s.matches(g, at(max+1)) {
		t.Fatalf("an attestation %d bits away joins, %d bits away does not", max, max+1)
	}
	rec := &Record{Groups: []Group{g}}
	if gi := h.s.closestGroup(rec, anchor^(1<<uint(max)-1)); gi != 0 {
		t.Fatalf("a copy %d bits away verifies the group: %d", max, gi)
	}
	if gi := h.s.closestGroup(rec, anchor^(1<<uint(max+1)-1)); gi != -1 {
		t.Fatalf("a copy %d bits away does not: %d", max+1, gi)
	}
}

func TestTheFirstGroupToReachTwoIsTheVerifiedOne(t *testing.T) {
	h := newHarness(t)
	h.add(ClassErrored, pageURL)
	honest, forged, _ := fixture(t, "injected-paragraph-short-page")
	_, forger, _, _ := h.attest(forged)
	h.attest(honest)
	res, _, _, err := h.attest(honest)
	if err != nil || res.Verdict != "verified" {
		t.Fatalf("%+v %v", res, err)
	}
	rec := h.record(pageURL)
	if rec.VerifiedGroup != 1 || rec.Groups[rec.VerifiedGroup].Anchor != rec.Groups[1].Anchor || len(rec.Groups[0].Members) != 1 {
		t.Fatalf("the later group reached two first: %+v", rec.Groups)
	}
	if evs := h.settled(); len(evs) != 1 || fmt.Sprint(evs[0].Workers) != fmt.Sprint([]string{forger}) {
		t.Fatalf("%+v", evs)
	}
}

func TestOnePairCountsOnceWhateverTheNumberOfFailures(t *testing.T) {
	h := newHarness(t)
	h.push(pageURL)
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	e := h.s.pool[pageURL]
	p := pair{hexID(1), hexID(101)}
	for i := 0; i < 10; i++ {
		h.s.noteFailure(e, p, true, h.now())
	}
	if len(e.failed) != 1 || len(e.reported) != 1 || h.s.pool[pageURL] == nil {
		t.Fatalf("failed %d reported %d in pool %v", len(e.failed), len(e.reported), h.s.pool[pageURL] != nil)
	}
}
