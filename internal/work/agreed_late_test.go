package work

import (
	"strings"
	"testing"
)

func TestALateAttestationOnAnErroredPageFollowsTheSameRule(t *testing.T) {
	for _, c := range loadAgreed(t).Cases {
		t.Run(c.ID, func(t *testing.T) {
			h := newHarness(t)
			h.erroredPage()
			var ws, ns [3]string
			var ls [3]LeaseGrant
			for i := range ls {
				ws[i], ns[i] = h.nextPair()
				ls[i], _ = h.claimFor(ws[i], ns[i])
			}
			anchor := c.Anchor
			for i, text := range []string{anchor.Text, anchor.Text} {
				if _, err := h.s.Submit(requestAs(ls[i], ws[i], ns[i], pageURL, anchor.Title, anchor.Lang, text)); err != nil {
					t.Fatal(err)
				}
			}
			att := c.Attestation
			res, err := h.s.Submit(requestAs(ls[2], ws[2], ns[2], pageURL, att.Title, att.Lang, att.Text))
			if err != nil {
				t.Fatal(err)
			}
			rec := h.record(pageURL)
			if c.Expect.Agree {
				if res.Verdict != "verified" || len(rec.Groups) != 1 || len(rec.Groups[0].Members) != 3 {
					t.Fatalf("a late attestation that agrees: %+v %+v", res, rec.Groups)
				}
				if last := rec.Groups[0].Members[2]; last.Len != len(att.Text) || last.TitleSha != titleSha(att.Title) {
					t.Fatalf("a late attestation keeps its length and title_sha: %+v", last)
				}
			} else {
				if res.Verdict != "mismatch" || len(rec.Groups) != 2 {
					t.Fatalf("a late attestation that fails %v: %+v", c.Expect.Fails, res)
				}
				h.wantParts(1, att.Text)
			}
			doc := h.indexedOnce()
			if want := strings.Join(strings.Fields(anchor.Text), " "); doc.Text != want {
				t.Errorf("the first quorum's agreed text is indexed once: %.80q", doc.Text)
			}
			h.moderate()
			if len(h.f.indexed) != 1 {
				t.Fatalf("a late attestation indexed again: %d documents", len(h.f.indexed))
			}
		})
	}
}

func TestARepeatOfTheCompletingSubmissionRepeatsItsAnswer(t *testing.T) {
	c := loadAgreed(t).byID(t, "injected-paragraph")
	h := newHarness(t)
	h.erroredPage()
	w1, n1 := h.nextPair()
	w2, n2 := h.nextPair()
	l1, _ := h.claimFor(w1, n1)
	l2, _ := h.claimFor(w2, n2)
	if _, err := h.s.Submit(requestAs(l1, w1, n1, pageURL, c.Anchor.Title, c.Anchor.Lang, c.Anchor.Text)); err != nil {
		t.Fatal(err)
	}
	completing := requestAs(l2, w2, n2, pageURL, c.Attestation.Title, c.Attestation.Lang, c.Attestation.Text)
	first, err := h.s.Submit(completing)
	if err != nil || first.Verdict != "verified" {
		t.Fatalf("%+v %v", first, err)
	}
	again, err := h.s.Submit(completing)
	if err != nil || again.Verdict != "verified" {
		t.Fatalf("a repeat of the completing submission: %+v %v", again, err)
	}
	if doc := h.indexedOnce(); doc.Text != c.Expect.AgreedText {
		t.Fatalf("%.80q", doc.Text)
	}
}

func TestALateAttestationIsHeldToTheAnchorsLengthNotALaterMembers(t *testing.T) {
	vs := loadAgreed(t)
	anchor := vs.byID(t, "agreed-text-one-byte-below-60-percent-of-the-anchor")
	second := vs.byID(t, "agreed-text-at-exactly-60-percent-of-the-anchor").Anchor
	h := newHarness(t)
	h.erroredPage()
	var ws, ns [3]string
	var ls [3]LeaseGrant
	for i := range ls {
		ws[i], ns[i] = h.nextPair()
		ls[i], _ = h.claimFor(ws[i], ns[i])
	}
	for i, side := range []agreedSide{anchor.Anchor, second} {
		if _, err := h.s.Submit(requestAs(ls[i], ws[i], ns[i], pageURL, side.Title, side.Lang, side.Text)); err != nil {
			t.Fatal(err)
		}
	}
	if rec := h.record(pageURL); rec.State == StateAwaiting || len(rec.Groups) != 1 || len(rec.Groups[0].Members) != 2 {
		t.Fatalf("the two longer texts do not make a quorum: %+v", rec)
	}
	att := anchor.Attestation
	res, err := h.s.Submit(requestAs(ls[2], ws[2], ns[2], pageURL, att.Title, att.Lang, att.Text))
	if err != nil {
		t.Fatal(err)
	}
	if rec := h.record(pageURL); res.Verdict != "mismatch" || len(rec.Groups) != 2 {
		t.Fatalf("a text one byte under 60 %% of the anchor joined: %+v %+v", res, rec.Groups)
	}
}
