package work

import (
	"strings"
	"testing"
)

// agreeThrough has two fresh accounts attest the sides, anchor first, and returns the answer and the indexed text.
func agreeThrough(t *testing.T, anchor, att agreedSide) (SubmitResult, string) {
	t.Helper()
	h := newHarness(t)
	h.erroredPage()
	res := h.submitPair(anchor, att)
	if res.Verdict != "verified" {
		h.nothingIndexed()
		return res, ""
	}
	return res, h.indexedOnce().Text
}

func TestACompleterThatRepeatsHonestPartsToClearBothFloorsGetsNoQuorum(t *testing.T) {
	c := loadAgreed(t).byID(t, "completer-repeats-honest-parts-to-clear-the-floors")
	anchor, completer := splitParts(c.Anchor.Text), splitParts(c.Attestation.Text)
	known := map[uint64]bool{}
	for _, h := range anchor.hashes {
		known[h] = true
	}
	for i, h := range completer.hashes {
		if !known[h] {
			t.Fatalf("part %d of the completer is not an honest part: the repetition must be the whole attack", i)
		}
	}
	h := newHarness(t)
	h.erroredPage()
	if res := h.submitPair(c.Anchor, c.Attestation); res.Verdict != "awaiting_quorum" {
		t.Fatalf("a completer made of repeated honest parts completed a quorum: %+v", res)
	}
	h.nothingIndexed()
	if rec := h.record(pageURL); rec.State != StateAwaiting || len(rec.Groups) != 2 {
		t.Fatalf("%+v", rec)
	}
	if got := c.Expect.MatchedBytes; got*10 >= 6*len(c.Attestation.Text) || got*10 >= 6*len(c.Anchor.Text) {
		t.Fatalf("matched %d bytes clear a floor", got)
	}
}

func TestACompleterThatOmitsAnchorPartsShowsAGapAtEachOmission(t *testing.T) {
	v := loadAgreed(t)
	c := v.byID(t, "completer-omits-parts")
	res, text := agreeThrough(t, c.Anchor, c.Attestation)
	if res.Verdict != "verified" {
		t.Fatalf("%+v", res)
	}
	if want := strings.Count(c.Expect.AgreedText, gap); want < 3 || strings.Count(text, gap) != want || text != c.Expect.AgreedText {
		t.Errorf("%d gaps in %q, want %d in %q", strings.Count(text, gap), text, want, c.Expect.AgreedText)
	}
}

func TestADishonestAnchorThatOmittedAPartShowsAGapInTheHonestText(t *testing.T) {
	c := loadAgreed(t).byID(t, "dishonest-anchor-removed-a-part")
	res, text := agreeThrough(t, c.Anchor, c.Attestation)
	if res.Verdict != "verified" || strings.Count(text, gap) != 1 || text != c.Expect.AgreedText {
		t.Fatalf("%+v %q", res, text)
	}
}

func TestAReorderedCompleterKeepsTheAnchorsOrderAndShowsTheRestAsGaps(t *testing.T) {
	for _, id := range []string{"completer-swaps-two-parts", "completer-moves-the-last-part-to-the-front"} {
		t.Run(id, func(t *testing.T) {
			c := loadAgreed(t).byID(t, id)
			res, text := agreeThrough(t, c.Anchor, c.Attestation)
			if res.Verdict != "verified" || text != c.Expect.AgreedText || strings.Count(text, gap) != 2 {
				t.Fatalf("%+v %q", res, text)
			}
			if strings.Join(strings.Fields(c.Anchor.Text), " ") == text {
				t.Fatal("the reordered text agreed as the anchor's own")
			}
		})
	}
}

func TestAnAgreedTextOf80BytesCountsGapMarkersAndOneOf79DoesNot(t *testing.T) {
	vs := loadAgreed(t)
	at := vs.byID(t, "agreed-text-of-exactly-80-bytes")
	if res, text := agreeThrough(t, at.Anchor, at.Attestation); res.Verdict != "verified" || len(text) != 80 || !strings.HasSuffix(text, " "+gap) {
		t.Fatalf("80 bytes with a trailing gap: %+v %d %q", res, len(text), text)
	}
	for _, id := range []string{"agreed-text-of-79-bytes", "agreed-text-far-under-80-bytes"} {
		c := vs.byID(t, id)
		if res, _ := agreeThrough(t, c.Anchor, c.Attestation); res.Verdict != "awaiting_quorum" {
			t.Errorf("%s: an agreed text under 80 bytes made a quorum: %+v", id, res)
		}
	}
}

func TestTheCapLeavesPartsBeyondItAGapOnWhicheverSideHasThem(t *testing.T) {
	c := loadAgreed(t).Cap
	full, cut := agreedSide{Title: "Parts", Lang: "en", Text: capText(c.Parts, 0)}, agreedSide{Title: "Parts", Lang: "en", Text: capText(c.Considered, 0)}
	for name, sides := range map[string][2]agreedSide{
		"the anchor has parts beyond the cap":    {full, cut},
		"the completer has parts beyond the cap": {cut, full},
		"both have them":                         {full, full},
	} {
		t.Run(name, func(t *testing.T) {
			res, text := agreeThrough(t, sides[0], sides[1])
			if res.Verdict != "verified" || len(text) != c.AgreedBytes || strings.Count(text, gap) != 1 || !strings.HasSuffix(text, " "+gap) {
				t.Fatalf("%+v: %d bytes, %d gaps, want %d bytes ending in one gap", res, len(text), strings.Count(text, gap), c.AgreedBytes)
			}
		})
	}
}
