package work

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

// agreedOf is what a quorum of the two texts, a first, indexes: the second's parts that are in the first's.
func agreedOf(first, second string) string {
	return splitParts(second).agreed(partSet(splitParts(first).hashes))
}

func (h *harness) erroredPage() {
	h.t.Helper()
	h.add(ClassErrored, pageURL)
	h.f.rows[pageURL] = "errored"
}

// submitPair has two fresh accounts attest the two sides of a vector case, anchor first, and returns the second answer.
func (h *harness) submitPair(anchor, att agreedSide) SubmitResult {
	h.t.Helper()
	if res, _, _, err := h.attestAs(anchor.Title, anchor.Lang, anchor.Text); err != nil || res.Verdict != "awaiting_quorum" {
		h.t.Fatalf("the anchor: %+v %v", res, err)
	}
	res, _, _, err := h.attestAs(att.Title, att.Lang, att.Text)
	if err != nil {
		h.t.Fatal(err)
	}
	return res
}

// indexedOnce runs the moderation queue and returns the one document that was indexed.
func (h *harness) indexedOnce() crawler.AttestedDoc {
	h.t.Helper()
	h.s.runModerations(context.Background())
	h.f.mu.Lock()
	defer h.f.mu.Unlock()
	if len(h.f.indexed) != 1 {
		h.t.Fatalf("%d documents indexed, want 1", len(h.f.indexed))
	}
	return h.f.indexed[0]
}

func (h *harness) nothingIndexed() {
	h.t.Helper()
	h.s.runModerations(context.Background())
	h.f.mu.Lock()
	defer h.f.mu.Unlock()
	if len(h.f.indexed) != 0 {
		h.t.Fatalf("indexed %q", h.f.indexed[0].Text)
	}
}

func TestOneDishonestAccountCannotInjectAParagraphIntoAnErroredPage(t *testing.T) {
	c := loadAgreed(t).byID(t, "injected-paragraph")
	for _, tc := range []struct {
		name        string
		first, then agreedSide
		wantAgreed  string
	}{
		{"the dishonest account completes", c.Anchor, c.Attestation, c.Expect.AgreedText},
		{"the dishonest account is the anchor", c.Attestation, c.Anchor, c.Expect.AgreedText},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.erroredPage()
			if res := h.submitPair(tc.first, tc.then); res.Verdict != "verified" {
				t.Fatalf("%+v", res)
			}
			doc := h.indexedOnce()
			for _, word := range []string{"unsafe", "investors", "holdings"} {
				if strings.Contains(doc.Text, word) {
					t.Errorf("the injected %q was indexed", word)
				}
			}
			if doc.Text != tc.wantAgreed || doc.VerifiedBy != "quorum" {
				t.Errorf("indexed %q by %q, want %q", doc.Text, doc.VerifiedBy, tc.wantAgreed)
			}
			if rec := h.record(pageURL); rec.ContentHash != sha(doc.Text) {
				t.Errorf("content_hash %s is not the hash of the indexed text", rec.ContentHash)
			}
		})
	}
}

func TestTheCompletingSubmissionCannotChooseTheTitle(t *testing.T) {
	c := loadAgreed(t).byID(t, "different-title")
	for name, sides := range map[string][2]agreedSide{
		"the completing title differs":   {c.Anchor, c.Attestation},
		"the anchor's title differs":     {c.Attestation, c.Anchor},
		"a different language only":      {loadAgreed(t).byID(t, "different-lang").Anchor, loadAgreed(t).byID(t, "different-lang").Attestation},
		"the title in other capitals":    {c.Anchor, {Title: strings.ToUpper(c.Anchor.Title), Lang: c.Anchor.Lang, Text: c.Anchor.Text}},
		"the language in other capitals": {c.Anchor, {Title: c.Anchor.Title, Lang: "EN", Text: c.Anchor.Text}},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.erroredPage()
			res := h.submitPair(sides[0], sides[1])
			if res.Verdict != "awaiting_quorum" {
				t.Fatalf("two accounts that disagree on the title or language made a quorum: %+v", res)
			}
			h.nothingIndexed()
			if rec := h.record(pageURL); rec.State != StateAwaiting || len(rec.Groups) != 2 {
				t.Fatalf("%+v", rec)
			}
		})
	}
	t.Run("an honest third account completes the anchor's group with the anchor's title", func(t *testing.T) {
		h := newHarness(t)
		h.erroredPage()
		h.submitPair(c.Anchor, c.Attestation)
		res, _, _, err := h.attestAs(c.Anchor.Title, c.Anchor.Lang, c.Anchor.Text)
		if err != nil || res.Verdict != "verified" || len(res.Agreeing) != 2 {
			t.Fatalf("%+v %v", res, err)
		}
		if doc := h.indexedOnce(); doc.Title != c.Anchor.Title || doc.Text != c.Expect.AgreedText {
			t.Fatalf("title %q text %q", doc.Title, doc.Text)
		}
	})
	t.Run("white space in the title is not a difference, and the completing title is the one indexed", func(t *testing.T) {
		same := loadAgreed(t).byID(t, "same-words-different-white-space")
		h := newHarness(t)
		h.erroredPage()
		if res := h.submitPair(same.Anchor, same.Attestation); res.Verdict != "verified" {
			t.Fatalf("%+v", res)
		}
		if doc := h.indexedOnce(); doc.Title != same.Attestation.Title {
			t.Fatalf("title %q, want the completing submission's %q", doc.Title, same.Attestation.Title)
		}
	})
}

func TestAnEntitySwappedInsideOnePartOfALongPageIsDropped(t *testing.T) {
	c := loadAgreed(t).byID(t, "long-page-entity-swap")
	if c.Expect.Distance > 6 {
		t.Fatalf("the reproduction is not within simhash_max: %d", c.Expect.Distance)
	}
	for name, sides := range map[string][2]agreedSide{
		"the swap completes":     {c.Anchor, c.Attestation},
		"the swap is the anchor": {c.Attestation, c.Anchor},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.erroredPage()
			if res := h.submitPair(sides[0], sides[1]); res.Verdict != "verified" {
				t.Fatalf("%+v", res)
			}
			doc := h.indexedOnce()
			for _, entity := range []string{"Solent", "Pentland"} {
				if strings.Contains(doc.Text, entity) {
					t.Errorf("the swapped part is in the indexed text (%s)", entity)
				}
			}
			if strings.Count(doc.Text, gap) != 1 {
				t.Errorf("want one gap where the part was: %q", doc.Text)
			}
		})
	}
}

func TestEveryVectorCaseAgreesOrNotThroughTheService(t *testing.T) {
	v := loadAgreed(t)
	for _, c := range v.Cases {
		t.Run(c.ID, func(t *testing.T) {
			sa, _ := crawler.Simhash(c.Anchor.Text)
			sb, _ := crawler.Simhash(c.Attestation.Text)
			if fmt.Sprint(c.Expect.Simhashes) != fmt.Sprint([]string{crawler.SimhashHex(sa), crawler.SimhashHex(sb)}) || crawler.SimhashDistance(sa, sb) != c.Expect.Distance {
				t.Fatalf("simhashes %s %s distance %d, vectors %v %d", crawler.SimhashHex(sa), crawler.SimhashHex(sb), crawler.SimhashDistance(sa, sb), c.Expect.Simhashes, c.Expect.Distance)
			}
			h := newHarness(t)
			h.erroredPage()
			res := h.submitPair(c.Anchor, c.Attestation)
			if !c.Expect.Agree {
				if res.Verdict != "awaiting_quorum" {
					t.Fatalf("%v fail but the engine answered %+v", c.Expect.Fails, res)
				}
				h.nothingIndexed()
				return
			}
			if res.Verdict != "verified" {
				t.Fatalf("the vectors agree but the engine answered %+v", res)
			}
			doc := h.indexedOnce()
			if doc.Text != c.Expect.AgreedText || doc.Title != c.Attestation.Title || doc.Lang != c.Attestation.Lang || doc.VerifiedBy != "quorum" {
				t.Errorf("indexed %q %q %q by %q, want the agreed text and the completing title and lang", doc.Title, doc.Lang, doc.Text, doc.VerifiedBy)
			}
			if rec := h.record(pageURL); rec.ContentHash != c.Expect.ContentHash {
				t.Errorf("content_hash %s, want %s", rec.ContentHash, c.Expect.ContentHash)
			}
		})
	}
}

func TestAnErroredQuorumIndexesTheTextAgreedWithTheQuorateGroupsOwnAnchor(t *testing.T) {
	c := loadAgreed(t).byID(t, "injected-paragraph")
	h := newHarness(t)
	h.erroredPage()
	if res, _, _, err := h.attestAs("Other title", c.Attestation.Lang, c.Attestation.Text); err != nil || res.Verdict != "awaiting_quorum" {
		t.Fatalf("%+v %v", res, err)
	}
	if res := h.submitPair(c.Anchor, c.Attestation); res.Verdict != "verified" {
		t.Fatalf("%+v", res)
	}
	if rec := h.record(pageURL); rec.VerifiedGroup != 1 || len(rec.Groups) != 2 {
		t.Fatalf("the quorum is not the second group: %+v", rec.Groups)
	}
	if doc := h.indexedOnce(); doc.Text != c.Expect.AgreedText {
		t.Fatalf("not cut against the quorate group's anchor: %.80q", doc.Text)
	}
}
