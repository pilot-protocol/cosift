package work

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/crawler"
)

const (
	collidingA = "zkizavdbumdssf"
	collidingB = "cldkfccxppkppb"
)

// collidingPart is a 96-word part whose last word is filler: collidingA and collidingB give it one FNV-1a-64 hash.
func collidingPart(filler string) string {
	words := make([]string, 0, 96)
	for i := 0; i < 95; i++ {
		words = append(words, fmt.Sprintf("gull%02de", i))
	}
	return strings.Join(append(words, filler), " ")
}

func fnvOf(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func TestAPartThatCollidesUnderFNVIsNotAgreed(t *testing.T) {
	pa, pb := collidingPart(collidingA), collidingPart(collidingB)
	if fnvOf(pa) != fnvOf(pb) || pa == pb {
		t.Fatal("the two parts must collide under FNV-1a-64")
	}
	rest := loadAgreed(t).byID(t, "identical-texts").Anchor.Text
	honest, forged := pa+" "+rest, pb+" "+rest
	sa, sb := splitParts(honest), splitParts(forged)
	if sa.partWords()[0] != 96 || sb.partWords()[0] != 96 || sa.hashes[0] == sb.hashes[0] || fmt.Sprint(sa.hashes[1:]) != fmt.Sprint(sb.hashes[1:]) {
		t.Fatalf("the texts must differ in their first part only: %v %v", sa.partWords(), sb.partWords())
	}
	h := newHarness(t)
	h.erroredPage()
	side := func(text string) agreedSide { return agreedSide{Title: "Gulls", Lang: "en", Text: text} }
	if res := h.submitPair(side(honest), side(forged)); res.Verdict != "verified" {
		t.Fatalf("%+v", res)
	}
	doc := h.indexedOnce()
	if strings.Contains(doc.Text, collidingB) || doc.Text != gap+" "+strings.Join(strings.Fields(rest), " ") {
		t.Fatalf("the part with the colliding FNV hash was agreed: %.120q", doc.Text)
	}
}

func (h *harness) rawRecord() string {
	h.t.Helper()
	raw, err := h.s.get(keyOf(prefixRecord, pageURL))
	if err != nil || raw == nil {
		h.t.Fatalf("no record: %v", err)
	}
	return string(raw)
}

func TestEveryMemberStoresItsTitleLanguageAndLengthAndAnAnchorItsParts(t *testing.T) {
	v := loadAgreed(t)
	c := v.byID(t, "different-title")
	h := newHarness(t, func(c *config.WorkSettings) { c.ModerationQueueMax = 1 })
	h.s.texts["taken"] = struct{}{}
	h.erroredPage()
	sides := []agreedSide{c.Anchor, c.Anchor, c.Attestation}
	for _, s := range sides {
		if _, _, _, err := h.attestAs(s.Title, s.Lang, s.Text); err != nil {
			t.Fatal(err)
		}
	}
	rec := h.record(pageURL)
	if len(rec.Groups) != 2 || len(rec.Groups[0].Members) != 2 || len(rec.Groups[1].Members) != 1 || rec.State != StateAwaiting {
		t.Fatalf("setup: %+v", rec.Groups)
	}
	for i, g := range rec.Groups {
		h.wantParts(i, map[int]string{0: c.Anchor.Text, 1: c.Attestation.Text}[i])
		for _, m := range g.Members {
			text := c.Anchor.Text
			title := c.Anchor.Title
			if i == 1 {
				text, title = c.Attestation.Text, c.Attestation.Title
			}
			if m.TitleSha != titleSha(title) || m.Lang != "en" || m.Len != len(text) {
				t.Errorf("group %d member: title_sha %q lang %q len %d", i, m.TitleSha, m.Lang, m.Len)
			}
		}
	}
	raw := h.rawRecord()
	if strings.Contains(raw, `"parts"`) {
		t.Error("the record names its parts: they are in W p")
	}
	if n := strings.Count(raw, `"title_sha"`); n != 3 {
		t.Errorf("%d title_sha in the record, want one for each of the 3 attestations", n)
	}
}

func TestAPushedOrFrontierRecordStoresNoPartsTitleOrLength(t *testing.T) {
	c := loadAgreed(t).byID(t, "different-title")
	for _, class := range []Class{ClassPushed, ClassFrontier} {
		t.Run(string(class), func(t *testing.T) {
			h := newHarness(t)
			h.add(class, pageURL)
			for _, s := range []agreedSide{c.Anchor, c.Attestation} {
				res, _, _, err := h.attestAs(s.Title, s.Lang, s.Text)
				if err != nil || res.Verdict != "awaiting_quorum" {
					t.Fatalf("%+v %v", res, err)
				}
			}
			rec := h.record(pageURL)
			if len(rec.Groups) != 1 || len(rec.Groups[0].Members) != 2 {
				t.Fatalf("titles do not split a %s group: %+v", class, rec.Groups)
			}
			raw := h.rawRecord()
			for _, key := range []string{`"parts"`, `"title_sha"`, `"lang"`, `"len"`} {
				if strings.Contains(raw, key) {
					t.Errorf("a %s record stores %s", class, key)
				}
			}
			h.nothingIndexed()
		})
	}
}

func TestAnAnchorKeepsAtMost2048PartHashesAndHowManyPartsItHad(t *testing.T) {
	c := loadAgreed(t).Cap
	for _, tc := range []struct {
		name       string
		completing string
		agreed     int
	}{
		{"a copy of the text", capText(c.Parts, 0), c.AgreedBytes},
		{"parts beyond the cap that repeat earlier parts", capText(c.Parts, c.Considered), c.TailCopies.AgreedBytes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.erroredPage()
			if res, _, _, err := h.attestAs("Parts", "en", capText(c.Parts, 0)); err != nil || res.Verdict != "awaiting_quorum" {
				t.Fatalf("%+v %v", res, err)
			}
			if n := len(h.storedParts(0)); n != 8*c.Considered {
				t.Fatalf("%d bytes of part hashes stored, want %d", n, 8*c.Considered)
			}
			if n := h.record(pageURL).Groups[0].PartsN; n != c.Parts {
				t.Fatalf("parts_n %d, want the %d parts the text had before the cap", n, c.Parts)
			}
			res, _, _, err := h.attestAs("Parts", "en", tc.completing)
			if err != nil || res.Verdict != "verified" {
				t.Fatalf("%+v %v", res, err)
			}
			doc := h.indexedOnce()
			if len(doc.Text) != tc.agreed || !strings.HasSuffix(doc.Text, " "+gap) || strings.Count(doc.Text, gap) != 1 {
				t.Fatalf("agreed text of %d bytes, want %d ending in one gap", len(doc.Text), tc.agreed)
			}
		})
	}
}

func TestTheAnchorsPartsSurviveARestart(t *testing.T) {
	c := loadAgreed(t).byID(t, "injected-paragraph")
	h := newHarness(t)
	h.erroredPage()
	if res, _, _, err := h.attestAs(c.Anchor.Title, c.Anchor.Lang, c.Anchor.Text); err != nil || res.Verdict != "awaiting_quorum" {
		t.Fatalf("%+v %v", res, err)
	}
	h.db.Close()
	h.open()
	h.f.errored = []FrontierRow{{URL: pageURL, LastError: "http 403"}}
	h.s.Feed(context.Background())
	res, _, _, err := h.attestAs(c.Attestation.Title, c.Attestation.Lang, c.Attestation.Text)
	if err != nil || res.Verdict != "verified" {
		t.Fatalf("%+v %v", res, err)
	}
	if doc := h.indexedOnce(); doc.Text != c.Expect.AgreedText {
		t.Fatalf("after a restart the agreed text is %.100q", doc.Text)
	}
}

func TestTheArtifactOfAQuorumOnAnErroredPageIsKeptEvenWhenItsChunkIsInADroppedPart(t *testing.T) {
	c := loadAgreed(t).byID(t, "injected-paragraph")
	at := strings.Index(c.Attestation.Text, "unsafe")
	chunk := c.Attestation.Text[at-20 : at+20]
	h := newHarness(t)
	h.erroredPage()
	if res, _, _, err := h.attestAs(c.Anchor.Title, c.Anchor.Lang, c.Anchor.Text); err != nil || res.Verdict != "awaiting_quorum" {
		t.Fatalf("%+v %v", res, err)
	}
	w, n := h.nextPair()
	l, _ := h.claimFor(w, n)
	req := requestAs(l, w, n, pageURL, c.Attestation.Title, c.Attestation.Lang, c.Attestation.Text)
	req.Artifact = &Artifact{Model: "m", Chunks: []crawler.IndexedChunk{{Text: chunk, Embedding: []float32{1, 2}}}}
	res, err := h.s.Submit(req)
	if err != nil || res.Verdict != "verified" {
		t.Fatalf("a chunk in text the quorum drops must not fail the submission: %+v %v", res, err)
	}
	doc := h.indexedOnce()
	if doc.Artifact == nil || strings.Contains(doc.Title+"\n\n"+doc.Text, chunk) {
		t.Fatalf("the artifact reaches indexing, which then finds its chunk is not in the agreed text: %+v", doc.Artifact)
	}
}
