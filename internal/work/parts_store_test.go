package work

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
)

var wp = []byte("Wp")

// packedOf is the W p value of a text: the first 8 bytes of SHA-256 over each part's words, in order.
func packedOf(text string) []byte {
	p := splitParts(text)
	var out []byte
	start := 0
	for _, end := range p.ends {
		sum := sha256.Sum256([]byte(strings.Join(p.words[start:end], " ")))
		out = append(out, sum[:8]...)
		start = end
	}
	return out
}

// storedParts is the raw W p value of group gi of the page, nil when there is none.
func (h *harness) storedParts(gi int) []byte {
	h.t.Helper()
	v, err := h.s.get([]byte(fmt.Sprintf("Wp%s%02x", urlSHA(pageURL), gi)))
	if err != nil {
		h.t.Fatal(err)
	}
	return v
}

func (h *harness) wantParts(gi int, text string) {
	h.t.Helper()
	if got := h.storedParts(gi); !bytes.Equal(got, packedOf(text)) || len(got) == 0 {
		h.t.Errorf("W p of group %d holds %d bytes, want the %d packed bytes of the anchor's parts", gi, len(got), len(packedOf(text)))
	}
}

func (h *harness) noParts() {
	h.t.Helper()
	if n := countPrefix(h, wp); n != 0 {
		h.t.Fatalf("%d W p left", n)
	}
}

// twoGroups are two sides that differ in title and in parts, so each starts a group of its own.
func twoGroups(t *testing.T) (agreedSide, agreedSide) {
	t.Helper()
	c := loadAgreed(t).byID(t, "injected-paragraph")
	a, b := c.Anchor, c.Attestation
	b.Title = "Another title"
	if bytes.Equal(packedOf(a.Text), packedOf(b.Text)) || titleSha(a.Title) == titleSha(b.Title) {
		t.Fatal("the sides must differ in title and in parts")
	}
	return a, b
}

func TestAnAnchorsPartsLiveInWPAndTheRecordStaysSmall(t *testing.T) {
	c := loadAgreed(t).Cap
	text := capText(c.Parts, 0)
	h := newHarness(t)
	h.erroredPage()
	if res, _, _, err := h.attestAs("Parts", "en", text); err != nil || res.Verdict != "awaiting_quorum" {
		t.Fatalf("%+v %v", res, err)
	}
	h.wantParts(0, text)
	if got := len(h.storedParts(0)); got != 8*c.Considered {
		t.Errorf("W p holds %d bytes, want 8 for each of the %d parts", got, c.Considered)
	}
	raw := h.rawRecord()
	if len(raw) > 2048 || strings.Contains(raw, `"parts"`) {
		t.Errorf("the W a record is %d bytes and names its parts: an anchor's hashes belong in W p", len(raw))
	}
}

func TestOnlyAnAnchorHasAWPAndAMemberAddsNone(t *testing.T) {
	a, b := twoGroups(t)
	h := newHarness(t, func(c *config.WorkSettings) { c.ModerationQueueMax = 1 })
	h.s.texts["taken"] = struct{}{}
	h.erroredPage()
	for _, s := range []agreedSide{a, a, b, a} {
		if _, _, _, err := h.attestAs(s.Title, s.Lang, s.Text); err != nil {
			t.Fatal(err)
		}
	}
	if rec := h.record(pageURL); rec.State != StateAwaiting || len(rec.Groups) != 2 || len(rec.Groups[0].Members) != 3 {
		t.Fatalf("setup: %+v", rec.Groups)
	}
	h.wantParts(0, a.Text)
	h.wantParts(1, b.Text)
	if n := countPrefix(h, wp); n != 2 || h.storedParts(2) != nil {
		t.Errorf("%d W p, want one for each of the 2 anchors", n)
	}
}

func TestAMemberWhoseTextDiffersLeavesTheAnchorsWP(t *testing.T) {
	c := loadAgreed(t).byID(t, "injected-paragraph")
	h := newHarness(t, func(c *config.WorkSettings) { c.ModerationQueueMax = 1 })
	h.s.texts["taken"] = struct{}{}
	h.erroredPage()
	for _, s := range []agreedSide{c.Anchor, c.Attestation} {
		if _, _, _, err := h.attestAs(s.Title, s.Lang, s.Text); err != nil {
			t.Fatal(err)
		}
	}
	if rec := h.record(pageURL); rec.State != StateAwaiting || len(rec.Groups) != 1 || len(rec.Groups[0].Members) != 2 {
		t.Fatalf("setup: %+v", rec)
	}
	h.wantParts(0, c.Anchor.Text)
	if n := countPrefix(h, wp); n != 1 {
		t.Errorf("%d W p, want the anchor's alone", n)
	}
}

func TestALateAgreeingAttestationLeavesTheVerifiedGroupsWP(t *testing.T) {
	c := loadAgreed(t).byID(t, "injected-paragraph")
	h := newHarness(t)
	h.erroredPage()
	var ws, ns [3]string
	var ls [3]LeaseGrant
	for i := range ls {
		ws[i], ns[i] = h.nextPair()
		ls[i], _ = h.claimFor(ws[i], ns[i])
	}
	for i, s := range []agreedSide{c.Anchor, c.Attestation, c.Attestation} {
		if _, err := h.s.Submit(requestAs(ls[i], ws[i], ns[i], pageURL, s.Title, s.Lang, s.Text)); err != nil {
			t.Fatal(err)
		}
	}
	if rec := h.record(pageURL); rec.State == StateAwaiting || len(rec.Groups) != 1 || len(rec.Groups[0].Members) != 3 {
		t.Fatalf("setup: %+v", rec)
	}
	h.wantParts(0, c.Anchor.Text)
	if n := countPrefix(h, wp); n != 1 {
		t.Errorf("%d W p, want the anchor's alone", n)
	}
}

func TestAPushedOrFrontierRecordHasNoWP(t *testing.T) {
	c := loadAgreed(t).byID(t, "different-title")
	for _, class := range []Class{ClassPushed, ClassFrontier} {
		h := newHarness(t)
		h.add(class, pageURL)
		for _, s := range []agreedSide{c.Anchor, c.Attestation} {
			if _, _, _, err := h.attestAs(s.Title, s.Lang, s.Text); err != nil {
				t.Fatal(err)
			}
		}
		h.noParts()
	}
}

func TestALateAttestationThatDissentsStartsAGroupWithItsOwnWP(t *testing.T) {
	a, b := twoGroups(t)
	h := newHarness(t)
	h.erroredPage()
	var ws, ns [3]string
	var ls [3]LeaseGrant
	for i := range ls {
		ws[i], ns[i] = h.nextPair()
		ls[i], _ = h.claimFor(ws[i], ns[i])
	}
	for i, s := range []agreedSide{a, a, b} {
		if _, err := h.s.Submit(requestAs(ls[i], ws[i], ns[i], pageURL, s.Title, s.Lang, s.Text)); err != nil {
			t.Fatal(err)
		}
	}
	if rec := h.record(pageURL); rec.State == StateAwaiting || len(rec.Groups) != 2 {
		t.Fatalf("setup: %+v", rec)
	}
	h.wantParts(0, a.Text)
	h.wantParts(1, b.Text)
}
