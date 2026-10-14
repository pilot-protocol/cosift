package work

import (
	"context"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
)

func TestWPGoesWithItsRecordAtTheTTL(t *testing.T) {
	c := loadAgreed(t).byID(t, "injected-paragraph")
	a, b := twoGroups(t)
	t.Run("an awaiting record of two groups that expires", func(t *testing.T) {
		h := newHarness(t)
		h.erroredPage()
		for _, s := range []agreedSide{a, b} {
			if _, _, _, err := h.attestAs(s.Title, s.Lang, s.Text); err != nil {
				t.Fatal(err)
			}
		}
		h.wantParts(0, a.Text)
		h.wantParts(1, b.Text)
		h.advance(168*time.Hour + time.Minute)
		h.sweep()
		if h.record(pageURL).State != StateExpired {
			t.Fatalf("setup: %+v", h.record(pageURL))
		}
		h.noParts()
	})
	t.Run("a verified group keeps its W p until the record is deleted", func(t *testing.T) {
		h := newHarness(t)
		h.erroredPage()
		if res := h.submitPair(c.Anchor, c.Attestation); res.Verdict != "verified" {
			t.Fatalf("%+v", res)
		}
		h.indexedOnce()
		h.sweep()
		h.wantParts(0, c.Anchor.Text)
		h.advance(168*time.Hour + time.Minute)
		h.sweep()
		if h.record(pageURL) != nil {
			t.Fatal("setup: the finished record stays past the TTL")
		}
		h.noParts()
	})
}

func TestWPGoesWithItsRecordOnPurge(t *testing.T) {
	a, b := twoGroups(t)
	h := newHarness(t)
	h.erroredPage()
	for _, s := range []agreedSide{a, b} {
		if _, _, _, err := h.attestAs(s.Title, s.Lang, s.Text); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.s.Purge(context.Background(), pageURL, false); err != nil {
		t.Fatal(err)
	}
	h.wantParts(0, a.Text)
	h.wantParts(1, b.Text)
	if _, err := h.s.Purge(context.Background(), pageURL, true); err != nil {
		t.Fatal(err)
	}
	h.noParts()
}

func TestVoidTakesTheWPOfADiscardedGroupAndKeepsTheOthersUnderTheirNewIndex(t *testing.T) {
	a, b := twoGroups(t)
	setup := func(t *testing.T) (*harness, [4]string) {
		h := newHarness(t, func(c *config.WorkSettings) { c.ModerationQueueMax = 1 })
		h.s.texts["taken"] = struct{}{}
		h.erroredPage()
		var ws [4]string
		for i, s := range []agreedSide{a, b, a, b} {
			_, ws[i], _, _ = h.attestAs(s.Title, s.Lang, s.Text)
		}
		if rec := h.record(pageURL); len(rec.Groups) != 2 || countPrefix(h, wp) != 2 {
			t.Fatalf("setup: %+v", rec.Groups)
		}
		return h, ws
	}
	void := func(t *testing.T, h *harness, worker string, apply bool) {
		t.Helper()
		if _, err := h.s.Void(context.Background(), worker, apply); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("the first group's anchor", func(t *testing.T) {
		h, ws := setup(t)
		void(t, h, ws[0], false)
		h.wantParts(0, a.Text)
		h.wantParts(1, b.Text)
		void(t, h, ws[0], true)
		h.wantParts(0, b.Text)
		if n := countPrefix(h, wp); n != 1 {
			t.Errorf("%d W p, want the surviving group's alone", n)
		}
	})
	t.Run("the second group's anchor", func(t *testing.T) {
		h, ws := setup(t)
		void(t, h, ws[1], true)
		h.wantParts(0, a.Text)
		if n := countPrefix(h, wp); n != 1 {
			t.Errorf("%d W p, want the surviving group's alone", n)
		}
	})
	t.Run("a member behind an anchor", func(t *testing.T) {
		h, ws := setup(t)
		void(t, h, ws[3], true)
		h.wantParts(0, a.Text)
		h.wantParts(1, b.Text)
	})
	t.Run("the only attestation, so the record goes", func(t *testing.T) {
		h := newHarness(t)
		h.erroredPage()
		_, w, _, _ := h.attestAs(a.Title, a.Lang, a.Text)
		void(t, h, w, true)
		if h.record(pageURL) != nil {
			t.Fatal("setup: the record stays")
		}
		h.noParts()
	})
}

func TestDeletingAURLsWPLeavesTheWPOfEveryOtherURL(t *testing.T) {
	c := loadAgreed(t).byID(t, "injected-paragraph")
	a, b := twoGroups(t)
	other := urlSHA("https://other.example/page")
	stageOther := func(t *testing.T, h *harness) {
		t.Helper()
		tx := h.s.begin()
		_ = tx.putParts(other, 0, []uint64{1, 2, 3})
		_ = tx.putParts(other, 1, []uint64{4, 5})
		if err := tx.commit(); err != nil {
			t.Fatal(err)
		}
	}
	wantOther := func(t *testing.T, h *harness) {
		t.Helper()
		for gi, bytes := range []int{24, 16} {
			if v, err := h.s.get(keyParts(other, gi)); err != nil || len(v) != bytes {
				t.Errorf("the other URL's W p of group %d holds %d bytes (%v), want %d", gi, len(v), err, bytes)
			}
		}
	}
	twoGroupPage := func(t *testing.T) (*harness, [2]string) {
		h := newHarness(t)
		h.erroredPage()
		var ws [2]string
		for i, s := range []agreedSide{a, b} {
			_, ws[i], _, _ = h.attestAs(s.Title, s.Lang, s.Text)
		}
		stageOther(t, h)
		return h, ws
	}
	t.Run("an awaiting record that expires", func(t *testing.T) {
		h, _ := twoGroupPage(t)
		h.advance(168*time.Hour + time.Minute)
		h.sweep()
		if h.record(pageURL).State != StateExpired || h.storedParts(0) != nil || h.storedParts(1) != nil {
			t.Fatalf("setup: %+v", h.record(pageURL))
		}
		wantOther(t, h)
	})
	t.Run("a finished record deleted after the TTL", func(t *testing.T) {
		h := newHarness(t)
		h.erroredPage()
		if res := h.submitPair(c.Anchor, c.Attestation); res.Verdict != "verified" {
			t.Fatalf("%+v", res)
		}
		h.indexedOnce()
		stageOther(t, h)
		h.advance(168*time.Hour + time.Minute)
		h.sweep()
		if h.record(pageURL) != nil || h.storedParts(0) != nil {
			t.Fatal("setup: the finished record stays past the TTL")
		}
		wantOther(t, h)
	})
	t.Run("a purge", func(t *testing.T) {
		h, _ := twoGroupPage(t)
		if _, err := h.s.Purge(context.Background(), pageURL, true); err != nil {
			t.Fatal(err)
		}
		if h.storedParts(0) != nil || h.storedParts(1) != nil {
			t.Fatal("setup: the purged page keeps a W p")
		}
		wantOther(t, h)
	})
	t.Run("a void that re-keys the groups", func(t *testing.T) {
		h, ws := twoGroupPage(t)
		if _, err := h.s.Void(context.Background(), ws[0], true); err != nil {
			t.Fatal(err)
		}
		h.wantParts(0, b.Text)
		if h.storedParts(1) != nil {
			t.Fatal("setup: the void leaves a second W p on the page")
		}
		wantOther(t, h)
	})
	t.Run("a void that removes the record", func(t *testing.T) {
		h := newHarness(t)
		h.erroredPage()
		_, w, _, _ := h.attestAs(a.Title, a.Lang, a.Text)
		stageOther(t, h)
		if _, err := h.s.Void(context.Background(), w, true); err != nil {
			t.Fatal(err)
		}
		if h.record(pageURL) != nil || h.storedParts(0) != nil {
			t.Fatal("setup: the record stays")
		}
		wantOther(t, h)
	})
}
