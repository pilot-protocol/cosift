package work

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
)

func TestVoidOnAnErroredRecordDiscardsTheGroupItAnchorsAndKeepsTheOthers(t *testing.T) {
	c := loadAgreed(t).byID(t, "different-title")
	setup := func(t *testing.T) (*harness, [4]string, [4]string) {
		h := newHarness(t, func(c *config.WorkSettings) { c.ModerationQueueMax = 1 })
		h.s.texts["taken"] = struct{}{}
		h.erroredPage()
		var ws, ns [4]string
		for i, s := range []agreedSide{c.Anchor, c.Attestation, c.Anchor, c.Attestation} {
			if _, ws[i], ns[i], _ = h.attestAs(s.Title, s.Lang, s.Text); ws[i] == "" {
				t.Fatal("no worker")
			}
		}
		rec := h.record(pageURL)
		if rec.State != StateAwaiting || len(rec.Groups) != 2 || len(rec.Groups[0].Members) != 2 || len(rec.Groups[1].Members) != 2 {
			t.Fatalf("setup: two awaiting groups of two: %+v", rec)
		}
		return h, ws, ns
	}
	members := func(h *harness) string {
		var out []string
		for _, g := range h.record(pageURL).Groups {
			out = append(out, fmt.Sprint(len(g.Members)))
		}
		return strings.Join(out, ",")
	}
	for _, tc := range []struct {
		name      string
		voided    int
		wantSizes string
	}{
		{"the anchor of a group takes the group with it", 0, "2"},
		{"a member behind an anchor leaves the anchor's group", 3, "2,1"},
		{"the anchor of the other group", 1, "2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, ws, ns := setup(t)
			res, err := h.s.Void(context.Background(), ws[tc.voided], true)
			if err != nil || res != (VoidResult{RecordsChanged: 1, AttestationsDropped: 1}) {
				t.Fatalf("%+v %v", res, err)
			}
			if got := members(h); got != tc.wantSizes {
				t.Fatalf("groups of %s, want %s", got, tc.wantSizes)
			}
			for i, g := range h.record(pageURL).Groups {
				if len(h.storedParts(i)) == 0 || g.Members[0].Worker == ws[tc.voided] {
					t.Errorf("a remaining group must keep its anchor and its parts: %+v", g.Anchor)
				}
			}
			if tc.voided == 0 || tc.voided == 1 {
				if _, ok := h.claimFor(ws[tc.voided+2], ns[tc.voided+2]); !ok {
					t.Error("a member of a discarded group may attest again")
				}
			}
		})
	}
	t.Run("two members gone leave no pair and park the URL", func(t *testing.T) {
		h, ws, _ := setup(t)
		for _, i := range []int{2, 3} {
			if _, err := h.s.Void(context.Background(), ws[i], true); err != nil {
				t.Fatal(err)
			}
		}
		if got := members(h); got != "1,1" || !h.s.isParked(urlSHA(pageURL)) {
			t.Fatalf("groups of %s, parked %v", got, h.s.isParked(urlSHA(pageURL)))
		}
	})
}
