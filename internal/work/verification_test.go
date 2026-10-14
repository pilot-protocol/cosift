package work

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

func TestALapsedLeaseReturnsToThePoolAndCountsOnePair(t *testing.T) {
	h := newHarness(t)
	h.push(pageURL)
	if _, ok := h.claimFor(hexID(1), hexID(101)); !ok {
		t.Fatal("lease")
	}
	h.advance(10*time.Minute + time.Second)
	h.sweep()
	h.s.mu.Lock()
	e := h.s.pool[pageURL]
	attempts, leases := len(e.failed), len(h.s.leases)
	h.s.mu.Unlock()
	if attempts != 1 || leases != 0 {
		t.Fatalf("attempts %d leases %d", attempts, leases)
	}
	if _, ok := h.claimFor(hexID(1), hexID(101)); ok {
		t.Fatal("a pair that let a lease lapse was leased the URL again")
	}
	if _, ok := h.claimFor(hexID(2), hexID(102)); !ok {
		t.Fatal("another pair should get the URL")
	}
}

func TestSixDistinctPairsParkAnEntry(t *testing.T) {
	h := newHarness(t)
	h.push(pageURL)
	for i := 1; i <= 6; i++ {
		if _, ok := h.claimFor(hexID(i), hexID(100+i)); !ok {
			t.Fatalf("pair %d got no lease", i)
		}
		h.advance(10*time.Minute + time.Second)
		h.sweep()
	}
	if _, ok := h.claimFor(hexID(7), hexID(107)); ok {
		t.Fatal("a parked entry was leased")
	}
	res := h.s.consider(context.Background(), pageURL, ClassPushed)
	if res != fedDuplicate {
		t.Fatalf("a parked URL must not be fed again: %v", res)
	}
	h.advance(168*time.Hour + time.Minute)
	h.sweep()
	if res := h.s.consider(context.Background(), pageURL, ClassPushed); res != fedAdded {
		t.Fatalf("the park ends with the attestation TTL: %v", res)
	}
}

// serverCopy is a third variant of the fixture page, within the bound of both attesters' texts.
func serverCopy(t *testing.T, base string) string {
	t.Helper()
	return strings.Replace(base, "Tidal notes", "Tidal notes today", 1)
}

func TestWorkVerificationVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/work_verification.json")
	if err != nil {
		t.Fatal(err)
	}
	var vec struct {
		Cases []struct {
			ID     string   `json:"id"`
			Class  Class    `json:"class"`
			Events []string `json:"events"`
			Expect struct {
				State       string  `json:"state"`
				VerifiedBy  *string `json:"verified_by"`
				IndexedText *string `json:"indexed_text"`
				FrontierRow *string `json:"frontier_row"`
				RefetchDue  bool    `json:"refetch_due"`
			} `json:"expect"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &vec); err != nil {
		t.Fatal(err)
	}
	if len(vec.Cases) < 12 {
		t.Fatalf("vectors truncated: %d", len(vec.Cases))
	}
	for _, c := range vec.Cases {
		t.Run(c.ID, func(t *testing.T) {
			h := newHarness(t)
			first, second, _ := fixture(t, "rotated-ad-and-timestamp")
			server := serverCopy(t, first)
			sa, _ := crawler.Simhash(first)
			sb, _ := crawler.Simhash(second)
			ss, _ := crawler.Simhash(server)
			if crawler.SimhashDistance(sa, ss) > h.cfg.SimhashMax || crawler.SimhashDistance(sa, sb) > h.cfg.SimhashMax {
				t.Fatal("the fixture texts are not within the bound")
			}
			h.add(c.Class, pageURL)
			switch c.Class {
			case ClassFrontier:
				h.f.rows[pageURL] = "queued"
			case ClassErrored:
				h.f.rows[pageURL] = "errored"
			}
			attests := 0
			for _, ev := range c.Events {
				switch ev {
				case "attest":
					text := first
					if attests++; attests == 2 {
						text = second
					}
					if _, _, _, err := h.attest(text); err != nil {
						t.Fatal(err)
					}
				case "window_end":
					h.advance(31 * time.Minute)
				case "refetch_ok":
					h.page(pageURL, server)
					h.sweep()
				case "refetch_fail":
					h.failFetch(pageURL)
					h.sweep()
				default:
					t.Fatalf("unknown event %q", ev)
				}
			}
			h.s.runModerations(context.Background())
			rec := h.record(pageURL)
			state := map[State]string{StateAwaiting: "awaiting", StateDone: "verified"}[rec.State]
			if state != c.Expect.State {
				t.Errorf("state %s (%s), want %s", state, rec.State, c.Expect.State)
			}
			if got := orNull(&rec.VerifiedBy); got != orNull(c.Expect.VerifiedBy) {
				t.Errorf("verified_by %v, want %v", got, orNull(c.Expect.VerifiedBy))
			}
			var indexed *string
			if len(h.f.indexed) > 1 {
				t.Fatalf("indexed %d times", len(h.f.indexed))
			}
			if len(h.f.indexed) == 1 {
				d := h.f.indexed[0]
				by := map[string]string{first: "first", second: "quorum", server: "server"}[d.Text]
				if by == "first" || by != d.VerifiedBy {
					t.Errorf("indexed text is %q but verified by %q", by, d.VerifiedBy)
				}
				indexed = &by
			}
			if got := orNull(indexed); got != orNull(c.Expect.IndexedText) {
				t.Errorf("indexed_text %v, want %v", got, orNull(c.Expect.IndexedText))
			}
			row := h.f.rows[pageURL]
			if got := orNull(&row); got != orNull(c.Expect.FrontierRow) {
				t.Errorf("frontier_row %v, want %v", got, orNull(c.Expect.FrontierRow))
			}
			if due := rec.State == StateAwaiting && !rec.RefetchAt.After(h.now()); due != c.Expect.RefetchDue {
				t.Errorf("refetch_due %v, want %v (refetch_at %v, now %v)", due, c.Expect.RefetchDue, rec.RefetchAt, h.now())
			}
		})
	}
}

func orNull(s *string) any {
	if s == nil || *s == "" {
		return nil
	}
	return *s
}
