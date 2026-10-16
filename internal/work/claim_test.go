package work

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
)

func claimN(t *testing.T, h *harness, w, n string, max int) ClaimResult {
	t.Helper()
	res, err := h.s.Claim(w, n, max)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func wantInvalid(t *testing.T, err error, field, rule string) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Status != 422 || e.Code != "invalid_field" || e.Field != field || e.Rule != rule {
		t.Fatalf("want invalid_field %s %s, got %v", field, rule, err)
	}
}

func TestClaimValidatesItsFields(t *testing.T) {
	h := newHarness(t)
	_, err := h.s.Claim("nothex", hexID(2), 1)
	wantInvalid(t, err, "worker", "pattern")
	_, err = h.s.Claim(hexID(1), "ABCDEF0123456789ABCDEF0123456789", 1)
	wantInvalid(t, err, "net", "pattern")
	for _, max := range []int{0, 33, -1} {
		_, err = h.s.Claim(hexID(1), hexID(2), max)
		wantInvalid(t, err, "max", "range")
	}
}

func TestClaimDrainsPushedThenErroredThenFrontier(t *testing.T) {
	h := newHarness(t)
	h.add(ClassFrontier, "https://c.example.org/f")
	h.add(ClassErrored, "https://b.example.org/e")
	h.push("https://a.example.org/p")
	res := claimN(t, h, hexID(1), hexID(2), 3)
	var got []Class
	for _, l := range res.Leases {
		got = append(got, l.Source)
		if l.ExtractVersion != 1 || len(l.ID) != 32 || !l.ExpiresAt.Equal(h.now().Add(10*time.Minute)) {
			t.Errorf("lease %+v", l)
		}
	}
	if fmt.Sprint(got) != "[pushed errored frontier]" {
		t.Fatalf("classes %v", got)
	}
}

func TestClaimSpreadsOverHostsRoundRobin(t *testing.T) {
	h := newHarness(t)
	h.push("https://a.example.org/1", "https://a.example.org/2", "https://a.example.org/3", "https://b.example.org/1", "https://b.example.org/2", "https://c.example.org/1")
	res := claimN(t, h, hexID(1), hexID(2), 6)
	var hosts []string
	for _, l := range res.Leases {
		hosts = append(hosts, hostOf(l.URL))
	}
	if fmt.Sprint(hosts) != "[a.example.org b.example.org c.example.org a.example.org b.example.org a.example.org]" {
		t.Fatalf("hosts %v", hosts)
	}
}

func TestClaimSpacesLeasesPerHost(t *testing.T) {
	h := newHarness(t, func(c *config.WorkSettings) { c.HostSpacingMS = 2000 })
	h.push("https://a.example.org/1", "https://a.example.org/2", "https://b.example.org/1")
	res := claimN(t, h, hexID(1), hexID(2), 3)
	if len(res.Leases) != 2 || hostOf(res.Leases[0].URL) == hostOf(res.Leases[1].URL) {
		t.Fatalf("one lease per host per spacing: %+v", res.Leases)
	}
	if again := claimN(t, h, hexID(3), hexID(4), 3); len(again.Leases) != 0 || again.RetryAfterS != 30 {
		t.Fatalf("within the spacing: %+v", again)
	}
	h.advance(2 * time.Second)
	if again := claimN(t, h, hexID(3), hexID(4), 3); len(again.Leases) != 2 {
		t.Fatalf("after the spacing: %+v", again)
	}
}

func TestClaimRespectsTheWorkerAndTotalCaps(t *testing.T) {
	h := newHarness(t, func(c *config.WorkSettings) { c.MaxLeasesPerWorker, c.MaxLeasesTotal = 2, 3 })
	for i := 0; i < 6; i++ {
		h.push(fmt.Sprintf("https://h%d.example.org/p", i))
	}
	if res := claimN(t, h, hexID(1), hexID(11), 5); len(res.Leases) != 2 {
		t.Fatalf("a worker's cap clamps max, it does not refuse: %d", len(res.Leases))
	}
	if res := claimN(t, h, hexID(1), hexID(11), 1); len(res.Leases) != 0 || res.RetryAfterS != 30 {
		t.Fatalf("at the cap: %+v", res)
	}
	if res := claimN(t, h, hexID(2), hexID(12), 5); len(res.Leases) != 1 {
		t.Fatalf("the total cap: %d", len(res.Leases))
	}
	if res := claimN(t, h, hexID(3), hexID(13), 5); len(res.Leases) != 0 {
		t.Fatalf("beyond the total cap: %d", len(res.Leases))
	}
}

func TestOneURLLeasesToDistinctWorkersAndNetworks(t *testing.T) {
	h := newHarness(t)
	h.push(pageURL)
	if _, ok := h.claimFor(hexID(1), hexID(101)); !ok {
		t.Fatal("first lease")
	}
	if _, ok := h.claimFor(hexID(1), hexID(102)); ok {
		t.Fatal("the same worker on another network was leased the URL again")
	}
	if _, ok := h.claimFor(hexID(2), hexID(101)); ok {
		t.Fatal("the same network under another worker was leased the URL again")
	}
	if _, ok := h.claimFor(hexID(2), hexID(102)); !ok {
		t.Fatal("second lease")
	}
	if _, ok := h.claimFor(hexID(3), hexID(103)); !ok {
		t.Fatal("third lease")
	}
	if _, ok := h.claimFor(hexID(4), hexID(104)); ok {
		t.Fatal("a fourth concurrent lease on one URL")
	}
}

func TestClaimStopsAtTheModerationCeiling(t *testing.T) {
	h := newHarness(t, func(c *config.WorkSettings) { c.WorkModerationsPerDay = 0 })
	h.push(pageURL)
	res := claimN(t, h, hexID(1), hexID(2), 1)
	want := int(nextMidnight(h.now()).Sub(h.now()).Seconds())
	if len(res.Leases) != 0 || !res.ModerationCeiling || res.RetryAfterS != min(want, 3600) {
		t.Fatalf("%+v", res)
	}
}
