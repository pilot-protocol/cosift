package work

import (
	"context"
	"fmt"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
)

func TestThePoolIsBounded(t *testing.T) {
	h := newHarness(t, func(c *config.WorkSettings) { c.PoolMax = 2 })
	res, err := h.s.Push(context.Background(), []string{"https://a.example.org/1", "https://a.example.org/2", "https://a.example.org/3", "https://a.example.org/1"})
	if err != nil || res.Accepted != 2 || res.Duplicates != 1 || fmt.Sprint(res.Rejected) != "[{2 pool_full}]" {
		t.Fatalf("%+v %v", res, err)
	}
}

func TestPushRules(t *testing.T) {
	h := newHarness(t)
	h.f.blockedFor["blocked.example.org"] = true
	h.f.corpus["https://stored.example.org/doc"] = true
	res, err := h.s.Push(context.Background(), []string{
		"https://ok.example.org/1", "https://ok.example.org/1", "http://127.0.0.1/x", "ftp://ok.example.org/x", "https://blocked.example.org/y", "https://stored.example.org/doc",
	})
	if err != nil || res.Accepted != 1 || res.Duplicates != 2 || fmt.Sprint(res.Rejected) != "[{2 url} {3 url} {4 policy}]" {
		t.Fatalf("%+v %v", res, err)
	}
	_, err = h.s.Push(context.Background(), nil)
	wantInvalid(t, err, "urls", "required")
	_, err = h.s.Push(context.Background(), make([]string, 101))
	wantInvalid(t, err, "urls", "too_many")
}

func TestOnlyBlockingErrorsAreFed(t *testing.T) {
	h := newHarness(t)
	h.f.errored = []FrontierRow{
		{URL: "https://a.example.org/403", LastError: "http 403"},
		{URL: "https://b.example.org/429", LastError: "http 429 (rate limited)"},
		{URL: "https://c.example.org/timeout", LastError: `Get "x": context deadline exceeded (Client.Timeout exceeded while awaiting headers)`},
		{URL: "https://d.example.org/challenge", LastError: "cloudflare challenge page"},
		{URL: "https://e.example.org/404", LastError: "http 404"},
		{URL: "https://f.example.org/410", LastError: "http 410"},
		{URL: "https://g.example.org/dns", LastError: "dial tcp: lookup g.example.org: no such host"},
		{URL: "https://h.example.org/parse", LastError: "empty content"},
		{URL: "https://i.example.org/4030", LastError: "http 4030"},
	}
	h.s.Feed(context.Background())
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	for u := range h.s.pool {
		t.Log("fed", u)
	}
	if len(h.s.pool) != 4 {
		t.Fatalf("fed %d URLs, want the four blocking ones", len(h.s.pool))
	}
}

func TestTheURLInAnErrorIsNotClassified(t *testing.T) {
	for _, c := range []struct {
		msg  string
		want bool
	}{
		{`Get "https://no-such-host.invalid/how-to-fix-a-timeout": dial tcp: lookup no-such-host.invalid: no such host`, false},
		{`Get "https://www.hackerrank.com/challenges/solve": dial tcp 192.0.2.1:443: connect: connection refused`, false},
		{`Get "https://example.org/captcha-solver": dial tcp: lookup example.org: no such host`, false},
		{`Get "https://captcha.example.org/": dial tcp: lookup captcha.example.org: no such host`, false},
		{`Get "https://example.org/challenge/1": context deadline exceeded (Client.Timeout exceeded while awaiting headers)`, true},
		{`Get "https://example.org/a": net/http: request canceled (Client.Timeout exceeded while awaiting headers)`, true},
	} {
		if got := BlockingError(c.msg); got != c.want {
			t.Errorf("%q: %v, want %v", c.msg, got, c.want)
		}
	}
}

func TestTheErroredPassResumesAndWraps(t *testing.T) {
	h := newHarness(t, func(c *config.WorkSettings) { c.ErroredScanBatch = 2 })
	for _, u := range []string{"https://a.example.org/1", "https://b.example.org/2", "https://c.example.org/3"} {
		h.f.errored = append(h.f.errored, FrontierRow{URL: u, LastError: "http 403"})
	}
	h.s.Feed(context.Background())
	if h.s.errCursor != "https://b.example.org/2" || len(h.s.pool) != 2 {
		t.Fatalf("cursor %q pool %d", h.s.errCursor, len(h.s.pool))
	}
	if v, _ := h.s.get(keyPoolCur); string(v) != "https://b.example.org/2" {
		t.Fatalf("the cursor is not stored: %q", v)
	}
	h.s.Feed(context.Background())
	if h.s.errCursor != "" || len(h.s.pool) != 3 {
		t.Fatalf("a pass that reaches the end wraps: cursor %q pool %d", h.s.errCursor, len(h.s.pool))
	}
}

func TestTheFrontierFeederLeavesHalfThePoolToThePushedAndErroredPages(t *testing.T) {
	h := newHarness(t, func(c *config.WorkSettings) { c.PoolMax, c.ErroredScanBatch = 10, 4 })
	for i := 0; i < 20; i++ {
		h.f.queued = append(h.f.queued, FrontierRow{URL: fmt.Sprintf("https://q%02d.example.org/p", i)})
	}
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		h.s.Feed(ctx)
	}
	if len(h.s.pool) != 5 {
		t.Fatalf("the frontier holds %d of 10 slots", len(h.s.pool))
	}
	res, err := h.s.Push(ctx, []string{"https://p1.example.org/a", "https://p2.example.org/b", "https://p3.example.org/c"})
	if err != nil || res.Accepted != 3 || len(res.Rejected) != 0 {
		t.Fatalf("a push found the pool full: %+v %v", res, err)
	}
	h.s.mu.Lock()
	h.s.poolDrop("https://q00.example.org/p")
	h.s.poolDrop("https://q01.example.org/p")
	h.s.mu.Unlock()
	h.s.Feed(ctx)
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	for _, u := range []string{"https://q05.example.org/p", "https://q06.example.org/p"} {
		if h.s.pool[u] == nil {
			t.Errorf("%s was skipped when the cap held the pass back", u)
		}
	}
	if len(h.s.pool) != 8 {
		t.Fatalf("pool %d", len(h.s.pool))
	}
}

func TestTheFrontierFeederHonoursItsSwitch(t *testing.T) {
	h := newHarness(t)
	h.f.queued = []FrontierRow{{URL: "https://c.example.org/f"}}
	h.s.frontier = false
	h.s.Feed(context.Background())
	if len(h.s.pool) != 0 {
		t.Fatal("the frontier feeder ran while switched off")
	}
	h.s.frontier = true
	h.s.Feed(context.Background())
	if len(h.s.pool) != 1 {
		t.Fatal("the frontier feeder did not run")
	}
}
