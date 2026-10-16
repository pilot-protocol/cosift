package community

import (
	"context"
	"strings"
	"testing"
	"time"
)

type workShared struct {
	*fakeShared
	*fakeWork
}

func openWork(t *testing.T, mutate func(*Config)) (*Server, error) {
	t.Helper()
	cfg := Config{DataDir: t.TempDir(), Backend: "http://127.0.0.1:1", PublicURL: "http://localhost:7780", BrokerKey: testBrokerKey,
		Shared: workShared{&fakeShared{}, newFakeWork()}, Work: true, Environment: "prod"}
	if mutate != nil {
		mutate(&cfg)
	}
	s, err := Open(cfg)
	if err == nil {
		t.Cleanup(func() { s.Close() })
	}
	return s, err
}

func TestOpenRefusesWorkThatCannotRun(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"no shared accounts", func(c *Config) { c.Shared = nil }, "COSIFT_COMMUNITY_WORK requires COSIFT_AUTH_MODE=shared"},
		{"a provider that cannot serve work", func(c *Config) { c.Shared = &fakeShared{} }, "COSIFT_COMMUNITY_WORK requires COSIFT_AUTH_MODE=shared"},
		{"no broker key", func(c *Config) { c.BrokerKey = "" }, "COSIFT_COMMUNITY_WORK requires COSIFT_COMMUNITY_BROKER_KEY"},
		{"no environment", func(c *Config) { c.Environment = "" }, "COSIFT_ENV must be staging or prod"},
		{"another environment", func(c *Config) { c.Environment = "dev" }, "COSIFT_ENV must be staging or prod"},
		{"a cap of zero", func(c *Config) { c.FetchDailyCap = -1 }, "fetch daily cap"},
		{"a huge cap", func(c *Config) { c.FetchDailyCap = 10001 }, "fetch daily cap"},
	} {
		if _, err := openWork(t, tc.mutate); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want %q", tc.name, err, tc.want)
		}
	}
}

func TestOpenAcceptsWorkAndPreparesIt(t *testing.T) {
	for _, env := range []string{"prod", "staging"} {
		s, err := openWork(t, func(c *Config) { c.Environment = env })
		if err != nil {
			t.Fatal(err)
		}
		if s.work.source == nil || s.work.joins == nil || len(s.work.salts.worker) != 32 || s.fetchCap() != 500 {
			t.Fatalf("%s: work state not prepared, cap %d", env, s.fetchCap())
		}
	}
	s, err := openWork(t, func(c *Config) { c.FetchDailyCap = 7 })
	if err != nil || s.fetchCap() != 7 {
		t.Fatalf("cap: %v", err)
	}
	off, err := openWork(t, func(c *Config) { c.Work, c.Shared, c.BrokerKey, c.Environment = false, nil, "", "" })
	if err != nil || off.work.source != nil {
		t.Fatalf("work off: %v", err)
	}
}

func TestRunDrivesTheSettledCounterAlertAndPushLoopsAndFlushesAtShutdown(t *testing.T) {
	for _, d := range []*time.Duration{&settledEvery, &counterEvery, &pushEvery} {
		old := *d
		*d = 20 * time.Millisecond
		t.Cleanup(func() { *d = old })
	}
	r, fd, ws := settledRig(t)
	r.engine.answer(func(call engineRequest) (int, string) {
		if call.path == "/v1/work/push" {
			return 200, `{"accepted":1,"duplicates":0,"rejected":[]}`
		}
		return fd.serve(call)
	})
	fd.add("verified", true, sha(1), ws.a)
	fd.ceiling = true
	r.submission("alice", "https://example.org/blocked", "unverified", failFetch, r.clk.now())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.s.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.f.mu.Lock()
		counted, alerted := r.f.counters["0000000000000001"]["fetch_credits"] == 2, len(r.f.alerts) == 1
		r.f.mu.Unlock()
		if counted && alerted && len(r.engine.on("/v1/work/push")) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if r.fetchRows("acct-0") != 1 {
		t.Error("the settled loop did not pay")
	}
	if r.f.counter("0000000000000001", "fetch_credits") != 2 || len(r.f.alerts) != 1 || len(r.engine.on("/v1/work/push")) == 0 {
		t.Errorf("counters %v, alerts %d, pushes %d", r.f.counters, len(r.f.alerts), len(r.engine.on("/v1/work/push")))
	}
	r.s.bumpFetch(testCID, fetchClaimed, 3)
	cancel()
	<-done
	if r.f.counter(testCID, "fetch_claimed") != 3 {
		t.Fatal("the buffered counters were not flushed at shutdown")
	}
}
