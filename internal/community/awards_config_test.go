package community

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/community/wiki"
)

type awardsProvider struct {
	*fakeShared
	*fakeAwards
}

func openWithAwards(t *testing.T, mutate func(*Config)) (*Server, error) {
	t.Helper()
	src := newFakeAwards()
	cfg := Config{DataDir: t.TempDir(), Backend: "http://127.0.0.1:9", PublicURL: "http://localhost:7780", Awards: true, Shared: awardsProvider{&fakeShared{}, src}, Articles: &fakeArticles{}}
	if mutate != nil {
		mutate(&cfg)
	}
	s, err := Open(cfg)
	if err == nil {
		t.Cleanup(func() { s.Close() })
	}
	return s, err
}

func TestAwardsNeedSharedAccountsAndTheArticleCrossCheck(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*Config)
		want   string
	}{
		"local mode":      {func(c *Config) { c.Shared = nil }, "COSIFT_AUTH_MODE=shared"},
		"no award source": {func(c *Config) { c.Shared = &fakeShared{} }, "COSIFT_AUTH_MODE=shared"},
		"no wiki key":     {func(c *Config) { c.Articles = nil }, "COSIFT_WIKI_ENGINE_KEY"},
		"bad wiki url": {func(c *Config) {
			c.Articles = nil
			c.Wiki = wiki.Config{EngineKey: wikiTestKey, EngineURL: "https://example.com"}
		}, "COSIFT_WIKI_ENGINE_URL"},
		"shared key": {func(c *Config) {
			c.Articles = nil
			c.BrokerKey = wikiTestKey
			c.Wiki = wiki.Config{EngineKey: wikiTestKey}
		}, "different keys"},
	} {
		if _, err := openWithAwards(t, tc.mutate); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if s, err := openWithAwards(t, func(c *Config) { c.Articles = nil; c.Wiki = wiki.Config{EngineKey: wikiTestKey} }); err != nil || s.awards == nil {
		t.Fatalf("a wiki key enables the default reader: %v", err)
	}
	if s, err := openWithAwards(t, func(c *Config) { c.Awards, c.Shared = false, nil }); err != nil || s.awards != nil {
		t.Fatalf("awards off: %v", err)
	}
}

func TestRunPollsAwardsOnItsInterval(t *testing.T) {
	old := awardPollEvery
	awardPollEvery = 20 * time.Millisecond
	t.Cleanup(func() { awardPollEvery = old })
	logs := captureLog(t)
	src := newFakeAwards()
	arts := &fakeArticles{rec: map[string]ArticleRecord{}, err: map[string]error{}}
	s, err := openWithAwards(t, func(c *Config) { c.Shared = awardsProvider{&fakeShared{}, src}; c.Articles = arts })
	if err != nil {
		t.Fatal(err)
	}
	r := &awardRig{logs: logs, s: s, src: src, art: arts, now: time.Now()}
	s.awards.src = src
	a := r.articleAward(1, 1, 1000)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for r.src.result(a.ID) == nil && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
	if res := r.src.result(a.ID); res == nil || res.Status != "applied" {
		t.Fatalf("the loop never applied the award: %+v", res)
	}
}
