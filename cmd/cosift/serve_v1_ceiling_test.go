package main

import (
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/store"
)

type countingChat struct {
	reply string
	err   error
	calls atomic.Int64
}

func (c *countingChat) Model() string { return "count-test" }
func (c *countingChat) Chat(context.Context, []embed.ChatMsg) (string, error) {
	c.calls.Add(1)
	return c.reply, c.err
}

const allowReply = `{"decision":"allow","category":"safe"}`

// ceilingServer is a moderation handler over a real store with a daily ceiling and a settable clock.
func ceilingServer(t *testing.T, limit int, chat embed.ChatClient) (*pebbleHTTP, *time.Time) {
	t.Helper()
	ps, err := store.OpenPebble(filepath.Join(t.TempDir(), "pebble"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ps.Close() })
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	s := &pebbleHTTP{store: ps, chat: chat, communityModerationsPerDay: &limit}
	s.communityNow = func() time.Time { return now }
	return s, &now
}

func moderate(t *testing.T, s *pebbleHTTP, mut func(m map[string]any)) *httptest.ResponseRecorder {
	t.Helper()
	m := map[string]any{}
	for k, v := range moderateBody {
		m[k] = v
	}
	if mut != nil {
		mut(m)
	}
	return v1Call(t, s.handleCommunityModerate, string(mustMarshal(m)))
}

// At the ceiling the model is not called and the answer says so; a day later the allowance is back.
func TestCommunityModerationCeiling(t *testing.T) {
	chat := &countingChat{reply: allowReply}
	s, now := ceilingServer(t, 2, chat)
	for i := range 2 {
		if w := moderate(t, s, nil); w.Code != 200 {
			t.Fatalf("call %d: %d %s", i, w.Code, w.Body)
		}
	}
	w := moderate(t, s, nil)
	e := v1ErrCode(t, w)
	if w.Code != 503 || e.Code != "moderation_unavailable" || !e.Ceiling || w.Header().Get("Retry-After") != strconv.Itoa(14*3600) {
		t.Fatalf("at the ceiling: %d %s %v", w.Code, w.Body, w.Header())
	}
	if !strings.Contains(w.Body.String(), `"ceiling":true`) {
		t.Fatalf("body %s", w.Body)
	}
	if chat.calls.Load() != 2 {
		t.Fatalf("the model ran %d times", chat.calls.Load())
	}
	*now = now.Add(14 * time.Hour)
	if w := moderate(t, s, nil); w.Code != 200 {
		t.Fatalf("the next UTC day: %d %s", w.Code, w.Body)
	}
	if chat.calls.Load() != 3 {
		t.Fatalf("the model ran %d times", chat.calls.Load())
	}
}

// Only a call that reaches the model takes a unit, and only the ceiling answer carries ceiling.
func TestCommunityModerationCeilingCountsModelCalls(t *testing.T) {
	chat := &countingChat{reply: allowReply}
	s, _ := ceilingServer(t, 3, chat)
	rejects := []func(m map[string]any){
		func(m map[string]any) { m["title"] = "Domain for sale" },
		func(m map[string]any) { m["url"] = "http://localhost/x" },
		func(m map[string]any) { m["text"] = "short" },
	}
	for i, mut := range rejects {
		if w := moderate(t, s, mut); w.Code == 200 && i != 0 {
			t.Fatalf("reject %d answered 200", i)
		}
	}
	if chat.calls.Load() != 0 {
		t.Fatalf("a document refused before the model called it %d times", chat.calls.Load())
	}
	for range 3 {
		if w := moderate(t, s, nil); w.Code != 200 {
			t.Fatalf("units spent by refused documents: %d %s", w.Code, w.Body)
		}
	}
	if w := moderate(t, s, nil); !v1ErrCode(t, w).Ceiling {
		t.Fatalf("the fourth model call: %d %s", w.Code, w.Body)
	}

	chat = &countingChat{err: errors.New("down")}
	s, _ = ceilingServer(t, 1, chat)
	w := moderate(t, s, nil)
	if e := v1ErrCode(t, w); w.Code != 503 || e.Code != "moderation_unavailable" || e.Ceiling || w.Header().Get("Retry-After") != "" {
		t.Fatalf("a model failure: %d %s %v", w.Code, w.Body, w.Header())
	}
	if w := moderate(t, s, nil); !v1ErrCode(t, w).Ceiling || chat.calls.Load() != 1 {
		t.Fatalf("a failed call spent its unit: %d %s (%d calls)", w.Code, w.Body, chat.calls.Load())
	}

	s, _ = ceilingServer(t, 0, nil)
	if w := moderate(t, s, nil); w.Code != 503 || v1ErrCode(t, w).Ceiling {
		t.Fatalf("no model, ceiling 0: %d %s", w.Code, w.Body)
	}
}

func TestCommunityModerationCeilingHaltsAtZero(t *testing.T) {
	chat := &countingChat{reply: allowReply}
	s, _ := ceilingServer(t, 0, chat)
	if w := moderate(t, s, nil); !v1ErrCode(t, w).Ceiling || chat.calls.Load() != 0 {
		t.Fatalf("a ceiling of 0 must halt: %d %s", w.Code, w.Body)
	}
}

// The count survives a restart and holds under concurrency.
func TestCommunityModerationCeilingIsDurableAndAtomic(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	chat := &countingChat{reply: allowReply}
	limit := 40
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	s := &pebbleHTTP{store: ps, chat: chat, communityModerationsPerDay: &limit, communityNow: func() time.Time { return now }}
	var wg sync.WaitGroup
	var served atomic.Int64
	for range 60 {
		wg.Go(func() {
			if moderate(t, s, nil).Code == 200 {
				served.Add(1)
			}
		})
	}
	wg.Wait()
	if served.Load() != 40 || chat.calls.Load() != 40 {
		t.Fatalf("%d served, %d model calls; want 40", served.Load(), chat.calls.Load())
	}
	ps.Close()
	if ps, err = store.OpenPebble(dir); err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	s = &pebbleHTTP{store: ps, chat: chat, communityModerationsPerDay: &limit, communityNow: func() time.Time { return now }}
	if w := moderate(t, s, nil); !v1ErrCode(t, w).Ceiling {
		t.Fatalf("after a restart: %d %s", w.Code, w.Body)
	}
}

func TestCommunityModerationMetrics(t *testing.T) {
	s, _ := ceilingServer(t, 1, &countingChat{reply: allowReply})
	moderate(t, s, nil)
	moderate(t, s, nil)
	moderate(t, s, func(m map[string]any) { m["url"] = "http://localhost/x" })
	w := httptest.NewRecorder()
	writeCommunityModerationMetrics(w, s)
	for _, want := range []string{
		`cosift_community_moderations_total{result="ok"} 1`,
		`cosift_community_moderations_total{result="ceiling"} 1`,
		`cosift_community_moderations_total{result="error"} 1`,
	} {
		if !strings.Contains(w.Body.String(), want) {
			t.Errorf("missing %s in\n%s", want, w.Body)
		}
	}
}

func TestServeRefusesACommunityCeilingAboveTheMaximum(t *testing.T) {
	over := config.MaxCommunityModerationsPerDay + 1
	cfg := &config.Config{Work: config.Work{CommunityModerationsPerDay: &over}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := runPebbleServe(ctx, cfg, []string{"-dir", filepath.Join(t.TempDir(), "pebble"), "-addr", freeAddr(t)})
	if err == nil || !strings.Contains(err.Error(), "work.community_moderations_per_day") {
		t.Fatalf("started: %v", err)
	}
}
