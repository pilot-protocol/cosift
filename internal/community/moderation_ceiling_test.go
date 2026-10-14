package community

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	ceilingBody = `{"error":"x","status":503,"code":"moderation_unavailable","detail":"d","ceiling":true}`
	modelDown   = `{"error":"x","status":503,"code":"moderation_unavailable","detail":"d"}`
)

type ceilingRig struct {
	*workRig
	moderate  []string
	enqueue   int
	mu        sync.Mutex
	moderated func() (int, string)
}

func newCeilingRig(t *testing.T) *ceilingRig {
	t.Helper()
	r := &ceilingRig{workRig: newWorkRig(t)}
	r.engine.answer(func(call engineRequest) (int, string) {
		r.mu.Lock()
		defer r.mu.Unlock()
		switch call.path {
		case "/v1/community/moderate":
			var doc ModerationDocument
			json.Unmarshal(call.body, &doc)
			r.moderate = append(r.moderate, doc.URL)
			return r.moderated()
		case "/v1/community/enqueue":
			r.enqueue++
			return 200, `{"indexed":true,"novel":false,"content_hash":"` + strings.Repeat("a", 64) + `"}`
		}
		return 404, `{}`
	})
	r.moderated = func() (int, string) { return 503, ceilingBody }
	setTestPage(r.s, `<html><title>Technical documentation</title><body><article>This educational page explains programming tools, reliable systems, and defensive software engineering. It contains useful public documentation for developers.</article></body></html>`)
	return r
}

func (r *ceilingRig) add(user, url string) {
	r.submission(user, url, "pending", "", r.clk.now())
}

func (r *ceilingRig) row(url string) (status string, attempts int, next int64, reason string) {
	r.s.db.QueryRow(`SELECT status,attempts,next_attempt,reason FROM submissions WHERE url=?`, url).Scan(&status, &attempts, &next, &reason)
	return
}

func (r *ceilingRig) dispatch(t *testing.T) {
	t.Helper()
	if err := r.s.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestModerateCeilingRequeuesToMidnightWithoutSpendingAnAttempt(t *testing.T) {
	r := newCeilingRig(t)
	r.add("alice", "https://example.com/one")
	r.dispatch(t)
	midnight := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC).Unix()
	status, attempts, next, reason := r.row("https://example.com/one")
	if status != "pending" || attempts != 0 || next != midnight || reason == "" || strings.Contains(reason, "ceiling") {
		t.Fatalf("%s attempts=%d next=%d (want %d) reason=%q", status, attempts, next, midnight, reason)
	}
	if len(r.moderate) != 1 || r.enqueue != 0 {
		t.Fatalf("moderate %v, enqueue %d", r.moderate, r.enqueue)
	}
	r.s.drainAlerts(context.Background())
	if len(r.f.alerts) != 1 || r.f.alerts[0].Subject != "2026-10-14/moderate" {
		t.Fatalf("alerts %+v", r.f.alerts)
	}
}

func TestNoSubmissionCallsModerateBeforeMidnightAfterACeilingAnswer(t *testing.T) {
	r := newCeilingRig(t)
	r.add("alice", "https://example.com/one")
	r.dispatch(t)
	r.add("bob", "https://example.com/two")
	r.add("carol", "https://example.com/three")
	r.clk.advance(3 * time.Hour)
	r.dispatch(t)
	if len(r.moderate) != 1 {
		t.Fatalf("moderate was called %d times: %v", len(r.moderate), r.moderate)
	}
	for _, u := range []string{"https://example.com/two", "https://example.com/three"} {
		if status, attempts, next, _ := r.row(u); status != "pending" || attempts != 0 || next != time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC).Unix() {
			t.Errorf("%s: %s attempts=%d next=%d", u, status, attempts, next)
		}
	}
	r.clk.advance(14 * time.Hour)
	r.moderated = func() (int, string) { return 200, `{"decision":"allow","category":"safe"}` }
	r.dispatch(t)
	if len(r.moderate) != 4 || r.enqueue != 3 {
		t.Fatalf("after midnight: moderate %d, enqueue %d", len(r.moderate), r.enqueue)
	}
}

func TestTwelveCeilingAnswersInARowLeaveTheSubmissionPendingWithItsAttempts(t *testing.T) {
	r := newCeilingRig(t)
	r.add("alice", "https://example.com/one")
	for day := 0; day < 12; day++ {
		r.dispatch(t)
		r.clk.advance(24 * time.Hour)
	}
	status, attempts, _, _ := r.row("https://example.com/one")
	if status != "pending" || attempts != 0 || len(r.moderate) != 12 {
		t.Fatalf("%s attempts=%d moderate calls=%d", status, attempts, len(r.moderate))
	}
}

func TestTheSame503WithoutCeilingIsAnAttemptAndHoldsNothing(t *testing.T) {
	r := newCeilingRig(t)
	r.moderated = func() (int, string) { return 503, modelDown }
	r.add("alice", "https://example.com/one")
	r.add("bob", "https://example.com/two")
	r.dispatch(t)
	if len(r.moderate) != 2 {
		t.Fatalf("a model failure held the other submission: %v", r.moderate)
	}
	for _, u := range []string{"https://example.com/one", "https://example.com/two"} {
		if status, attempts, _, _ := r.row(u); status != "pending" || attempts != 1 {
			t.Errorf("%s: %s attempts=%d", u, status, attempts)
		}
	}
	r.s.drainAlerts(context.Background())
	if len(r.f.alerts) != 0 {
		t.Fatal("a model failure raised the ceiling alert")
	}
	for _, body := range []string{`{"code":"moderation_unavailable","ceiling":"true"}`, `{"code":"moderation_unavailable","ceiling":false}`, `{"code":"other","ceiling":true}`, `ceiling`} {
		r.moderated = func() (int, string) { return 503, body }
		r.s.db.Exec(`UPDATE submissions SET next_attempt=0`)
		r.dispatch(t)
		r.s.drainAlerts(context.Background())
		if len(r.f.alerts) != 0 {
			t.Fatalf("%s raised the ceiling alert", body)
		}
	}
}

func TestACeilingStatusOtherThan503IsNotACeiling(t *testing.T) {
	r := newCeilingRig(t)
	r.moderated = func() (int, string) { return 500, ceilingBody }
	r.add("alice", "https://example.com/one")
	r.dispatch(t)
	if _, attempts, _, _ := r.row("https://example.com/one"); attempts != 1 {
		t.Fatalf("attempts %d", attempts)
	}
}

func TestAnApprovedSubmissionStillDeliversWhileTheCeilingHolds(t *testing.T) {
	r := newCeilingRig(t)
	r.moderated = func() (int, string) { return 200, `{"decision":"allow","category":"safe"}` }
	r.add("alice", "https://example.com/one")
	r.dispatch(t)
	r.moderated = func() (int, string) { return 503, ceilingBody }
	r.add("bob", "https://example.com/two")
	r.dispatch(t)
	if status, attempts, _, _ := r.row("https://example.com/two"); status != "pending" || attempts != 0 {
		t.Fatalf("bob: %s attempts=%d", status, attempts)
	}
	r.s.db.Exec(`UPDATE submissions SET status='pending',next_attempt=0 WHERE url='https://example.com/one'`)
	moderated, delivered := len(r.moderate), r.enqueue
	r.dispatch(t)
	if len(r.moderate) != moderated || r.enqueue != delivered+1 {
		t.Fatalf("an approved page called moderate again or was held: moderate %d→%d, enqueue %d→%d", moderated, len(r.moderate), delivered, r.enqueue)
	}
}
