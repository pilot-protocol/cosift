package community

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

const allowVerdict = `{"decision":"allow","category":"safe"}`

var (
	nextMidnight = time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	enqueueOK    = `{"indexed":true,"novel":false,"content_hash":"` + strings.Repeat("a", 64) + `"}`
)

func newShareRig(t *testing.T, share int) *ceilingRig {
	t.Helper()
	r := newCeilingRig(t)
	r.s.cfg.ModerationsPerAccount = share
	r.moderated = func() (int, string) { return 200, allowVerdict }
	return r
}

func (r *ceilingRig) count(query string, args ...any) (n int) {
	r.s.db.QueryRow(query, args...).Scan(&n)
	return
}

func (r *ceilingRig) usage(user string) int {
	return r.count(`SELECT COALESCE(sum(count),0) FROM moderation_usage WHERE user_id=?`, user)
}

func TestOneAccountUsingItsShareDoesNotHoldAnotherAccount(t *testing.T) {
	r := newShareRig(t, 2)
	for i := 0; i < 4; i++ {
		r.add("alice", fmt.Sprintf("https://example.com/a%d", i))
	}
	r.add("bob", "https://example.org/b")
	r.dispatch(t)
	if len(r.moderate) != 3 || r.enqueue != 3 {
		t.Fatalf("moderate %v, enqueue %d", r.moderate, r.enqueue)
	}
	if status, _, _, _ := r.row("https://example.org/b"); status != "indexed" {
		t.Fatalf("bob: %s", status)
	}
	if indexed := r.count(`SELECT count(*) FROM submissions WHERE user_id='alice' AND status='indexed'`); indexed != 2 {
		t.Fatalf("alice has %d checked submissions, want her share of 2", indexed)
	}
	if held := r.count(`SELECT count(*) FROM submissions WHERE user_id='alice' AND status='pending' AND attempts=0 AND next_attempt=? AND reason<>''`, nextMidnight.Unix()); held != 2 {
		t.Fatalf("%d of alice's submissions are held to midnight without an attempt, want 2", held)
	}
	if r.s.now().Before(r.s.moderationHold()) {
		t.Fatal("an account's share held every account")
	}
	r.s.drainAlerts(t.Context())
	if len(r.f.alerts) != 0 {
		t.Fatalf("an account's share raised the ceiling alert: %+v", r.f.alerts)
	}
}

func TestTheShareSpendsNoAttemptAndRenewsAtMidnightUTC(t *testing.T) {
	r := newShareRig(t, 1)
	r.add("alice", "https://example.com/one")
	r.add("alice", "https://example.com/two")
	r.dispatch(t)
	held := "https://example.com/two"
	if status, attempts, next, reason := r.row(held); status != "pending" || attempts != 0 || next != nextMidnight.Unix() || reason == "" || strings.Contains(reason, "ceiling") {
		t.Fatalf("%s attempts=%d next=%d reason=%q", status, attempts, next, reason)
	}
	r.clk.advance(3 * time.Hour)
	r.dispatch(t)
	r.clk.advance(11*time.Hour + 29*time.Minute + 59*time.Second)
	r.dispatch(t)
	if len(r.moderate) != 1 {
		t.Fatalf("a held submission was checked before midnight: %v", r.moderate)
	}
	if _, attempts, _, _ := r.row(held); attempts != 0 {
		t.Fatalf("attempts %d", attempts)
	}
	r.clk.advance(time.Second)
	r.dispatch(t)
	if len(r.moderate) != 2 {
		t.Fatalf("the share did not renew at 00:00 UTC: %v", r.moderate)
	}
	if status, _, _, _ := r.row(held); status != "indexed" {
		t.Fatalf("%s after midnight", status)
	}
	if rows := r.count(`SELECT count(*) FROM moderation_usage`); rows != 1 || r.usage("alice") != 1 {
		t.Fatalf("%d usage rows, alice %d: yesterday's count was kept", rows, r.usage("alice"))
	}
}

func TestTheCountSurvivesARestart(t *testing.T) {
	r := newShareRig(t, 2)
	r.add("alice", "https://example.com/one")
	r.add("alice", "https://example.com/two")
	r.dispatch(t)
	cfg := r.s.cfg
	cfg.Work, cfg.Shared, cfg.Environment = false, nil, ""
	if err := r.s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	reopened.now = r.clk.now
	setTestPage(reopened, `<html><title>Technical documentation</title><body><article>This educational page explains programming tools, reliable systems, and defensive software engineering. It contains useful public documentation for developers.</article></body></html>`)
	r.s = reopened
	r.add("alice", "https://example.com/three")
	r.add("bob", "https://example.com/four")
	r.clk.advance(time.Hour)
	r.dispatch(t)
	if len(r.moderate) != 3 || r.usage("alice") != 2 {
		t.Fatalf("moderate %v, alice used %d", r.moderate, r.usage("alice"))
	}
	if status, attempts, next, _ := r.row("https://example.com/three"); status != "pending" || attempts != 0 || next != nextMidnight.Unix() {
		t.Fatalf("three: %s attempts=%d next=%d", status, attempts, next)
	}
}

func TestCitedSourcePushesCountAgainstTheContributorsShare(t *testing.T) {
	r := newShareRig(t, 2)
	w := r.post("/api/submissions", sharedTestToken, map[string]any{"urls": []string{"https://docs.example.org/a", "https://docs.example.org/b", "https://docs.example.org/c"}}, nil)
	if w.Code != http.StatusAccepted {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	r.add("bob", "https://example.org/other")
	r.dispatch(t)
	if r.usage(r.userID) != 2 || r.usage("bob") != 1 || len(r.moderate) != 3 {
		t.Fatalf("contributor used %d, bob %d, moderate %v", r.usage(r.userID), r.usage("bob"), r.moderate)
	}
	if held := r.count(`SELECT count(*) FROM submissions WHERE user_id=? AND status='pending' AND attempts=0 AND next_attempt=?`, r.userID, nextMidnight.Unix()); held != 1 {
		t.Fatalf("%d of the contributor's pushes held to midnight, want 1", held)
	}
}

func TestOnlyAModelCheckCountsAgainstTheShare(t *testing.T) {
	r := newShareRig(t, 1)
	delivered := 0
	r.engine.answer(func(call engineRequest) (int, string) {
		switch call.path {
		case "/v1/community/moderate":
			r.mu.Lock()
			r.moderate = append(r.moderate, "")
			r.mu.Unlock()
			return 200, allowVerdict
		case "/v1/community/enqueue":
			delivered++
			if delivered == 1 {
				return 502, `{}`
			}
			return 200, enqueueOK
		}
		return 404, `{}`
	})
	r.add("alice", "https://example.com/one")
	r.dispatch(t)
	r.s.db.Exec(`UPDATE submissions SET next_attempt=0`)
	r.dispatch(t)
	if status, _, _, _ := r.row("https://example.com/one"); status != "indexed" || len(r.moderate) != 1 || r.usage("alice") != 1 {
		t.Fatalf("%s after %d checks, %d counted", status, len(r.moderate), r.usage("alice"))
	}
}

func TestTheGlobalHoldSpendsNoShare(t *testing.T) {
	r := newShareRig(t, 5)
	r.moderated = func() (int, string) { return 503, ceilingBody }
	r.add("alice", "https://example.com/one")
	r.dispatch(t)
	r.add("bob", "https://example.org/two")
	r.dispatch(t)
	if len(r.moderate) != 1 || r.usage("alice") != 1 || r.usage("bob") != 0 {
		t.Fatalf("moderate %v, alice %d, bob %d", r.moderate, r.usage("alice"), r.usage("bob"))
	}
}

func TestTheDefaultShareIsOneHundredAChecksPerAccountEachDay(t *testing.T) {
	r := newCeilingRig(t)
	r.moderated = func() (int, string) { return 200, allowVerdict }
	if r.s.cfg.ModerationsPerAccount != 100 {
		t.Fatalf("default share %d", r.s.cfg.ModerationsPerAccount)
	}
	for i := 0; i < 101; i++ {
		r.add("alice", fmt.Sprintf("https://example.com/p%03d", i))
	}
	for i := 0; i < 8; i++ {
		r.dispatch(t)
	}
	if len(r.moderate) != 100 || r.count(`SELECT count(*) FROM submissions WHERE status='pending'`) != 1 {
		t.Fatalf("%d checks", len(r.moderate))
	}
}

func TestOpenValidatesTheShare(t *testing.T) {
	for _, tc := range []struct {
		share int
		ok    bool
	}{{-1, false}, {0, true}, {1, true}, {100, true}, {10000, true}, {10001, false}} {
		s, err := Open(Config{DataDir: t.TempDir(), Backend: "http://127.0.0.1:1", PublicURL: "http://localhost:7780", BrokerKey: testBrokerKey, ModerationsPerAccount: tc.share})
		if err == nil {
			s.Close()
		}
		if (err == nil) != tc.ok || (err != nil && !strings.Contains(err.Error(), "moderations per account")) {
			t.Errorf("share %d: %v", tc.share, err)
		}
	}
}

func TestAnOlderDatabaseGainsTheUsageTable(t *testing.T) {
	r := newShareRig(t, 1)
	if _, err := r.s.db.Exec(`DROP TABLE moderation_usage`); err != nil {
		t.Fatal(err)
	}
	cfg := r.s.cfg
	cfg.Work, cfg.Shared, cfg.Environment = false, nil, ""
	r.s.Close()
	reopened, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if ok, err := reopened.reserveModeration(t.Context(), "alice"); !ok || err != nil {
		t.Fatalf("reserve on a migrated database: %v %v", ok, err)
	}
}

func TestACheckIsNotMadeWhenItCannotBeCounted(t *testing.T) {
	r := newShareRig(t, 5)
	if _, err := r.s.db.Exec(`CREATE TRIGGER no_usage BEFORE INSERT ON moderation_usage BEGIN SELECT RAISE(ABORT, 'full'); END`); err != nil {
		t.Fatal(err)
	}
	r.add("alice", "https://example.com/one")
	r.dispatch(t)
	status, attempts, next, _ := r.row("https://example.com/one")
	if len(r.moderate) != 0 || status != "pending" || attempts != 1 || next == nextMidnight.Unix() {
		t.Fatalf("moderate %v, %s attempts=%d next=%d", r.moderate, status, attempts, next)
	}
}
