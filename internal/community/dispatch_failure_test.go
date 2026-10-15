package community

import (
	"context"
	"testing"
)

func refuseSubmissionUpdates(t *testing.T, s *Server) {
	t.Helper()
	if _, err := s.db.Exec(`CREATE TRIGGER refuse_status BEFORE UPDATE ON submissions BEGIN SELECT RAISE(ABORT, 'store offline'); END`); err != nil {
		t.Fatal(err)
	}
}

func TestDispatchReportsAFailedStatusWriteAndLeavesTheSubmissionRetryable(t *testing.T) {
	submit := func(t *testing.T, s *Server) {
		t.Helper()
		cookie := account(t, s, "member@example.com")
		expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
	}
	for name, setup := range map[string]func(*testing.T) *Server{
		"backend unreachable": func(t *testing.T) *Server {
			captureLog(t)
			s := testServer(t, nil)
			s.engine.base = refusedBackend(t)
			s.backendDown.Store(true)
			return s
		},
		"broker key missing": func(t *testing.T) *Server {
			captureLog(t)
			s := testServer(t, nil)
			s.engine.key = ""
			return s
		},
		"page check inconclusive": func(t *testing.T) *Server {
			captureLog(t)
			code := 500
			return routeServer(t, map[string]*int{"/v1/community/moderate": &code})
		},
		"delivery acknowledged": func(t *testing.T) *Server {
			return routeServer(t, nil)
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := setup(t)
			submit(t, s)
			refuseSubmissionUpdates(t, s)
			if err := s.dispatch(context.Background()); err == nil {
				t.Fatal("a failed status write was swallowed")
			}
			if status, attempts := attemptsAndStatus(t, s); status != "pending" || attempts != 0 {
				t.Fatalf("row left %s after %d attempts", status, attempts)
			}
		})
	}
}

func TestDispatchReportsAFailedRequeueOfACrawlDelayedPage(t *testing.T) {
	s := testServer(t, (&approvalBackend{}).handler(t))
	delayedSite(t, s, "5")
	twoPages(t, s)
	if err := s.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE submissions SET next_attempt=0 WHERE status='pending'`); err != nil {
		t.Fatal(err)
	}
	refuseSubmissionUpdates(t, s)
	if err := s.dispatch(context.Background()); err == nil {
		t.Fatal("a failed requeue was swallowed")
	}
	var attempts int
	s.db.QueryRow(`SELECT attempts FROM submissions WHERE status='pending'`).Scan(&attempts)
	if attempts != 0 {
		t.Fatalf("a held-back page used %d attempts", attempts)
	}
}

func TestDispatchKeepsASubmissionPendingWhenItsRewardCannotBeWritten(t *testing.T) {
	s := rewardServer(t, func(c *Config) { c.ContributionReward = true }, novelReceipt(true))
	cookie := account(t, s, "member@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://docs.example.com/guide"}}, cookie), 202)
	if _, err := s.db.Exec(`CREATE TRIGGER refuse_reward BEFORE INSERT ON credit_ledger WHEN NEW.reason='verified_contribution' BEGIN SELECT RAISE(ABORT, 'store offline'); END`); err != nil {
		t.Fatal(err)
	}
	if err := s.dispatch(context.Background()); err == nil {
		t.Fatal("a failed reward write was swallowed")
	}
	if status, _ := attemptsAndStatus(t, s); status != "pending" {
		t.Fatalf("submission is %s although its reward was never recorded", status)
	}
	if _, err := s.db.Exec(`DROP TRIGGER refuse_reward`); err != nil {
		t.Fatal(err)
	}
	s.db.Exec(`UPDATE submissions SET next_attempt=0`)
	if err := s.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	var userID string
	s.db.QueryRow(`SELECT id FROM users WHERE email='member@example.com'`).Scan(&userID)
	if status, _ := attemptsAndStatus(t, s); status != "indexed" || rewards(t, s, userID) != 1 {
		t.Fatalf("retry: status %s, rewards %d", status, rewards(t, s, userID))
	}
}

func TestOpenRefusesANonLoopbackEngine(t *testing.T) {
	s, err := Open(Config{DataDir: t.TempDir(), Backend: "http://127.0.0.1:1", PublicURL: "http://localhost:7780", BrokerKey: testBrokerKey, EngineURL: "http://example.com:7779"})
	if err == nil {
		s.Close()
		t.Fatal("opened with an engine that is not on loopback")
	}
}

func TestLogoutReportsAFailedSessionDeleteAndKeepsTheSession(t *testing.T) {
	s := testServer(t, nil)
	cookie := account(t, s, "member@example.com")
	if _, err := s.db.Exec(`CREATE TRIGGER refuse_logout BEFORE DELETE ON sessions BEGIN SELECT RAISE(ABORT, 'store offline'); END`); err != nil {
		t.Fatal(err)
	}
	w := request(t, s, "POST", "/api/logout", map[string]string{}, cookie)
	expect(t, w, 500)
	for _, c := range w.Result().Cookies() {
		if c.Name == cookie.Name && c.MaxAge < 0 {
			t.Fatal("the cookie was cleared although the session survived")
		}
	}
	var n int
	s.db.QueryRow(`SELECT count(*) FROM sessions`).Scan(&n)
	if n != 1 {
		t.Fatalf("%d sessions", n)
	}
}
