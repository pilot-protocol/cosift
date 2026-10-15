package community

import (
	"context"
	"testing"
)

func linkOnce(t *testing.T, s *Server, uid, email string) (string, error) {
	t.Helper()
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := linkSharedUser(context.Background(), tx, uid, email)
	if err != nil {
		tx.Rollback()
		return "", err
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return id, nil
}

func sessionCount(t *testing.T, s *Server, userID string) int {
	t.Helper()
	var n int
	s.db.QueryRow(`SELECT count(*) FROM sessions WHERE user_id=?`, userID).Scan(&n)
	return n
}

func TestLinkSharedUserCreatesLinksAndKeepsAnExistingLinkRegardlessOfEmail(t *testing.T) {
	s := testServer(t, nil)
	id, err := linkOnce(t, s, "0123456789abcdef", "new.person@example.com")
	if err != nil {
		t.Fatal(err)
	}
	var name, email string
	s.db.QueryRow(`SELECT name,email FROM users WHERE id=?`, id).Scan(&name, &email)
	if name != "new.person" || email != "new.person@example.com" {
		t.Fatalf("created %q %q", name, email)
	}
	again, err := linkOnce(t, s, "0123456789abcdef", "changed@example.com")
	if err != nil || again != id {
		t.Fatalf("existing link by uid: %q %v", again, err)
	}
	var users int
	s.db.QueryRow(`SELECT count(*) FROM users`).Scan(&users)
	if users != 1 {
		t.Fatalf("%d users after a changed email", users)
	}
}

func TestLinkSharedUserAdoptsTheUserOfTheVerifiedEmailAndClearsItsSessionsAndPassword(t *testing.T) {
	s := testServer(t, nil)
	cookie := account(t, s, "member@example.com")
	var id string
	s.db.QueryRow(`SELECT id FROM users WHERE email='member@example.com'`).Scan(&id)
	s.db.Exec(`INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES('kept',?,42,'test',0)`, id)
	if sessionCount(t, s, id) == 0 || cookie == nil {
		t.Fatal("no session to clear")
	}
	got, err := linkOnce(t, s, "0123456789abcdef", "member@example.com")
	if err != nil || got != id {
		t.Fatalf("adopted %q want %q (%v)", got, id, err)
	}
	var hash string
	var balance int
	s.db.QueryRow(`SELECT password_hash FROM users WHERE id=?`, id).Scan(&hash)
	s.db.QueryRow(`SELECT sum(delta) FROM credit_ledger WHERE user_id=?`, id).Scan(&balance)
	if sessionCount(t, s, id) != 0 || hash != "" || balance != 42 {
		t.Fatalf("sessions %d hash %q balance %d", sessionCount(t, s, id), hash, balance)
	}
}

func TestLinkSharedUserConflictChangesNothing(t *testing.T) {
	s := testServer(t, nil)
	account(t, s, "member@example.com")
	var id string
	s.db.QueryRow(`SELECT id FROM users WHERE email='member@example.com'`).Scan(&id)
	s.db.Exec(`INSERT INTO shared_identities(uid,user_id) VALUES('fedcba9876543210',?)`, id)
	before := sessionCount(t, s, id)
	if _, err := linkOnce(t, s, "0123456789abcdef", "member@example.com"); err == nil {
		t.Fatal("a second uid was linked to the same user")
	}
	var links int
	s.db.QueryRow(`SELECT count(*) FROM shared_identities`).Scan(&links)
	if links != 1 || sessionCount(t, s, id) != before {
		t.Fatalf("links %d sessions %d->%d", links, before, sessionCount(t, s, id))
	}
}

func TestSharedUserStillRefusesALoginWhoseEmailChanged(t *testing.T) {
	s := testServer(t, nil)
	s.cfg.Shared = &fakeShared{}
	expect(t, bearerRequest(s, "/api/me", sharedTestToken), 200)
	s.db.Exec(`UPDATE users SET email='moved@example.com'`)
	if w := bearerRequest(s, "/api/me", sharedTestToken); w.Code != 503 {
		t.Fatalf("%d", w.Code)
	}
}
