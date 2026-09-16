// Package community implements the optional contributor portal. Account data is
// kept separately from the search corpus and never sent to the search backend.
package community

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

func openDB(dir string) (*sql.DB, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "community.db")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	f.Close()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS users (
 id TEXT PRIMARY KEY, email TEXT NOT NULL UNIQUE, name TEXT NOT NULL,
 salt TEXT NOT NULL, password_hash TEXT NOT NULL, interests TEXT NOT NULL DEFAULT '[]',
 onboarded INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS sessions (
 hash TEXT PRIMARY KEY, user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 expires_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS saved_searches (
 id TEXT PRIMARY KEY, user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 query TEXT NOT NULL, mode TEXT NOT NULL DEFAULT 'search', created_at INTEGER NOT NULL, UNIQUE(user_id,query,mode));
CREATE TABLE IF NOT EXISTS submissions (
 id TEXT PRIMARY KEY, user_id TEXT REFERENCES users(id) ON DELETE CASCADE,
 url TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', reason TEXT NOT NULL DEFAULT '', attempts INTEGER NOT NULL DEFAULT 0,
 next_attempt INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL,
 UNIQUE(user_id,url));
CREATE INDEX IF NOT EXISTS submissions_pending ON submissions(status,next_attempt);
CREATE INDEX IF NOT EXISTS submissions_owner ON submissions(user_id,created_at);
CREATE INDEX IF NOT EXISTS sessions_expiry ON sessions(expires_at);
CREATE TABLE IF NOT EXISTS credit_ledger (
 id TEXT PRIMARY KEY,user_id TEXT NOT NULL REFERENCES users(id),delta INTEGER NOT NULL,
 reason TEXT NOT NULL,created_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS credit_owner ON credit_ledger(user_id);
CREATE TABLE IF NOT EXISTS submission_artifacts (
 submission_id TEXT PRIMARY KEY REFERENCES submissions(id) ON DELETE CASCADE,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS payment_checkouts (
 id TEXT PRIMARY KEY,user_id TEXT NOT NULL REFERENCES users(id),
 amount_cents INTEGER NOT NULL,credits INTEGER NOT NULL,currency TEXT NOT NULL,
 session_id TEXT UNIQUE,checkout_url TEXT NOT NULL DEFAULT '',payment_intent TEXT UNIQUE,
 refunded_cents INTEGER NOT NULL DEFAULT 0,created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS payment_events (
 provider TEXT NOT NULL,event_id TEXT NOT NULL,user_id TEXT NOT NULL REFERENCES users(id),
 credits INTEGER NOT NULL,created_at INTEGER NOT NULL,PRIMARY KEY(provider,event_id));
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS retrieval_usage (identity TEXT NOT NULL, mode TEXT NOT NULL, count INTEGER NOT NULL, expires_at INTEGER NOT NULL, PRIMARY KEY(identity,mode));
CREATE INDEX IF NOT EXISTS retrieval_usage_expiry ON retrieval_usage(expires_at);
CREATE TABLE IF NOT EXISTS guest_usage (ip_hash TEXT PRIMARY KEY, expires_at INTEGER NOT NULL, reservation TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS guest_usage_expiry ON guest_usage(expires_at);`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("community schema: %w", err)
	}
	if err := migrateSavedModes(db); err != nil {
		db.Close()
		return nil, err
	}
	rows, err := db.Query(`PRAGMA table_info(submissions)`)
	if err != nil {
		db.Close()
		return nil, err
	}
	hasReason := false
	for rows.Next() {
		var cid, nn, pk int
		var name, typ string
		var def any
		if err = rows.Scan(&cid, &name, &typ, &nn, &def, &pk); err != nil {
			rows.Close()
			db.Close()
			return nil, err
		}
		if name == "reason" {
			hasReason = true
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		db.Close()
		return nil, err
	}
	if !hasReason {
		if _, err = db.Exec(`ALTER TABLE submissions ADD COLUMN reason TEXT NOT NULL DEFAULT ''`); err != nil {
			db.Close()
			return nil, err
		}
	}
	return db, nil
}

// Older community databases stored only searches. Preserve their IDs and
// timestamps while making the uniqueness key include Search/Answer/Research.
func migrateSavedModes(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(saved_searches)`)
	if err != nil {
		return err
	}
	found := false
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var def any
		if err := rows.Scan(&cid, &name, &typ, &notnull, &def, &pk); err != nil {
			rows.Close()
			return err
		}
		if name == "mode" {
			found = true
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil || found {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`ALTER TABLE saved_searches RENAME TO saved_searches_old;
CREATE TABLE saved_searches (
 id TEXT PRIMARY KEY,user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 query TEXT NOT NULL,mode TEXT NOT NULL DEFAULT 'search',created_at INTEGER NOT NULL,UNIQUE(user_id,query,mode));
INSERT INTO saved_searches(id,user_id,query,mode,created_at) SELECT id,user_id,query,'search',created_at FROM saved_searches_old;
DROP TABLE saved_searches_old;`)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func randomID() string          { return rand.Text() }
func tokenHash(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }

type User struct {
	ID        string   `json:"id"`
	Email     string   `json:"email"`
	Name      string   `json:"name"`
	Interests []string `json:"interests"`
	Onboarded bool     `json:"onboarded"`
}

func scanUser(row *sql.Row) (User, error) {
	var u User
	var interests string
	err := row.Scan(&u.ID, &u.Email, &u.Name, &interests, &u.Onboarded)
	if err != nil {
		return u, err
	}
	err = json.Unmarshal([]byte(interests), &u.Interests)
	return u, err
}
