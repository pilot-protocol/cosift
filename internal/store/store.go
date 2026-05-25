// Package store wraps the SQLite-backed document store.
//
// Schema is kept minimal. One DB file, three tables: documents, postings, terms.
// The postings table is the inverted index used by the BM25 ranker.
package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Store is the persistent layer. Safe for concurrent use.
type Store struct {
	db   *sql.DB
	path string
}

// Document is the canonical parsed-page record.
type Document struct {
	ID         int64
	URL        string
	Domain     string
	Title      string
	Text       string
	Lang       string
	Source     string // 'crawl' | 'fineweb' | 'fetched'
	Quality    float64
	FetchedAt  time.Time
	ContentSHA []byte
	// HTTP cache validators captured at fetch time. Empty if the server didn't
	// send them. Used on subsequent fetches for If-None-Match / If-Modified-Since.
	ETag         string
	LastModified string
	// Re-crawl scheduling signals. ChangeCount = number of times the content
	// hash changed across fetches. LastChangedAt = unix seconds of the most
	// recent content change. Both feed DueForRefresh's interval heuristic.
	ChangeCount   int64
	LastChangedAt int64
	// PublishedAt is the document's publication date, extracted from JSON-LD
	// `datePublished` when present. Zero-value time.Time means "unknown" —
	// /search?since= and ?until= filters skip docs with zero PublishedAt
	// rather than treating them as 1970-01-01. Iter 77.
	PublishedAt time.Time
	// Author extracted from JSON-LD `author.name` or `author` (string form) at
	// parse time. Empty string means "unknown" (most pages don't carry it).
	// Iter 150 —SearchHit.author.
	Author string
	// Image URL extracted from og:image / twitter:image / JSON-LD `image`.
	// Empty when absent. Iter 155 —SearchHit.image rendering.
	Image string
	// Favicon URL extracted from <link rel="icon"> (and apple-touch-icon /
	// mask-icon variants). Resolved to absolute against the page URL. Empty
	// when absent. Iter 156 —"link-card" rendering.
	Favicon string
}

// Stats summarizes index state for the CLI.
type Stats struct {
	Documents int64
	Terms     int64
}

const schema = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS documents (
  id            INTEGER PRIMARY KEY,
  url           TEXT NOT NULL UNIQUE,
  domain        TEXT,
  title         TEXT,
  text          TEXT,
  lang          TEXT,
  source        TEXT,
  quality       REAL,
  fetched_at    INTEGER,
  content_sha   BLOB,
  doc_len       INTEGER NOT NULL DEFAULT 0,
  etag          TEXT NOT NULL DEFAULT '',
  last_modified TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_documents_domain ON documents(domain);

CREATE TABLE IF NOT EXISTS terms (
  id       INTEGER PRIMARY KEY,
  term     TEXT NOT NULL UNIQUE,
  doc_freq INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS postings (
  term_id  INTEGER NOT NULL,
  doc_id   INTEGER NOT NULL,
  tf       INTEGER NOT NULL,
  PRIMARY KEY (term_id, doc_id),
  FOREIGN KEY (term_id) REFERENCES terms(id) ON DELETE CASCADE,
  FOREIGN KEY (doc_id)  REFERENCES documents(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_postings_doc ON postings(doc_id);

CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v TEXT
);

CREATE TABLE IF NOT EXISTS frontier (
  url        TEXT PRIMARY KEY,
  depth      INTEGER NOT NULL,
  priority   REAL    NOT NULL DEFAULT 0,
  enqueued_at INTEGER NOT NULL,
  attempts   INTEGER NOT NULL DEFAULT 0,
  status     TEXT NOT NULL DEFAULT 'queued'   -- 'queued' | 'in_flight' | 'done' | 'error'
);
CREATE INDEX IF NOT EXISTS idx_frontier_status ON frontier(status, priority DESC);

CREATE TABLE IF NOT EXISTS passages (
  id          INTEGER PRIMARY KEY,
  doc_id      INTEGER NOT NULL,
  offset      INTEGER NOT NULL DEFAULT 0,
  length      INTEGER NOT NULL DEFAULT 0,
  model       TEXT    NOT NULL,
  dim         INTEGER NOT NULL,
  embedding   BLOB    NOT NULL,                -- float32 little-endian, length = dim * 4
  embedded_at INTEGER NOT NULL,
  FOREIGN KEY (doc_id) REFERENCES documents(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_passages_doc   ON passages(doc_id);
CREATE INDEX IF NOT EXISTS idx_passages_model ON passages(model);

-- Calibration scaffold: every recorded outcome lets us later fit a model
-- that maps raw confidence scores to P(useful). The endpoint emitting these
-- rows is opt-in (POST /feedback); the table just exists waiting for data.
CREATE TABLE IF NOT EXISTS query_outcomes (
  id          INTEGER PRIMARY KEY,
  query       TEXT NOT NULL,
  url         TEXT NOT NULL,
  score       REAL NOT NULL DEFAULT 0,
  useful      INTEGER NOT NULL,                -- 0 = not useful, 1 = useful
  source      TEXT NOT NULL DEFAULT '',        -- 'thumbs' | 'click' | 'explicit' | etc.
  recorded_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_outcomes_useful   ON query_outcomes(useful);
CREATE INDEX IF NOT EXISTS idx_outcomes_recorded ON query_outcomes(recorded_at);

-- Persistent paraphrase cache. (query_hash, model) is the key; paraphrases is
-- a JSON array. Lets cold processes (refresh sidecar, second Cloud Run instance,
-- post-restart server) skip the LLM call on popular queries.
CREATE TABLE IF NOT EXISTS query_paraphrases (
  id          INTEGER PRIMARY KEY,
  query_hash  BLOB NOT NULL,
  model       TEXT NOT NULL,
  paraphrases TEXT NOT NULL,                   -- JSON array
  created_at  INTEGER NOT NULL,
  UNIQUE(query_hash, model)
);
CREATE INDEX IF NOT EXISTS idx_paraphrases_lookup ON query_paraphrases(query_hash, model);

-- Persistent HyDE (Hypothetical Document Embeddings) passage cache, iter 162.
-- Same shape as query_paraphrases — single passage string instead of a JSON
-- array. Keyed on (query_hash, model) so different chat models (gpt-4o-mini
-- vs claude-haiku) don't share cache entries, since their hypothetical
-- passages have different stylistic shapes.
CREATE TABLE IF NOT EXISTS query_hyde (
  id          INTEGER PRIMARY KEY,
  query_hash  BLOB NOT NULL,
  model       TEXT NOT NULL,
  passage     TEXT NOT NULL,
  created_at  INTEGER NOT NULL,
  UNIQUE(query_hash, model)
);
CREATE INDEX IF NOT EXISTS idx_hyde_lookup ON query_hyde(query_hash, model);
`

// Open initializes (or opens) a store rooted at dataDir. Idempotent: safe to
// call against an existing DB. Applies any pending schema migrations.
func Open(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dataDir, err)
	}
	dbPath := filepath.Join(dataDir, "cosift.db")
	db, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	// Iter 181: SQLite only allows ONE writer at a time. The default Go
	// database/sql connection pool (unlimited) means N goroutines each grab
	// their own connection and then queue on SQLite's writer-lock — paying
	// connection-pool overhead for no parallelism. SetMaxOpenConns(1) forces
	// all operations through a single connection, which SQLite serializes
	// internally with no wasted lock-wait. Confirmed: 30s real-crawl test
	// went from ~73 docs/60s (post-atomic-claim, unlimited pool) → faster
	// throughput because writes serialize cleanly through one connection.
	// Read-heavy workloads (after crawl, during /search) are unaffected:
	// SQLite single-connection reads are still memory-fast.
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(context.Background(), schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("schema: %w", err)
	}
	s := &Store{db: db, path: dbPath}
	if err := s.migrate(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

// OpenMemory returns a store backed by an in-memory SQLite database. Useful
// for tests that don't need on-disk persistence. Honors the project's
// "keep disk usage low for tests" directive language. Iter 133.
//
// Implementation note: SQLite's `:memory:` mode is per-connection — each
// connection gets its own empty DB. To make a single Store usable, we cap the
// connection pool at 1, so every query uses the same underlying connection
// and sees the same data. This trades write parallelism for memory-mode
// simplicity, which is the right call for tests.
//
// Returned store has no on-disk artifacts. Close() releases the memory.
func OpenMemory() (*Store, error) {
	db, err := sql.Open("sqlite", ":memory:?_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	// `:memory:` is per-connection. Cap at 1 so the pool always reuses the
	// same connection; without this, the second query would get a fresh
	// empty DB and the schema wouldn't be found.
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(context.Background(), schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("schema: %w", err)
	}
	s := &Store{db: db, path: ":memory:"}
	if err := s.migrate(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

// migrate applies missing columns to existing databases that pre-date a feature.
// SQLite's CREATE TABLE IF NOT EXISTS doesn't ALTER existing tables, so any
// new column added to the canonical schema needs an explicit, idempotent ALTER.
func (s *Store) migrate(ctx context.Context) error {
	for _, m := range []struct{ table, column, def string }{
		{"documents", "etag", "TEXT NOT NULL DEFAULT ''"},
		{"documents", "last_modified", "TEXT NOT NULL DEFAULT ''"},
		{"documents", "change_count", "INTEGER NOT NULL DEFAULT 0"},
		{"documents", "last_changed_at", "INTEGER NOT NULL DEFAULT 0"},
		// Iter 77: publication date from JSON-LD datePublished. 0 = unknown.
		{"documents", "published_at", "INTEGER NOT NULL DEFAULT 0"},
		// Iter 150: JSON-LD author.name. Empty = unknown.
		{"documents", "author", "TEXT NOT NULL DEFAULT ''"},
		// Iter 155: og:image / twitter:image / JSON-LD image URL. Empty = none.
		{"documents", "image", "TEXT NOT NULL DEFAULT ''"},
		// Iter 156: <link rel="icon"> URL (resolved to absolute). Empty = none.
		{"documents", "favicon", "TEXT NOT NULL DEFAULT ''"},
		// Iter 85: last error message from a failed crawl attempt. Surfaces in
		// `cosift crawl-errors` for operator visibility on why URLs failed.
		// Empty string means "no error" (queued/in_flight/done items).
		{"frontier", "last_error", "TEXT NOT NULL DEFAULT ''"},
		// Iter 190: hostname extracted from URL at enqueue time. Populated by
		// PushFrontier; used by ClaimFrontier for host-fair scheduling so the
		// worker pool spreads across hosts instead of stacking on a single
		// fanout-heavy host (cppreference, Wikipedia, ...).
		{"frontier", "host", "TEXT NOT NULL DEFAULT ''"},
		// Iter 194: distinguishes the FIRST index time from the LAST fetch
		// time. fetched_at is bumped on every re-fetch (including
		// content-hash-deduped idempotent re-fetches); without a separate
		// "first indexed" column, crawl-status rolling rates double-count
		// re-fetched docs as new ones. UpsertDocument only writes this on
		// initial INSERT (the ON CONFLICT UPDATE clause leaves it unchanged).
		{"documents", "first_indexed_at", "INTEGER NOT NULL DEFAULT 0"},
	} {
		has, err := s.columnExists(ctx, m.table, m.column)
		if err != nil {
			return err
		}
		if has {
			continue
		}
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;", m.table, m.column, m.def)
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("add %s.%s: %w", m.table, m.column, err)
		}
	}
	// Iter 190: backfill host for any frontier rows missing it (existing data
	// dirs predate the column). Cheap one-shot UPDATE using SQLite string fns.
	// instr/substr extract the host portion between '://' and the next '/'.
	if _, err := s.db.ExecContext(ctx, `
UPDATE frontier
SET host = CASE
  WHEN instr(substr(url, instr(url, '://') + 3), '/') > 0
    THEN substr(substr(url, instr(url, '://') + 3), 1, instr(substr(url, instr(url, '://') + 3), '/') - 1)
  ELSE substr(url, instr(url, '://') + 3)
END
WHERE host = '' AND url LIKE '%://%';`); err != nil {
		return fmt.Errorf("backfill frontier.host: %w", err)
	}
	// Iter 194: backfill first_indexed_at from fetched_at for docs that
	// predate the column. Best approximation — we lost the first-fetch
	// timestamp for those rows. New rows get the right value via the
	// INSERT clause in UpsertDocument.
	if _, err := s.db.ExecContext(ctx,
		`UPDATE documents SET first_indexed_at = fetched_at WHERE first_indexed_at = 0;`); err != nil {
		return fmt.Errorf("backfill documents.first_indexed_at: %w", err)
	}
	// Iter 190: index for host-fair claim. EXISTS subquery on in_flight rows
	// per host needs this to stay fast as the queue grows to 100k+.
	if _, err := s.db.ExecContext(ctx,
		`CREATE INDEX IF NOT EXISTS idx_frontier_host_status ON frontier(host, status);`); err != nil {
		return fmt.Errorf("create idx_frontier_host_status: %w", err)
	}
	return nil
}

func (s *Store) columnExists(ctx context.Context, table, column string) (bool, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s);", table))
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid     int
			name    string
			ctype   string
			notnull int
			dflt    sql.NullString
			pk      int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

// Close flushes and closes the database.
func (s *Store) Close() error {
	return s.db.Close()
}

// DB exposes the underlying sql.DB for callers that need transactional control
// (the indexer batches term inserts in one tx).
func (s *Store) DB() *sql.DB { return s.db }

// UpsertDocument inserts a document or updates an existing row by URL.
// Returns the document ID (existing or newly assigned).
func (s *Store) UpsertDocument(ctx context.Context, d *Document) (int64, error) {
	// Iter 194: first_indexed_at is set ONLY on initial INSERT. The ON CONFLICT
	// UPDATE clause deliberately omits it so re-fetches don't bump it. This is
	// what crawl-status' rolling-rate windows rely on to distinguish new
	// indexed docs from idempotent re-fetches.
	const q = `
INSERT INTO documents (url, domain, title, text, lang, source, quality, fetched_at, content_sha, doc_len, etag, last_modified, change_count, last_changed_at, published_at, author, image, favicon, first_indexed_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(url) DO UPDATE SET
  domain          = excluded.domain,
  title           = excluded.title,
  text            = excluded.text,
  lang            = excluded.lang,
  source          = excluded.source,
  quality         = excluded.quality,
  fetched_at      = excluded.fetched_at,
  content_sha     = excluded.content_sha,
  doc_len         = excluded.doc_len,
  etag            = excluded.etag,
  last_modified   = excluded.last_modified,
  change_count    = excluded.change_count,
  last_changed_at = excluded.last_changed_at,
  published_at    = excluded.published_at,
  author          = excluded.author,
  image           = excluded.image,
  favicon         = excluded.favicon
RETURNING id;`
	var publishedUnix int64
	if !d.PublishedAt.IsZero() {
		publishedUnix = d.PublishedAt.Unix()
	}
	var id int64
	err := s.db.QueryRowContext(ctx, q,
		d.URL, d.Domain, d.Title, d.Text, d.Lang,
		d.Source, d.Quality, d.FetchedAt.Unix(), d.ContentSHA, len(d.Text),
		d.ETag, d.LastModified, d.ChangeCount, d.LastChangedAt,
		publishedUnix, d.Author, d.Image, d.Favicon,
		d.FetchedAt.Unix(), // first_indexed_at — set on INSERT only; ON CONFLICT UPDATE leaves it
	).Scan(&id)
	if err != nil {
		return 0, err
	}
	d.ID = id
	return id, nil
}

// GetDocByURL retrieves a single document, or sql.ErrNoRows if missing.
func (s *Store) GetDocByURL(ctx context.Context, url string) (*Document, error) {
	const q = `SELECT id, url, COALESCE(domain,''), COALESCE(title,''), COALESCE(text,''), COALESCE(lang,''), COALESCE(source,''), COALESCE(quality,0), COALESCE(fetched_at,0), content_sha, COALESCE(etag,''), COALESCE(last_modified,''), COALESCE(change_count,0), COALESCE(last_changed_at,0), COALESCE(published_at,0), COALESCE(author,''), COALESCE(image,''), COALESCE(favicon,'') FROM documents WHERE url = ?;`
	d := &Document{}
	var fetched, published int64
	err := s.db.QueryRowContext(ctx, q, url).Scan(
		&d.ID, &d.URL, &d.Domain, &d.Title, &d.Text, &d.Lang, &d.Source, &d.Quality, &fetched, &d.ContentSHA, &d.ETag, &d.LastModified, &d.ChangeCount, &d.LastChangedAt, &published, &d.Author, &d.Image, &d.Favicon,
	)
	if err != nil {
		return nil, err
	}
	d.FetchedAt = time.Unix(fetched, 0)
	if published > 0 {
		d.PublishedAt = time.Unix(published, 0)
	}
	return d, nil
}

// DocMeta is a slimmed-down view of a document carrying only the fields the
// /search response needs. Iter-82 added Domain + PublishedAt to avoid pulling
// full document text per hit. Iter-83 added Excerpt — a short prefix of the
// document body, retrieved server-side via SQL `substr` so we don't pull KB
// of text just to extract the first few hundred chars.
type DocMeta struct {
	URL         string
	Domain      string
	PublishedAt time.Time
	Excerpt     string // first ~500 chars of body, for /search fallback when no Highlight is available
	Author      string // JSON-LD author.name; empty when absent. Iter 150.
	Image       string // og:image / twitter:image / JSON-LD image. Empty when absent. Iter 155.
	Favicon     string // <link rel="icon"> absolute URL. Empty when absent. Iter 156.
}

// GetDocMetas batched-fetches DocMeta records for the given URLs in one SQL
// query. Result is keyed by URL; missing URLs are absent from the map (not an
// error). Used by `/search` (iter 82) to enrich its response with domain +
// PublishedAt without N round-trips for a K-result page.
//
// SQLite has a default ~999 parameter ceiling per query; we chunk into batches
// of 500 to stay well below. /search K is bounded to 100 so a single call
// always fits in one batch in practice.
func (s *Store) GetDocMetas(ctx context.Context, urls []string) (map[string]DocMeta, error) {
	out := make(map[string]DocMeta, len(urls))
	if len(urls) == 0 {
		return out, nil
	}
	const batchSize = 500
	for start := 0; start < len(urls); start += batchSize {
		end := start + batchSize
		if end > len(urls) {
			end = len(urls)
		}
		chunk := urls[start:end]
		placeholders := strings.Repeat("?,", len(chunk))
		placeholders = placeholders[:len(placeholders)-1] // trim trailing comma
		// Iter-83: substr(text, 1, 500) pulls only the first 500 chars of body
		// for use as a /search excerpt fallback. SQLite does the truncation
		// server-side, so even multi-MB documents transfer only the small slice.
		query := `SELECT url, COALESCE(domain,''), COALESCE(published_at,0), COALESCE(substr(text, 1, 500), ''), COALESCE(author,''), COALESCE(image,''), COALESCE(favicon,'') FROM documents WHERE url IN (` + placeholders + `);`
		args := make([]any, len(chunk))
		for i, u := range chunk {
			args[i] = u
		}
		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var m DocMeta
			var published int64
			if err := rows.Scan(&m.URL, &m.Domain, &published, &m.Excerpt, &m.Author, &m.Image, &m.Favicon); err != nil {
				_ = rows.Close()
				return nil, err
			}
			if published > 0 {
				m.PublishedAt = time.Unix(published, 0)
			}
			out[m.URL] = m
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		_ = rows.Close()
	}
	return out, nil
}

// GetDocTexts batched-fetches the body text of the given URLs, truncated to
// maxLen chars via SQLite-side substr. Iter 148: powers `/search?include_text=true`
// without forcing a /contents round-trip. Same chunking strategy as GetDocMetas
// to stay under SQLite's 999-parameter ceiling. maxLen ≤ 0 → no truncation;
// callers should cap maxLen explicitly so a stray ?include_text=true on a
// multi-MB corpus doesn't blow up bandwidth.
func (s *Store) GetDocTexts(ctx context.Context, urls []string, maxLen int) (map[string]string, error) {
	out := make(map[string]string, len(urls))
	if len(urls) == 0 {
		return out, nil
	}
	const batchSize = 500
	textCol := "COALESCE(text, '')"
	if maxLen > 0 {
		textCol = fmt.Sprintf("COALESCE(substr(text, 1, %d), '')", maxLen)
	}
	for start := 0; start < len(urls); start += batchSize {
		end := start + batchSize
		if end > len(urls) {
			end = len(urls)
		}
		chunk := urls[start:end]
		placeholders := strings.Repeat("?,", len(chunk))
		placeholders = placeholders[:len(placeholders)-1]
		query := `SELECT url, ` + textCol + ` FROM documents WHERE url IN (` + placeholders + `);`
		args := make([]any, len(chunk))
		for i, u := range chunk {
			args[i] = u
		}
		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var url, text string
			if err := rows.Scan(&url, &text); err != nil {
				_ = rows.Close()
				return nil, err
			}
			out[url] = text
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		_ = rows.Close()
	}
	return out, nil
}

// Stats returns counts for the CLI.
func (s *Store) Stats(ctx context.Context) (Stats, error) {
	var st Stats
	row := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM documents;`)
	if err := row.Scan(&st.Documents); err != nil {
		return st, err
	}
	row = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM terms;`)
	if err := row.Scan(&st.Terms); err != nil {
		return st, err
	}
	return st, nil
}

// CrawlStatusReport bundles the operator-facing crawl snapshot returned by
// CrawlStatus. Single read per field, safe to run against a SQLite WAL DB
// being actively written by a `cosift crawl` process. Iter 193.
type CrawlStatusReport struct {
	Documents        int64
	Terms            int64
	UniqueHosts      int64
	FrontierByStatus []FrontierStatusCount
	TopHosts         []HostDocCount
	ErrorClasses     []ErrorClassCount
	RateWindows      []RateWindow
}

type FrontierStatusCount struct {
	Status string
	Count  int64
}

type HostDocCount struct {
	Host  string
	Count int64
}

type ErrorClassCount struct {
	LastError string
	Count     int64
}

type RateWindow struct {
	WindowSec int   // 300, 900, 1800 — match cosift crawl-status default windows
	Count     int64 // docs with fetched_at >= now - WindowSec
}

// CrawlStatus assembles the cosift-crawl-status snapshot. WAL mode lets us
// read this while a crawler writes — no locking required for queries.
func (s *Store) CrawlStatus(ctx context.Context, topHostsN, topErrorsN int) (CrawlStatusReport, error) {
	var r CrawlStatusReport
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM documents;`).Scan(&r.Documents); err != nil {
		return r, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM terms;`).Scan(&r.Terms); err != nil {
		return r, err
	}
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(DISTINCT host) FROM frontier WHERE status='done';`).Scan(&r.UniqueHosts); err != nil {
		return r, err
	}

	// Frontier breakdown by status — ordered for stable display.
	rows, err := s.db.QueryContext(ctx,
		`SELECT status, COUNT(*) FROM frontier GROUP BY status ORDER BY 2 DESC;`)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var fs FrontierStatusCount
		if err := rows.Scan(&fs.Status, &fs.Count); err != nil {
			rows.Close()
			return r, err
		}
		r.FrontierByStatus = append(r.FrontierByStatus, fs)
	}
	rows.Close()

	// Top hosts by indexed doc count (frontier.status='done' implies a doc row
	// was successfully written; using frontier.host avoids a join).
	rows, err = s.db.QueryContext(ctx,
		`SELECT host, COUNT(*) FROM frontier WHERE status='done' AND host!='' GROUP BY host ORDER BY 2 DESC LIMIT ?;`,
		topHostsN)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var h HostDocCount
		if err := rows.Scan(&h.Host, &h.Count); err != nil {
			rows.Close()
			return r, err
		}
		r.TopHosts = append(r.TopHosts, h)
	}
	rows.Close()

	// Top error classes — operators triaging a failing crawl care about which
	// failure mode is dominant. last_error already lives on frontier (iter 85).
	rows, err = s.db.QueryContext(ctx,
		`SELECT last_error, COUNT(*) FROM frontier WHERE status='error' AND last_error!='' GROUP BY last_error ORDER BY 2 DESC LIMIT ?;`,
		topErrorsN)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var e ErrorClassCount
		if err := rows.Scan(&e.LastError, &e.Count); err != nil {
			rows.Close()
			return r, err
		}
		r.ErrorClasses = append(r.ErrorClasses, e)
	}
	rows.Close()

	// Rolling-window rates: 5, 15, 30 minute windows on documents.first_indexed_at.
	// Iter 194: switched from fetched_at to first_indexed_at because fetched_at
	// is bumped on every re-fetch (including content-hash-deduped idempotent
	// re-fetches), which made the "rate" the FETCH rate, not the NEW-DOC rate
	// that operators want. first_indexed_at is set once on initial INSERT.
	for _, sec := range []int{300, 900, 1800} {
		var c int64
		if err := s.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM documents WHERE first_indexed_at >= strftime('%s','now')-?;`, sec,
		).Scan(&c); err != nil {
			return r, err
		}
		r.RateWindows = append(r.RateWindows, RateWindow{WindowSec: sec, Count: c})
	}
	return r, nil
}

// ErrNotFound is a convenience alias.
var ErrNotFound = errors.New("not found")

// ListDocuments returns all documents in id order. limit=0 means no limit.
// Streams via the standard rows interface — fine up to a few million rows.
// Above that, callers should use cursor-based pagination instead.
func (s *Store) ListDocuments(ctx context.Context, limit int) ([]*Document, error) {
	// Iter 104: include published_at so callers can filter by date without an
	// N+1 lookup. Zero published_at means "no JSON-LD datePublished extracted"
	// (same convention as Document.PublishedAt elsewhere).
	q := `SELECT id, url, COALESCE(domain,''), COALESCE(title,''), COALESCE(text,''), COALESCE(lang,''), COALESCE(source,''), COALESCE(quality,0), COALESCE(fetched_at,0), content_sha, COALESCE(etag,''), COALESCE(last_modified,''), COALESCE(change_count,0), COALESCE(last_changed_at,0), COALESCE(published_at,0), COALESCE(author,''), COALESCE(image,''), COALESCE(favicon,'') FROM documents ORDER BY id ASC`
	args := []any{}
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, q+";", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Document
	for rows.Next() {
		d := &Document{}
		var fetched, published int64
		if err := rows.Scan(&d.ID, &d.URL, &d.Domain, &d.Title, &d.Text, &d.Lang, &d.Source, &d.Quality, &fetched, &d.ContentSHA, &d.ETag, &d.LastModified, &d.ChangeCount, &d.LastChangedAt, &published, &d.Author, &d.Image, &d.Favicon); err != nil {
			return nil, err
		}
		d.FetchedAt = time.Unix(fetched, 0)
		if published > 0 {
			d.PublishedAt = time.Unix(published, 0)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// CountByDomain returns top-N (domain, count) buckets for the admin dashboard.
// SitemapEntry is one row for /sitemap.xml output: URL + lastmod proxy.
// Iter 86 added this for the sitemap-output endpoint. LastChangedAt is the
// best "lastmod" signal cosift has — when the indexed content last changed.
// Zero value means "never changed since first fetch" (the lastmod is just
// omitted in that case).
type SitemapEntry struct {
	URL           string
	LastChangedAt time.Time
}

// ListDocSitemapEntries returns slim (URL, LastChangedAt) records for sitemap
// output, in deterministic ID order. Caller is responsible for the sitemap-spec
// 50,000-URL limit; pass `limit=50000`. Iter 86.
func (s *Store) ListDocSitemapEntries(ctx context.Context, limit int) ([]SitemapEntry, error) {
	q := `SELECT url, COALESCE(last_changed_at,0) FROM documents ORDER BY id ASC`
	args := []any{}
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, q+";", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]SitemapEntry, 0, 128)
	for rows.Next() {
		var e SitemapEntry
		var changed int64
		if err := rows.Scan(&e.URL, &changed); err != nil {
			return nil, err
		}
		if changed > 0 {
			e.LastChangedAt = time.Unix(changed, 0)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) CountByDomain(ctx context.Context, topN int) (map[string]int64, error) {
	if topN <= 0 {
		topN = 20
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT COALESCE(domain,''), COUNT(*) FROM documents GROUP BY domain ORDER BY 2 DESC LIMIT ?;`, topN)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]int64, topN)
	for rows.Next() {
		var d string
		var n int64
		if err := rows.Scan(&d, &n); err != nil {
			return nil, err
		}
		out[d] = n
	}
	return out, rows.Err()
}

// ListURLsByDomain returns every document URL whose domain matches `pattern`
// using iter-79 suffix-on-dot-boundary semantics: `example.com` matches
// `example.com` AND `blog.example.com`, but NOT `evilexample.com`.
// Caller-side filtering would require a full table scan; pushing this into
// SQL uses the existing idx_documents_domain index. Iter 110.
func (s *Store) ListURLsByDomain(ctx context.Context, pattern string) ([]string, error) {
	pattern = strings.ToLower(strings.TrimSpace(pattern))
	if pattern == "" {
		return nil, errors.New("domain pattern required")
	}
	// `domain = ?` catches the exact match; `domain LIKE '%.' || ?` catches
	// any subdomain. Together they implement the iter-79 suffix-on-dot-boundary
	// rule: bare-host equality OR strict subdomain.
	rows, err := s.db.QueryContext(ctx,
		`SELECT url FROM documents WHERE domain = ? OR domain LIKE '%.' || ? ORDER BY id ASC;`,
		pattern, pattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// CountPassagesAllModels returns total passage count across all models.
func (s *Store) CountPassagesAllModels(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM passages;`).Scan(&n)
	return n, err
}

// CountParaphrases returns the total cached paraphrase entry count.
func (s *Store) CountParaphrases(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM query_paraphrases;`).Scan(&n)
	return n, err
}

// CountDocsWithPublishedAt returns the number of documents that have a known
// publication date (iter-77 published_at column > 0). Lets operators see how
// much of their corpus is filterable via /search?since=/?until= (iter 77) or
// sortable via ?sort=date_desc/date_asc (iter 78). Iter 80.
func (s *Store) CountDocsWithPublishedAt(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM documents WHERE published_at > 0;`).Scan(&n)
	return n, err
}

// DropPassagesNotModel removes all passage rows whose model != the argument.
// Used by `cosift reembed -drop-old` to finalize an embedding-model upgrade.
// Returns the row count removed.
func (s *Store) DropPassagesNotModel(ctx context.Context, model string) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM passages WHERE model != ?;`, model)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// DropOrphanedPassages removes passages whose parent doc is gone. The schema
// has ON DELETE CASCADE but rows can still get orphaned by raw SQL maintenance
// or by an older binary that wrote without the FK. Defensive cleanup.
func (s *Store) DropOrphanedPassages(ctx context.Context) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM passages WHERE doc_id NOT IN (SELECT id FROM documents);`)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// DropOrphanedTerms removes terms whose postings no longer reference any doc.
// Happens naturally when documents are removed: postings cascade-delete, but
// the terms row stays. At million-doc scale these add up.
func (s *Store) DropOrphanedTerms(ctx context.Context) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM terms WHERE id NOT IN (SELECT DISTINCT term_id FROM postings);`)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// PassageModels returns the distinct embedding model_ids currently in the
// passages table, sorted. Empty slice if none.
func (s *Store) PassageModels(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT model FROM passages ORDER BY model ASC;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var m string
		if err := rows.Scan(&m); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// Outcome is one (query, url, useful?) row used for confidence calibration.
type Outcome struct {
	ID         int64
	Query      string
	URL        string
	Score      float64
	Useful     bool
	Source     string
	RecordedAt time.Time
}

// RecordOutcome inserts a feedback row. Tiny by design — the calibration
// model is fit offline once enough rows accumulate.
func (s *Store) RecordOutcome(ctx context.Context, o *Outcome) error {
	useful := 0
	if o.Useful {
		useful = 1
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO query_outcomes (query, url, score, useful, source, recorded_at) VALUES (?, ?, ?, ?, ?, ?);`,
		o.Query, o.URL, o.Score, useful, o.Source, time.Now().Unix())
	return err
}

// CountOutcomes returns total + count where useful=1. Used to know when
// calibration becomes feasible (~10k entries with both classes represented).
func (s *Store) CountOutcomes(ctx context.Context) (total, useful int64, err error) {
	row := s.db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(useful), 0) FROM query_outcomes;`)
	err = row.Scan(&total, &useful)
	return
}

// GetParaphrases returns cached paraphrases for the given (query, model), or
// nil if absent. Normalizes the query (lower + trim) before hashing so trivial
// variations share a cache entry.
func (s *Store) GetParaphrases(ctx context.Context, model, query string) ([]string, error) {
	h := paraphraseHash(query)
	var raw string
	err := s.db.QueryRowContext(ctx,
		`SELECT paraphrases FROM query_paraphrases WHERE query_hash = ? AND model = ?;`,
		h[:], model).Scan(&raw)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var arr []string
	if err := json.Unmarshal([]byte(raw), &arr); err != nil {
		return nil, fmt.Errorf("decode paraphrases: %w", err)
	}
	return arr, nil
}

// PruneStaleParaphrases deletes rows older than maxAge. Drift defense — over
// time the indexed corpus shifts and old paraphrases stop matching well.
// maxAge=0 is a no-op.
func (s *Store) PruneStaleParaphrases(ctx context.Context, maxAge time.Duration) (int64, error) {
	if maxAge <= 0 {
		return 0, nil
	}
	cutoff := time.Now().Add(-maxAge).Unix()
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM query_paraphrases WHERE created_at < ?;`, cutoff)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// SaveParaphrases upserts a (query, model) paraphrase set. Idempotent on the
// (query_hash, model) unique key.
func (s *Store) SaveParaphrases(ctx context.Context, model, query string, paraphrases []string) error {
	if len(paraphrases) == 0 {
		return nil
	}
	h := paraphraseHash(query)
	b, err := json.Marshal(paraphrases)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO query_paraphrases (query_hash, model, paraphrases, created_at)
VALUES (?, ?, ?, ?)
ON CONFLICT(query_hash, model) DO UPDATE SET
  paraphrases = excluded.paraphrases,
  created_at = excluded.created_at;`,
		h[:], model, string(b), time.Now().Unix())
	return err
}

func paraphraseHash(query string) [32]byte {
	return sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(query))))
}

// GetHyDE returns the cached HyDE passage for (query, model), or "" if absent.
// Same normalization as paraphraseHash (lower + trim) so casing variants share
// an entry. Iter 162.
func (s *Store) GetHyDE(ctx context.Context, model, query string) (string, error) {
	h := paraphraseHash(query)
	var passage string
	err := s.db.QueryRowContext(ctx,
		`SELECT passage FROM query_hyde WHERE query_hash = ? AND model = ?;`,
		h[:], model).Scan(&passage)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return passage, nil
}

// SaveHyDE upserts a (query, model) → passage entry. Iter 162.
func (s *Store) SaveHyDE(ctx context.Context, model, query, passage string) error {
	if passage == "" {
		return nil
	}
	h := paraphraseHash(query)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO query_hyde (query_hash, model, passage, created_at)
VALUES (?, ?, ?, ?)
ON CONFLICT(query_hash, model) DO UPDATE SET
  passage = excluded.passage,
  created_at = excluded.created_at;`,
		h[:], model, passage, time.Now().Unix())
	return err
}

// PruneStaleHyDE deletes rows older than maxAge. Drift defense — over time
// the corpus or the chat model shifts and old passages stop matching well.
// maxAge=0 is a no-op. Iter 162.
func (s *Store) PruneStaleHyDE(ctx context.Context, maxAge time.Duration) (int64, error) {
	if maxAge <= 0 {
		return 0, nil
	}
	cutoff := time.Now().Add(-maxAge).Unix()
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM query_hyde WHERE created_at < ?;`, cutoff)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// CountHyDE returns the number of cached HyDE passages. Useful for the
// operator dashboard / metrics. Iter 162.
func (s *Store) CountHyDE(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM query_hyde;`).Scan(&n)
	return n, err
}

// ListOutcomes streams query_outcomes in recorded_at order. limit=0 → all.
func (s *Store) ListOutcomes(ctx context.Context, limit int) ([]*Outcome, error) {
	q := `SELECT id, query, url, score, useful, COALESCE(source,''), recorded_at FROM query_outcomes ORDER BY recorded_at ASC`
	args := []any{}
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, q+";", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Outcome
	for rows.Next() {
		o := &Outcome{}
		var useful int
		var ts int64
		if err := rows.Scan(&o.ID, &o.Query, &o.URL, &o.Score, &useful, &o.Source, &ts); err != nil {
			return nil, err
		}
		o.Useful = useful == 1
		o.RecordedAt = time.Unix(ts, 0)
		out = append(out, o)
	}
	return out, rows.Err()
}

// GCErroredFrontier deletes frontier rows that have failed `minAttempts` or
// more times. Returns the row count removed.
func (s *Store) GCErroredFrontier(ctx context.Context, minAttempts int) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM frontier WHERE status = 'error' AND attempts >= ?;`, minAttempts)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// Vacuum reclaims disk space after large deletes. Cheap on small DBs, can
// take minutes on multi-GB ones — caller's responsibility to schedule.
func (s *Store) Vacuum(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `VACUUM;`)
	return err
}

// FrontierItem is one URL waiting (or being processed) by the crawler.
type FrontierItem struct {
	URL      string
	Depth    int
	Priority float64
}

// FrontierStats summarizes the persistent frontier by status.
type FrontierStats struct {
	Queued   int64
	InFlight int64
	Done     int64
	Errored  int64
}

// extractHost returns the lowercased host portion of a URL string.
// Pure-string parsing — avoids the cost of url.Parse on the hot enqueue path,
// and the input is already canonicalized when it reaches the store.
func extractHost(rawURL string) string {
	i := strings.Index(rawURL, "://")
	if i < 0 {
		return ""
	}
	rest := rawURL[i+3:]
	if j := strings.IndexByte(rest, '/'); j >= 0 {
		rest = rest[:j]
	}
	if k := strings.IndexByte(rest, '?'); k >= 0 {
		rest = rest[:k]
	}
	return strings.ToLower(rest)
}

// PushFrontier inserts a URL into the queue at the given depth/priority.
// No-op if the URL is already present (regardless of its current status).
func (s *Store) PushFrontier(ctx context.Context, url string, depth int, priority float64) error {
	const q = `INSERT OR IGNORE INTO frontier (url, depth, priority, enqueued_at, host) VALUES (?, ?, ?, ?, ?);`
	_, err := s.db.ExecContext(ctx, q, url, depth, priority, time.Now().Unix(), extractHost(url))
	return err
}

// ClaimFrontier atomically grabs one queued item, flips it to in_flight, and returns it.
// Returns ok=false if the queue is empty.
func (s *Store) ClaimFrontier(ctx context.Context) (FrontierItem, bool, error) {
	// Iter 181: atomic single-statement claim via UPDATE...RETURNING. Pre-fix,
	// this was BeginTx → SELECT → UPDATE → COMMIT, which under SQLite WAL with
	// multiple crawler workers produced "database is locked (517) — SQLITE_BUSY_SNAPSHOT":
	// the SELECT established a read snapshot, another worker committed an UPDATE,
	// the snapshot was invalidated, the UPDATE failed. The atomic statement
	// acquires the write lock for its duration; SQLite's busy_timeout (5s, set
	// in Open) serializes concurrent claims cleanly without the snapshot race.
	//
	// Surfaced by the iter-181 real-crawl integration test (`cosift crawl
	// https://go.dev/doc/effective_go` with default max_concurrent=8) — unit
	// tests against a single-goroutine OpenMemory store never hit the race.
	// Iter 190: host-fair scheduling. Prefer URLs whose host has no in-flight
	// row — keeps the worker pool spread across hosts instead of stacking on a
	// single fanout-heavy domain (cppreference, Wikipedia, ...) and waiting on
	// its per-host delay gate. The first ORDER BY term evaluates to 0 when no
	// in-flight row exists for the same host, 1 when one does, so SQLite picks
	// the fairest URL first and falls back to priority + age within ties.
	var it FrontierItem
	err := s.db.QueryRowContext(ctx, `
UPDATE frontier SET status = 'in_flight'
WHERE url = (
  SELECT f1.url FROM frontier f1
  WHERE f1.status = 'queued'
  ORDER BY
    EXISTS (
      SELECT 1 FROM frontier f2
      WHERE f2.status = 'in_flight' AND f2.host = f1.host
    ) ASC,
    f1.priority DESC,
    f1.enqueued_at ASC
  LIMIT 1
)
RETURNING url, depth, priority;`).Scan(&it.URL, &it.Depth, &it.Priority)
	if err == sql.ErrNoRows {
		return FrontierItem{}, false, nil
	}
	if err != nil {
		return FrontierItem{}, false, err
	}
	return it, true, nil
}

// CompleteFrontier marks an item as successfully processed.
func (s *Store) CompleteFrontier(ctx context.Context, url string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE frontier SET status = 'done' WHERE url = ?;`, url)
	return err
}

// FailFrontier marks an item as errored, incrementing the attempts counter.
// errMsg is stored in the new (iter-85) last_error column for operator
// visibility via `cosift crawl-errors`. Long messages are truncated at 500
// chars to bound storage — error strings can be unbounded in pathological
// cases (e.g. multiline stack traces from upstream libraries).
// The caller can decide whether to re-enqueue (e.g. after backoff) by setting status back to 'queued'.
func (s *Store) FailFrontier(ctx context.Context, url, errMsg string) error {
	if len(errMsg) > 500 {
		errMsg = errMsg[:497] + "..."
	}
	_, err := s.db.ExecContext(ctx, `
UPDATE frontier SET status = 'error', attempts = attempts + 1, last_error = ? WHERE url = ?;`, errMsg, url)
	return err
}

// FrontierError is one errored frontier entry — used by `cosift crawl-errors`
// to surface failed URLs with their reason for operator diagnosis. Iter 85.
type FrontierError struct {
	URL        string
	LastError  string
	Attempts   int
	EnqueuedAt time.Time
}

// ListErroredFrontier returns the most recently enqueued errored URLs, up to
// `limit`. Each carries the last error message captured by FailFrontier. Used
// by `cosift crawl-errors` for operator diagnosis. Iter 85.
func (s *Store) ListErroredFrontier(ctx context.Context, limit int) ([]FrontierError, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT url, COALESCE(last_error,''), attempts, enqueued_at FROM frontier WHERE status='error' ORDER BY enqueued_at DESC LIMIT ?;`,
		limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]FrontierError, 0, limit)
	for rows.Next() {
		var fe FrontierError
		var enqueued int64
		if err := rows.Scan(&fe.URL, &fe.LastError, &fe.Attempts, &enqueued); err != nil {
			return nil, err
		}
		fe.EnqueuedAt = time.Unix(enqueued, 0)
		out = append(out, fe)
	}
	return out, rows.Err()
}

// RecoverInFlight resets any in_flight items to queued. Call on startup —
// any item still in_flight after a crash never finished and should retry.
func (s *Store) RecoverInFlight(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `UPDATE frontier SET status = 'queued' WHERE status = 'in_flight';`)
	return err
}

// DueForRefresh returns URLs whose adaptive re-crawl interval has elapsed.
//
// Heuristic: refresh interval ≈ (now - last_changed_at) / 2, clamped to
// [minInterval, maxInterval]. The classic Web-archive "TTFB" rule: a page
// that hasn't changed in 30 days gets re-checked every ~15 days; one that
// changed yesterday gets re-checked in ~12h. Free for pages that send 304s.
//
// Pages with last_changed_at = 0 (never changed since first seen, or just
// added) get the minInterval treatment — re-check soon to learn their cadence.
func (s *Store) DueForRefresh(ctx context.Context, minInterval, maxInterval time.Duration, limit int) ([]string, error) {
	const q = `
SELECT url, fetched_at, last_changed_at FROM documents
WHERE fetched_at > 0
ORDER BY fetched_at ASC
LIMIT ?;`
	rows, err := s.db.QueryContext(ctx, q, limit*4) // over-fetch; we filter in Go
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	now := time.Now().Unix()
	minS := int64(minInterval.Seconds())
	maxS := int64(maxInterval.Seconds())
	var due []string
	for rows.Next() {
		var url string
		var fetched, changed int64
		if err := rows.Scan(&url, &fetched, &changed); err != nil {
			return nil, err
		}
		var interval int64
		if changed > 0 {
			interval = (now - changed) / 2
		} else {
			interval = minS // never observed a change → re-check at min cadence
		}
		if interval < minS {
			interval = minS
		}
		if interval > maxS {
			interval = maxS
		}
		if now-fetched >= interval {
			due = append(due, url)
			if len(due) >= limit {
				break
			}
		}
	}
	return due, rows.Err()
}

// MarkContentChanged records that this URL's content changed (or first-seen).
// Increments change_count, sets last_changed_at = now. Caller does the rest
// of the upsert; this is just the bookkeeping.
func (s *Store) MarkContentChanged(ctx context.Context, url string) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE documents SET change_count = change_count + 1, last_changed_at = ? WHERE url = ?;`,
		time.Now().Unix(), url)
	return err
}

// RecrawlURL forces a URL back into the queue regardless of its current status.
// If the URL is in the frontier, status flips to 'queued' and attempts resets.
// If not present, it's inserted at depth 0. Useful for refreshing stale pages,
// retrying errored rows, and forcing a re-embed after a model upgrade.
func (s *Store) RecrawlURL(ctx context.Context, url string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE frontier SET status = 'queued', attempts = 0 WHERE url = ?;`, url)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		return nil
	}
	return s.PushFrontier(ctx, url, 0, 1.0)
}

// Passage is one (doc_id, offset, length, model, vec) row.
type Passage struct {
	ID        int64
	DocID     int64
	Offset    int
	Length    int
	Model     string
	Embedding []float32
}

// PassageDoc is the joined (passage, doc.url, doc.title) tuple returned by
// LoadPassagesByModel — it's what the vector index actually needs.
type PassageDoc struct {
	PassageID int64
	URL       string
	Title     string
	Offset    int
	Length    int
	Vec       []float32
}

// UpsertPassage replaces any existing passage for (doc_id, offset) and writes
// the embedding. The embedding slice is encoded little-endian into the BLOB
// — same wire format as the embedding cache, so the two are byte-compatible.
func (s *Store) UpsertPassage(ctx context.Context, p *Passage) error {
	if p == nil || len(p.Embedding) == 0 {
		return errors.New("passage: empty embedding")
	}
	blob := encodeF32(p.Embedding)
	const q = `
INSERT INTO passages (doc_id, offset, length, model, dim, embedding, embedded_at)
VALUES (?, ?, ?, ?, ?, ?, ?);`
	// Simpler than upsert: delete-then-insert the (doc_id, offset, model) row.
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM passages WHERE doc_id = ? AND offset = ? AND model = ?;`,
		p.DocID, p.Offset, p.Model); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, q,
		p.DocID, p.Offset, p.Length, p.Model, len(p.Embedding), blob, time.Now().Unix())
	return err
}

// LoadPassagesByModel scans all passages for a given model and joins doc info.
// Returned vectors are independent slices, safe to retain.
func (s *Store) LoadPassagesByModel(ctx context.Context, model string) ([]PassageDoc, error) {
	const q = `
SELECT p.id, COALESCE(d.url, ''), COALESCE(d.title, ''), p.offset, p.length, p.embedding, p.dim
FROM passages p JOIN documents d ON d.id = p.doc_id
WHERE p.model = ?;`
	rows, err := s.db.QueryContext(ctx, q, model)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PassageDoc
	for rows.Next() {
		var pd PassageDoc
		var blob []byte
		var dim int
		if err := rows.Scan(&pd.PassageID, &pd.URL, &pd.Title, &pd.Offset, &pd.Length, &blob, &dim); err != nil {
			return nil, err
		}
		pd.Vec = decodeF32(blob, dim)
		out = append(out, pd)
	}
	return out, rows.Err()
}

// CountPassagesByModel returns how many embeddings exist for a given model.
func (s *Store) CountPassagesByModel(ctx context.Context, model string) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM passages WHERE model = ?;`, model).Scan(&n)
	return n, err
}

func encodeF32(v []float32) []byte {
	b := make([]byte, len(v)*4)
	for i, f := range v {
		u := math.Float32bits(f)
		b[i*4+0] = byte(u)
		b[i*4+1] = byte(u >> 8)
		b[i*4+2] = byte(u >> 16)
		b[i*4+3] = byte(u >> 24)
	}
	return b
}

func decodeF32(b []byte, dim int) []float32 {
	if len(b) < dim*4 {
		return nil
	}
	v := make([]float32, dim)
	for i := 0; i < dim; i++ {
		u := uint32(b[i*4]) | uint32(b[i*4+1])<<8 | uint32(b[i*4+2])<<16 | uint32(b[i*4+3])<<24
		v[i] = math.Float32frombits(u)
	}
	return v
}

// GetFrontierStats returns counts per status.
func (s *Store) GetFrontierStats(ctx context.Context) (FrontierStats, error) {
	var st FrontierStats
	rows, err := s.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM frontier GROUP BY status;`)
	if err != nil {
		return st, err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return st, err
		}
		switch status {
		case "queued":
			st.Queued = count
		case "in_flight":
			st.InFlight = count
		case "done":
			st.Done = count
		case "error":
			st.Errored = count
		}
	}
	return st, nil
}
