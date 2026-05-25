// Package index implements a BM25 lexical index over the document store.
//
// Algorithm: classic BM25 with k1=1.2, b=0.75. Stopword filter + simple
// alphanumeric tokenizer. Postings persisted in the SQLite store.
//
// Why custom (not Bleve / Tantivy): zero dep, fits in ~300 LOC, and at v0 scale
// (≤1M docs) the throughput is not the bottleneck. We can swap in Tantivy via
// cgo if BM25F or phrase queries become necessary.
package index

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/calinteodor/cosift/internal/store"
)

// BM25 parameters. Standard defaults; exposed if we want to tune per corpus.
const (
	K1 = 1.2
	B  = 0.75

	// TitleBoost is the per-occurrence multiplier applied to title tokens at
	// index time. Iter 197: title text is far more informative per token than
	// body text — modern search systems weight it 2-5x. We boost TF (not
	// doc_len), so docs with title-matching terms get a higher score without
	// changing length normalization. 3.0 is the value most IR literature
	// converges on; lower values undershoot, higher values over-promote
	// short pages with keyword-stuffed titles. Tunable per build; not a
	// per-corpus runtime knob yet (would need a re-index pass to take effect).
	TitleBoost = 3
)

// BM25 is the indexer + searcher.
type BM25 struct {
	store *store.Store
}

// Hit is a single search result.
type Hit struct {
	DocID int64
	URL   string
	Title string
	Score float64

	// Snippet is a short window of the document body centered on the earliest
	// query-term match, populated for top-k hits when text is available.
	// Empty when computation was skipped (text not indexed, or no terms hit).
	// Iter 199 — replaces BM25 hits' generic body-prefix excerpt with a
	// query-aware passage. Dense/hybrid hits keep their existing per-passage
	// Highlight (computed from the embedded passage span); this fills the
	// equivalent gap for BM25-only hits.
	Snippet       string
	SnippetOffset int
}

func NewBM25(s *store.Store) *BM25 {
	return &BM25{store: s}
}

// IndexDocument tokenizes and writes postings for the given doc.
// Replaces any existing postings for the doc.
//
// Iter 197: title tokens get TitleBoost-weighted TF. doc_len keeps the raw
// token count, so length normalization remains correct. Net effect: docs
// with the query term in their title rank above body-only matches.
func (b *BM25) IndexDocument(ctx context.Context, docID int64, title, text string) error {
	titleTokens := Tokenize(title)
	bodyTokens := Tokenize(text)
	if len(titleTokens)+len(bodyTokens) == 0 {
		return nil
	}

	// term -> tf in this doc. Title tokens contribute TitleBoost each.
	tf := make(map[string]int, len(titleTokens)+len(bodyTokens))
	for _, t := range titleTokens {
		tf[t] += TitleBoost
	}
	for _, t := range bodyTokens {
		tf[t]++
	}

	db := b.store.DB()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Remove existing postings for this doc (and decrement doc_freq).
	rows, err := tx.QueryContext(ctx, `SELECT term_id FROM postings WHERE doc_id = ?;`, docID)
	if err != nil {
		return err
	}
	var oldTerms []int64
	for rows.Next() {
		var tid int64
		if err := rows.Scan(&tid); err != nil {
			rows.Close()
			return err
		}
		oldTerms = append(oldTerms, tid)
	}
	rows.Close()
	if len(oldTerms) > 0 {
		if _, err := tx.ExecContext(ctx, `DELETE FROM postings WHERE doc_id = ?;`, docID); err != nil {
			return err
		}
		for _, tid := range oldTerms {
			if _, err := tx.ExecContext(ctx, `UPDATE terms SET doc_freq = doc_freq - 1 WHERE id = ?;`, tid); err != nil {
				return err
			}
		}
	}

	// Upsert terms + write postings.
	for term, freq := range tf {
		var termID int64
		err := tx.QueryRowContext(ctx, `
INSERT INTO terms (term, doc_freq) VALUES (?, 1)
ON CONFLICT(term) DO UPDATE SET doc_freq = doc_freq + 1
RETURNING id;`, term).Scan(&termID)
		if err != nil {
			return fmt.Errorf("upsert term %q: %w", term, err)
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO postings (term_id, doc_id, tf) VALUES (?, ?, ?);`,
			termID, docID, freq); err != nil {
			return fmt.Errorf("insert posting %q: %w", term, err)
		}
	}

	return tx.Commit()
}

// parsePhrases extracts double-quoted substrings from q. Returns the search
// query (with quote marks stripped — phrase tokens still participate in BM25)
// and a slice of verbatim phrases to filter the result set by. Iter 198.
//
//	parsePhrases(`raft "leader election"`)
//	  → searchQuery="raft  leader election", phrases=["leader election"]
//
// An unterminated trailing quote is treated as unquoted text (the user
// probably typed the leading quote by accident, not a syntax error).
func parsePhrases(q string) (string, []string) {
	var unquoted []string
	var phrases []string
	rest := q
	for {
		i := strings.IndexByte(rest, '"')
		if i < 0 {
			unquoted = append(unquoted, rest)
			break
		}
		unquoted = append(unquoted, rest[:i])
		rest = rest[i+1:]
		j := strings.IndexByte(rest, '"')
		if j < 0 {
			unquoted = append(unquoted, rest)
			break
		}
		phrase := strings.TrimSpace(rest[:j])
		if phrase != "" {
			phrases = append(phrases, strings.ToLower(phrase))
			// Include the phrase content in BM25 search too — it's still
			// term-level evidence, just additionally constrained.
			unquoted = append(unquoted, phrase)
		}
		rest = rest[j+1:]
	}
	return strings.Join(unquoted, " "), phrases
}

// Search returns the top-k hits for the query string. Supports phrase queries
// via double quotes: `"machine learning"` requires the phrase to appear
// verbatim (case-insensitive) in the document text. Multiple phrases are
// AND-combined. Iter 198.
func (b *BM25) Search(ctx context.Context, q string, k int) ([]Hit, error) {
	searchQ, phrases := parsePhrases(q)
	tokens := Tokenize(searchQ)
	if len(tokens) == 0 {
		return nil, nil
	}

	db := b.store.DB()

	var (
		totalDocs int64
		avgDocLen float64
	)
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(AVG(doc_len), 1) FROM documents WHERE doc_len > 0;`).Scan(&totalDocs, &avgDocLen); err != nil {
		return nil, err
	}
	if totalDocs == 0 {
		return nil, nil
	}
	if avgDocLen < 1 {
		avgDocLen = 1
	}

	// Aggregate scores per doc.
	scores := make(map[int64]float64)
	titles := make(map[int64]string)
	urls := make(map[int64]string)

	uniqTerms := dedupeStrings(tokens)
	for _, term := range uniqTerms {
		var termID int64
		var df int64
		err := db.QueryRowContext(ctx, `SELECT id, doc_freq FROM terms WHERE term = ?;`, term).Scan(&termID, &df)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return nil, err
		}
		if df == 0 {
			continue
		}
		idf := math.Log(1.0 + (float64(totalDocs)-float64(df)+0.5)/(float64(df)+0.5))

		const pq = `
SELECT p.doc_id, p.tf, d.doc_len, COALESCE(d.url,''), COALESCE(d.title,'')
FROM postings p JOIN documents d ON d.id = p.doc_id
WHERE p.term_id = ?;`
		rows, err := db.QueryContext(ctx, pq, termID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var docID int64
			var tf, docLen int64
			var url, title string
			if err := rows.Scan(&docID, &tf, &docLen, &url, &title); err != nil {
				rows.Close()
				return nil, err
			}
			if docLen <= 0 {
				docLen = 1
			}
			tfF := float64(tf)
			lenNorm := 1.0 - B + B*(float64(docLen)/avgDocLen)
			score := idf * ((tfF * (K1 + 1.0)) / (tfF + K1*lenNorm))
			scores[docID] += score
			if _, ok := titles[docID]; !ok {
				titles[docID] = title
				urls[docID] = url
			}
		}
		rows.Close()
	}

	hits := make([]Hit, 0, len(scores))
	for docID, sc := range scores {
		hits = append(hits, Hit{DocID: docID, URL: urls[docID], Title: titles[docID], Score: sc})
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i].Score > hits[j].Score })

	if len(phrases) > 0 {
		hits = b.filterByPhrases(ctx, hits, phrases, k)
	} else if k > 0 && len(hits) > k {
		hits = hits[:k]
	}
	return hits, nil
}

// filterByPhrases walks hits in score order and keeps docs whose text
// contains every phrase verbatim (case-insensitive). Fetches text in
// chunks to amortize the per-doc SQL round-trip — phrase filters typically
// keep a high fraction of top-k candidates so chunk size 64 is plenty.
//
// Iter 198: enables `?q="exact phrase"` queries without a positional-index
// schema change. Cost is one batched text fetch on top of BM25 scoring.
// Tradeoff: docs that match phrases but DIDN'T make the BM25 top set
// will be missed; in practice this is rare because phrase-bearing terms
// also score well term-wise. Acceptable for the v0 IR surface.
func (b *BM25) filterByPhrases(ctx context.Context, hits []Hit, phrases []string, k int) []Hit {
	if k <= 0 {
		k = 10
	}
	out := make([]Hit, 0, k)
	const chunkSize = 64
	for chunkStart := 0; chunkStart < len(hits) && len(out) < k; chunkStart += chunkSize {
		end := chunkStart + chunkSize
		if end > len(hits) {
			end = len(hits)
		}
		batch := hits[chunkStart:end]
		ids := make([]int64, len(batch))
		for i, h := range batch {
			ids[i] = h.DocID
		}
		texts, err := b.fetchTextsByID(ctx, ids)
		if err != nil {
			// SQL error: fall back to unfiltered top-k from what we have so
			// far. Better than failing the whole query.
			out = append(out, batch...)
			if len(out) > k {
				out = out[:k]
			}
			return out
		}
		for _, h := range batch {
			body := strings.ToLower(texts[h.DocID])
			titleLC := strings.ToLower(h.Title)
			ok := true
			for _, p := range phrases {
				// A phrase can match in title OR body (each is its own text
				// span; the user doesn't care which one carried it).
				if !strings.Contains(body, p) && !strings.Contains(titleLC, p) {
					ok = false
					break
				}
			}
			if ok {
				out = append(out, h)
				if len(out) >= k {
					return out
				}
			}
		}
	}
	return out
}

// fetchTextsByID returns id → text for the given ids in a single SELECT.
func (b *BM25) fetchTextsByID(ctx context.Context, ids []int64) (map[int64]string, error) {
	if len(ids) == 0 {
		return map[int64]string{}, nil
	}
	placeholders := strings.Repeat("?,", len(ids))
	placeholders = placeholders[:len(placeholders)-1]
	query := fmt.Sprintf("SELECT id, COALESCE(text,'') FROM documents WHERE id IN (%s);", placeholders)
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := b.store.DB().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[int64]string, len(ids))
	for rows.Next() {
		var id int64
		var text string
		if err := rows.Scan(&id, &text); err != nil {
			return nil, err
		}
		out[id] = text
	}
	return out, nil
}

func dedupeStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := in[:0]
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// Tokenize lowercases, splits on non-alphanumerics, drops stopwords and tokens < 2 chars.
//
// English-only. A multilingual tokenizer is on the v1 list.
func Tokenize(s string) []string {
	s = strings.ToLower(s)
	tokens := make([]string, 0, 32)
	var cur strings.Builder
	flush := func() {
		if cur.Len() == 0 {
			return
		}
		t := cur.String()
		cur.Reset()
		if len(t) < 2 {
			return
		}
		if _, stop := stopwords[t]; stop {
			return
		}
		tokens = append(tokens, t)
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			cur.WriteRune(r)
		default:
			flush()
		}
	}
	flush()
	return tokens
}

// Minimal English stopword list. Trimmed to high-frequency function words;
// keeping the list small reduces accidental query degradation.
var stopwords = map[string]struct{}{
	"a": {}, "an": {}, "the": {}, "and": {}, "or": {}, "but": {},
	"is": {}, "are": {}, "was": {}, "were": {}, "be": {}, "been": {}, "being": {},
	"to": {}, "of": {}, "in": {}, "on": {}, "at": {}, "by": {}, "for": {}, "with": {},
	"as": {}, "from": {}, "into": {}, "about": {},
	"this": {}, "that": {}, "these": {}, "those": {},
	"it": {}, "its": {}, "their": {}, "they": {}, "them": {},
	"i": {}, "you": {}, "we": {}, "he": {}, "she": {},
	"will": {}, "would": {}, "can": {}, "could": {}, "should": {}, "may": {}, "might": {},
	"do": {}, "does": {}, "did": {}, "done": {},
	"has": {}, "have": {}, "had": {},
	"not": {}, "no": {},
}
