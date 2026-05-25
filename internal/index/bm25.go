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
}

func NewBM25(s *store.Store) *BM25 {
	return &BM25{store: s}
}

// IndexDocument tokenizes and writes postings for the given doc.
// Replaces any existing postings for the doc.
func (b *BM25) IndexDocument(ctx context.Context, docID int64, title, text string) error {
	tokens := Tokenize(title + " " + text)
	if len(tokens) == 0 {
		return nil
	}

	// term -> tf in this doc
	tf := make(map[string]int, len(tokens))
	for _, t := range tokens {
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

// Search returns the top-k hits for the query string.
func (b *BM25) Search(ctx context.Context, q string, k int) ([]Hit, error) {
	tokens := Tokenize(q)
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
	if k > 0 && len(hits) > k {
		hits = hits[:k]
	}
	return hits, nil
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
