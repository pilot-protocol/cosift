// Package eval is the retrieval evaluation harness.
//
// Standard IR metrics: recall@K, MRR@K, nDCG@K. Implementations are textbook —
// no learned components, fully deterministic, ~150 LOC pure Go.
//
// The harness is retriever-agnostic. Any implementation of the Retriever
// interface can be scored against the same query set; that's what makes
// "BM25 vs hybrid vs reranked" A/B comparisons cheap.
package eval

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"time"
)

// Retriever is the contract every search approach must satisfy.
type Retriever interface {
	Search(ctx context.Context, query string, k int) ([]string, error)
}

// QuerySet is the on-disk shape of an eval set.
type QuerySet struct {
	Name    string  `json:"name"`
	Queries []Query `json:"queries"`
}

// Query is a single labeled query: the text, plus URLs judged relevant.
// Binary judgments only (relevant / not). Graded relevance is a v2 concern.
//
// Paraphrases (optional): when present, the eval runs the retriever once for
// the main Text AND once for each paraphrase, then RRF-fuses the result lists.
// Tests whether query-side rewriting closes retrieval gaps that retriever
// upgrades couldn't (iter-42 measured: stronger embedder doesn't recover the
// 10k+ diverse-noise ceiling — query rewriting is the alternative path).
type Query struct {
	Text        string   `json:"text"`
	Relevant    []string `json:"relevant"`
	Paraphrases []string `json:"paraphrases,omitempty"`
}

// Corpus is the on-disk shape of a fixture document set.
type Corpus struct {
	Docs []CorpusDoc `json:"docs"`
}

type CorpusDoc struct {
	URL   string `json:"url"`
	Title string `json:"title"`
	Text  string `json:"text"`
}

// Metrics holds per-query scores. Means roll up across the set.
type Metrics struct {
	Recall1  float64 `json:"recall@1"`
	Recall3  float64 `json:"recall@3"`
	Recall10 float64 `json:"recall@10"`
	MRR10    float64 `json:"mrr@10"`
	NDCG10   float64 `json:"ndcg@10"`
}

// PerQuery is a single row in the result table.
type PerQuery struct {
	Query    string   `json:"query"`
	Got      []string `json:"got"`
	Relevant []string `json:"relevant"`
	Metrics  Metrics  `json:"metrics"`
}

// Summary is the aggregated result of a run.
type Summary struct {
	Name       string     `json:"name"`
	When       time.Time  `json:"when"`
	NumQueries int        `json:"num_queries"`
	PerQuery   []PerQuery `json:"per_query"`
	Mean       Metrics    `json:"mean"`
}

// LoadQuerySet reads a QuerySet from disk.
func LoadQuerySet(path string) (*QuerySet, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	qs := &QuerySet{}
	if err := json.Unmarshal(b, qs); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return qs, nil
}

// LoadCorpus reads a Corpus from disk.
func LoadCorpus(path string) (*Corpus, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	c := &Corpus{}
	if err := json.Unmarshal(b, c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return c, nil
}

// Run executes every query against the retriever and aggregates metrics.
// If a Query has Paraphrases, the retriever runs once for each (main + paraphrases)
// and the results are RRF-fused before scoring.
func Run(ctx context.Context, qs *QuerySet, r Retriever) (*Summary, error) {
	summary := &Summary{
		Name:       qs.Name,
		When:       time.Now().UTC(),
		NumQueries: len(qs.Queries),
		PerQuery:   make([]PerQuery, 0, len(qs.Queries)),
	}
	for _, q := range qs.Queries {
		var got []string
		var err error
		if len(q.Paraphrases) == 0 {
			got, err = r.Search(ctx, q.Text, 10)
			if err != nil {
				return nil, fmt.Errorf("search %q: %w", q.Text, err)
			}
		} else {
			// Fan out to main + paraphrases. Failures on a single paraphrase
			// don't abort — partial fan-out is fine.
			lists := make([][]string, 0, 1+len(q.Paraphrases))
			main, err := r.Search(ctx, q.Text, 10)
			if err != nil {
				return nil, fmt.Errorf("search %q (main): %w", q.Text, err)
			}
			lists = append(lists, main)
			for _, p := range q.Paraphrases {
				para, err := r.Search(ctx, p, 10)
				if err != nil {
					continue
				}
				lists = append(lists, para)
			}
			got = rrfFuse(lists, 10, 60.0)
		}
		m := score(got, q.Relevant)
		summary.PerQuery = append(summary.PerQuery, PerQuery{
			Query:    q.Text,
			Got:      got,
			Relevant: q.Relevant,
			Metrics:  m,
		})
	}
	summary.Mean = meanOf(summary.PerQuery)
	return summary, nil
}

// rrfFuse — small standalone reciprocal-rank-fusion so the eval package
// doesn't import internal/index. Same algorithm as index.RRF.
func rrfFuse(lists [][]string, k int, rrfK float64) []string {
	if rrfK <= 0 {
		rrfK = 60
	}
	scores := make(map[string]float64)
	for _, list := range lists {
		for rank, url := range list {
			scores[url] += 1.0 / (rrfK + float64(rank+1))
		}
	}
	type pair struct {
		url   string
		score float64
	}
	pairs := make([]pair, 0, len(scores))
	for u, s := range scores {
		pairs = append(pairs, pair{u, s})
	}
	sort.Slice(pairs, func(a, b int) bool { return pairs[a].score > pairs[b].score })
	if k <= 0 || k > len(pairs) {
		k = len(pairs)
	}
	out := make([]string, 0, k)
	for i := 0; i < k; i++ {
		out = append(out, pairs[i].url)
	}
	return out
}

func score(got, relevant []string) Metrics {
	return Metrics{
		Recall1:  recallAtK(got, relevant, 1),
		Recall3:  recallAtK(got, relevant, 3),
		Recall10: recallAtK(got, relevant, 10),
		MRR10:    mrrAtK(got, relevant, 10),
		NDCG10:   ndcgAtK(got, relevant, 10),
	}
}

func recallAtK(got, relevant []string, k int) float64 {
	if len(relevant) == 0 {
		return 0
	}
	rel := setOf(relevant)
	limit := minInt(k, len(got))
	found := 0
	for i := 0; i < limit; i++ {
		if rel[got[i]] {
			found++
		}
	}
	return float64(found) / float64(len(relevant))
}

func mrrAtK(got, relevant []string, k int) float64 {
	rel := setOf(relevant)
	limit := minInt(k, len(got))
	for i := 0; i < limit; i++ {
		if rel[got[i]] {
			return 1.0 / float64(i+1)
		}
	}
	return 0
}

func ndcgAtK(got, relevant []string, k int) float64 {
	if len(relevant) == 0 {
		return 0
	}
	rel := setOf(relevant)
	limit := minInt(k, len(got))
	dcg := 0.0
	for i := 0; i < limit; i++ {
		if rel[got[i]] {
			dcg += 1.0 / math.Log2(float64(i+2))
		}
	}
	// Ideal DCG: place all relevant docs (or k, whichever is smaller) at the top.
	ideal := minInt(k, len(relevant))
	idcg := 0.0
	for i := 0; i < ideal; i++ {
		idcg += 1.0 / math.Log2(float64(i+2))
	}
	if idcg == 0 {
		return 0
	}
	return dcg / idcg
}

func meanOf(rows []PerQuery) Metrics {
	if len(rows) == 0 {
		return Metrics{}
	}
	var m Metrics
	for _, r := range rows {
		m.Recall1 += r.Metrics.Recall1
		m.Recall3 += r.Metrics.Recall3
		m.Recall10 += r.Metrics.Recall10
		m.MRR10 += r.Metrics.MRR10
		m.NDCG10 += r.Metrics.NDCG10
	}
	n := float64(len(rows))
	m.Recall1 /= n
	m.Recall3 /= n
	m.Recall10 /= n
	m.MRR10 /= n
	m.NDCG10 /= n
	return m
}

func setOf(s []string) map[string]bool {
	out := make(map[string]bool, len(s))
	for _, x := range s {
		out[x] = true
	}
	return out
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// PrintTable writes a human-readable summary to w-shaped output.
// Format: query, recall@10, mrr@10, ndcg@10 — full table plus mean row.
func PrintTable(s *Summary) string {
	sorted := make([]PerQuery, len(s.PerQuery))
	copy(sorted, s.PerQuery)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Metrics.NDCG10 > sorted[j].Metrics.NDCG10
	})

	out := fmt.Sprintf("eval set: %s   queries: %d   when: %s\n", s.Name, s.NumQueries, s.When.Format(time.RFC3339))
	out += fmt.Sprintf("%-40s  R@1   R@3   R@10  MRR   nDCG\n", "query")
	out += fmt.Sprintf("%-40s  ----  ----  ----  ----  ----\n", "")
	for _, r := range sorted {
		q := r.Query
		if len(q) > 38 {
			q = q[:38] + ".."
		}
		out += fmt.Sprintf("%-40s  %.2f  %.2f  %.2f  %.2f  %.2f\n",
			q, r.Metrics.Recall1, r.Metrics.Recall3, r.Metrics.Recall10, r.Metrics.MRR10, r.Metrics.NDCG10)
	}
	out += fmt.Sprintf("%-40s  ----  ----  ----  ----  ----\n", "")
	out += fmt.Sprintf("%-40s  %.2f  %.2f  %.2f  %.2f  %.2f\n",
		"MEAN", s.Mean.Recall1, s.Mean.Recall3, s.Mean.Recall10, s.Mean.MRR10, s.Mean.NDCG10)
	return out
}

// SaveSummary persists a run to JSON for later baseline comparison.
func SaveSummary(s *Summary, path string) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// LoadSummary reads a previously-saved run for baseline comparison.
func LoadSummary(path string) (*Summary, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	s := &Summary{}
	if err := json.Unmarshal(b, s); err != nil {
		return nil, err
	}
	return s, nil
}

// Diff produces a side-by-side mean-only comparison string.
func Diff(baseline, current *Summary) string {
	b, c := baseline.Mean, current.Mean
	delta := func(b, c float64) string {
		d := c - b
		sign := "+"
		if d < 0 {
			sign = "-"
			d = -d
		}
		return fmt.Sprintf("%s%.3f", sign, d)
	}
	return fmt.Sprintf(
		"            baseline    current     delta\n"+
			"R@1         %.3f       %.3f       %s\n"+
			"R@3         %.3f       %.3f       %s\n"+
			"R@10        %.3f       %.3f       %s\n"+
			"MRR@10      %.3f       %.3f       %s\n"+
			"nDCG@10     %.3f       %.3f       %s\n",
		b.Recall1, c.Recall1, delta(b.Recall1, c.Recall1),
		b.Recall3, c.Recall3, delta(b.Recall3, c.Recall3),
		b.Recall10, c.Recall10, delta(b.Recall10, c.Recall10),
		b.MRR10, c.MRR10, delta(b.MRR10, c.MRR10),
		b.NDCG10, c.NDCG10, delta(b.NDCG10, c.NDCG10),
	)
}
