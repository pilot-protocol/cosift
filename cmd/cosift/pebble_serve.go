// Iter 205 — minimal HTTP server backed by PebbleStore + PebbleBM25.
//
// Parallel to the SQLite-backed `cosift serve`. Read-only endpoints only:
// /healthz, /stats, /search, /contents. No crawler, no admin, no /answer
// or /research yet — those need the SQLite-side server's chat-client +
// admin-token plumbing, which is the iter-206+ work.
//
// Purpose: proves the path-2 storage rework works end-to-end through HTTP,
// and gives a clean benchmark surface against the existing SQLite server.
// Operators evaluating cosift's billion-scale capability run this against
// a Pebble store and compare /search latency + index size to the SQLite
// equivalent.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/calinteodor/cosift/internal/config"
	"github.com/calinteodor/cosift/internal/index"
	"github.com/calinteodor/cosift/internal/store"
)

func runPebbleServe(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("pebble-serve", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (required; the SQLite cfg.DataDir is ignored)")
	addr := fs.String("addr", cfg.Server.Addr, "listen address (defaults to server.addr from cosift.json)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return errors.New("pebble-serve: -dir is required")
	}
	if *addr == "" {
		*addr = "127.0.0.1:7777"
	}

	ps, err := store.OpenPebble(*dir)
	if err != nil {
		return fmt.Errorf("open pebble: %w", err)
	}
	defer ps.Close()

	idx := index.NewPebbleBM25(ps)
	srv := &pebbleHTTP{store: ps, idx: idx, started: time.Now()}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", srv.handleHealthz)
	mux.HandleFunc("GET /stats", srv.handleStats)
	mux.HandleFunc("GET /search", srv.handleSearch)
	mux.HandleFunc("GET /contents", srv.handleContents)
	mux.HandleFunc("GET /verify", srv.handleVerify)
	mux.HandleFunc("GET /metrics", srv.handleMetrics)
	mux.HandleFunc("GET /find_similar", srv.handleFindSimilar)

	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
	}

	log.Printf("pebble-serve: listening on %s (PebbleStore at %s)", *addr, *dir)
	go func() {
		<-ctx.Done()
		log.Printf("pebble-serve: shutting down")
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutCtx)
	}()
	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// pebbleHTTP holds the read-side handles. Stays small on purpose — the
// SQLite-side Server struct accumulated a lot of config knobs over many iters;
// the Pebble surface starts minimal and grows feature-by-feature.
type pebbleHTTP struct {
	store   *store.PebbleStore
	idx     *index.PebbleBM25
	started time.Time
}

func (s *pebbleHTTP) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *pebbleHTTP) handleStats(w http.ResponseWriter, r *http.Request) {
	st, err := s.store.Stats(r.Context())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"documents": st.Documents,
		"terms":     st.Terms,
		"uptime":    time.Since(s.started).String(),
		"backend":   "pebble",
	})
}

// Iter 231: Prometheus-format scrape endpoint. Hand-written plain text
// (no client_golang dep) — exposition format is simple enough that
// pulling in a dep just to print four gauges isn't justified.
// Quantities chosen to be O(1) reads (CorpusStats counters + uptime);
// frontier scans deliberately excluded so scrape latency stays flat
// regardless of corpus size.
func (s *pebbleHTTP) handleMetrics(w http.ResponseWriter, r *http.Request) {
	sumLen, count, err := s.store.CorpusStats(r.Context())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	var avg float64
	if count > 0 {
		avg = float64(sumLen) / float64(count)
	}
	uptime := time.Since(s.started).Seconds()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "# HELP cosift_indexed_docs Number of documents passed through IndexDocument (iter-207 counter).\n")
	fmt.Fprintf(w, "# TYPE cosift_indexed_docs gauge\n")
	fmt.Fprintf(w, "cosift_indexed_docs %d\n", count)
	fmt.Fprintf(w, "# HELP cosift_sum_doc_len_total Sum of indexed document lengths in tokens.\n")
	fmt.Fprintf(w, "# TYPE cosift_sum_doc_len_total counter\n")
	fmt.Fprintf(w, "cosift_sum_doc_len_total %d\n", sumLen)
	fmt.Fprintf(w, "# HELP cosift_avg_doc_len Average document length in tokens (derived).\n")
	fmt.Fprintf(w, "# TYPE cosift_avg_doc_len gauge\n")
	fmt.Fprintf(w, "cosift_avg_doc_len %.2f\n", avg)
	fmt.Fprintf(w, "# HELP cosift_uptime_seconds Seconds since pebble-serve started.\n")
	fmt.Fprintf(w, "# TYPE cosift_uptime_seconds counter\n")
	fmt.Fprintf(w, "cosift_uptime_seconds %.0f\n", uptime)
}

// Iter 230: HTTP form of `cosift verify`. Same comparison (iter-207 running
// counters vs authoritative 'l' family scan) on the already-open store, so
// monitoring can poll without contending for the single-writer lock or
// shelling into the container. 503 on drift makes this composable with
// Kubernetes liveness / load-balancer health checks.
func (s *pebbleHTTP) handleVerify(w http.ResponseWriter, r *http.Request) {
	counterSum, counterCount, err := s.store.CorpusStats(r.Context())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	scanSum, scanCount, err := s.store.SumDocLengths(r.Context())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	driftCount := counterCount - scanCount
	driftSum := counterSum - scanSum
	ok := driftCount == 0 && driftSum == 0
	body := map[string]any{
		"ok":                     ok,
		"indexed_docs_counter":   counterCount,
		"indexed_docs_scan":      scanCount,
		"indexed_docs_drift":     driftCount,
		"sum_doc_len_counter":    counterSum,
		"sum_doc_len_scan":       scanSum,
		"sum_doc_len_drift":      driftSum,
	}
	status := http.StatusOK
	if !ok {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, body)
}

// searchHit is the minimal hit shape returned by pebble-serve's /search.
// Intentionally narrower than the SQLite server's SearchHit — feature
// parity (highlight, excerpt, calibration, paragraph filters) grows as
// follow-up iters port each one through the Pebble side.
type searchHit struct {
	URL          string     `json:"url"`
	Title        string     `json:"title"`
	Score        float64    `json:"score"`
	Excerpt      string     `json:"excerpt,omitempty"`
	PublishedAt  *time.Time `json:"published_at,omitempty"`
	Author       string     `json:"author,omitempty"`
}

type searchResponse struct {
	Query     string      `json:"query"`
	Retriever string      `json:"retriever"`
	Hits      []searchHit `json:"hits"`
	Took      string      `json:"took"`
}

func (s *pebbleHTTP) handleSearch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	q := r.URL.Query().Get("q")
	if q == "" {
		writeProblem(w, http.StatusBadRequest, "missing q parameter")
		return
	}
	k := 10
	if v := r.URL.Query().Get("k"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 100 {
			k = n
		}
	}
	// Iter 232: include_domains / exclude_domains — mirrors the
	// SQLite-side server semantics (CSV, dot-boundary suffix match).
	// Iter 234: since / until — ISO-date filters on doc.PublishedAt.
	// When any filter is active we over-fetch so the post-filter has
	// enough candidates to fill k; same brute-force shape SQLite used
	// before it grew a proper index, fine at pebble-serve's scale.
	include := splitDomainsCSV(r.URL.Query().Get("include_domains"))
	exclude := splitDomainsCSV(r.URL.Query().Get("exclude_domains"))
	since, sinceErr := parseDateBound(r.URL.Query().Get("since"))
	if sinceErr != nil {
		writeProblem(w, http.StatusBadRequest, "since: "+sinceErr.Error())
		return
	}
	until, untilErr := parseDateBound(r.URL.Query().Get("until"))
	if untilErr != nil {
		writeProblem(w, http.StatusBadRequest, "until: "+untilErr.Error())
		return
	}
	dateFilter := !since.IsZero() || !until.IsZero()
	fetchK := k
	if len(include) > 0 || len(exclude) > 0 || dateFilter {
		mult := 5
		if dateFilter {
			mult = 10
		}
		fetchK = k * mult
		if fetchK > 500 {
			fetchK = 500
		}
	}
	hits, err := s.idx.Search(r.Context(), q, fetchK)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	// Iter 233: enrich each surviving hit with Excerpt + PublishedAt + Author
	// via a single GetDocByURL per hit. Cost: k extra Gets — block-cache hot,
	// ~ms-scale at the k≤100 we accept. Opt out with ?enrich=false for callers
	// that only need scoring. The date filter (iter 234) forces a doc fetch
	// even when enrich=false, since PublishedAt lives on the gob.
	enrich := r.URL.Query().Get("enrich") != "false"
	out := make([]searchHit, 0, k)
	for _, h := range hits {
		if len(include) > 0 || len(exclude) > 0 {
			host := hostOf(h.URL)
			if len(include) > 0 && !matchesAnyDomain(host, include) {
				continue
			}
			if len(exclude) > 0 && matchesAnyDomain(host, exclude) {
				continue
			}
		}
		hit := searchHit{URL: h.URL, Title: h.Title, Score: h.Score}
		if enrich || dateFilter {
			doc, derr := s.store.GetDocByURL(r.Context(), h.URL)
			if derr != nil || doc == nil {
				continue
			}
			if dateFilter {
				if doc.PublishedAt.IsZero() {
					continue // zero PublishedAt = unknown; drop under any date filter
				}
				if !since.IsZero() && doc.PublishedAt.Before(since) {
					continue
				}
				if !until.IsZero() && doc.PublishedAt.After(until) {
					continue
				}
			}
			if enrich {
				hit.Excerpt = textExcerpt(doc.Text, 320)
				if !doc.PublishedAt.IsZero() {
					t := doc.PublishedAt
					hit.PublishedAt = &t
				}
				hit.Author = doc.Author
			}
		}
		out = append(out, hit)
		if len(out) >= k {
			break
		}
	}
	// Iter 235: sort=date_desc | date_asc | relevance (default). Applies to
	// the already-collected top-k pool; raise k to widen the pool before
	// re-sorting if you need more candidates for date ordering.
	switch r.URL.Query().Get("sort") {
	case "date_desc":
		sortHitsByDate(out, false)
	case "date_asc":
		sortHitsByDate(out, true)
	}
	writeJSON(w, http.StatusOK, searchResponse{
		Query:     q,
		Retriever: "bm25",
		Hits:      out,
		Took:      time.Since(start).String(),
	})
}

// sortHitsByDate orders hits by PublishedAt. Hits with no PublishedAt sink to
// the end regardless of direction — they have no comparable value. asc=true
// gives oldest-first; asc=false gives newest-first. Iter 235.
func sortHitsByDate(hits []searchHit, asc bool) {
	sort.SliceStable(hits, func(i, j int) bool {
		ai := hits[i].PublishedAt != nil
		aj := hits[j].PublishedAt != nil
		if ai != aj {
			return ai // hits with dates come before hits without
		}
		if !ai {
			return false
		}
		if asc {
			return hits[i].PublishedAt.Before(*hits[j].PublishedAt)
		}
		return hits[i].PublishedAt.After(*hits[j].PublishedAt)
	})
}

// Iter 236: BM25-only "more like this". Mirrors EXA's /findSimilar shape but
// stays dependency-free: no embeddings required. Algorithm is Lucene's MLT —
// pick the source doc's top-N terms by tf·idf, build a query from them, run
// the existing BM25 search, drop the source URL itself. With dense vectors
// off the table on the Pebble path (HNSW indexing during crawl is the iter
// follow-up), this is the cheapest credible /find_similar we can ship.
func (s *pebbleHTTP) handleFindSimilar(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rawURL := r.URL.Query().Get("url")
	if rawURL == "" {
		writeProblem(w, http.StatusBadRequest, "missing url parameter")
		return
	}
	decoded, err := url.QueryUnescape(rawURL)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "url parameter is not URL-encoded")
		return
	}
	src, err := s.store.GetDocByURL(r.Context(), decoded)
	if errors.Is(err, store.ErrNotFound) {
		writeProblem(w, http.StatusNotFound, "url not in index")
		return
	}
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	k := 10
	if v := r.URL.Query().Get("k"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 100 {
			k = n
		}
	}

	// Tokenize title (×3) and body; mirror IndexDocument's title boost so
	// title terms dominate the similarity query.
	tf := make(map[string]int, 256)
	for _, t := range index.Tokenize(src.Title) {
		tf[t] += 3
	}
	for _, t := range index.Tokenize(src.Text) {
		tf[t]++
	}

	_, n, _ := s.store.CorpusStats(r.Context())
	if n <= 0 {
		n = 1
	}
	logN := math.Log(float64(n))

	type termScore struct {
		term  string
		score float64
	}
	scored := make([]termScore, 0, len(tf))
	for term, freq := range tf {
		info, ok, err := s.store.GetTermInfo(r.Context(), term)
		if err != nil || !ok || info.DocFreq <= 0 {
			continue
		}
		idf := logN - math.Log(float64(info.DocFreq))
		if idf <= 0 {
			continue // term appears in every doc; useless signal
		}
		scored = append(scored, termScore{term: term, score: float64(freq) * idf})
	}
	sort.Slice(scored, func(i, j int) bool { return scored[i].score > scored[j].score })

	topN := 10
	if len(scored) < topN {
		topN = len(scored)
	}
	if topN == 0 {
		writeJSON(w, http.StatusOK, searchResponse{
			Query: decoded, Retriever: "bm25-mlt",
			Hits: []searchHit{}, Took: time.Since(start).String(),
		})
		return
	}
	terms := make([]string, topN)
	for i := 0; i < topN; i++ {
		terms[i] = scored[i].term
	}
	queryStr := strings.Join(terms, " ")

	hits, err := s.idx.Search(r.Context(), queryStr, k+1)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]searchHit, 0, k)
	for _, h := range hits {
		if h.URL == src.URL {
			continue
		}
		hit := searchHit{URL: h.URL, Title: h.Title, Score: h.Score}
		if doc, derr := s.store.GetDocByURL(r.Context(), h.URL); derr == nil && doc != nil {
			hit.Excerpt = textExcerpt(doc.Text, 320)
			if !doc.PublishedAt.IsZero() {
				t := doc.PublishedAt
				hit.PublishedAt = &t
			}
			hit.Author = doc.Author
		}
		out = append(out, hit)
		if len(out) >= k {
			break
		}
	}
	writeJSON(w, http.StatusOK, searchResponse{
		Query:     queryStr,
		Retriever: "bm25-mlt",
		Hits:      out,
		Took:      time.Since(start).String(),
	})
}

func splitDomainsCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		t := strings.ToLower(strings.TrimSpace(p))
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}

func matchesAnyDomain(host string, patterns []string) bool {
	host = strings.ToLower(host)
	for _, p := range patterns {
		if host == p || strings.HasSuffix(host, "."+p) {
			return true
		}
	}
	return false
}

func hostOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// parseDateBound accepts the same forms as the SQLite-side server: empty
// (zero time), RFC3339 ("2026-01-15T00:00:00Z"), or a bare date
// ("2026-01-15", treated as UTC midnight). Iter 234.
func parseDateBound(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("expected YYYY-MM-DD or RFC3339, got %q", s)
}

// textExcerpt returns a body-prefix excerpt no longer than maxBytes, cut at a
// word boundary if one is available within the trailing 16 bytes. Iter 233.
// Static prefix (not query-aware) — matches the SQLite-side DocMeta.Excerpt
// shape; a query-aware passage extractor is the iter-199 path on the index
// hot path and lives there, not in the HTTP handler.
func textExcerpt(text string, maxBytes int) string {
	if len(text) <= maxBytes {
		return text
	}
	cut := text[:maxBytes]
	for i := len(cut) - 1; i >= len(cut)-16 && i > 0; i-- {
		if cut[i] == ' ' || cut[i] == '\n' || cut[i] == '\t' {
			cut = cut[:i]
			break
		}
	}
	return strings.TrimSpace(cut) + "…"
}

func (s *pebbleHTTP) handleContents(w http.ResponseWriter, r *http.Request) {
	rawURL := r.URL.Query().Get("url")
	if rawURL == "" {
		writeProblem(w, http.StatusBadRequest, "missing url parameter")
		return
	}
	decoded, err := url.QueryUnescape(rawURL)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "url parameter is not URL-encoded")
		return
	}
	doc, err := s.store.GetDocByURL(r.Context(), decoded)
	if errors.Is(err, store.ErrNotFound) {
		writeProblem(w, http.StatusNotFound, "url not in index")
		return
	}
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"url":          doc.URL,
		"title":        doc.Title,
		"text":         doc.Text,
		"author":       doc.Author,
		"published_at": doc.PublishedAt,
		"fetched_at":   doc.FetchedAt,
	})
}

// runPebbleInfo prints Pebble's built-in metrics for the store at -dir.
// Iter 217 — operator visibility into LSM levels, WAL state, on-disk
// size, and compaction queue, surfaced via pebble.Metrics().String().
func runPebbleInfo(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("pebble-info", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (defaults to <cfg.DataDir>/pebble)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	d := *dir
	if d == "" {
		d = filepath.Join(cfg.DataDir, "pebble")
	}
	ps, err := store.OpenPebble(d)
	if err != nil {
		return fmt.Errorf("open pebble at %s: %w", d, err)
	}
	defer ps.Close()

	st, err := ps.Stats(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("PebbleStore: %s\n\n", d)
	fmt.Printf("  documents:   %d\n", st.Documents)
	fmt.Println()
	fmt.Println("--- pebble.Metrics ---")
	fmt.Println(ps.Metrics().String())
	return nil
}

// Iter 228: verify the iter-207 running counters ('m'+indexed_docs,
// 'm'+sum_doc_len) against an authoritative scan of the 'l' family.
// Drift would mean a crash or bug left the counter inconsistent —
// flagged here so it can be re-derived from the scan.
func runVerifyPebble(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (defaults to <cfg.DataDir>/pebble)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	d := *dir
	if d == "" {
		d = filepath.Join(cfg.DataDir, "pebble")
	}
	ps, err := store.OpenPebble(d)
	if err != nil {
		return fmt.Errorf("open pebble at %s: %w", d, err)
	}
	defer ps.Close()

	counterSum, counterCount, err := ps.CorpusStats(ctx)
	if err != nil {
		return fmt.Errorf("read counters: %w", err)
	}
	scanSum, scanCount, err := ps.SumDocLengths(ctx)
	if err != nil {
		return fmt.Errorf("scan 'l' family: %w", err)
	}

	fmt.Printf("PebbleStore: %s\n\n", d)
	fmt.Printf("  indexed_docs (counter): %d\n", counterCount)
	fmt.Printf("  indexed_docs (scan):    %d\n", scanCount)
	fmt.Printf("  sum_doc_len  (counter): %d\n", counterSum)
	fmt.Printf("  sum_doc_len  (scan):    %d\n", scanSum)

	driftCount := counterCount - scanCount
	driftSum := counterSum - scanSum
	if driftCount == 0 && driftSum == 0 {
		fmt.Println("\nOK: counters match the 'l' family scan.")
		return nil
	}
	fmt.Printf("\nDRIFT: indexed_docs Δ=%+d, sum_doc_len Δ=%+d\n", driftCount, driftSum)
	return fmt.Errorf("counter drift detected")
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeProblem(w http.ResponseWriter, status int, detail string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error":  http.StatusText(status),
		"status": status,
		"detail": detail,
	})
}
