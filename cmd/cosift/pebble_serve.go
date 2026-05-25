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
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/calinteodor/cosift/internal/config"
	"github.com/calinteodor/cosift/internal/embed"
	"github.com/calinteodor/cosift/internal/index"
	"github.com/calinteodor/cosift/internal/rerank"
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
	srv := &pebbleHTTP{
		store:     ps,
		idx:       idx,
		hydeCache: make(map[string]string, 256),
		started:   time.Now(),
	}
	// Iter 240: optional /answer wiring. Uses the same OpenAI-compatible chat
	// client the SQLite-side server uses; works against OpenAI, Together,
	// Azure, llama.cpp, vLLM, Ollama, anything speaking /v1/chat/completions.
	if cfg.Chat.Model != "" {
		apiKey := os.Getenv("OPENAI_API_KEY")
		if apiKey == "" {
			apiKey = os.Getenv("OPENAI")
		}
		srv.chat = embed.NewOpenAIChat(apiKey, cfg.Chat.URL, cfg.Chat.Model)
		log.Printf("pebble-serve: /answer enabled (chat model=%s)", cfg.Chat.Model)
	}
	// Iter 248: wire rerank.Reranker when cfg.Rerank is configured. Two paths
	// (matching the SQLite-side): cfg.Rerank.URL → HTTPReranker (Cohere/Voyage/
	// Jina/TEI wire shape); otherwise cfg.Chat.Model present → LLMReranker
	// (RankGPT-style listwise via chat). /search?rerank=true gates the wrapper.
	if cfg.Rerank.URL != "" {
		apiKey := cfg.Rerank.APIKey
		if apiKey == "" {
			apiKey = os.Getenv("COHERE_API_KEY")
			if apiKey == "" {
				apiKey = os.Getenv("VOYAGE_API_KEY")
			}
		}
		srv.reranker = rerank.NewHTTPReranker(cfg.Rerank.URL, apiKey, cfg.Rerank.Model)
		log.Printf("pebble-serve: rerank enabled (http: %s, model=%s)", cfg.Rerank.URL, cfg.Rerank.Model)
	} else if cfg.Rerank.Enabled && srv.chat != nil {
		srv.reranker = rerank.NewLLMReranker(srv.chat)
		log.Printf("pebble-serve: rerank enabled (llm: %s)", cfg.Chat.Model)
	}
	srv.rerankCandK = cfg.Rerank.CandidateK
	if srv.rerankCandK <= 0 {
		srv.rerankCandK = 20
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", srv.count(srv.handleHealthz))
	mux.HandleFunc("GET /stats", srv.count(srv.handleStats))
	mux.HandleFunc("GET /search", srv.count(srv.handleSearch))
	mux.HandleFunc("GET /contents", srv.count(srv.handleContents))
	mux.HandleFunc("POST /contents", srv.count(srv.handleContentsBatch))
	mux.HandleFunc("GET /verify", srv.count(srv.handleVerify))
	mux.HandleFunc("GET /metrics", srv.count(srv.handleMetrics))
	mux.HandleFunc("GET /find_similar", srv.count(srv.handleFindSimilar))
	mux.HandleFunc("GET /answer", srv.count(srv.handleAnswer))
	mux.HandleFunc("GET /research", srv.count(srv.handleResearch))

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
	store      *store.PebbleStore
	idx        *index.PebbleBM25
	chat       embed.ChatClient // nil when cfg.Chat.Model is unset; /answer returns 501
	reranker   rerank.Reranker  // nil when no rerank is configured; ?rerank=true is a no-op then
	rerankCandK int            // candidates pulled for rerank; default 20

	// Iter 259: bounded in-memory HyDE cache. /research?expand=true issues a
	// chat call PER sub-query, so a sticky workload (repeated queries, slow
	// sub-query rephrasings from the planner) hits the chat provider many
	// times for the same passage. Cap at 256 entries; on overflow drop one
	// arbitrary entry (Go map iteration order is randomized, good enough).
	hydeMu    sync.RWMutex
	hydeCache map[string]string

	// Iter 260: atomic counters surfaced on /metrics so operators can size the
	// HyDE cache against a real workload — if misses dominate, raise the cap
	// or move to an L2 store; if hits dominate at a tight working set, the
	// cap is sufficient and you can mostly forget about it.
	hydeHits   atomic.Int64
	hydeMisses atomic.Int64

	// Iter 263: rerank attempt + failure counters. Rerank failures fall back
	// to BM25 order silently — that's the right reliability move, but without
	// a counter operators can't tell whether their LLM/HTTP reranker is
	// healthy or quietly broken.
	rerankAttempts atomic.Int64
	rerankFailures atomic.Int64

	// Iter 261/262: per-endpoint request counters + duration sums via a
	// counting middleware wrapping every mux entry. sync.Map keeps the hot
	// path lock-free; /metrics reads via Range. Path is the label so a
	// misrouted call (404) doesn't get counted under an existing endpoint.
	// rate(sum)/rate(count) over the duration sum gives mean latency in PromQL.
	requestCounts sync.Map // map[string]*endpointMetrics

	started    time.Time
}

type endpointMetrics struct {
	count    atomic.Int64
	sumNanos atomic.Int64
}

// count is the iter-261/262 request-counting middleware. Bumps a per-path
// {count, sumNanos} struct, lazily created on first request to a path. Hot
// path is sync.Map.Load + two atomic Adds — no contention even under high
// RPS. Duration is sampled after the handler returns so streaming endpoints
// account for their full open connection time.
func (s *pebbleHTTP) count(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path
		v, ok := s.requestCounts.Load(key)
		if !ok {
			v, _ = s.requestCounts.LoadOrStore(key, &endpointMetrics{})
		}
		m := v.(*endpointMetrics)
		m.count.Add(1)
		start := time.Now()
		h(w, r)
		m.sumNanos.Add(time.Since(start).Nanoseconds())
	}
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
	// Iter 238: surface the iter-207 running counters here too so /stats is
	// the one canonical "shape of the index" call instead of "ask /stats for
	// doc count, then ask /metrics for average length". Both reads are O(1).
	sumLen, indexedDocs, _ := s.store.CorpusStats(r.Context())
	var avg float64
	if indexedDocs > 0 {
		avg = float64(sumLen) / float64(indexedDocs)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"documents":    st.Documents,
		"terms":        st.Terms,
		"indexed_docs": indexedDocs,
		"sum_doc_len":  sumLen,
		"avg_doc_len":  avg,
		"uptime":       time.Since(s.started).String(),
		"backend":      "pebble",
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
	// Iter 260: HyDE cache effectiveness. Hits/misses both monotonic so
	// Prometheus rate() over these gives cache pressure under load.
	fmt.Fprintf(w, "# HELP cosift_hyde_cache_hits_total HyDE cache hits (expandQuery served from memory).\n")
	fmt.Fprintf(w, "# TYPE cosift_hyde_cache_hits_total counter\n")
	fmt.Fprintf(w, "cosift_hyde_cache_hits_total %d\n", s.hydeHits.Load())
	fmt.Fprintf(w, "# HELP cosift_hyde_cache_misses_total HyDE cache misses (expandQuery called the LLM).\n")
	fmt.Fprintf(w, "# TYPE cosift_hyde_cache_misses_total counter\n")
	fmt.Fprintf(w, "cosift_hyde_cache_misses_total %d\n", s.hydeMisses.Load())
	fmt.Fprintf(w, "# HELP cosift_rerank_attempts_total Rerank calls invoked (any endpoint with ?rerank=true).\n")
	fmt.Fprintf(w, "# TYPE cosift_rerank_attempts_total counter\n")
	fmt.Fprintf(w, "cosift_rerank_attempts_total %d\n", s.rerankAttempts.Load())
	fmt.Fprintf(w, "# HELP cosift_rerank_failures_total Rerank calls that returned an error (silently fell back to BM25 order).\n")
	fmt.Fprintf(w, "# TYPE cosift_rerank_failures_total counter\n")
	fmt.Fprintf(w, "cosift_rerank_failures_total %d\n", s.rerankFailures.Load())
	// Iter 261/262: per-endpoint request counters + duration sums. PromQL
	// rate(cosift_request_duration_seconds_sum) / rate(cosift_requests_total)
	// gives mean latency in any window. Labels = path; misrouted calls (404)
	// don't share a label with any handled path.
	fmt.Fprintf(w, "# HELP cosift_requests_total HTTP requests served, by endpoint.\n")
	fmt.Fprintf(w, "# TYPE cosift_requests_total counter\n")
	fmt.Fprintf(w, "# HELP cosift_request_duration_seconds_sum Cumulative request duration, by endpoint.\n")
	fmt.Fprintf(w, "# TYPE cosift_request_duration_seconds_sum counter\n")
	s.requestCounts.Range(func(k, v any) bool {
		path := k.(string)
		m := v.(*endpointMetrics)
		fmt.Fprintf(w, "cosift_requests_total{endpoint=%q} %d\n", path, m.count.Load())
		fmt.Fprintf(w, "cosift_request_duration_seconds_sum{endpoint=%q} %.6f\n", path, float64(m.sumNanos.Load())/1e9)
		return true
	})
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
	Text         string     `json:"text,omitempty"`
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
	// Iter 248: rerank widens both the fetch and the keep-cap before filtering,
	// so the reranker sees a healthy candidate pool even with restrictive filters.
	wantRerank := r.URL.Query().Get("rerank") == "true" && s.reranker != nil
	keepCap := k
	if wantRerank {
		keepCap = s.rerankCandK
		if keepCap < k {
			keepCap = k
		}
	}
	fetchK := keepCap
	if len(include) > 0 || len(exclude) > 0 || dateFilter {
		mult := 5
		if dateFilter {
			mult = 10
		}
		fetchK = keepCap * mult
		if fetchK > 500 {
			fetchK = 500
		}
	} else if wantRerank {
		fetchK = keepCap * 2
	}
	// Iter 252/258: ?expand=true asks the chat client for a HyDE-style
	// hypothetical passage and appends it to q for the BM25 call. The
	// reranker still scores against the original q.
	wantExpand := r.URL.Query().Get("expand") == "true"
	effectiveQuery := q
	if wantExpand {
		effectiveQuery = s.expandQuery(r.Context(), q)
	}
	hits, err := s.idx.Search(r.Context(), effectiveQuery, fetchK)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	// Iter 233: enrich each surviving hit with Excerpt + PublishedAt + Author
	// via a single GetDocByURL per hit. Cost: k extra Gets — block-cache hot,
	// ~ms-scale at the k≤100 we accept. Opt out with ?enrich=false for callers
	// that only need scoring. The date filter (iter 234) forces a doc fetch
	// even when enrich=false, since PublishedAt lives on the gob.
	// Iter 237: ?include_text=true inlines doc.Text on each hit so research
	// pipelines avoid the N+1 round trip to /contents. Off by default —
	// payload size grows linearly with k and average doc length.
	enrich := r.URL.Query().Get("enrich") != "false"
	if wantRerank {
		enrich = true // rerank needs per-doc text — overrides enrich opt-out
	}
	includeText := r.URL.Query().Get("include_text") == "true"
	out := make([]searchHit, 0, keepCap)
	var rerankTexts []string
	if wantRerank {
		rerankTexts = make([]string, 0, keepCap)
	}
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
		if enrich || dateFilter || includeText {
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
			if includeText {
				hit.Text = doc.Text
			}
			if wantRerank {
				rerankTexts = append(rerankTexts, doc.Title+"\n"+doc.Text)
			}
		}
		out = append(out, hit)
		if len(out) >= keepCap {
			break
		}
	}
	// Iter 248: rerank now that we have keepCap candidates with text.
	if wantRerank && len(out) > 1 && len(rerankTexts) == len(out) {
		cands := make([]rerank.Candidate, len(out))
		for i := range out {
			cands[i] = rerank.Candidate{ID: strconv.Itoa(i), Text: rerankTexts[i]}
		}
		if order, rerr := s.doRerank(r.Context(), q, cands); rerr == nil && len(order) > 0 {
			reordered := make([]searchHit, 0, len(out))
			seen := make(map[int]bool, len(out))
			for _, id := range order {
				if n, perr := strconv.Atoi(id); perr == nil && n >= 0 && n < len(out) && !seen[n] {
					reordered = append(reordered, out[n])
					seen[n] = true
				}
			}
			// Drop nothing: if the reranker omitted any IDs, append in original order.
			for i, h := range out {
				if !seen[i] {
					reordered = append(reordered, h)
				}
			}
			out = reordered
		}
	}
	if len(out) > k {
		out = out[:k]
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
	retrieverLabel := "bm25"
	if wantExpand && effectiveQuery != q {
		retrieverLabel = "bm25+hyde"
	}
	if wantRerank {
		retrieverLabel += "+rerank:" + s.reranker.Name()
	}
	writeJSON(w, http.StatusOK, searchResponse{
		Query:     q,
		Retriever: retrieverLabel,
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
	// Iter 239: optional ?q= augments the auto-derived MLT query so callers
	// can constrain "more like this URL" with an extra concept (e.g.
	// /find_similar?url=...&q=pricing). Appended verbatim — supports the
	// same quoted-phrase shape /search accepts.
	if extra := strings.TrimSpace(r.URL.Query().Get("q")); extra != "" {
		queryStr = queryStr + " " + extra
	}

	// Iter 245: scope MLT with the same retrieval filters /search and /answer
	// accept. 'find pages similar to X but only on docs.example.com' is the
	// archetype EXA findSimilar shape. Over-fetch enough that the source
	// exclusion + filter still fills k.
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
	includeText := r.URL.Query().Get("include_text") == "true"
	// Iter 251: ?rerank=true closes /find_similar's parity gap with the other
	// retrieval endpoints. The MLT query is auto-derived from the source doc,
	// so reranking the candidate neighbors against THAT query is exactly what
	// EXA's findSimilar quality boost is doing under the hood.
	wantRerank := r.URL.Query().Get("rerank") == "true" && s.reranker != nil
	keepCap := k
	if wantRerank {
		keepCap = s.rerankCandK
		if keepCap < k {
			keepCap = k
		}
	}
	fetchK := keepCap + 1 // +1 to absorb the source-URL exclusion
	if len(include) > 0 || len(exclude) > 0 || dateFilter {
		mult := 5
		if dateFilter {
			mult = 10
		}
		fetchK = keepCap * mult
		if fetchK > 500 {
			fetchK = 500
		}
	} else if wantRerank {
		fetchK = keepCap * 2
	}

	hits, err := s.idx.Search(r.Context(), queryStr, fetchK)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	type fsCand struct {
		hit        searchHit
		rerankText string
	}
	cands := make([]fsCand, 0, keepCap)
	for _, h := range hits {
		if h.URL == src.URL {
			continue
		}
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
		doc, derr := s.store.GetDocByURL(r.Context(), h.URL)
		if derr != nil || doc == nil {
			continue
		}
		if dateFilter {
			if doc.PublishedAt.IsZero() {
				continue
			}
			if !since.IsZero() && doc.PublishedAt.Before(since) {
				continue
			}
			if !until.IsZero() && doc.PublishedAt.After(until) {
				continue
			}
		}
		hit.Excerpt = textExcerpt(doc.Text, 320)
		if !doc.PublishedAt.IsZero() {
			t := doc.PublishedAt
			hit.PublishedAt = &t
		}
		hit.Author = doc.Author
		if includeText {
			hit.Text = doc.Text
		}
		c := fsCand{hit: hit}
		if wantRerank {
			c.rerankText = doc.Title + "\n" + doc.Text
		}
		cands = append(cands, c)
		if len(cands) >= keepCap {
			break
		}
	}
	if wantRerank && len(cands) > 1 {
		rc := make([]rerank.Candidate, len(cands))
		for i := range cands {
			rc[i] = rerank.Candidate{ID: strconv.Itoa(i), Text: cands[i].rerankText}
		}
		if order, rerr := s.doRerank(r.Context(), queryStr, rc); rerr == nil && len(order) > 0 {
			reordered := make([]fsCand, 0, len(cands))
			seen := make(map[int]bool, len(cands))
			for _, id := range order {
				if n, perr := strconv.Atoi(id); perr == nil && n >= 0 && n < len(cands) && !seen[n] {
					reordered = append(reordered, cands[n])
					seen[n] = true
				}
			}
			for i, c := range cands {
				if !seen[i] {
					reordered = append(reordered, c)
				}
			}
			cands = reordered
		}
	}
	if len(cands) > k {
		cands = cands[:k]
	}
	out := make([]searchHit, 0, len(cands))
	for _, c := range cands {
		out = append(out, c.hit)
	}
	retrieverLabel := "bm25-mlt"
	if wantRerank {
		retrieverLabel = "bm25-mlt+rerank:" + s.reranker.Name()
	}
	writeJSON(w, http.StatusOK, searchResponse{
		Query:     queryStr,
		Retriever: retrieverLabel,
		Hits:      out,
		Took:      time.Since(start).String(),
	})
}

// Iter 240: /answer — synthesis over BM25 retrieval. Mirrors the SQLite-side
// /answer in spirit (top-k sources → cited synthesis) but stays minimal: no
// streaming, no rerank, no query expansion. Those land in follow-up iters
// once this surface is exercised. Returns 501 when no chat model is
// configured so the absent capability fails loud instead of silent.
const answerSystemPrompt = `You are a research assistant. Answer the user's question using ONLY the provided sources.
- Cite sources by their numeric id in square brackets, e.g. [1] or [2,3]. Every factual claim needs a citation.
- If the sources do not contain the answer, say so plainly. Do not invent facts.
- Keep the answer focused on what the sources actually say; do not pad.`

// Iter 252: HyDE-style query expansion prompt. Borrowed verbatim from
// internal/server/hyde.go so the SQLite and Pebble paths produce comparable
// expansions and operators don't have to learn two prompt shapes.
const hydeSystemPrompt = `Write a brief, factual passage (2-4 sentences) that would directly answer the user's question. Output ONLY the passage — no preamble, no commentary, no apology if you're uncertain. If the question is ambiguous, pick the most plausible interpretation and answer that. The passage doesn't need to be true; it needs to be the SHAPE of what a relevant document would say. Embedding this passage and searching by its vector will find documents that look like real answers, even if the user's original query was just a few keywords.`

// doRerank wraps s.reranker.Rerank with attempt/failure counters. Iter 263.
// Returning the original error unchanged so callers keep their existing
// silent-fallback behavior; only the operator-side visibility changed.
func (s *pebbleHTTP) doRerank(ctx context.Context, q string, cands []rerank.Candidate) ([]string, error) {
	s.rerankAttempts.Add(1)
	order, err := s.reranker.Rerank(ctx, q, cands)
	if err != nil {
		s.rerankFailures.Add(1)
	}
	return order, err
}

// expandQuery returns q + " " + a HyDE-generated passage when a chat client is
// configured. On any error (no chat client, chat call fails, empty passage),
// returns q unchanged so callers can compose safely without explicit guards.
// Iter 257; iter 259 added a bounded in-memory cache to skip the chat call on
// repeat queries — important for /research?expand=true where the same
// sub-query rephrasing can fire across many similar research requests.
func (s *pebbleHTTP) expandQuery(ctx context.Context, q string) string {
	if s.chat == nil {
		return q
	}
	s.hydeMu.RLock()
	cached, hit := s.hydeCache[q]
	s.hydeMu.RUnlock()
	if hit {
		s.hydeHits.Add(1)
		return q + " " + cached
	}
	s.hydeMisses.Add(1)
	passage, err := s.chat.Chat(ctx, []embed.ChatMsg{
		{Role: "system", Content: hydeSystemPrompt},
		{Role: "user", Content: q},
	})
	if err != nil {
		return q
	}
	passage = strings.TrimSpace(passage)
	if passage == "" {
		return q
	}
	s.hydeMu.Lock()
	if len(s.hydeCache) >= 256 {
		// Bounded: drop one arbitrary entry (Go map range is randomized).
		for k := range s.hydeCache {
			delete(s.hydeCache, k)
			break
		}
	}
	s.hydeCache[q] = passage
	s.hydeMu.Unlock()
	return q + " " + passage
}

type answerSource struct {
	URL         string     `json:"url"`
	Title       string     `json:"title"`
	Excerpt     string     `json:"excerpt,omitempty"`
	PublishedAt *time.Time `json:"published_at,omitempty"`
	Author      string     `json:"author,omitempty"`
	Text        string     `json:"text,omitempty"`
}

type answerResponse struct {
	Query   string         `json:"query"`
	Answer  string         `json:"answer"`
	Sources []answerSource `json:"sources"`
	Model   string         `json:"model"`
	Took    string         `json:"took"`
}

func (s *pebbleHTTP) handleAnswer(w http.ResponseWriter, r *http.Request) {
	if s.chat == nil {
		writeProblem(w, http.StatusNotImplemented,
			"/answer requires cfg.Chat.Model to be set (point cosift.json's chat.url at any OpenAI-compatible endpoint)")
		return
	}
	start := time.Now()
	q := r.URL.Query().Get("q")
	if q == "" {
		writeProblem(w, http.StatusBadRequest, "missing q parameter")
		return
	}
	k := 5
	if v := r.URL.Query().Get("k"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 20 {
			k = n
		}
	}
	// Iter 241: /answer respects the same retrieval filters /search does —
	// scoping research to a domain or date window is the common EXA shape.
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
	includeText := r.URL.Query().Get("include_text") == "true"
	// Iter 249: ?rerank=true reorders the BM25 top-pool before synth.
	// Rerank quality > BM25 quality for "which 5 sources answer this question",
	// so this is the highest-impact iter for /answer beyond getting LLMs hooked
	// up. Widens the candidate pool to rerankCandK before truncation.
	wantRerank := r.URL.Query().Get("rerank") == "true" && s.reranker != nil
	keepCap := k
	if wantRerank {
		keepCap = s.rerankCandK
		if keepCap < k {
			keepCap = k
		}
	}
	fetchK := keepCap
	if len(include) > 0 || len(exclude) > 0 || dateFilter {
		mult := 5
		if dateFilter {
			mult = 10
		}
		fetchK = keepCap * mult
		if fetchK > 200 {
			fetchK = 200 // /answer caps tighter than /search — full doc text per source is expensive
		}
	} else if wantRerank {
		fetchK = keepCap * 2
	}
	// Iter 256/258: ?expand=true HyDE on retrieval; rerank scores original q.
	effectiveQuery := q
	if r.URL.Query().Get("expand") == "true" {
		effectiveQuery = s.expandQuery(r.Context(), q)
	}

	hits, err := s.idx.Search(r.Context(), effectiveQuery, fetchK)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	type cand struct {
		src        answerSource
		excerpt    string
		rerankText string
	}
	cands := make([]cand, 0, keepCap)
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
		doc, derr := s.store.GetDocByURL(r.Context(), h.URL)
		if derr != nil || doc == nil {
			continue
		}
		if dateFilter {
			if doc.PublishedAt.IsZero() {
				continue
			}
			if !since.IsZero() && doc.PublishedAt.Before(since) {
				continue
			}
			if !until.IsZero() && doc.PublishedAt.After(until) {
				continue
			}
		}
		excerpt := textExcerpt(doc.Text, 1200)
		src := answerSource{URL: doc.URL, Title: doc.Title, Excerpt: excerpt, Author: doc.Author}
		if !doc.PublishedAt.IsZero() {
			t := doc.PublishedAt
			src.PublishedAt = &t
		}
		if includeText {
			src.Text = doc.Text
		}
		c := cand{src: src, excerpt: excerpt}
		if wantRerank {
			c.rerankText = doc.Title + "\n" + doc.Text
		}
		cands = append(cands, c)
		if len(cands) >= keepCap {
			break
		}
	}
	// Optional rerank, then truncate to k. Done in this order so citation
	// numbers in the prompt match the final rank-ordered sources list.
	if wantRerank && len(cands) > 1 {
		rc := make([]rerank.Candidate, len(cands))
		for i := range cands {
			rc[i] = rerank.Candidate{ID: strconv.Itoa(i), Text: cands[i].rerankText}
		}
		if order, rerr := s.doRerank(r.Context(), q, rc); rerr == nil && len(order) > 0 {
			reordered := make([]cand, 0, len(cands))
			seen := make(map[int]bool, len(cands))
			for _, id := range order {
				if n, perr := strconv.Atoi(id); perr == nil && n >= 0 && n < len(cands) && !seen[n] {
					reordered = append(reordered, cands[n])
					seen[n] = true
				}
			}
			for i, c := range cands {
				if !seen[i] {
					reordered = append(reordered, c)
				}
			}
			cands = reordered
		}
	}
	if len(cands) > k {
		cands = cands[:k]
	}
	sources := make([]answerSource, 0, len(cands))
	var promptSources strings.Builder
	for i, c := range cands {
		sources = append(sources, c.src)
		fmt.Fprintf(&promptSources, "[%d] %s\n%s\n%s\n\n", i+1, c.src.Title, c.src.URL, c.excerpt)
	}
	if len(sources) == 0 {
		writeJSON(w, http.StatusOK, answerResponse{
			Query: q, Answer: "No matching sources in the index.", Sources: sources,
			Model: s.chat.Model(), Took: time.Since(start).String(),
		})
		return
	}

	msgs := []embed.ChatMsg{
		{Role: "system", Content: answerSystemPrompt},
		{Role: "user", Content: "Sources:\n\n" + promptSources.String() + "Question: " + q},
	}

	// Iter 242: SSE streaming. Opt in via ?stream=true OR
	// Accept: text/event-stream. Emits three event types:
	//   sources — once, immediately after retrieval (so the client can render
	//             links / citations while the LLM is still thinking)
	//   chunk   — per chat delta; payload = {"delta": "..."}
	//   done    — once, with the final {"took": "..."}
	// Falls back to sync if the chat client doesn't implement StreamingChatClient.
	wantStream := r.URL.Query().Get("stream") == "true" ||
		strings.Contains(r.Header.Get("Accept"), "text/event-stream")
	if wantStream {
		if sc, ok := s.chat.(embed.StreamingChatClient); ok {
			streamAnswer(w, r, sc, msgs, sources, q, start)
			return
		}
		// Not a streaming client — degrade silently to sync rather than 501.
	}

	answer, err := s.chat.Chat(r.Context(), msgs)
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "chat: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, answerResponse{
		Query: q, Answer: answer, Sources: sources,
		Model: s.chat.Model(), Took: time.Since(start).String(),
	})
}

func streamAnswer(w http.ResponseWriter, r *http.Request, sc embed.StreamingChatClient, msgs []embed.ChatMsg, sources []answerSource, q string, start time.Time) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeProblem(w, http.StatusInternalServerError, "streaming requires http.Flusher")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	sse := func(payload any) {
		buf, _ := json.Marshal(payload)
		fmt.Fprintf(w, "data: %s\n\n", buf)
		flusher.Flush()
	}
	sse(map[string]any{"type": "sources", "query": q, "sources": sources, "model": sc.Model()})

	_, err := sc.ChatStream(r.Context(), msgs, func(delta string) {
		sse(map[string]any{"type": "chunk", "delta": delta})
	})
	if err != nil {
		sse(map[string]any{"type": "error", "error": err.Error()})
		return
	}
	sse(map[string]any{"type": "done", "took": time.Since(start).String()})
}

// Iter 243: /research — multi-step retrieval + synthesis. LLM decomposes the
// question into 2-3 sub-queries, each sub-query runs BM25, results are deduped
// by URL keeping the best score, top-k feed a cited synthesis. Mirrors the
// SQLite-side /research planner strategy. No streaming, no rerank, no
// paraphrase strategy yet — those follow once this surface is exercised.
const researchPlanPrompt = `Decompose the user's research question into 2-3 focused sub-queries that, taken together, would cover the answer. Output ONLY a JSON array of strings — no prose, no markdown. Example: ["sub-query 1", "sub-query 2"]`

const researchSynthPrompt = `You are a research assistant. Synthesize an answer to the original question using ONLY the provided sources.
- Cite sources by their numeric id, e.g. [1] or [2,3]. Every factual claim needs a citation.
- If the sources don't cover something, say so plainly — do not invent.
- Keep the answer focused on what the sources actually say.`

type researchResponse struct {
	Query   string         `json:"query"`
	Plan    []string       `json:"plan"`
	Answer  string         `json:"answer"`
	Sources []answerSource `json:"sources"`
	Model   string         `json:"model"`
	Took    string         `json:"took"`
}

func (s *pebbleHTTP) handleResearch(w http.ResponseWriter, r *http.Request) {
	if s.chat == nil {
		writeProblem(w, http.StatusNotImplemented,
			"/research requires cfg.Chat.Model to be set")
		return
	}
	start := time.Now()
	q := r.URL.Query().Get("q")
	if q == "" {
		writeProblem(w, http.StatusBadRequest, "missing q parameter")
		return
	}
	k := 8
	if v := r.URL.Query().Get("k"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 20 {
			k = n
		}
	}
	// Iter 246: same retrieval filters /search/answer/find_similar accept.
	// Parsed once here so the streaming and sync paths share semantics.
	filt, err := parseRetrievalFilters(r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, err.Error())
		return
	}
	includeText := r.URL.Query().Get("include_text") == "true"
	// Iter 244: SSE streaming for /research. Same trigger as /answer.
	// Emits phase-aware events so the UI can render the plan and source list
	// before the synth call completes — /research often runs 10–30s
	// (2-3 plan→retrieve→synth chat rounds), so phase visibility matters.
	wantStream := r.URL.Query().Get("stream") == "true" ||
		strings.Contains(r.Header.Get("Accept"), "text/event-stream")
	if wantStream {
		if sc, ok := s.chat.(embed.StreamingChatClient); ok {
			s.streamResearch(w, r, sc, q, k, filt, start)
			return
		}
	}

	// Plan
	planRaw, err := s.chat.Chat(r.Context(), []embed.ChatMsg{
		{Role: "system", Content: researchPlanPrompt},
		{Role: "user", Content: q},
	})
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "plan: "+err.Error())
		return
	}
	subs := parseSubQueries(planRaw, q)
	if len(subs) > 5 {
		subs = subs[:5]
	}

	// Retrieve per sub-query, dedupe by URL keeping best score.
	type ranked struct {
		score float64
		hit   index.Hit
	}
	best := make(map[string]ranked, k*len(subs))
	perSub := k * 2
	if perSub > 40 {
		perSub = 40
	}
	wantExpand := r.URL.Query().Get("expand") == "true"
	for _, sq := range subs {
		effective := sq
		if wantExpand {
			effective = s.expandQuery(r.Context(), sq)
		}
		hits, err := s.idx.Search(r.Context(), effective, perSub)
		if err != nil {
			continue // one sub-query failure shouldn't fail the whole research
		}
		for _, h := range hits {
			if prev, ok := best[h.URL]; !ok || h.Score > prev.score {
				best[h.URL] = ranked{score: h.Score, hit: h}
			}
		}
	}
	pooled := make([]ranked, 0, len(best))
	for _, v := range best {
		pooled = append(pooled, v)
	}
	sort.Slice(pooled, func(i, j int) bool { return pooled[i].score > pooled[j].score })
	// Iter 250: same rerank wiring as /search and /answer — pool widens to
	// rerankCandK before materialization, rerank reorders the pool, truncate
	// to k after rerank so citation numbers track the final order.
	wantRerank := r.URL.Query().Get("rerank") == "true" && s.reranker != nil
	keepCap := k
	if wantRerank {
		keepCap = s.rerankCandK
		if keepCap < k {
			keepCap = k
		}
	}
	if len(pooled) > keepCap {
		pooled = pooled[:keepCap]
	}

	// Materialize candidates; defer sources + prompt build until after rerank.
	type cand struct {
		src        answerSource
		excerpt    string
		rerankText string
	}
	cands := make([]cand, 0, len(pooled))
	for _, p := range pooled {
		doc, derr := s.store.GetDocByURL(r.Context(), p.hit.URL)
		if derr != nil || doc == nil {
			continue
		}
		if !filt.allow(doc.URL, doc.PublishedAt) {
			continue
		}
		excerpt := textExcerpt(doc.Text, 1200)
		src := answerSource{URL: doc.URL, Title: doc.Title, Excerpt: excerpt, Author: doc.Author}
		if !doc.PublishedAt.IsZero() {
			t := doc.PublishedAt
			src.PublishedAt = &t
		}
		if includeText {
			src.Text = doc.Text
		}
		c := cand{src: src, excerpt: excerpt}
		if wantRerank {
			c.rerankText = doc.Title + "\n" + doc.Text
		}
		cands = append(cands, c)
	}
	if wantRerank && len(cands) > 1 {
		rc := make([]rerank.Candidate, len(cands))
		for i := range cands {
			rc[i] = rerank.Candidate{ID: strconv.Itoa(i), Text: cands[i].rerankText}
		}
		if order, rerr := s.doRerank(r.Context(), q, rc); rerr == nil && len(order) > 0 {
			reordered := make([]cand, 0, len(cands))
			seen := make(map[int]bool, len(cands))
			for _, id := range order {
				if n, perr := strconv.Atoi(id); perr == nil && n >= 0 && n < len(cands) && !seen[n] {
					reordered = append(reordered, cands[n])
					seen[n] = true
				}
			}
			for i, c := range cands {
				if !seen[i] {
					reordered = append(reordered, c)
				}
			}
			cands = reordered
		}
	}
	if len(cands) > k {
		cands = cands[:k]
	}
	sources := make([]answerSource, 0, len(cands))
	var promptSources strings.Builder
	for i, c := range cands {
		sources = append(sources, c.src)
		fmt.Fprintf(&promptSources, "[%d] %s\n%s\n%s\n\n", i+1, c.src.Title, c.src.URL, c.excerpt)
	}
	if len(sources) == 0 {
		writeJSON(w, http.StatusOK, researchResponse{
			Query: q, Plan: subs, Answer: "No matching sources for any sub-query.",
			Sources: sources, Model: s.chat.Model(), Took: time.Since(start).String(),
		})
		return
	}

	answer, err := s.chat.Chat(r.Context(), []embed.ChatMsg{
		{Role: "system", Content: researchSynthPrompt},
		{Role: "user", Content: "Sources:\n\n" + promptSources.String() + "Original question: " + q},
	})
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "synth: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, researchResponse{
		Query: q, Plan: subs, Answer: answer, Sources: sources,
		Model: s.chat.Model(), Took: time.Since(start).String(),
	})
}

// streamResearch is the SSE form of handleResearch. Event sequence:
//   plan    — sub-queries returned by the planner
//   sources — deduped + ranked pool fed to the synthesizer
//   chunk   — per chat delta during synth
//   done    — final {"took": "..."}
//   error   — terminal; either plan, retrieval, or synth failed
func (s *pebbleHTTP) streamResearch(w http.ResponseWriter, r *http.Request, sc embed.StreamingChatClient, q string, k int, filt retrievalFilters, start time.Time) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeProblem(w, http.StatusInternalServerError, "streaming requires http.Flusher")
		return
	}
	includeText := r.URL.Query().Get("include_text") == "true"
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	sse := func(payload any) {
		buf, _ := json.Marshal(payload)
		fmt.Fprintf(w, "data: %s\n\n", buf)
		flusher.Flush()
	}

	planRaw, err := sc.Chat(r.Context(), []embed.ChatMsg{
		{Role: "system", Content: researchPlanPrompt},
		{Role: "user", Content: q},
	})
	if err != nil {
		sse(map[string]any{"type": "error", "phase": "plan", "error": err.Error()})
		return
	}
	subs := parseSubQueries(planRaw, q)
	if len(subs) > 5 {
		subs = subs[:5]
	}
	sse(map[string]any{"type": "plan", "query": q, "plan": subs, "model": sc.Model()})

	type ranked struct {
		score float64
		hit   index.Hit
	}
	best := make(map[string]ranked, k*len(subs))
	perSub := k * 2
	if perSub > 40 {
		perSub = 40
	}
	wantExpand := r.URL.Query().Get("expand") == "true"
	for _, sq := range subs {
		effective := sq
		if wantExpand {
			effective = s.expandQuery(r.Context(), sq)
		}
		hits, err := s.idx.Search(r.Context(), effective, perSub)
		if err != nil {
			continue
		}
		for _, h := range hits {
			if prev, ok := best[h.URL]; !ok || h.Score > prev.score {
				best[h.URL] = ranked{score: h.Score, hit: h}
			}
		}
	}
	pooled := make([]ranked, 0, len(best))
	for _, v := range best {
		pooled = append(pooled, v)
	}
	sort.Slice(pooled, func(i, j int) bool { return pooled[i].score > pooled[j].score })
	// Iter 250: same rerank wiring as /research sync. Pool widens to
	// rerankCandK, rerank reorders, then truncate to k before SSE 'sources'
	// fires so the client sees the final rank-ordered list.
	wantRerank := r.URL.Query().Get("rerank") == "true" && s.reranker != nil
	keepCap := k
	if wantRerank {
		keepCap = s.rerankCandK
		if keepCap < k {
			keepCap = k
		}
	}
	if len(pooled) > keepCap {
		pooled = pooled[:keepCap]
	}

	type cand struct {
		src        answerSource
		excerpt    string
		rerankText string
	}
	cands := make([]cand, 0, len(pooled))
	for _, p := range pooled {
		doc, derr := s.store.GetDocByURL(r.Context(), p.hit.URL)
		if derr != nil || doc == nil {
			continue
		}
		if !filt.allow(doc.URL, doc.PublishedAt) {
			continue
		}
		excerpt := textExcerpt(doc.Text, 1200)
		src := answerSource{URL: doc.URL, Title: doc.Title, Excerpt: excerpt, Author: doc.Author}
		if !doc.PublishedAt.IsZero() {
			t := doc.PublishedAt
			src.PublishedAt = &t
		}
		if includeText {
			src.Text = doc.Text
		}
		c := cand{src: src, excerpt: excerpt}
		if wantRerank {
			c.rerankText = doc.Title + "\n" + doc.Text
		}
		cands = append(cands, c)
	}
	if wantRerank && len(cands) > 1 {
		rc := make([]rerank.Candidate, len(cands))
		for i := range cands {
			rc[i] = rerank.Candidate{ID: strconv.Itoa(i), Text: cands[i].rerankText}
		}
		if order, rerr := s.doRerank(r.Context(), q, rc); rerr == nil && len(order) > 0 {
			reordered := make([]cand, 0, len(cands))
			seen := make(map[int]bool, len(cands))
			for _, id := range order {
				if n, perr := strconv.Atoi(id); perr == nil && n >= 0 && n < len(cands) && !seen[n] {
					reordered = append(reordered, cands[n])
					seen[n] = true
				}
			}
			for i, c := range cands {
				if !seen[i] {
					reordered = append(reordered, c)
				}
			}
			cands = reordered
		}
	}
	if len(cands) > k {
		cands = cands[:k]
	}
	sources := make([]answerSource, 0, len(cands))
	var promptSources strings.Builder
	for i, c := range cands {
		sources = append(sources, c.src)
		fmt.Fprintf(&promptSources, "[%d] %s\n%s\n%s\n\n", i+1, c.src.Title, c.src.URL, c.excerpt)
	}
	sse(map[string]any{"type": "sources", "sources": sources})
	if len(sources) == 0 {
		sse(map[string]any{"type": "done", "took": time.Since(start).String(), "empty": true})
		return
	}

	_, err = sc.ChatStream(r.Context(), []embed.ChatMsg{
		{Role: "system", Content: researchSynthPrompt},
		{Role: "user", Content: "Sources:\n\n" + promptSources.String() + "Original question: " + q},
	}, func(delta string) {
		sse(map[string]any{"type": "chunk", "delta": delta})
	})
	if err != nil {
		sse(map[string]any{"type": "error", "phase": "synth", "error": err.Error()})
		return
	}
	sse(map[string]any{"type": "done", "took": time.Since(start).String()})
}

// retrievalFilters bundles the four post-retrieval predicates /search,
// /answer, /find_similar, and /research all share. Centralized in iter 246
// so /research's sync + stream paths stay in lockstep.
type retrievalFilters struct {
	include    []string
	exclude    []string
	since      time.Time
	until      time.Time
	dateActive bool
}

func parseRetrievalFilters(r *http.Request) (retrievalFilters, error) {
	f := retrievalFilters{
		include: splitDomainsCSV(r.URL.Query().Get("include_domains")),
		exclude: splitDomainsCSV(r.URL.Query().Get("exclude_domains")),
	}
	since, err := parseDateBound(r.URL.Query().Get("since"))
	if err != nil {
		return f, fmt.Errorf("since: %w", err)
	}
	until, err := parseDateBound(r.URL.Query().Get("until"))
	if err != nil {
		return f, fmt.Errorf("until: %w", err)
	}
	f.since = since
	f.until = until
	f.dateActive = !since.IsZero() || !until.IsZero()
	return f, nil
}

// allow reports whether (url, publishedAt) clears the filter. publishedAt
// can be the zero Time when the caller doesn't have it loaded; zero is
// treated as "unknown" and dropped under any date filter.
func (f retrievalFilters) allow(rawURL string, publishedAt time.Time) bool {
	if len(f.include) > 0 || len(f.exclude) > 0 {
		host := hostOf(rawURL)
		if len(f.include) > 0 && !matchesAnyDomain(host, f.include) {
			return false
		}
		if len(f.exclude) > 0 && matchesAnyDomain(host, f.exclude) {
			return false
		}
	}
	if f.dateActive {
		if publishedAt.IsZero() {
			return false
		}
		if !f.since.IsZero() && publishedAt.Before(f.since) {
			return false
		}
		if !f.until.IsZero() && publishedAt.After(f.until) {
			return false
		}
	}
	return true
}

func parseSubQueries(raw, fallback string) []string {
	raw = strings.TrimSpace(raw)
	for _, fence := range []string{"```json", "```"} {
		raw = strings.TrimPrefix(raw, fence)
		raw = strings.TrimSuffix(raw, "```")
	}
	raw = strings.TrimSpace(raw)
	startIdx := strings.Index(raw, "[")
	endIdx := strings.LastIndex(raw, "]")
	if startIdx >= 0 && endIdx > startIdx {
		var arr []string
		if err := json.Unmarshal([]byte(raw[startIdx:endIdx+1]), &arr); err == nil && len(arr) > 0 {
			return arr
		}
	}
	return []string{fallback}
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

// Iter 254/255: POST /contents — batch URL → document. Up to 100 URLs in
// one round-trip. URLs not in the index emit a {url, found:false, error}
// stub in the same slot so the caller can correlate positionally. Wire
// shape (results+took, per-item found+cached+lang) mirrors the SQLite-side
// /contents POST so `cosift contents <url> <url>` works against either
// backend without code changes.
type contentsBatchReq struct {
	URLs []string `json:"urls"`
}

type contentsBatchItem struct {
	URL         string    `json:"url"`
	Found       bool      `json:"found"`
	Title       string    `json:"title,omitempty"`
	Text        string    `json:"text,omitempty"`
	Lang        string    `json:"lang,omitempty"`
	Cached      bool      `json:"cached"`
	Author      string    `json:"author,omitempty"`
	PublishedAt time.Time `json:"published_at,omitempty"`
	FetchedAt   time.Time `json:"fetched_at,omitempty"`
	Error       string    `json:"error,omitempty"`
}

func (s *pebbleHTTP) handleContentsBatch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var req contentsBatchReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if len(req.URLs) == 0 {
		writeProblem(w, http.StatusBadRequest, "urls: must be a non-empty array")
		return
	}
	if len(req.URLs) > 100 {
		writeProblem(w, http.StatusBadRequest, "urls: at most 100 per request")
		return
	}
	results := make([]contentsBatchItem, 0, len(req.URLs))
	for _, u := range req.URLs {
		item := contentsBatchItem{URL: u}
		doc, err := s.store.GetDocByURL(r.Context(), u)
		switch {
		case errors.Is(err, store.ErrNotFound):
			item.Error = "not in index"
		case err != nil:
			item.Error = err.Error()
		case doc == nil:
			item.Error = "not in index"
		default:
			item.Found = true
			item.Cached = true
			item.Title = doc.Title
			item.Text = doc.Text
			item.Lang = doc.Lang
			item.Author = doc.Author
			item.PublishedAt = doc.PublishedAt
			item.FetchedAt = doc.FetchedAt
		}
		results = append(results, item)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"results": results,
		"took":    time.Since(start).String(),
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
