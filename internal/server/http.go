// Package server exposes cosift's retrieval over HTTP.
//
// v0 surface: /search and /stats. /research, /answer, /find_similar, /contents
// land in later iterations as their underlying pipelines arrive.
//
// Stdlib net/http only. The mux uses Go 1.22+ method-pattern routing — no
// router dep. All responses are JSON; errors use the standard application/json
// problem shape.
package server

import (
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/calinteodor/cosift/internal/embed"
	"github.com/calinteodor/cosift/internal/index"
	"github.com/calinteodor/cosift/internal/rerank"
	"github.com/calinteodor/cosift/internal/store"
)

// FetchFn fetches and parses a single URL. Used by /contents on store-miss.
// Empty title/text + nil error is acceptable; callers can decide what to do.
// Implementations are responsible for politeness — server doesn't rate-limit.
type FetchFn func(ctx context.Context, url string) (title, text, lang string, err error)

// Server bundles the dependencies an HTTP handler needs.
type Server struct {
	store           *store.Store
	idx             *index.BM25
	vidx            *index.VectorIndex // optional; nil disables dense/hybrid
	emb             embed.Embedder     // optional; nil disables dense/hybrid
	chat            embed.ChatClient   // optional; nil disables /answer + /research
	fetcher         FetchFn            // optional; nil disables on-demand /contents
	reranker        rerank.Reranker    // optional; nil disables ?rerank=true
	rerankCandK     int                // candidates pulled before rerank
	adminToken      string             // optional; empty disables /admin/*
	feedbackLimiter *ipLimiter         // nil = no rate limiting (tests opt out)
	llmLimiter      *ipLimiter         // protects /answer + /research (LLM credits)
	contentsLimiter *ipLimiter         // protects /contents (batch fetch / enumeration)
	adminLimiter    *ipLimiter         // protects /admin/* (defense-in-depth on token leak)
	metrics         *Metrics           // never nil — initialized in New()
	ipResolver      *clientIPResolver  // nil = direct peer only
	paraphraser     *paraphraser       // nil disables ?expand=true
	hyde            *hydePassager      // iter 162: nil disables ?hyde=true. Initialized in WithChat alongside chat.
	defaults        Defaults           // instance-wide retrieval defaults (iter 55)
	chunkSize       int                // iter 145: 0 = index.NewChunker() default (320)
	chunkOverlap    int                // iter 145: 0 = index.NewChunker() default (64)
}

// Defaults are instance-wide retrieval defaults used when a request omits the
// relevant query parameter. Per-request params always override. Mirrors
// config.Defaults but lives in the server package so the config package stays
// import-free of the rest of the project.
//
// Rerank is deliberately NOT a default field: WithReranker already
// auto-enables rerank when configured (iter 13). To get "rerank off by default"
// behavior, callers should pass ?rerank=false explicitly. Adding a Defaults.Rerank
// boolean would need three-state semantics (unset/on/off) to avoid breaking
// existing deployments — not worth the API complexity.
type Defaults struct {
	Retriever        string `json:"retriever"`         // "" | "bm25" | "dense" | "hybrid"
	Expand           bool   `json:"expand"`            //
	ResearchStrategy string `json:"research_strategy"` // "" | "planner" | "paraphrase"
	ResearchSynthK   int    `json:"research_synth_k"`  // 0 = use built-in default (10); positive = cap synth-source count

	// ExpandMainWeight scales the main query's RRF contribution in the
	// expand path relative to paraphrase lists (which always weight 1.0).
	// 0 / non-positive → 1.0 (equal-weight; identical to standard RRF).
	// See config.Defaults.ExpandMainWeight for full docs.
	ExpandMainWeight float64 `json:"expand_main_weight"`

	// HybridDenseWeight scales the dense retriever's RRF contribution in
	// /search?retriever=hybrid; BM25 keeps weight 1.0. 0 / non-positive → 1.0.
	// See config.Defaults.HybridDenseWeight for full docs.
	HybridDenseWeight float64 `json:"hybrid_dense_weight"`
}

// hybridDenseWeightKey is the context-value key for the iter-143 per-request
// override of Defaults.HybridDenseWeight. Threading via context keeps
// runSearch's signature unchanged across its 8+ callsites while still letting
// the handler inject ?hybrid_dense_weight= without touching server state.
type hybridDenseWeightKey struct{}

// bm25QueryOverrideKey carries the iter-163 hybrid-PRF expanded query so the
// hybrid branch of runSearch uses the EXPANDED query for the BM25 sub-call
// while keeping the dense sub-call on the original (or iter-161 HyDE) query.
// PRF mines lexical terms; expanding them only helps the lexical retriever.
type bm25QueryOverrideKey struct{}

// mmrParams carries the iter-158 ?mmr=true&mmr_lambda=N pair through ctx so
// runDense can opt into MMR re-ranking without a signature change.
type mmrParamsKey struct{}
type mmrParams struct {
	enabled bool
	lambda  float64
}

// mmrFromQuery parses ?mmr=true&mmr_lambda=N off a request and returns a ctx
// with mmrParamsKey set when active. Otherwise returns ctx unchanged. Used
// by all retrieval-using handlers (/search, /answer, /research, +streaming)
// so the parsing logic lives in one place — iter-168 extraction at N=5 sites.
//
// Capability checking happens downstream: runDense only swaps Search →
// SearchMMR when the ctx value is present AND retriever is dense/hybrid.
// BM25-only retrieval silently no-ops MMR (matches the iter-158 contract).
func mmrFromQuery(ctx context.Context, r *http.Request) context.Context {
	if v := r.URL.Query().Get("mmr"); v != "true" && v != "1" {
		return ctx
	}
	p := mmrParams{enabled: true, lambda: 0.7}
	if lv := r.URL.Query().Get("mmr_lambda"); lv != "" {
		if parsed, perr := strconv.ParseFloat(lv, 64); perr == nil {
			p.lambda = parsed
		}
	}
	return context.WithValue(ctx, mmrParamsKey{}, p)
}

// New constructs a BM25-only Server. Caller owns store lifecycle.
// Default per-IP limits:
//   - /feedback: 60/min  (cheap write to query_outcomes)
//   - /answer + /research: 10/min  (each call burns LLM tokens)
// Opt out via WithFeedbackLimiter(0, 0) / WithLLMLimiter(0, 0).
func New(s *store.Store) *Server {
	srv := &Server{
		store:           s,
		idx:             index.NewBM25(s),
		feedbackLimiter: newIPLimiter(60, time.Minute),
		llmLimiter:      newIPLimiter(10, time.Minute),
		// /contents batch: conservative — 120/min/IP covers normal
		// operator use while blocking bulk DB enumeration.
		contentsLimiter: newIPLimiter(120, time.Minute),
		// /admin/*: tighter — 30/min/IP is plenty for human ops use
		// and far below an attacker spam pattern if the token leaks.
		adminLimiter: newIPLimiter(30, time.Minute),
		metrics:         NewMetrics(),
	}
	// Wire the corpus-size gauge — read lazily at scrape time so the numbers
	// stay accurate without polling. Errors fall through to zero (don't break /metrics).
	srv.metrics.WithCorpusSize(func() (docs, passages, paraphrases int64) {
		ctx := context.Background()
		if st, err := s.Stats(ctx); err == nil {
			docs = st.Documents
		}
		passages, _ = s.CountPassagesAllModels(ctx)
		paraphrases, _ = s.CountParaphrases(ctx)
		return
	})
	return srv
}

// WithFeedbackLimiter replaces the default /feedback rate limit.
// limit=0 disables limiting (handy in tests).
func (s *Server) WithFeedbackLimiter(limit int, window time.Duration) *Server {
	if limit <= 0 {
		s.feedbackLimiter = nil
	} else {
		s.feedbackLimiter = newIPLimiter(limit, window)
	}
	return s
}

// WithLLMLimiter replaces the default /answer + /research rate limit.
// These endpoints burn LLM credits per call — the limit caps blast radius
// when a public-facing deployment gets discovered.
func (s *Server) WithLLMLimiter(limit int, window time.Duration) *Server {
	if limit <= 0 {
		s.llmLimiter = nil
	} else {
		s.llmLimiter = newIPLimiter(limit, window)
	}
	return s
}

// rateLimit returns a handler wrapper that enforces a per-IP cap before
// invoking next. Use case: protect public mutate / costly endpoints.
// Nil limiter is a no-op so callers can drop it in unconditionally.
func (s *Server) rateLimit(limiter *ipLimiter, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if limiter != nil && !limiter.Allow(s.resolveClientIP(r)) {
			if s.metrics != nil {
				s.metrics.RecordRateLimit()
			}
			writeProblem(w, http.StatusTooManyRequests, "rate limit exceeded for this IP")
			return
		}
		next(w, r)
	}
}

// WithVector wires a VectorIndex and Embedder. Enables ?retriever=dense|hybrid
// and /find_similar.
func (s *Server) WithVector(vi *index.VectorIndex, e embed.Embedder) *Server {
	s.vidx = vi
	s.emb = e
	return s
}

// WithChat wires a chat-completion client. Enables /answer and (iter 161/162)
// the ?hyde=true query expansion. The HyDE passager carries a 2-level cache —
// L1 in-memory + L2 SQLite — so cold processes skip the LLM call on popular
// queries via the L2 hit. Same shape as iter-89's paraphraser cache.
func (s *Server) WithChat(c embed.ChatClient) *Server {
	s.chat = c
	s.hyde = newHydePassager(c, s.store, s.metrics)
	return s
}

// WithFetcher wires an on-demand fetcher used by /contents when the URL isn't
// already in the store. Without this, a store miss returns 404.
func (s *Server) WithFetcher(f FetchFn) *Server {
	s.fetcher = f
	return s
}

// paraphraser handles query expansion via a chat client. Iter-45 measured
// auto-paraphrase recovers 0.02 nDCG at the 10k-diverse-noise ceiling and even
// nudges 0-distractor pipelines from 0.99 → 1.00. Iter-47 added a SQLite-backed
// cache so cold processes (refresh sidecar, second Cloud Run instance,
// post-restart server) skip the LLM call on popular queries.
//
// Lookup order: in-memory L1 → store L2 → LLM. Save order: LLM → store + L1.
type paraphraser struct {
	chat    embed.ChatClient
	n       int
	store   *store.Store // optional L2; nil = L1-only
	metrics *Metrics     // optional; nil disables cache observability
	mu      sync.Mutex
	cache   map[string][]string // L1 key: model + "\x00" + query
}

func newParaphraser(chat embed.ChatClient, n int, s *store.Store, m *Metrics) *paraphraser {
	if n <= 0 {
		n = 2
	}
	return &paraphraser{chat: chat, n: n, store: s, metrics: m, cache: make(map[string][]string)}
}

// generate returns N paraphrases, with the L1+L2 cache pattern above.
func (p *paraphraser) generate(ctx context.Context, q string) []string {
	key := p.chat.Model() + "\x00" + q
	p.mu.Lock()
	if cached, ok := p.cache[key]; ok {
		p.mu.Unlock()
		if p.metrics != nil {
			p.metrics.RecordParaphraseL1Hit()
		}
		return cached
	}
	p.mu.Unlock()

	// L2: SQLite-persisted cache.
	if p.store != nil {
		if cached, err := p.store.GetParaphrases(ctx, p.chat.Model(), q); err == nil && len(cached) > 0 {
			p.mu.Lock()
			p.cache[key] = cached
			p.mu.Unlock()
			if p.metrics != nil {
				p.metrics.RecordParaphraseL2Hit()
			}
			return cached
		}
	}

	// L3: LLM call. Record the miss whether or not the LLM ultimately succeeds.
	if p.metrics != nil {
		p.metrics.RecordParaphraseMiss()
	}

	const sys = `Generate paraphrases of a search query. Each paraphrase preserves the semantic intent but uses different vocabulary — different keywords that a target document might also use. Output ONLY a JSON array of strings.
Example output for "go programming language": ["golang concurrent compiled language", "Google's systems programming language with goroutines"]`

	resp, err := p.chat.Chat(ctx, []embed.ChatMsg{
		{Role: "system", Content: sys},
		{Role: "user", Content: fmt.Sprintf("Generate %d paraphrases of: %s", p.n, q)},
	})
	if err != nil {
		return nil
	}
	raw := strings.TrimSpace(resp)
	for _, fence := range []string{"```json", "```"} {
		raw = strings.TrimPrefix(raw, fence)
		raw = strings.TrimSuffix(raw, "```")
	}
	raw = strings.TrimSpace(raw)
	start := strings.Index(raw, "[")
	end := strings.LastIndex(raw, "]")
	if start < 0 || end <= start {
		return nil
	}
	var arr []string
	if err := json.Unmarshal([]byte(raw[start:end+1]), &arr); err != nil {
		return nil
	}
	if len(arr) > p.n {
		arr = arr[:p.n]
	}
	p.mu.Lock()
	p.cache[key] = arr
	p.mu.Unlock()
	if p.store != nil {
		_ = p.store.SaveParaphrases(ctx, p.chat.Model(), q, arr) // best-effort
	}
	return arr
}

// WithParaphraser enables /search?expand=true. The chat client should be
// configured for low latency (gpt-4o-mini, claude-haiku, or a local model)
// since this is on the user-facing critical path.
func (s *Server) WithParaphraser(chat embed.ChatClient, n int) *Server {
	if chat == nil {
		return s
	}
	s.paraphraser = newParaphraser(chat, n, s.store, s.metrics)
	return s
}

// WithReranker enables /search?rerank=true (and makes it the default).
// candidateK is how many candidates the inner retriever produces before reranking;
// pass 0 for the sensible default (max(20, 5*k)).
// WithChunker overrides the default passage chunker settings used by the
// dense indexer + /admin/reembed. Values ≤0 fall back to index.NewChunker()
// defaults (320 words / 64 overlap). Iter 145: lets operators keep reembed
// passage shapes consistent with cfg.Crawler.ChunkSize-driven crawl indexing.
func (s *Server) WithChunker(size, overlap int) *Server {
	if size > 0 {
		s.chunkSize = size
	}
	if overlap > 0 {
		s.chunkOverlap = overlap
	}
	return s
}

func (s *Server) WithReranker(r rerank.Reranker, candidateK int) *Server {
	s.reranker = r
	s.rerankCandK = candidateK
	return s
}

// WithDefaults sets the instance-wide retrieval defaults applied when a
// request omits the relevant query parameter. Per-request params override.
// Lets operators tweak a cosift deployment for a specific use case without
// forcing every client URL to repeat the params.
func (s *Server) WithDefaults(d Defaults) *Server {
	s.defaults = d
	return s
}

// WithAdminToken protects /admin/* endpoints. Empty token leaves them disabled
// (return 403 with a clear message). Same Bearer pattern as the embed clients.
func (s *Server) WithAdminToken(token string) *Server {
	s.adminToken = token
	return s
}

// WithTrustedProxies parses the given CIDR list as reverse proxies whose
// X-Forwarded-For headers the server should trust when extracting client IPs
// for rate limiting. Returns an error on malformed CIDRs so misconfiguration
// fails loud instead of silently falling back to "trust nothing."
func (s *Server) WithTrustedProxies(cidrs []string) (*Server, error) {
	r, err := newClientIPResolver(cidrs)
	if err != nil {
		return nil, err
	}
	s.ipResolver = r
	return s, nil
}

// resolveClientIP returns the per-request client identifier used for rate
// limiting. With no trusted proxies configured this is the direct TCP peer's
// IP; with proxies configured and the peer in the trusted set, walks XFF.
func (s *Server) resolveClientIP(r *http.Request) string {
	if s.ipResolver != nil {
		return s.ipResolver.Resolve(r)
	}
	return clientIP(r)
}

// Handler returns an http.Handler ready to mount.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	// /health alias: some platforms (Cloud Run, App Engine) intercept the
	// /healthz path at their LB before it reaches the container. /health works
	// everywhere. The Dockerfile HEALTHCHECK uses /healthz which is correct
	// inside the container's network namespace; this alias is for external
	// callers / proxies that have to traverse the platform LB.
	mux.HandleFunc("GET /health", s.handleHealthz)
	mux.HandleFunc("GET /stats", s.handleStats)
	mux.HandleFunc("GET /search", s.handleSearch)
	mux.HandleFunc("GET /sitemap.xml", s.handleSitemap)
	mux.HandleFunc("GET /robots.txt", s.handleRobotsTxt)
	mux.HandleFunc("GET /find_similar", s.handleFindSimilar)
	mux.HandleFunc("GET /contents", s.rateLimit(s.contentsLimiter, s.handleContents))
	mux.HandleFunc("POST /contents", s.rateLimit(s.contentsLimiter, s.handleContentsBatch))
	mux.HandleFunc("GET /answer", s.rateLimit(s.llmLimiter, s.handleAnswer))
	mux.HandleFunc("GET /research", s.rateLimit(s.llmLimiter, s.handleResearch))
	mux.HandleFunc("POST /admin/recrawl", s.rateLimit(s.adminLimiter, s.requireAdmin(s.handleAdminRecrawl)))
	mux.HandleFunc("POST /admin/recrawl-by-domain", s.rateLimit(s.adminLimiter, s.requireAdmin(s.handleAdminRecrawlByDomain)))
	mux.HandleFunc("POST /admin/reembed", s.rateLimit(s.adminLimiter, s.requireAdmin(s.handleAdminReembed)))
	mux.HandleFunc("GET /admin/stats", s.rateLimit(s.adminLimiter, s.requireAdmin(s.handleAdminStats)))
	mux.HandleFunc("GET /admin/config", s.rateLimit(s.adminLimiter, s.requireAdmin(s.handleAdminConfig)))
	mux.HandleFunc("GET /dashboard", s.handleDashboard)
	mux.HandleFunc("POST /feedback", s.handleFeedback)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	return s.logMiddleware(mux)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	s.metrics.WritePrometheus(w)
	s.writeInfoMetric(w)
}

// Version is set at link time via -ldflags="-X github.com/.../server.Version=..."
// Default is "dev" for unstamped builds. Exposed in the cosift_info metric so
// Grafana / alerts can pin charts to a specific release.
var Version = "dev"

// writeInfoMetric emits the cosift_info{...} 1 line — the Prometheus pattern
// for build + capability labels. Always value=1; the information lives in the
// labels. Queries: `cosift_info` to read all labels, joined with other metrics
// via group_left if needed.
func (s *Server) writeInfoMetric(w http.ResponseWriter) {
	embedder, chat, reranker := "", "", ""
	if s.emb != nil {
		embedder = s.emb.Model()
	}
	if s.chat != nil {
		chat = s.chat.Model()
	}
	if s.reranker != nil {
		reranker = s.reranker.Name()
	}
	bool01 := func(b bool) int {
		if b {
			return 1
		}
		return 0
	}
	_, _ = w.Write([]byte("# HELP cosift_info Build and capability labels. Value is always 1.\n"))
	_, _ = w.Write([]byte("# TYPE cosift_info gauge\n"))
	_, _ = w.Write([]byte("cosift_info{"))
	_, _ = w.Write([]byte("version=\"" + Version + "\","))
	_, _ = w.Write([]byte("embedder=\"" + embedder + "\","))
	_, _ = w.Write([]byte("chat=\"" + chat + "\","))
	_, _ = w.Write([]byte("reranker=\"" + reranker + "\","))
	_, _ = w.Write([]byte("dense_enabled=\"" + boolStr(s.vidx != nil && s.emb != nil) + "\","))
	_, _ = w.Write([]byte("answer_enabled=\"" + boolStr(s.chat != nil) + "\","))
	_, _ = w.Write([]byte("admin_enabled=\"" + boolStr(s.adminToken != "") + "\","))
	_, _ = w.Write([]byte("trusted_xff=\"" + boolStr(s.ipResolver != nil) + "\""))
	_, _ = w.Write([]byte("} 1\n"))
	_ = bool01 // silence unused (used inline)
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// FeedbackRequest is what clients POST when reporting a user's verdict on
// a retrieval result. Public endpoint (no admin auth) — feedback is opt-in,
// not sensitive, and we want as little friction as possible to collect it.
//
// Future calibration: once query_outcomes has ≥10k rows with both useful=true
// and useful=false represented, fit a logistic regression mapping (score,
// retriever_kind, length, ...) → P(useful). Until then the data just sits.
type FeedbackRequest struct {
	Query  string  `json:"query"`
	URL    string  `json:"url"`
	Score  float64 `json:"score"`
	Useful bool    `json:"useful"`
	Source string  `json:"source"` // 'thumbs' | 'click' | 'explicit' | ...
}

func (s *Server) handleFeedback(w http.ResponseWriter, r *http.Request) {
	// Reuse the shared limiter wrapper. Inline here because /feedback is the
	// only feedback-specific endpoint; the LLM endpoints go through rateLimit
	// at mux registration.
	if s.feedbackLimiter != nil && !s.feedbackLimiter.Allow(s.resolveClientIP(r)) {
		writeProblem(w, http.StatusTooManyRequests, "rate limit exceeded (60 requests / minute / IP)")
		return
	}
	var req FeedbackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Query == "" || req.URL == "" {
		writeProblem(w, http.StatusBadRequest, "query and url are required")
		return
	}
	if err := s.store.RecordOutcome(r.Context(), &store.Outcome{
		Query: req.Query, URL: req.URL, Score: req.Score, Useful: req.Useful, Source: req.Source,
	}); err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"recorded": true})
}

// AdminStatsResponse is the richer counterpart of /stats — what an operator
// monitoring script wants to scrape and chart.
type AdminStatsResponse struct {
	Documents      int64               `json:"documents"`
	Terms          int64               `json:"terms"`
	Passages       int64               `json:"passages"`
	Frontier       store.FrontierStats `json:"frontier"`
	TopDomains     map[string]int64    `json:"top_domains"`
	RerankerName   string              `json:"reranker,omitempty"`
	EmbedderModel  string              `json:"embedder,omitempty"`
	ChatModel      string              `json:"chat,omitempty"`
	DenseEnabled   bool                `json:"dense_enabled"`
	AnswerEnabled  bool                `json:"answer_enabled"`
	FetcherEnabled bool                `json:"fetcher_enabled"`
	// DocsWithPublishedAt counts how many documents have a known publication
	// date — tells operators what fraction of the corpus is filterable via
	// `/search?since=/?until=` (iter 77) or sortable via `?sort=date_desc`
	// (iter 78). Iter 80.
	DocsWithPublishedAt int64 `json:"docs_with_published_at"`
	// Paraphrases is the count of cached LLM paraphrase entries (iter 47
	// added the cache; iter 49 added metric counters; iter 171 finally
	// surfaces the corpus-wide total here). High value → operators have a
	// repetitive query workload and the cache is paying off.
	Paraphrases int64 `json:"paraphrases"`
	// HyDECache is the count of cached HyDE passages (iter 162 added the
	// cache; iter 171 surfaces the size). Same operator signal as
	// Paraphrases — repeat queries skip the LLM call.
	HyDECache int64 `json:"hyde_cache"`
}

func (s *Server) handleAdminStats(w http.ResponseWriter, r *http.Request) {
	st, err := s.store.Stats(r.Context())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	fs, err := s.store.GetFrontierStats(r.Context())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	passages, _ := s.store.CountPassagesAllModels(r.Context())
	topDomains, _ := s.store.CountByDomain(r.Context(), 20)
	docsWithPublishedAt, _ := s.store.CountDocsWithPublishedAt(r.Context())
	// Iter 171: surface LLM-cache sizes so operators can spot under-warming
	// (low count + expected high traffic = cache not picking up repeats) or
	// drift (count plateaued at a stale value across paraphraser/HyDE model
	// changes). Best-effort — failure leaves the counter at zero.
	paraphrases, _ := s.store.CountParaphrases(r.Context())
	hydeCache, _ := s.store.CountHyDE(r.Context())

	resp := AdminStatsResponse{
		Documents:           st.Documents,
		Terms:               st.Terms,
		Passages:            passages,
		Frontier:            fs,
		TopDomains:          topDomains,
		DenseEnabled:        s.vidx != nil && s.emb != nil,
		AnswerEnabled:       s.chat != nil,
		FetcherEnabled:      s.fetcher != nil,
		DocsWithPublishedAt: docsWithPublishedAt,
		Paraphrases:         paraphrases,
		HyDECache:           hydeCache,
	}
	if s.emb != nil {
		resp.EmbedderModel = s.emb.Model()
	}
	if s.chat != nil {
		resp.ChatModel = s.chat.Model()
	}
	if s.reranker != nil {
		resp.RerankerName = s.reranker.Name()
	}
	writeJSON(w, http.StatusOK, resp)
}

// AdminConfigResponse reports the running instance's resolved retrieval
// defaults + capability flags. Lets an operator verify what their cosift has
// actually loaded without restarting or reading log lines. Iter-55 made
// defaults configurable; iter-57 makes them inspectable.
type AdminConfigResponse struct {
	Version      string   `json:"version"`
	Defaults     Defaults `json:"defaults"`
	Capabilities Caps     `json:"capabilities"`
}

// Caps mirrors what `cosift_info` exposes on /metrics, in JSON shape. Lets the
// operator answer "is dense enabled?" with curl + jq, no metric scraping.
type Caps struct {
	DenseEnabled       bool   `json:"dense_enabled"`
	ChatEnabled        bool   `json:"chat_enabled"`
	RerankEnabled      bool   `json:"rerank_enabled"`
	ParaphraserEnabled bool   `json:"paraphraser_enabled"`
	FetcherEnabled     bool   `json:"fetcher_enabled"`
	AdminEnabled       bool   `json:"admin_enabled"`
	EmbedderModel      string `json:"embedder_model,omitempty"`
	ChatModel          string `json:"chat_model,omitempty"`
	RerankerName       string `json:"reranker_name,omitempty"`
}

func (s *Server) handleAdminConfig(w http.ResponseWriter, _ *http.Request) {
	resp := AdminConfigResponse{
		Version:  Version,
		Defaults: s.defaults,
		Capabilities: Caps{
			DenseEnabled:       s.vidx != nil && s.emb != nil,
			ChatEnabled:        s.chat != nil,
			RerankEnabled:      s.reranker != nil,
			ParaphraserEnabled: s.paraphraser != nil,
			FetcherEnabled:     s.fetcher != nil,
			AdminEnabled:       s.adminToken != "",
		},
	}
	if s.emb != nil {
		resp.Capabilities.EmbedderModel = s.emb.Model()
	}
	if s.chat != nil {
		resp.Capabilities.ChatModel = s.chat.Model()
	}
	if s.reranker != nil {
		resp.Capabilities.RerankerName = s.reranker.Name()
	}
	writeJSON(w, http.StatusOK, resp)
}

// requireAdmin wraps an HTTP handler with a Bearer-token check.
// Always-403 when no admin token is configured — we don't want a missing
// config silently leaving the admin surface open.
func (s *Server) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.adminToken == "" {
			writeProblem(w, http.StatusForbidden, "admin endpoints disabled (no admin_token configured)")
			return
		}
		auth := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if len(auth) < len(prefix) || auth[:len(prefix)] != prefix {
			writeProblem(w, http.StatusUnauthorized, "invalid or missing bearer token")
			return
		}
		// Constant-time comparison closes the per-byte timing side
		// channel that `!=` exposed. Without this, an attacker who can
		// measure response timing can brute-force the admin token one
		// byte at a time. subtle.ConstantTimeCompare returns 1 only
		// when both slices have equal length AND equal bytes.
		if subtle.ConstantTimeCompare([]byte(auth[len(prefix):]), []byte(s.adminToken)) != 1 {
			writeProblem(w, http.StatusUnauthorized, "invalid or missing bearer token")
			return
		}
		next(w, r)
	}
}

// AdminRecrawlRequest is the body shape for /admin/recrawl.
type AdminRecrawlRequest struct {
	URLs []string `json:"urls"`
}

// AdminRecrawlResponse reports per-URL outcome.
type AdminRecrawlResponse struct {
	Queued []string          `json:"queued"`
	Errors map[string]string `json:"errors,omitempty"`
}

// handleAdminRecrawl re-enqueues URLs into the frontier. Does NOT run the
// crawler — the refresh-due daemon (or `cosift crawl`) picks them up on the
// next pass. This split keeps the API endpoint stateless and quick.
func (s *Server) handleAdminRecrawl(w http.ResponseWriter, r *http.Request) {
	var req AdminRecrawlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if len(req.URLs) == 0 {
		writeProblem(w, http.StatusBadRequest, "urls array required and non-empty")
		return
	}
	if len(req.URLs) > 1000 {
		writeProblem(w, http.StatusBadRequest, "max 1000 URLs per request")
		return
	}
	resp := AdminRecrawlResponse{Queued: make([]string, 0, len(req.URLs)), Errors: map[string]string{}}
	for _, u := range req.URLs {
		if err := s.store.RecrawlURL(r.Context(), u); err != nil {
			resp.Errors[u] = err.Error()
			continue
		}
		resp.Queued = append(resp.Queued, u)
	}
	if len(resp.Errors) == 0 {
		resp.Errors = nil
	}
	writeJSON(w, http.StatusOK, resp)
}

// AdminRecrawlByDomainRequest is the body shape for POST /admin/recrawl-by-domain.
// Iter 110 — bulk recrawl every doc whose domain matches `domain` (iter-79
// suffix-on-dot-boundary semantics: `example.com` matches `example.com` AND
// `*.example.com`, not `evilexample.com`).
type AdminRecrawlByDomainRequest struct {
	Domain string `json:"domain"`
	// DryRun (iter 111) — if true, enumerate matched URLs but skip the actual
	// RecrawlURL calls. Operator sees the count before committing. `queued`
	// in the response is 0 for dry-run; `matched` reports the would-be queue size.
	DryRun bool `json:"dry_run,omitempty"`
}

// AdminRecrawlByDomainResponse reports the bulk-recrawl outcome.
type AdminRecrawlByDomainResponse struct {
	Domain  string            `json:"domain"`
	Matched int               `json:"matched"`           // URLs found by the domain pattern
	Queued  int               `json:"queued"`            // URLs successfully re-enqueued (0 when dry_run)
	DryRun  bool              `json:"dry_run,omitempty"` // echo of the request flag — lets clients distinguish "0 queued because dry-run" from "0 queued because all errored"
	Errors  map[string]string `json:"errors,omitempty"`  // per-URL enqueue failures
	// URLs (iter 123) — populated ONLY when dry_run=true. Lets operators
	// preview "show me WHICH URLs the pattern matches" before committing -y.
	// In non-dry-run mode, the URLs were queued (or errored) and `urls` would
	// duplicate `queued` + `errors`; omitempty keeps the non-dry-run response shape small.
	URLs []string `json:"urls,omitempty"`
}

// handleAdminRecrawlByDomain enumerates docs by domain and re-enqueues them
// into the frontier in one call. The composition operators currently do as
// `cosift export -include-domains X | jq -r .url | xargs cosift admin recrawl -y`
// becomes a single endpoint. Iter 110 — pairs with iter-104's export filter.
//
// Capped at 10000 URLs per call to bound blast radius (matches iter-88's
// batch-/contents cap pattern). Larger sweeps should be split.
func (s *Server) handleAdminRecrawlByDomain(w http.ResponseWriter, r *http.Request) {
	var req AdminRecrawlByDomainRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if strings.TrimSpace(req.Domain) == "" {
		writeProblem(w, http.StatusBadRequest, "domain required and non-empty")
		return
	}

	urls, err := s.store.ListURLsByDomain(r.Context(), req.Domain)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, fmt.Sprintf("enumerate: %v", err))
		return
	}
	const maxURLs = 10000
	if len(urls) > maxURLs {
		writeProblem(w, http.StatusBadRequest,
			fmt.Sprintf("matched %d URLs; max %d per call — narrow the pattern or split the sweep", len(urls), maxURLs))
		return
	}

	resp := AdminRecrawlByDomainResponse{
		Domain:  req.Domain,
		Matched: len(urls),
		DryRun:  req.DryRun,
		Errors:  map[string]string{},
	}
	if req.DryRun {
		// Iter 123: return the matched URLs in dry-run so operators can
		// preview which docs would be re-crawled. Non-dry-run skips this
		// (URLs were just queued; client doesn't need them echoed back).
		resp.URLs = urls
	} else {
		for _, u := range urls {
			if err := s.store.RecrawlURL(r.Context(), u); err != nil {
				resp.Errors[u] = err.Error()
				continue
			}
			resp.Queued++
		}
	}
	if len(resp.Errors) == 0 {
		resp.Errors = nil
	}
	writeJSON(w, http.StatusOK, resp)
}

// AdminReembedRequest is the body shape for POST /admin/reembed. Iter 112 —
// re-embed every document with the server's currently-configured embedder
// (s.emb.Model()). Operators who want to swap models do it via cfg restart
// then trigger this; the new-model passages coexist with old-model passages
// until `drop_old` is set, matching the iter-39 reembed semantics.
type AdminReembedRequest struct {
	DropOld bool `json:"drop_old,omitempty"`
	// Since (iter 116) — when non-empty, restrict reembed to docs with
	// `published_at >= since`. Accepts YYYY-MM-DD or RFC3339, same as the
	// iter-77 /search?since=. Useful when only recent docs need re-embedding
	// (e.g., model swap mid-quarter, skipping older docs to save LLM credits).
	// Undated docs (zero PublishedAt) are skipped when filter is active.
	Since string `json:"since,omitempty"`
	// DryRun (iter 125) — when true, enumerate matched docs via the filter
	// pipeline but skip the embed loop entirely. `started` event reports the
	// would-be-processed count; `done` event echoes dry_run:true with
	// docs_processed:0 + passages_written:0. Lets operators preview an
	// expensive op before committing LLM credits.
	DryRun bool `json:"dry_run,omitempty"`
}

// handleAdminReembed re-embeds every document via SSE-streamed progress. Long-
// running op — synchronous in the request goroutine but emits `progress`
// events every ~2s so clients see liveness. ctx cancellation (client
// disconnect) aborts cleanly. Iter 112 — server side of the iter 112/113 arc.
//
// Event types (same shape as iter-98/108 streaming pattern):
//   - "started":  { "total_docs": N, "target_model": "..." }
//   - "progress": { "docs_processed": N, "passages_written": N }
//   - "done":     { "docs_processed": N, "passages_written": N, "dropped_old": N, "took": "..." }
//   - "error":    { "detail": "..." }
//
// Stream always ends with exactly one terminal event (done or error).
func (s *Server) handleAdminReembed(w http.ResponseWriter, r *http.Request) {
	if s.emb == nil {
		writeProblem(w, http.StatusBadRequest, "/admin/reembed requires a configured embedder (set cfg.Embeddings.Model + OPENAI key)")
		return
	}
	var req AdminReembedRequest
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeProblem(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
	}

	// Iter 116: parse `since` BEFORE opening the SSE stream — a malformed
	// date should return 400 with a structured error, not an SSE error event
	// (operators using curl + jq expect the iter-77 /search?since= behavior).
	var sinceT time.Time
	if req.Since != "" {
		t, err := parseSearchDate(req.Since)
		if err != nil {
			writeProblem(w, http.StatusBadRequest, fmt.Sprintf("invalid since: %v", err))
			return
		}
		sinceT = t
	}

	emit, bail, ok := sseHandler(w)
	if !ok {
		return
	}

	start := time.Now()
	docs, err := s.store.ListDocuments(r.Context(), 0)
	if err != nil {
		bail(fmt.Sprintf("list docs: %v", err))
		return
	}
	// Iter 116: in-place filter for since=DATE. Undated docs (zero PublishedAt)
	// are dropped when the filter is active, matching iter-77 /search?since=
	// semantics. Done before the started event so total_docs reflects what
	// will actually be processed.
	if !sinceT.IsZero() {
		kept := docs[:0]
		for _, d := range docs {
			if d.PublishedAt.IsZero() || d.PublishedAt.Before(sinceT) {
				continue
			}
			kept = append(kept, d)
		}
		docs = kept
	}
	target := s.emb.Model()
	emit("started", map[string]any{
		"total_docs":   len(docs),
		"target_model": target,
	})
	// Iter 125: dry-run short-circuit. After the started event reports the
	// would-be-processed count, emit done immediately with zeros + dry_run:true.
	// Skips the embed loop entirely — no LLM credit spend.
	if req.DryRun {
		emit("done", map[string]any{
			"docs_processed":   0,
			"passages_written": 0,
			"dropped_old":      0,
			"dry_run":          true,
			"took":             time.Since(start).String(),
		})
		return
	}
	if len(docs) == 0 {
		emit("done", map[string]any{
			"docs_processed":   0,
			"passages_written": 0,
			"dropped_old":      0,
			"took":             time.Since(start).String(),
		})
		return
	}

	// Iter 145 / iter 147: honor server-configured chunker overrides via
	// iter-147's NewChunkerWith helper. Falls back to NewChunker defaults
	// (320/64) when both fields are zero.
	chunker := index.NewChunkerWith(s.chunkSize, s.chunkOverlap)
	const batchSize = 64
	var passagesWritten int
	lastProgress := time.Now()
	const progressInterval = 2 * time.Second

	for i, d := range docs {
		// Honor client cancellation.
		if err := r.Context().Err(); err != nil {
			bail(fmt.Sprintf("client cancelled: %v", err))
			return
		}
		chunks := chunker.Chunk(d.Title + "\n\n" + d.Text)
		if len(chunks) == 0 {
			continue
		}
		// Embed in batches.
		for off := 0; off < len(chunks); off += batchSize {
			end := off + batchSize
			if end > len(chunks) {
				end = len(chunks)
			}
			texts := make([]string, end-off)
			for j, c := range chunks[off:end] {
				texts[j] = c.Text
			}
			vecs, embedErr := s.emb.Embed(r.Context(), texts)
			if embedErr != nil {
				bail(fmt.Sprintf("embed (doc %s): %v", d.URL, embedErr))
				return
			}
			for j, v := range vecs {
				p := &store.Passage{
					DocID:     d.ID,
					Offset:    chunks[off+j].Offset,
					Length:    chunks[off+j].Length,
					Model:     target,
					Embedding: v,
				}
				if err := s.store.UpsertPassage(r.Context(), p); err != nil {
					bail(fmt.Sprintf("write passage (doc %s offset %d): %v", d.URL, chunks[off+j].Offset, err))
					return
				}
				passagesWritten++
			}
		}
		if time.Since(lastProgress) >= progressInterval {
			emit("progress", map[string]any{
				"docs_processed":   i + 1,
				"passages_written": passagesWritten,
			})
			lastProgress = time.Now()
		}
	}

	var dropped int64
	if req.DropOld {
		n, err := s.store.DropPassagesNotModel(r.Context(), target)
		if err != nil {
			bail(fmt.Sprintf("drop old: %v", err))
			return
		}
		dropped = n
	}

	emit("done", map[string]any{
		"docs_processed":   len(docs),
		"passages_written": passagesWritten,
		"dropped_old":      dropped,
		"took":             time.Since(start).String(),
	})
}

// SearchResponse is the on-the-wire shape of /search results.
type SearchResponse struct {
	Query string         `json:"query"`
	K     int            `json:"k"`
	Hits  []SearchHit    `json:"hits"`
	Took  string         `json:"took"`
	Meta  map[string]any `json:"meta,omitempty"`
}

type SearchHit struct {
	URL       string     `json:"url"`
	Title     string     `json:"title"`
	Score     float64    `json:"score"`
	Source    string     `json:"source"` // 'bm25' / 'dense' / 'hybrid' / 'pilot' / 'external'
	Highlight *Highlight `json:"highlight,omitempty"`
	// Iter 82: enrichment fields surfaced from the indexed document so callers
	// don't have to round-trip /contents per hit. Both fields are
	// `omitempty`-tagged so unindexed hosts (`Domain`) and undated pages
	// (`PublishedAt`) emit clean JSON without nulls.
	Domain      string     `json:"domain,omitempty"`
	PublishedAt *time.Time `json:"published_at,omitempty"`
	// Iter 83: short prefix of document body, populated only when Highlight is
	// absent (BM25-only hits). Gives every hit a preview text without
	// duplicating the dense/hybrid Highlight payload when both are available.
	// ~500 chars from the start of the body; callers wanting full text use /contents.
	Excerpt string `json:"excerpt,omitempty"`

	// Iter 148: full (or truncated) document body, populated only when the
	// request passes ?include_text=true.— saves a /contents
	// round-trip for one-shot research pipelines. Capped by ?max_text=N
	// (default 5000) so a stray ?include_text=true on a multi-MB corpus
	// doesn't blow up bandwidth. Omitted from JSON when unset.
	Text string `json:"text,omitempty"`

	// Iter 150: author extracted from JSON-LD `author.name` at parse time
	// (iter 76 wrote the parser; iter 150 persists + surfaces).
	// for SearchHit.author. Empty when absent (most pages don't carry it).
	Author string `json:"author,omitempty"`

	// Iter 155: image URL extracted from og:image / twitter:image / JSON-LD
	// `image`. Empty when absent."doc-card" rendering on the
	// caller side — no /contents round-trip needed for a search-result card.
	Image string `json:"image,omitempty"`

	// Iter 156: favicon URL (absolute) extracted from <link rel="icon"> etc.
	// Empty when the page doesn't declare one — callers can fall back to the
	// well-known `/favicon.ico` at the host themselves.for
	// "link-card" rendering.
	Favicon string `json:"favicon,omitempty"`

	// Iter 164: within-result score normalization. Populated when the request
	// passes ?calibrate=true. Computed as `score / max(score across hits)` so
	// the top hit is always 1.0 and lower-ranked hits express their relative
	// closeness as a fraction. This is NOT cross-query calibration — the
	// `Calibrated:false` field on /answer/research responses still reflects
	// the unsolved absolute-confidence problem (needs outcome data). Within
	// a single response, ScoreCalibrated is the right signal for "X is twice
	// as confident as Y" UIs and for filtering out low-confidence tails.
	// Omitempty: clients that don't ask for it never see the field.
	ScoreCalibrated float64 `json:"score_calibrated,omitempty"`
}

// Highlight is the matched passage span when retrieval picked a specific chunk.
// Populated by dense/hybrid hits where multi-passage chunking gives us the span.
// Nil on bm25-only hits (no per-passage offset yet).
type Highlight struct {
	Offset int    `json:"offset"`
	Length int    `json:"length"`
	Text   string `json:"text"`
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	q := r.URL.Query().Get("q")
	if q == "" {
		writeProblem(w, http.StatusBadRequest, "missing query parameter q")
		return
	}
	k := 10
	if v := r.URL.Query().Get("k"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > 100 {
			writeProblem(w, http.StatusBadRequest, "k must be an integer in [1, 100]")
			return
		}
		k = n
	}
	retriever := r.URL.Query().Get("retriever")
	if retriever == "" {
		retriever = s.defaults.Retriever // instance default; empty falls through to "bm25" below
	}
	if retriever == "" {
		retriever = "bm25"
	}
	if (retriever == "dense" || retriever == "hybrid") && (s.vidx == nil || s.emb == nil) {
		writeProblem(w, http.StatusBadRequest, "dense/hybrid not configured on this server")
		return
	}
	useRerank := s.reranker != nil
	if v := r.URL.Query().Get("rerank"); v != "" {
		useRerank = v == "true" || v == "1"
	}
	if useRerank && s.reranker == nil {
		writeProblem(w, http.StatusBadRequest, "rerank requested but no reranker configured")
		return
	}
	// ?expand=true triggers LLM query paraphrasing + RRF fusion.
	// Opt-in by default (adds 1 LLM call of latency on cold queries).
	// Instance default applies when query param is absent.
	useExpand := s.defaults.Expand
	if v := r.URL.Query().Get("expand"); v != "" {
		useExpand = v == "true" || v == "1"
	}
	if useExpand && s.paraphraser == nil {
		writeProblem(w, http.StatusBadRequest, "expand requested but no paraphraser configured")
		return
	}

	innerK := k
	if useRerank {
		innerK = s.rerankCandK
		if innerK < 5*k {
			innerK = 5 * k
		}
		if innerK < 20 {
			innerK = 20
		}
	}

	// Iter 143: per-request hybrid_dense_weight override propagates via context
	// so runSearch picks it up without a signature change.
	ctx := r.Context()
	if v := r.URL.Query().Get("hybrid_dense_weight"); v != "" {
		if parsed, perr := strconv.ParseFloat(v, 64); perr == nil {
			ctx = context.WithValue(ctx, hybridDenseWeightKey{}, parsed)
		}
	}
	// Iter 158: ?mmr=true&mmr_lambda=N for MMR re-ranking on dense / hybrid.
	// Iter 168: factored into mmrFromQuery so /answer, /research, etc. share
	// the same parsing.
	ctx = mmrFromQuery(ctx, r)
	// Iter 161: ?hyde=true generates a hypothetical-answer passage via the
	// chat client and threads it through ctx for runDense to use as the
	// embedding source. Requires both chat AND embedder configured. Pure
	// dense / hybrid only — BM25 uses the original query for lexical match.
	// Iter 162: backed by a 2-level cache (L1 in-memory + L2 SQLite) so
	// cold processes skip the LLM call on popular queries.
	if v := r.URL.Query().Get("hyde"); v == "true" || v == "1" {
		if s.hyde == nil {
			writeProblem(w, http.StatusBadRequest, "hyde requested but no chat client configured")
			return
		}
		if s.vidx == nil || s.emb == nil {
			writeProblem(w, http.StatusBadRequest, "hyde requested but dense/hybrid is not configured (no embedder)")
			return
		}
		passage := s.hyde.Passage(ctx, q)
		if passage != "" && passage != q {
			ctx = context.WithValue(ctx, hydeQueryKey{}, passage)
		}
		// Iter 174: hydeEnabledKey lets /search's expand block do
		// per-paraphrase HyDE generation, same shape as /answer's expandHits.
		ctx = context.WithValue(ctx, hydeEnabledKey{}, true)
	}
	hits, spans, source, err := s.runSearch(ctx, retriever, q, innerK)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, fmt.Sprintf("search: %v", err))
		return
	}

	// Iter 159/163: pseudo-relevance feedback for bm25 + hybrid retrievers.
	// Iter 170: factored into applyPRFIfRequested so /answer, /research can
	// share the same logic via the enumeration discipline.
	var prfTag string
	hits, spans, prfTag = s.applyPRFIfRequested(ctx, r, retriever, q, hits, spans, innerK)
	source += prfTag

	if useRerank && len(hits) > 1 {
		hits, spans = s.applyRerank(ctx, q, hits, spans, k)
		source += "+rerank"
	} else if len(hits) > k {
		hits = hits[:k]
		spans = spans[:k]
	}
	// Iter 158: tag source when MMR was applied so /search?mmr=true responses
	// are self-documenting. (runDense already swapped the algorithm via ctx;
	// this is the post-hoc tag.)
	if p, ok := ctx.Value(mmrParamsKey{}).(mmrParams); ok && p.enabled && (retriever == "dense" || retriever == "hybrid") {
		source += fmt.Sprintf("+mmr(lambda=%.2f)", p.lambda)
	}
	// Iter 161: tag source when HyDE was used. Only meaningful for dense/hybrid
	// (BM25 keeps original q for lexical match).
	if _, ok := ctx.Value(hydeQueryKey{}).(string); ok && (retriever == "dense" || retriever == "hybrid") {
		source += "+hyde"
	}

	// Query expansion: paraphrase q, search each variant, RRF-fuse with main hits.
	// Applied AFTER the main search returns so we can short-circuit on errors.
	if useExpand && len(hits) > 0 {
		paraphrases := s.paraphraser.generate(ctx, q)
		if len(paraphrases) > 0 {
			// Iter 174: per-paraphrase HyDE — same fix as /answer's expandHits.
			// Pre-iter-174, all paraphrase retrievals shared the main query's
			// HyDE passage on the dense leg, silently breaking diversification.
			hydeOn := false
			if v, ok := ctx.Value(hydeEnabledKey{}).(bool); ok && v {
				hydeOn = s.hyde != nil && s.vidx != nil && s.emb != nil
			}
			searchCtx := func(variant string) context.Context {
				if !hydeOn {
					return ctx
				}
				passage := s.hyde.Passage(ctx, variant)
				if passage == "" || passage == variant {
					return ctx
				}
				return context.WithValue(ctx, hydeQueryKey{}, passage)
			}

			lists := [][]string{urlsFromHits(hits)}
			spanByURL := make(map[string]denseSpan, len(hits))
			for i, h := range hits {
				if i < len(spans) {
					spanByURL[h.URL] = spans[i]
				}
			}
			for _, pq := range paraphrases {
				pHits, pSpans, _, err := s.runSearch(searchCtx(pq), retriever, pq, innerK)
				if err != nil {
					continue
				}
				if useRerank && len(pHits) > 1 {
					pHits, pSpans = s.applyRerank(ctx, pq, pHits, pSpans, k)
				} else if len(pHits) > k {
					pHits = pHits[:k]
					pSpans = pSpans[:k]
				}
				lists = append(lists, urlsFromHits(pHits))
				for i, h := range pHits {
					if _, ok := spanByURL[h.URL]; !ok && i < len(pSpans) {
						spanByURL[h.URL] = pSpans[i]
					}
				}
			}
			// Iter 136: weighted RRF — main list (lists[0]) gets ExpandMainWeight;
			// paraphrase lists each 1.0. Iter 143: per-request override via
			// ?expand_main_weight=X; falls back to s.defaults.ExpandMainWeight
			// when absent. Non-positive → nil weights (equal-weight RRF).
			emw := s.defaults.ExpandMainWeight
			if v := r.URL.Query().Get("expand_main_weight"); v != "" {
				if parsed, err := strconv.ParseFloat(v, 64); err == nil {
					emw = parsed
				}
			}
			var weights []float64
			if emw > 0 {
				weights = make([]float64, len(lists))
				weights[0] = emw
				for i := 1; i < len(weights); i++ {
					weights[i] = 1.0
				}
			}
			fused := index.RRFWeighted(lists, weights, k, 60)
			// Reassemble Hit slice in fused order, preserving title where known.
			titleByURL := make(map[string]string, len(hits))
			scoreByURL := make(map[string]float64, len(hits))
			for _, h := range hits {
				titleByURL[h.URL] = h.Title
				scoreByURL[h.URL] = h.Score
			}
			newHits := make([]index.Hit, 0, len(fused))
			newSpans := make([]denseSpan, 0, len(fused))
			for rank, u := range fused {
				score := scoreByURL[u]
				if score == 0 {
					score = 1.0 / float64(rank+1)
				}
				newHits = append(newHits, index.Hit{URL: u, Title: titleByURL[u], Score: score})
				newSpans = append(newSpans, spanByURL[u])
			}
			hits = newHits
			spans = newSpans
			source += "+expand"
		}
	}
	// Iter 77: date filtering via ?since= and ?until= (YYYY-MM-DD or RFC3339).
	// Docs with zero PublishedAt (unknown — no JSON-LD datePublished) are
	// skipped when any date filter is active. Loose behavior on filter parse
	// failure: error 400 so callers see the typo, don't silently drop the filter.
	since, sinceErr := parseSearchDate(r.URL.Query().Get("since"))
	until, untilErr := parseSearchDate(r.URL.Query().Get("until"))
	if sinceErr != nil {
		writeProblem(w, http.StatusBadRequest, fmt.Sprintf("since: %v (expected YYYY-MM-DD or RFC3339)", sinceErr))
		return
	}
	if untilErr != nil {
		writeProblem(w, http.StatusBadRequest, fmt.Sprintf("until: %v (expected YYYY-MM-DD or RFC3339)", untilErr))
		return
	}
	if !since.IsZero() || !until.IsZero() {
		hits, spans = s.filterByPublishedAt(r.Context(), hits, spans, since, until)
		source += "+date-filter"
	}

	// Iter 79: domain filtering.suffix-match semantics — suffix-match so "example.com"
	// matches "blog.example.com" too. Comma-separated host lists.
	include := splitCSV(r.URL.Query().Get("include_domains"))
	exclude := splitCSV(r.URL.Query().Get("exclude_domains"))
	if len(include) > 0 || len(exclude) > 0 {
		hits, spans = filterByDomain(hits, spans, include, exclude)
		source += "+domain-filter"
	}

	// Iter 78: ?sort= controls result ordering. Default is relevance (the
	// retriever's native scoring); date_desc and date_asc sort by PublishedAt.
	// Bad values → 400 so typos surface immediately.
	sortMode := r.URL.Query().Get("sort")
	if sortMode != "" && sortMode != "relevance" && sortMode != "date_desc" && sortMode != "date_asc" {
		writeProblem(w, http.StatusBadRequest, fmt.Sprintf("sort: unknown value %q (expected relevance | date_desc | date_asc)", sortMode))
		return
	}
	if sortMode == "date_desc" || sortMode == "date_asc" {
		hits, spans = s.sortByPublishedAt(r.Context(), hits, spans, sortMode == "date_desc")
		source += "+sort=" + sortMode
	}

	// Iter 82: enrich the response with Domain + PublishedAt per hit so callers
	// don't have to round-trip /contents. Batched single-SQL lookup.
	urls := make([]string, 0, len(hits))
	for _, h := range hits {
		urls = append(urls, h.URL)
	}
	metas, _ := s.store.GetDocMetas(r.Context(), urls) // best-effort: empty map on error

	// Iter 153: ?author= / ?exclude_author= filters. Case-insensitive substring
	// match against documents.author (iter 150). Reuses the metas map from
	// enrichment so no extra SQL round-trip. Applied AFTER iter-79 domain and
	// iter-77 date filters; sort runs BEFORE this so a date-desc + author filter
	// still returns the most recent matching author.
	authorInclude := splitCSV(r.URL.Query().Get("author"))
	authorExclude := splitCSV(r.URL.Query().Get("exclude_author"))
	if len(authorInclude) > 0 || len(authorExclude) > 0 {
		hits, spans = filterByAuthor(hits, spans, metas, authorInclude, authorExclude)
		source += "+author-filter"
	}

	// Iter 148: ?include_text=true populates SearchHit.Text inline.
	// ?max_text=N caps each hit's text (default 5000) so a stray include_text
	// can't balloon the response. Skip the SQL entirely when not requested.
	var texts map[string]string
	if v := r.URL.Query().Get("include_text"); v == "true" || v == "1" {
		maxText := 5000
		if mt := r.URL.Query().Get("max_text"); mt != "" {
			if parsed, perr := strconv.Atoi(mt); perr == nil && parsed > 0 {
				maxText = parsed
			}
		}
		texts, _ = s.store.GetDocTexts(r.Context(), urls, maxText) // best-effort
	}

	resp := SearchResponse{
		Query: q, K: k, Took: time.Since(start).String(),
		Hits: make([]SearchHit, 0, len(hits)),
	}
	for i, h := range hits {
		hit := SearchHit{URL: h.URL, Title: h.Title, Score: h.Score, Source: source}
		if i < len(spans) && spans[i].Length > 0 {
			hit.Highlight = s.buildHighlight(r.Context(), h.URL, spans[i])
		}
		if m, ok := metas[h.URL]; ok {
			hit.Domain = m.Domain
			if !m.PublishedAt.IsZero() {
				p := m.PublishedAt
				hit.PublishedAt = &p
			}
			// Iter 83: Excerpt is a fallback for hits without a Highlight.
			// When dense/hybrid retrieval gives a precision-aligned passage
			// span, the Highlight is more useful than the body prefix.
			if hit.Highlight == nil && m.Excerpt != "" {
				hit.Excerpt = m.Excerpt
			}
			// Iter 150.
			hit.Author = m.Author
			// Iter 155.
			hit.Image = m.Image
			// Iter 156.
			hit.Favicon = m.Favicon
		}
		if t, ok := texts[h.URL]; ok {
			hit.Text = t
		}
		resp.Hits = append(resp.Hits, hit)
	}

	// Iter 164: within-result score normalization. Opt-in via ?calibrate=true.
	// Divides each hit's score by the max score in the response so the top
	// hit gets 1.0 and lower-ranked hits express relative closeness as a
	// fraction. Skips when max ≤ 0 (defensive — shouldn't happen in practice).
	// This is NOT cross-query calibration; ScoreCalibrated is comparable
	// WITHIN one response, not across requests.
	if v := r.URL.Query().Get("calibrate"); v == "true" || v == "1" {
		calibrateHits(resp.Hits)
	}

	writeJSON(w, http.StatusOK, resp)
}

// calibrateHits populates SearchHit.ScoreCalibrated as the fraction of the
// max raw score in the slice. Iter 164.
func calibrateHits(hits []SearchHit) {
	if len(hits) == 0 {
		return
	}
	var maxScore float64
	for _, h := range hits {
		if h.Score > maxScore {
			maxScore = h.Score
		}
	}
	if maxScore <= 0 {
		return
	}
	for i := range hits {
		hits[i].ScoreCalibrated = hits[i].Score / maxScore
	}
}

// calibrateSources populates AnswerSource.ScoreCalibrated for /answer +
// /research responses. Same shape as iter-164's calibrateHits — top source
// = 1.0; others = Score / max(Score). Iter 178. Helper-extracted at N=2
// callsites for AnswerSource; SearchHit's calibrateHits stays separate
// because the types differ (helper extraction across types would need
// generics or interface — overkill for 2 callsites of 10 lines).
func calibrateSources(sources []AnswerSource) {
	if len(sources) == 0 {
		return
	}
	var maxScore float64
	for _, src := range sources {
		if src.Score > maxScore {
			maxScore = src.Score
		}
	}
	if maxScore <= 0 {
		return
	}
	for i := range sources {
		sources[i].ScoreCalibrated = sources[i].Score / maxScore
	}
}

// parseSearchDate accepts the two common formats users want to pass on the
// query string: YYYY-MM-DD (date-only, evaluated at UTC midnight) or full
// RFC3339. Empty input returns zero time + nil error (no filter).
func parseSearchDate(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("could not parse date %q", s)
}

// filterByPublishedAt drops hits whose document's PublishedAt is outside the
// [since, until] range. Documents with zero PublishedAt (unknown publication
// date — no JSON-LD datePublished was extracted) are skipped when any filter
// is active. The hits + spans slices are filtered in lockstep so highlights
// stay aligned to the right hit. Iter 77.
func (s *Server) filterByPublishedAt(ctx context.Context, hits []index.Hit, spans []denseSpan, since, until time.Time) ([]index.Hit, []denseSpan) {
	if len(hits) == 0 {
		return hits, spans
	}
	keptHits := make([]index.Hit, 0, len(hits))
	keptSpans := make([]denseSpan, 0, len(hits))
	for i, h := range hits {
		doc, err := s.store.GetDocByURL(ctx, h.URL)
		if err != nil || doc.PublishedAt.IsZero() {
			continue
		}
		if !since.IsZero() && doc.PublishedAt.Before(since) {
			continue
		}
		if !until.IsZero() && doc.PublishedAt.After(until) {
			continue
		}
		keptHits = append(keptHits, h)
		if i < len(spans) {
			keptSpans = append(keptSpans, spans[i])
		}
	}
	return keptHits, keptSpans
}

// splitCSV parses a comma-separated query parameter into a list of trimmed,
// non-empty, lowercase entries. Used by the iter-79 domain filter for
// `?include_domains=foo.com,bar.com` style inputs.
func splitCSV(s string) []string {
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

// matchesAnyDomain reports whether host (or any of its parent domains) appears
// in the patterns list.Suffix-match semantics: "example.com" matches "blog.example.com"
// but NOT "evilexample.com" — strict suffix on a dot boundary.
func matchesAnyDomain(host string, patterns []string) bool {
	host = strings.ToLower(host)
	for _, p := range patterns {
		if host == p || strings.HasSuffix(host, "."+p) {
			return true
		}
	}
	return false
}

// filterByAuthor drops hits whose document's Author doesn't match the include
// list (when non-empty) OR matches the exclude list. Case-insensitive substring
// matching — JSON-LD authors come in many forms ("Jane Doe", "By Jane Doe",
// "Doe, Jane") so substring is friendlier than exact match for the common
// case. Iter 153. Composes with iter-150's Author field.
//
// metas is the same map produced by store.GetDocMetas for enrichment — passed
// in so callers can reuse the batched fetch instead of forcing an N+1 lookup.
// Hits whose URL isn't in metas (e.g., stub hits without a backing document)
// are kept; the filter can only drop hits it has metadata for. Documents
// without an Author (most pages) are dropped only when include is non-empty.
func filterByAuthor(hits []index.Hit, spans []denseSpan, metas map[string]store.DocMeta, include, exclude []string) ([]index.Hit, []denseSpan) {
	if len(hits) == 0 || (len(include) == 0 && len(exclude) == 0) {
		return hits, spans
	}
	keptHits := make([]index.Hit, 0, len(hits))
	keptSpans := make([]denseSpan, 0, len(hits))
	for i, h := range hits {
		m, ok := metas[h.URL]
		// No metadata for this hit — keep it (can't responsibly filter without data).
		if !ok {
			keptHits = append(keptHits, h)
			if i < len(spans) {
				keptSpans = append(keptSpans, spans[i])
			}
			continue
		}
		author := strings.ToLower(m.Author)
		if len(include) > 0 {
			if author == "" {
				continue
			}
			matched := false
			for _, p := range include {
				if strings.Contains(author, p) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		if len(exclude) > 0 && author != "" {
			dropped := false
			for _, p := range exclude {
				if strings.Contains(author, p) {
					dropped = true
					break
				}
			}
			if dropped {
				continue
			}
		}
		keptHits = append(keptHits, h)
		if i < len(spans) {
			keptSpans = append(keptSpans, spans[i])
		}
	}
	return keptHits, keptSpans
}

// filterByDomain drops hits whose host isn't in the include list (when non-empty)
// OR is in the exclude list. Both lists use suffix matching on dot boundary.
// Iter 79. Pure function (no store lookups needed — URLs are in hits already).
func filterByDomain(hits []index.Hit, spans []denseSpan, include, exclude []string) ([]index.Hit, []denseSpan) {
	if len(hits) == 0 {
		return hits, spans
	}
	keptHits := make([]index.Hit, 0, len(hits))
	keptSpans := make([]denseSpan, 0, len(hits))
	for i, h := range hits {
		u, err := url.Parse(h.URL)
		if err != nil || u.Host == "" {
			continue
		}
		host := u.Host
		if len(include) > 0 && !matchesAnyDomain(host, include) {
			continue
		}
		if len(exclude) > 0 && matchesAnyDomain(host, exclude) {
			continue
		}
		keptHits = append(keptHits, h)
		if i < len(spans) {
			keptSpans = append(keptSpans, spans[i])
		}
	}
	return keptHits, keptSpans
}

// sortByPublishedAt sorts hits by PublishedAt in stable order. Docs with zero
// PublishedAt go to the END regardless of direction — they're un-dated so they
// have no place in a chronological ordering, but we keep them in the result
// list as a fallback section. Hits + spans are kept in lockstep so highlights
// stay aligned. Iter 78.
func (s *Server) sortByPublishedAt(ctx context.Context, hits []index.Hit, spans []denseSpan, desc bool) ([]index.Hit, []denseSpan) {
	if len(hits) <= 1 {
		return hits, spans
	}
	// Fetch PublishedAt once per hit. K is small (≤100); the round-trip cost
	// is dominated by retrieval already.
	pubAt := make([]time.Time, len(hits))
	for i, h := range hits {
		if doc, err := s.store.GetDocByURL(ctx, h.URL); err == nil {
			pubAt[i] = doc.PublishedAt
		}
	}
	// Pair-sort: hits, spans, pubAt move together. Use indices to avoid the
	// usual zip-and-sort dance.
	indices := make([]int, len(hits))
	for i := range indices {
		indices[i] = i
	}
	sort.SliceStable(indices, func(a, b int) bool {
		ai, bi := indices[a], indices[b]
		pa, pb := pubAt[ai], pubAt[bi]
		az, bz := pa.IsZero(), pb.IsZero()
		switch {
		case az && bz:
			return false // preserve original order between two zeros
		case az:
			return false // zero loses
		case bz:
			return true // non-zero beats zero
		case desc:
			return pa.After(pb)
		default:
			return pa.Before(pb)
		}
	})
	outHits := make([]index.Hit, len(hits))
	outSpans := make([]denseSpan, len(hits))
	for newIdx, oldIdx := range indices {
		outHits[newIdx] = hits[oldIdx]
		if oldIdx < len(spans) {
			outSpans[newIdx] = spans[oldIdx]
		}
	}
	return outHits, outSpans
}

// urlsFromHits projects []index.Hit → []string for RRF input.
func urlsFromHits(hs []index.Hit) []string {
	out := make([]string, len(hs))
	for i, h := range hs {
		out[i] = h.URL
	}
	return out
}

// expandHits runs paraphrases of q through the same retriever, RRF-fuses
// the result lists with `hits` (the main-query result), and returns the fused
// top-k. Used by /answer when ?expand=true; /search has the same logic inline
// because it also threads denseSpans through for highlight rendering.
//
// Returns the original hits unchanged when the paraphraser yields nothing or
// when the fanout would be a no-op.
func (s *Server) expandHits(ctx context.Context, q, retriever string, k int, hits []index.Hit) []index.Hit {
	if s.paraphraser == nil || len(hits) == 0 {
		return hits
	}
	paraphrases := s.paraphraser.generate(ctx, q)
	if len(paraphrases) == 0 {
		return hits
	}
	// Iter 174: per-paraphrase HyDE. Pre-iter-174, all paraphrase retrievals
	// inherited ctx with hydeQueryKey set to HyDE(q) — the original query's
	// passage. That silently broke expand's diversification (each dense leg
	// used the same HyDE vector). Now each paraphrase generates its own
	// HyDE passage via the iter-162 cache, then runSearch sees a per-call
	// ctx with the right passage. Matches iter-165's /research per-variant
	// pattern.
	hydeOn := false
	if v, ok := ctx.Value(hydeEnabledKey{}).(bool); ok && v {
		hydeOn = s.hyde != nil && s.vidx != nil && s.emb != nil
	}
	searchCtx := func(variant string) context.Context {
		if !hydeOn {
			return ctx
		}
		passage := s.hyde.Passage(ctx, variant)
		if passage == "" || passage == variant {
			return ctx
		}
		return context.WithValue(ctx, hydeQueryKey{}, passage)
	}

	titleByURL := make(map[string]string, len(hits))
	scoreByURL := make(map[string]float64, len(hits))
	for _, h := range hits {
		titleByURL[h.URL] = h.Title
		scoreByURL[h.URL] = h.Score
	}
	lists := [][]string{urlsFromHits(hits)}
	for _, pq := range paraphrases {
		ph, _, _, err := s.runSearch(searchCtx(pq), retriever, pq, k)
		if err != nil {
			continue
		}
		for _, h := range ph {
			if _, ok := titleByURL[h.URL]; !ok {
				titleByURL[h.URL] = h.Title
			}
		}
		lists = append(lists, urlsFromHits(ph))
	}
	fused := index.RRF(lists, k, 60)
	out := make([]index.Hit, 0, len(fused))
	for rank, u := range fused {
		score := scoreByURL[u]
		if score == 0 {
			score = 1.0 / float64(rank+1)
		}
		out = append(out, index.Hit{URL: u, Title: titleByURL[u], Score: score})
	}
	return out
}

// applyRerank reorders the inner retrieval results using the configured reranker.
// Candidate text comes from the store; the inner retrievers already returned
// the per-URL best passage (VectorIndex dedup) so spans line up 1:1 with hits.
// On reranker failure or empty output, falls back to the original order.
func (s *Server) applyRerank(ctx context.Context, q string, hits []index.Hit, spans []denseSpan, k int) ([]index.Hit, []denseSpan) {
	cands := make([]rerank.Candidate, 0, len(hits))
	for i, h := range hits {
		text := h.Title
		if doc, err := s.store.GetDocByURL(ctx, h.URL); err == nil {
			// Prefer the matched span when available — it's more targeted than the full doc.
			if i < len(spans) && spans[i].Length > 0 {
				composite := doc.Title + "\n\n" + doc.Text
				off := spans[i].Offset
				length := spans[i].Length
				if off >= 0 && off < len(composite) {
					if off+length > len(composite) {
						length = len(composite) - off
					}
					text = composite[off : off+length]
				}
			} else if doc.Text != "" {
				text = doc.Title + "\n\n" + doc.Text
			}
		}
		cands = append(cands, rerank.Candidate{ID: h.URL, Text: text})
	}

	order, err := s.reranker.Rerank(ctx, q, cands)
	if err != nil || len(order) == 0 {
		if len(hits) > k {
			hits = hits[:k]
			spans = spans[:k]
		}
		return hits, spans
	}

	// Build URL → (hit, span) map for the reorder.
	type pair struct {
		h index.Hit
		s denseSpan
	}
	byURL := make(map[string]pair, len(hits))
	for i, h := range hits {
		byURL[h.URL] = pair{h: h, s: spans[i]}
	}

	outHits := make([]index.Hit, 0, k)
	outSpans := make([]denseSpan, 0, k)
	for _, u := range order {
		p, ok := byURL[u]
		if !ok {
			continue
		}
		outHits = append(outHits, p.h)
		outSpans = append(outSpans, p.s)
		if len(outHits) >= k {
			break
		}
	}
	return outHits, outSpans
}

// buildHighlight reads doc.Text and slices out the passage span. Returns nil
// if the doc isn't in the store (rare — vector index entries point at indexed docs)
// or if the span overflows the text (defensive).
func (s *Server) buildHighlight(ctx context.Context, url string, span denseSpan) *Highlight {
	doc, err := s.store.GetDocByURL(ctx, url)
	if err != nil {
		return nil
	}
	// The passage was indexed against (title + "\n\n" + text). The offset is
	// relative to that composite, not to doc.Text. Reconstruct the same composite
	// and slice.
	composite := doc.Title + "\n\n" + doc.Text
	off, length := span.Offset, span.Length
	if off < 0 || length <= 0 || off >= len(composite) {
		return nil
	}
	if off+length > len(composite) {
		length = len(composite) - off
	}
	return &Highlight{Offset: off, Length: length, Text: composite[off : off+length]}
}

// runSearch dispatches to BM25, dense, or hybrid (RRF fusion).
//
// Returned hits keep BM25-style (URL, Title, Score). The parallel spans slice
// is populated for dense and hybrid (whichever URLs came from the dense side);
// BM25-only hits get a zero-value denseSpan and no highlight at the JSON layer.
func (s *Server) runSearch(ctx context.Context, retriever, q string, k int) ([]index.Hit, []denseSpan, string, error) {
	switch retriever {
	case "bm25":
		hits, err := s.idx.Search(ctx, q, k)
		return hits, make([]denseSpan, len(hits)), "bm25", err
	case "dense":
		hits, spans, err := s.runDense(ctx, q, k)
		return hits, spans, "dense", err
	case "hybrid":
		// Iter 163: hybrid PRF expands the BM25 sub-query only — dense still
		// works at the embedding level and would not benefit from lexical
		// expansion. The handler sets bm25QueryOverrideKey when PRF is active.
		bmQ := q
		if override, ok := ctx.Value(bm25QueryOverrideKey{}).(string); ok && override != "" {
			bmQ = override
		}
		bm, err := s.idx.Search(ctx, bmQ, k*2)
		if err != nil {
			return nil, nil, "hybrid", err
		}
		dn, dnSpans, err := s.runDense(ctx, q, k*2)
		if err != nil {
			return nil, nil, "hybrid", err
		}
		bmURLs := make([]string, len(bm))
		dnURLs := make([]string, len(dn))
		titleByURL := make(map[string]string, len(bm)+len(dn))
		spanByURL := make(map[string]denseSpan, len(dn))
		for i, h := range bm {
			bmURLs[i] = h.URL
			titleByURL[h.URL] = h.Title
		}
		for i, h := range dn {
			dnURLs[i] = h.URL
			spanByURL[h.URL] = dnSpans[i]
			if _, ok := titleByURL[h.URL]; !ok {
				titleByURL[h.URL] = h.Title
			}
		}
		// Iter 138: weighted RRF for the hybrid retriever. BM25 list keeps
		// weight 1.0; dense list gets HybridDenseWeight. Iter 143: per-request
		// override via context value (set in the handler from
		// ?hybrid_dense_weight=). Non-positive → nil weights (equal-weight RRF).
		hdw := s.defaults.HybridDenseWeight
		if v, ok := ctx.Value(hybridDenseWeightKey{}).(float64); ok {
			hdw = v
		}
		var weights []float64
		if hdw > 0 {
			weights = []float64{1.0, hdw}
		}
		fused := index.RRFWeighted([][]string{bmURLs, dnURLs}, weights, k, 60)
		out := make([]index.Hit, 0, len(fused))
		outSpans := make([]denseSpan, 0, len(fused))
		for rank, u := range fused {
			out = append(out, index.Hit{URL: u, Title: titleByURL[u], Score: 1.0 / float64(rank+1)})
			outSpans = append(outSpans, spanByURL[u]) // zero-valued if URL came only from BM25
		}
		return out, outSpans, "hybrid", nil
	default:
		return nil, nil, "", fmt.Errorf("unknown retriever %q (use bm25 | dense | hybrid)", retriever)
	}
}

// runDense embeds the query, hits the in-memory VectorIndex, joins back to BM25 doc shape.
//
// Returns a parallel slice of highlights (one per hit) so the JSON layer can
// emit them without re-scanning. Highlights with Length=0 mean "no span info"
// (an Add-style doc-level vector, not a chunk).
func (s *Server) runDense(ctx context.Context, q string, k int) ([]index.Hit, []denseSpan, error) {
	// Iter 161: if a HyDE-generated passage is in ctx, embed THAT instead of
	// the raw query. Hybrid retrieval calls runDense for the dense leg only;
	// BM25 still sees the original q upstream. Transparent to /find_similar
	// because /find_similar bypasses runDense (works off a stored embedding).
	embText := q
	if hyde, ok := ctx.Value(hydeQueryKey{}).(string); ok && hyde != "" {
		embText = hyde
	}
	vecs, err := s.emb.Embed(ctx, []string{embText})
	if err != nil {
		return nil, nil, fmt.Errorf("embed query: %w", err)
	}
	// Iter 158: ?mmr=true → swap Search for SearchMMR. candPool=0 → algorithm's
	// 5*k/50 default. Both routes return VectorHit so the rest of the function
	// is unchanged.
	var vh []index.VectorHit
	if p, ok := ctx.Value(mmrParamsKey{}).(mmrParams); ok && p.enabled {
		vh = s.vidx.SearchMMR(ctx, vecs[0], k, p.lambda, 0)
	} else {
		vh = s.vidx.Search(ctx, vecs[0], k)
	}
	out := make([]index.Hit, 0, len(vh))
	spans := make([]denseSpan, 0, len(vh))
	for _, h := range vh {
		out = append(out, index.Hit{URL: h.URL, Title: h.Title, Score: h.Score})
		spans = append(spans, denseSpan{Offset: h.Offset, Length: h.Length})
	}
	return out, spans, nil
}

type denseSpan struct{ Offset, Length int }

// StatsResponse is the on-the-wire shape of /stats.
type StatsResponse struct {
	Documents int64                `json:"documents"`
	Terms     int64                `json:"terms"`
	Frontier  store.FrontierStats  `json:"frontier"`
}

// handleSitemap emits a standards-compliant sitemap.xml of the indexed corpus.
// Spec: https://www.sitemaps.org/protocol.html — `<urlset>` root, per-URL
// `<loc>` + optional `<lastmod>`. Iter 86.
//
// Caps at 50,000 URLs per the sitemap spec's per-file limit. Larger corpora
// would need a sitemap-index file (a future iter — yagni for now).
// Content-Type: application/xml so browsers and crawlers parse correctly.
func (s *Server) handleSitemap(w http.ResponseWriter, r *http.Request) {
	entries, err := s.store.ListDocSitemapEntries(r.Context(), 50000)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	bw := bufio.NewWriter(w)
	defer bw.Flush()
	_, _ = bw.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	_, _ = bw.WriteString(`<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">` + "\n")
	for _, e := range entries {
		_, _ = bw.WriteString("  <url><loc>")
		_, _ = bw.WriteString(escapeXMLText(e.URL))
		_, _ = bw.WriteString("</loc>")
		if !e.LastChangedAt.IsZero() {
			_, _ = bw.WriteString("<lastmod>")
			_, _ = bw.WriteString(e.LastChangedAt.UTC().Format(time.RFC3339))
			_, _ = bw.WriteString("</lastmod>")
		}
		_, _ = bw.WriteString("</url>\n")
	}
	_, _ = bw.WriteString(`</urlset>` + "\n")
}

// handleRobotsTxt emits a standards-compliant robots.txt. Iter 87 — pairs with
// iter-86's /sitemap.xml so external crawlers can discover the sitemap via the
// robots.txt Sitemap directive (the canonical discovery mechanism for sitemaps
// that aren't manually submitted to a search console).
//
// Default policy:
//   - Allow everything for User-agent: *
//   - Disallow /admin/* — operator-only routes; no benefit from crawlers indexing them
//
// The Sitemap directive uses an absolute URL because the spec requires it. We
// derive scheme from r.TLS (set by net/http when serving HTTPS) and host from
// r.Host. Behind a reverse proxy (nginx/Caddy/Cloudflare), the proxy's host
// header is what's seen — which is correct (the public URL).
func (s *Server) handleRobotsTxt(w http.ResponseWriter, r *http.Request) {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	// X-Forwarded-Proto handling deliberately omitted in iter 87 — most crawlers
	// follow redirects, so even an http:// sitemap URL works on a https-proxied
	// deployment. Operators wanting absolute https in the body can run cosift
	// directly on TLS or set the URL via a future config knob if demand emerges.
	host := r.Host
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	body := "User-agent: *\nAllow: /\nDisallow: /admin/\n\nSitemap: " + scheme + "://" + host + "/sitemap.xml\n"
	_, _ = w.Write([]byte(body))
}

// escapeXMLText escapes the five XML predefined entities. We don't use
// encoding/xml for the sitemap body because it would force allocation per
// URL; manual escape on a small char set is significantly cheaper at 50,000-URL
// scale. URLs that legitimately contain these chars are rare but must be
// handled correctly to produce well-formed XML.
func escapeXMLText(s string) string {
	if !strings.ContainsAny(s, "<>&\"'") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case '<':
			b.WriteString("&lt;")
		case '>':
			b.WriteString("&gt;")
		case '&':
			b.WriteString("&amp;")
		case '"':
			b.WriteString("&quot;")
		case '\'':
			b.WriteString("&apos;")
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	st, err := s.store.Stats(r.Context())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	fs, err := s.store.GetFrontierStats(r.Context())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, StatsResponse{Documents: st.Documents, Terms: st.Terms, Frontier: fs})
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// FindSimilarResponse is /find_similar's wire shape.
type FindSimilarResponse struct {
	URL  string      `json:"url"`
	K    int         `json:"k"`
	Hits []SearchHit `json:"hits"`
	Took string      `json:"took"`
}

func (s *Server) handleFindSimilar(w http.ResponseWriter, r *http.Request) {
	if s.vidx == nil || s.emb == nil {
		writeProblem(w, http.StatusBadRequest, "/find_similar requires a configured embedder")
		return
	}
	start := time.Now()
	u := r.URL.Query().Get("url")
	if u == "" {
		writeProblem(w, http.StatusBadRequest, "missing url parameter")
		return
	}
	k := 10
	if v := r.URL.Query().Get("k"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > 100 {
			writeProblem(w, http.StatusBadRequest, "k must be an integer in [1, 100]")
			return
		}
		k = n
	}

	doc, err := s.store.GetDocByURL(r.Context(), u)
	if err != nil {
		writeProblem(w, http.StatusNotFound, "url not in index")
		return
	}
	vecs, err := s.emb.Embed(r.Context(), []string{doc.Title + "\n\n" + doc.Text})
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, fmt.Sprintf("embed: %v", err))
		return
	}
	// Iter 167: ?mmr=true on /find_similar — diversity re-rank for the kNN
	// result set. /find_similar's natural failure mode is returning N
	// near-duplicates of the seed (cosine-close docs cluster); MMR surfaces
	// alternative-but-related material. Same algorithm as iter-158 on
	// /search. Source tag changes to "dense+mmr(lambda=N.NN)" so the
	// response self-documents.
	useMMR := false
	lambda := 0.7
	if v := r.URL.Query().Get("mmr"); v == "true" || v == "1" {
		useMMR = true
		if lv := r.URL.Query().Get("mmr_lambda"); lv != "" {
			if parsed, perr := strconv.ParseFloat(lv, 64); perr == nil {
				lambda = parsed
			}
		}
	}
	var raw []index.VectorHit
	source := "dense"
	if useMMR {
		raw = s.vidx.SearchMMR(r.Context(), vecs[0], k+1, lambda, 0)
		source = fmt.Sprintf("dense+mmr(lambda=%.2f)", lambda)
	} else {
		raw = s.vidx.Search(r.Context(), vecs[0], k+1)
	}
	hits := make([]SearchHit, 0, k)
	for _, h := range raw {
		if h.URL == u {
			continue
		}
		hits = append(hits, SearchHit{URL: h.URL, Title: h.Title, Score: h.Score, Source: source})
		if len(hits) >= k {
			break
		}
	}
	writeJSON(w, http.StatusOK, FindSimilarResponse{URL: u, K: k, Hits: hits, Took: time.Since(start).String()})
}

// ContentsResponse is /contents' wire shape.
type ContentsResponse struct {
	URL       string    `json:"url"`
	Title     string    `json:"title"`
	Text      string    `json:"text"`
	Lang      string    `json:"lang,omitempty"`
	Cached    bool      `json:"cached"`
	FetchedAt time.Time `json:"fetched_at"`
}

func (s *Server) handleContents(w http.ResponseWriter, r *http.Request) {
	u := r.URL.Query().Get("url")
	if u == "" {
		writeProblem(w, http.StatusBadRequest, "missing url parameter")
		return
	}
	if doc, err := s.store.GetDocByURL(r.Context(), u); err == nil {
		writeJSON(w, http.StatusOK, ContentsResponse{
			URL: doc.URL, Title: doc.Title, Text: doc.Text, Lang: doc.Lang,
			Cached: true, FetchedAt: doc.FetchedAt,
		})
		return
	}
	// Store miss → on-demand fetch if configured. We deliberately do NOT
	// persist the result — /contents shouldn't be a side-channel for indexing.
	// Users who want a URL indexed should crawl it explicitly.
	if s.fetcher == nil {
		writeProblem(w, http.StatusNotFound, "url not in index and on-demand fetch not configured")
		return
	}
	title, text, lang, err := s.fetcher(r.Context(), u)
	if err != nil {
		writeProblem(w, http.StatusBadGateway, fmt.Sprintf("fetch: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, ContentsResponse{
		URL: u, Title: title, Text: text, Lang: lang,
		Cached: false, FetchedAt: time.Now(),
	})
}

// ContentsBatchRequest is the JSON body for POST /contents (iter 88).
type ContentsBatchRequest struct {
	URLs []string `json:"urls"`
}

// ContentsBatchResponse pairs each input URL with its outcome. Iter 88 — lets
// LLM agents fetch many documents in one round-trip instead of N. Mirrors
//'s batch /contents APIs.
//
// Missing URLs (not in the index AND no on-demand fetch configured) surface
// as `{url, found: false}` instead of erroring the whole request — partial
// results are useful, especially when callers don't know which URLs cosift
// has indexed.
type ContentsBatchResponse struct {
	Results []ContentsBatchItem `json:"results"`
	Took    string              `json:"took"`
}

type ContentsBatchItem struct {
	URL       string    `json:"url"`
	Found     bool      `json:"found"`
	Title     string    `json:"title,omitempty"`
	Text      string    `json:"text,omitempty"`
	Lang      string    `json:"lang,omitempty"`
	Cached    bool      `json:"cached"`
	FetchedAt time.Time `json:"fetched_at,omitempty"`
	Error     string    `json:"error,omitempty"` // when found=false, why (e.g. "not in index")
}

// handleContentsBatch fetches contents for multiple URLs in one call. Per-URL
// behavior matches handleContents (cache-first, optional on-demand fetch).
// Returns 200 with per-URL outcomes — never errors the whole batch over one
// missing URL. Iter 88.
func (s *Server) handleContentsBatch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var req ContentsBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeProblem(w, http.StatusBadRequest, fmt.Sprintf("invalid json body: %v", err))
		return
	}
	if len(req.URLs) == 0 {
		writeProblem(w, http.StatusBadRequest, "urls array is required and must be non-empty")
		return
	}
	// Cap batch size to keep memory bounded. 100 matches /search's per-query K cap.
	if len(req.URLs) > 100 {
		writeProblem(w, http.StatusBadRequest, fmt.Sprintf("batch size %d exceeds limit (100)", len(req.URLs)))
		return
	}
	results := make([]ContentsBatchItem, 0, len(req.URLs))
	for _, u := range req.URLs {
		if u == "" {
			results = append(results, ContentsBatchItem{URL: u, Found: false, Error: "empty url"})
			continue
		}
		// Cache-first.
		if doc, err := s.store.GetDocByURL(r.Context(), u); err == nil {
			results = append(results, ContentsBatchItem{
				URL: doc.URL, Found: true, Title: doc.Title, Text: doc.Text, Lang: doc.Lang,
				Cached: true, FetchedAt: doc.FetchedAt,
			})
			continue
		}
		// Miss → on-demand fetch if configured. Same semantics as the GET handler.
		if s.fetcher == nil {
			results = append(results, ContentsBatchItem{URL: u, Found: false, Error: "not in index"})
			continue
		}
		title, text, lang, err := s.fetcher(r.Context(), u)
		if err != nil {
			results = append(results, ContentsBatchItem{URL: u, Found: false, Error: fmt.Sprintf("fetch: %v", err)})
			continue
		}
		results = append(results, ContentsBatchItem{
			URL: u, Found: true, Title: title, Text: text, Lang: lang,
			Cached: false, FetchedAt: time.Now(),
		})
	}
	writeJSON(w, http.StatusOK, ContentsBatchResponse{Results: results, Took: time.Since(start).String()})
}

// ResearchResponse is /research's wire shape. Sub-queries (planner strategy) or
// paraphrases (paraphrase strategy) used during retrieval are returned in Plan
// for transparency. Strategy reports which expansion strategy was applied.
type ResearchResponse struct {
	Query      string         `json:"query"`
	Strategy   string         `json:"strategy"`
	Plan       []string       `json:"plan"`
	Answer     string         `json:"answer"`
	Sources    []AnswerSource `json:"sources"`
	Took       string         `json:"took"`
	Calibrated bool           `json:"calibrated"`
}

const researchPlanPrompt = `Decompose the user's research question into 2-3 focused sub-queries that, taken together, would cover the answer. Output ONLY a JSON array of strings — no prose, no markdown. Example: ["sub-query 1", "sub-query 2"]`

const researchSynthPrompt = `You are a research assistant. Synthesize an answer to the original question using ONLY the provided sources.
- Cite sources by their numeric id, e.g. [1] or [2,3]. Every factual claim needs a citation.
- If the sources don't cover something, say so plainly — do not invent.
- Keep the answer focused on what the sources actually say.
- When a source has an Author line, you may attribute claims to them inline ("Jane Doe argues that…") — only when the attribution is supported by the content.`

// expandForResearch produces the query variants /research uses to retrieve
// sources, plus the strategy label. Two strategies measured in iters 52-53:
//
//   - "planner" (default, original behavior): LLM decomposes q into 2-3 sub-queries.
//   - "paraphrase": paraphraser produces N rewordings of q (uses L1+L2 cache).
//
// Returns the variants list (NOT including q for planner; NOT including q for
// paraphrase either — callers add q themselves for paraphrase's RRF fan-out).
// Eval found paraphrase yields higher nDCG@10 in both single-doc and
// multi-faceted regimes; the strategy parameter exposes both.
func (s *Server) expandForResearch(ctx context.Context, q, strategy string) ([]string, string, error) {
	switch strategy {
	case "", "planner":
		planRaw, err := s.chat.Chat(ctx, []embed.ChatMsg{
			{Role: "system", Content: researchPlanPrompt},
			{Role: "user", Content: q},
		})
		if err != nil {
			return nil, "planner", fmt.Errorf("plan: %w", err)
		}
		subs := parseSubQueries(planRaw, q)
		if len(subs) > 5 {
			subs = subs[:5]
		}
		return subs, "planner", nil
	case "paraphrase":
		if s.paraphraser == nil {
			return nil, "paraphrase", errors.New("strategy=paraphrase requires a configured paraphraser (set cfg.Chat.Model in cosift.json or via env)")
		}
		paras := s.paraphraser.generate(ctx, q)
		// Cached miss with no paraphrases produced — degrade gracefully to
		// running the original query alone. /research is still useful when
		// the paraphraser fails; the caller just gets less coverage.
		return paras, "paraphrase", nil
	default:
		return nil, strategy, fmt.Errorf("unknown strategy %q (planner | paraphrase)", strategy)
	}
}

// gatherResearchPassages runs retrieval over the expanded variants and returns
// the deduped, capped passage list used for synthesis.
//   - planner: iterate variants, top-3 each, dedup by URL, cap at 10 (preserves
//     iter-7 behavior).
//   - paraphrase: RRF-fuse top-10 of (q ⊕ variants) into a single ranked URL list,
//     truncate to 10 (matches the iters 46/52/53 measurement methodology).
//
// onRetrieved is invoked for SSE transparency — called with (variant, urls) per
// retrieval call in planner mode, or once with ("rrf-fused", urls) in paraphrase
// mode. Pass nil to skip event emission.
func (s *Server) gatherResearchPassages(ctx context.Context, q, strategy string, variants []string, onRetrieved func(variant string, urls []string)) ([]researchPassage, error) {
	retriever := "bm25"
	if s.vidx != nil && s.emb != nil {
		retriever = "hybrid"
	}
	// cap controls how many sources reach the synth LLM. Defaults.ResearchSynthK
	// lets operators trade coverage for grounding per-deployment — iter-62
	// measured that lower K reduces the noise-citation failure mode at the
	// cost of a small coverage delta on multi-faceted workloads.
	cap := 10
	if s.defaults.ResearchSynthK > 0 {
		cap = s.defaults.ResearchSynthK
	}

	// Iter 165: when ?hyde=true is propagated via hydeEnabledKey, every
	// variant gets its OWN hypothetical-answer passage. The iter-162 cache
	// makes repeated sub-queries free.
	hydeOn := false
	if v, ok := ctx.Value(hydeEnabledKey{}).(bool); ok && v {
		hydeOn = s.hyde != nil && s.vidx != nil && s.emb != nil
	}
	searchCtx := func(variant string) context.Context {
		if !hydeOn {
			return ctx
		}
		passage := s.hyde.Passage(ctx, variant)
		if passage == "" || passage == variant {
			return ctx
		}
		return context.WithValue(ctx, hydeQueryKey{}, passage)
	}

	if strategy == "paraphrase" {
		all := append([]string{q}, variants...)
		hitLists := make([][]string, 0, len(all))
		for _, v := range all {
			hits, _, _, err := s.runSearch(searchCtx(v), retriever, v, cap)
			if err != nil {
				continue
			}
			urls := make([]string, 0, len(hits))
			for _, h := range hits {
				urls = append(urls, h.URL)
			}
			hitLists = append(hitLists, urls)
		}
		if len(hitLists) == 0 {
			return nil, nil
		}
		fused := index.RRF(hitLists, cap, 60)
		if onRetrieved != nil {
			onRetrieved("rrf-fused", fused)
		}
		passages := make([]researchPassage, 0, len(fused))
		for rank, u := range fused {
			doc, err := s.store.GetDocByURL(ctx, u)
			if err != nil {
				continue
			}
			// Iter 178: rank-derived score for paraphrase strategy. RRF
			// returns positions; convert to a relative score with the
			// standard 1/(rank+1) formula so AnswerSource.Score is non-zero
			// and ordering is preserved.
			passages = append(passages, researchPassage{
				url: doc.URL, title: doc.Title, text: doc.Text, author: doc.Author,
				score: 1.0 / float64(rank+1),
			})
		}
		return passages, nil
	}

	// planner: iterate + dedup-by-URL (original /research behavior).
	seen := make(map[string]bool)
	var passages []researchPassage
	for _, sq := range variants {
		hits, _, _, err := s.runSearch(searchCtx(sq), retriever, sq, 3)
		if err != nil {
			if onRetrieved != nil {
				onRetrieved(sq, nil)
			}
			continue
		}
		urls := make([]string, 0, len(hits))
		for _, h := range hits {
			if seen[h.URL] {
				continue
			}
			seen[h.URL] = true
			urls = append(urls, h.URL)
			doc, err := s.store.GetDocByURL(ctx, h.URL)
			if err != nil {
				continue
			}
			// Iter 178: preserve the retriever score on the passage so
			// AnswerSource.Score can carry it through to the response.
			passages = append(passages, researchPassage{
				url: doc.URL, title: doc.Title, text: doc.Text, author: doc.Author,
				score: h.Score,
			})
			if len(passages) >= cap {
				break
			}
		}
		if onRetrieved != nil {
			onRetrieved(sq, urls)
		}
		if len(passages) >= cap {
			break
		}
	}
	return passages, nil
}

type researchPassage struct {
	url, title, text, author string
	score                    float64 // iter 178: carried from retrieval hits for AnswerSource.Score
}

func (s *Server) handleResearch(w http.ResponseWriter, r *http.Request) {
	if s.chat == nil {
		writeProblem(w, http.StatusBadRequest, "/research requires a configured chat client (WithChat)")
		return
	}
	start := time.Now()
	q := r.URL.Query().Get("q")
	if q == "" {
		writeProblem(w, http.StatusBadRequest, "missing q parameter")
		return
	}
	strategy := r.URL.Query().Get("strategy")
	if strategy == "" {
		strategy = s.defaults.ResearchStrategy
	}

	// Iter 165: ?hyde=true → each sub-query gets its own HyDE passage at
	// retrieval time. Requires both chat AND embedder configured.
	ctx := r.Context()
	if v := r.URL.Query().Get("hyde"); v == "true" || v == "1" {
		if s.hyde == nil {
			writeProblem(w, http.StatusBadRequest, "hyde requested but no chat client configured")
			return
		}
		if s.vidx == nil || s.emb == nil {
			writeProblem(w, http.StatusBadRequest, "hyde requested but dense/hybrid is not configured (no embedder)")
			return
		}
		ctx = context.WithValue(ctx, hydeEnabledKey{}, true)
	}
	// Iter 168: ?mmr=true propagates through every retrieval call in
	// gatherResearchPassages (planner + paraphrase loops both consult ctx
	// downstream via runSearch → runDense).
	ctx = mmrFromQuery(ctx, r)

	// SSE streaming: opt in via ?stream=true or Accept: text/event-stream.
	// The non-streaming path below still produces the same final JSON shape
	// so existing clients don't break.
	wantStream := r.URL.Query().Get("stream") == "true" ||
		strings.Contains(r.Header.Get("Accept"), "text/event-stream")
	if wantStream {
		s.streamResearch(w, r, q, strategy, start)
		return
	}

	variants, chosenStrategy, err := s.expandForResearch(ctx, q, strategy)
	if err != nil {
		// User-side errors (unknown strategy, paraphraser unconfigured) → 400.
		// Upstream LLM errors → 502.
		if strings.HasPrefix(err.Error(), "unknown strategy") || strings.HasPrefix(err.Error(), "strategy=paraphrase requires") {
			writeProblem(w, http.StatusBadRequest, err.Error())
		} else {
			writeProblem(w, http.StatusBadGateway, err.Error())
		}
		return
	}

	passages, err := s.gatherResearchPassages(ctx, q, chosenStrategy, variants, nil)
	if err != nil {
		writeProblem(w, http.StatusBadGateway, err.Error())
		return
	}
	if len(passages) == 0 {
		writeProblem(w, http.StatusNotFound, "no sources matched any expanded query")
		return
	}
	// Iter 172: ?prf=true on /research — augment the passage list with one
	// extra BM25 retrieval over the term-expanded query. The +prf(N) suffix
	// goes on the Strategy field so the response self-documents.
	var prfTag string
	passages, prfTag = s.applyPRFToResearchPassages(ctx, r, q, passages)
	chosenStrategy += prfTag

	const perDocChars = 1000
	var sb strings.Builder
	sources := make([]AnswerSource, 0, len(passages))
	for i, p := range passages {
		text := p.text
		if len(text) > perDocChars {
			text = text[:perDocChars] + "…"
		}
		// Iter 154: include Author when present so synth can attribute claims
		// ("according to Jane Doe writing in <title>"). Skipped when absent —
		// most pages don't carry JSON-LD authors and a "Author: " line with
		// empty value confuses the LLM into hallucinating attribution.
		if p.author != "" {
			fmt.Fprintf(&sb, "[%d] Title: %s\nAuthor: %s\nURL: %s\nContent: %s\n\n", i+1, p.title, p.author, p.url, text)
		} else {
			fmt.Fprintf(&sb, "[%d] Title: %s\nURL: %s\nContent: %s\n\n", i+1, p.title, p.url, text)
		}
		sources = append(sources, AnswerSource{ID: i + 1, URL: p.url, Title: p.title, Score: p.score})
	}
	s.enrichSources(ctx, sources)
	variantLabel := "Sub-queries used"
	if chosenStrategy == "paraphrase" {
		variantLabel = "Query paraphrases used"
	}
	userMsg := fmt.Sprintf("Original question: %s\n\n%s: %v\n\nSources:\n%s\nSynthesize an answer citing every factual claim with [id].",
		q, variantLabel, variants, sb.String())
	answer, err := s.chat.Chat(ctx, []embed.ChatMsg{
		{Role: "system", Content: researchSynthPrompt},
		{Role: "user", Content: userMsg},
	})
	if err != nil {
		writeProblem(w, http.StatusBadGateway, fmt.Sprintf("synthesize: %v", err))
		return
	}

	// Iter 178: opt-in source-score normalization (mirrors iter-164's
	// /search ?calibrate=true). Top source = 1.0; others as fractions.
	if v := r.URL.Query().Get("calibrate"); v == "true" || v == "1" {
		calibrateSources(sources)
	}

	writeJSON(w, http.StatusOK, ResearchResponse{
		Query:      q,
		Strategy:   chosenStrategy,
		Plan:       variants,
		Answer:     strings.TrimSpace(answer),
		Sources:    sources,
		Took:       time.Since(start).String(),
		Calibrated: false,
	})
}

// streamResearch runs the same plan → fan-out → synthesize pipeline but emits
// SSE events at each stage so clients see progress on long calls.
//
// Event types:
//   - "plan":         { "strategy": "...", "variants": [...] }
//   - "retrieved":    { "variant": "...", "urls": [...] }   (one per variant in planner mode; one "rrf-fused" in paraphrase mode)
//   - "synthesizing": { "sources": N }
//   - "answer_chunk": { "text": "..." }  (when chat client supports streaming)
//   - "done":         the full ResearchResponse JSON
//   - "error":        { "detail": "..." }
//
// The stream always ends with exactly one terminal event ("done" or "error").
func (s *Server) streamResearch(w http.ResponseWriter, r *http.Request, q, strategy string, start time.Time) {
	emit, bail, ok := sseHandler(w)
	if !ok {
		return
	}

	// Iter 165: ?hyde=true also applies to streaming /research. Same checks as
	// non-streaming path; on success, set hydeEnabledKey for the variant loop
	// in gatherResearchPassages.
	ctx := r.Context()
	if v := r.URL.Query().Get("hyde"); v == "true" || v == "1" {
		if s.hyde == nil {
			bail("hyde requested but no chat client configured")
			return
		}
		if s.vidx == nil || s.emb == nil {
			bail("hyde requested but dense/hybrid is not configured (no embedder)")
			return
		}
		ctx = context.WithValue(ctx, hydeEnabledKey{}, true)
	}
	// Iter 168: ?mmr=true on streaming /research.
	ctx = mmrFromQuery(ctx, r)

	variants, chosenStrategy, err := s.expandForResearch(ctx, q, strategy)
	if err != nil {
		bail(err.Error())
		return
	}
	emit("plan", map[string]any{"strategy": chosenStrategy, "variants": variants})

	passages, err := s.gatherResearchPassages(ctx, q, chosenStrategy, variants, func(variant string, urls []string) {
		emit("retrieved", map[string]any{"variant": variant, "urls": urls})
	})
	if err != nil {
		bail(err.Error())
		return
	}
	if len(passages) == 0 {
		bail("no sources matched any expanded query")
		return
	}
	// Iter 172: post-fusion PRF augment for streaming /research too. Emits
	// a "prf" event with the augmentation tag so the client can show it.
	var prfTag string
	passages, prfTag = s.applyPRFToResearchPassages(ctx, r, q, passages)
	if prfTag != "" {
		chosenStrategy += prfTag
		emit("prf", map[string]any{"tag": prfTag, "passages": len(passages)})
	}

	emit("synthesizing", map[string]any{"sources": len(passages)})

	const perDocChars = 1000
	var sb strings.Builder
	sources := make([]AnswerSource, 0, len(passages))
	for i, p := range passages {
		text := p.text
		if len(text) > perDocChars {
			text = text[:perDocChars] + "…"
		}
		// Iter 154: include Author when present so synth can attribute claims
		// ("according to Jane Doe writing in <title>"). Skipped when absent —
		// most pages don't carry JSON-LD authors and a "Author: " line with
		// empty value confuses the LLM into hallucinating attribution.
		if p.author != "" {
			fmt.Fprintf(&sb, "[%d] Title: %s\nAuthor: %s\nURL: %s\nContent: %s\n\n", i+1, p.title, p.author, p.url, text)
		} else {
			fmt.Fprintf(&sb, "[%d] Title: %s\nURL: %s\nContent: %s\n\n", i+1, p.title, p.url, text)
		}
		sources = append(sources, AnswerSource{ID: i + 1, URL: p.url, Title: p.title, Score: p.score})
	}
	s.enrichSources(ctx, sources)
	variantLabel := "Sub-queries used"
	if chosenStrategy == "paraphrase" {
		variantLabel = "Query paraphrases used"
	}
	userMsg := fmt.Sprintf("Original question: %s\n\n%s: %v\n\nSources:\n%s\nSynthesize an answer citing every factual claim with [id].",
		q, variantLabel, variants, sb.String())
	msgs := []embed.ChatMsg{
		{Role: "system", Content: researchSynthPrompt},
		{Role: "user", Content: userMsg},
	}

	// Stream the synthesis token-by-token when the chat client supports it —
	// closes the streaming UX gap from iter 16. Falls back to a single Chat
	// call for clients that don't implement StreamingChatClient.
	var answer string
	if sc, ok := s.chat.(embed.StreamingChatClient); ok {
		full, streamErr := sc.ChatStream(ctx, msgs, func(chunk string) {
			emit("answer_chunk", map[string]string{"text": chunk})
		})
		if streamErr != nil {
			bail(fmt.Sprintf("synthesize: %v", streamErr))
			return
		}
		answer = full
	} else {
		full, callErr := s.chat.Chat(ctx, msgs)
		if callErr != nil {
			bail(fmt.Sprintf("synthesize: %v", callErr))
			return
		}
		answer = full
	}
	// Iter 178: stream variant honors ?calibrate=true too.
	if v := r.URL.Query().Get("calibrate"); v == "true" || v == "1" {
		calibrateSources(sources)
	}
	emit("done", ResearchResponse{
		Query:      q,
		Strategy:   chosenStrategy,
		Plan:       variants,
		Answer:     strings.TrimSpace(answer),
		Sources:    sources,
		Took:       time.Since(start).String(),
		Calibrated: false,
	})
}

// parseSubQueries extracts an array of strings from the planner's response.
// Falls back to the original query if parsing fails — keeps the endpoint
// resilient when the LLM occasionally adds prose around the JSON.
func parseSubQueries(raw, fallback string) []string {
	raw = strings.TrimSpace(raw)
	// Strip common code-fence wrappers.
	for _, fence := range []string{"```json", "```"} {
		raw = strings.TrimPrefix(raw, fence)
		raw = strings.TrimSuffix(raw, "```")
	}
	raw = strings.TrimSpace(raw)
	// Find the JSON array bounds.
	start := strings.Index(raw, "[")
	end := strings.LastIndex(raw, "]")
	if start >= 0 && end > start {
		var arr []string
		if err := json.Unmarshal([]byte(raw[start:end+1]), &arr); err == nil && len(arr) > 0 {
			return arr
		}
	}
	return []string{fallback}
}

// AnswerResponse is /answer's wire shape. Sources are ordered by retrieval rank;
// citation IDs in `answer` reference Source.ID.
type AnswerResponse struct {
	Query    string         `json:"query"`
	Answer   string         `json:"answer"`
	Sources  []AnswerSource `json:"sources"`
	Took     string         `json:"took"`
	Calibrated bool         `json:"calibrated"`
}

type AnswerSource struct {
	ID    int    `json:"id"`
	URL   string `json:"url"`
	Title string `json:"title"`
	// Iter 84: enrichment fields surfaced from the index. Mirrors iter-82/83
	// SearchHit additions so /research callers see the same metadata shape
	// /search callers do. All omitempty — undated docs and empty domains
	// emit clean JSON without nulls or empty strings.
	Domain      string     `json:"domain,omitempty"`
	PublishedAt *time.Time `json:"published_at,omitempty"`
	Excerpt     string     `json:"excerpt,omitempty"`
	// Iter 178: retrieval score per source. Raw retriever score (BM25 sum,
	// cosine, RRF reciprocal, or cross-encoder depending on retriever),
	// preserved from the underlying SearchHit. Operators building "show top
	// citation by confidence" UIs can rank by this. Omitempty — older clients
	// that don't expect the field never see it.
	Score float64 `json:"score,omitempty"`
	// Iter 178: within-response normalized score when ?calibrate=true is set
	// on /answer or /research. Top source = 1.0; others = Score / max(Score).
	// Mirrors iter-164's /search behavior. Empty (omitempty) otherwise.
	ScoreCalibrated float64 `json:"score_calibrated,omitempty"`
}

// enrichSources populates Domain/PublishedAt/Excerpt on the given sources
// via a single batched store lookup. Mirrors the /search enrichment pattern
// (iter-82/83). Best-effort: on SQL error, sources are returned with their
// existing fields only — no 500 just because the enrichment lookup failed.
// Iter 84.
func (s *Server) enrichSources(ctx context.Context, sources []AnswerSource) {
	if len(sources) == 0 {
		return
	}
	urls := make([]string, len(sources))
	for i := range sources {
		urls[i] = sources[i].URL
	}
	metas, err := s.store.GetDocMetas(ctx, urls)
	if err != nil {
		return
	}
	for i := range sources {
		m, ok := metas[sources[i].URL]
		if !ok {
			continue
		}
		sources[i].Domain = m.Domain
		if !m.PublishedAt.IsZero() {
			p := m.PublishedAt
			sources[i].PublishedAt = &p
		}
		sources[i].Excerpt = m.Excerpt
	}
}

const answerSystemPrompt = `You are a research assistant. Answer the user's question using ONLY the provided sources.
- Cite sources by their numeric id in square brackets, e.g. [1] or [2,3]. Every factual claim needs a citation.
- If the sources do not contain the answer, say so plainly. Do not invent facts.
- Keep the answer focused on what the sources actually say; do not pad.`

func (s *Server) handleAnswer(w http.ResponseWriter, r *http.Request) {
	if s.chat == nil {
		writeProblem(w, http.StatusBadRequest, "/answer requires a configured chat client (WithChat)")
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
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > 20 {
			writeProblem(w, http.StatusBadRequest, "k must be an integer in [1, 20]")
			return
		}
		k = n
	}
	// Iter 108: SSE streaming. Mirrors /research stream detection — opt in
	// via ?stream=true or Accept: text/event-stream. Routed to streamAnswer,
	// which emits retrieved/synthesizing/answer_chunk/done/error events.
	wantStream := r.URL.Query().Get("stream") == "true" ||
		strings.Contains(r.Header.Get("Accept"), "text/event-stream")
	if wantStream {
		s.streamAnswer(w, r, q, k, start)
		return
	}
	// /answer inherits ?expand=true from /search (default off). When enabled
	// and a paraphraser is configured, the LLM's input sources come from the
	// fused main+paraphrase retrieval — measurably +0.02 nDCG at scale per iter 46.
	// Instance default applies when query param is absent.
	useExpand := s.defaults.Expand
	if v := r.URL.Query().Get("expand"); v != "" {
		useExpand = v == "true" || v == "1"
	}
	if useExpand && s.paraphraser == nil {
		writeProblem(w, http.StatusBadRequest, "expand requested but no paraphraser configured")
		return
	}

	// Iter 166: ?hyde=true on /answer. iter-161 shipped HyDE for /search but
	// /answer's handler never parsed the query param — runSearch alone won't
	// set the passage in ctx. Same shape as iter-161's /search wiring +
	// iter-165's /research wiring.
	// Iter 174: also set hydeEnabledKey so expandHits knows to do per-paraphrase
	// HyDE generation (otherwise paraphrase retrievals reuse HyDE-of-q for
	// their dense leg, silently breaking expand's diversification).
	ctx := r.Context()
	if v := r.URL.Query().Get("hyde"); v == "true" || v == "1" {
		if s.hyde == nil {
			writeProblem(w, http.StatusBadRequest, "hyde requested but no chat client configured")
			return
		}
		if s.vidx == nil || s.emb == nil {
			writeProblem(w, http.StatusBadRequest, "hyde requested but dense/hybrid is not configured (no embedder)")
			return
		}
		passage := s.hyde.Passage(ctx, q)
		if passage != "" && passage != q {
			ctx = context.WithValue(ctx, hydeQueryKey{}, passage)
		}
		ctx = context.WithValue(ctx, hydeEnabledKey{}, true)
	}
	// Iter 168: ?mmr=true on /answer. Same shape as iter-158's /search wiring;
	// no capability check — runDense / runSearch decide whether to actually
	// fire MMR based on retriever (dense/hybrid only). BM25-only retrievers
	// silently no-op the param.
	ctx = mmrFromQuery(ctx, r)

	retriever := "bm25"
	if s.vidx != nil && s.emb != nil {
		retriever = "hybrid"
	}
	hits, _, _, err := s.runSearch(ctx, retriever, q, k)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, fmt.Sprintf("retrieval: %v", err))
		return
	}
	// Iter 170: PRF runs BEFORE expand so the paraphrase fusion sees the
	// term-expanded candidate set. Source-tag suffix is discarded — /answer
	// doesn't surface per-hit source tags in its response shape.
	hits, _, _ = s.applyPRFIfRequested(ctx, r, retriever, q, hits, nil, k)
	if useExpand && len(hits) > 0 {
		hits = s.expandHits(ctx, q, retriever, k, hits)
	}

	// Build the sources block. Truncate per-doc text to ~1.2k chars so we don't
	// blow the context window on a small corpus with long docs.
	const perDocChars = 1200
	var sb strings.Builder
	sources := make([]AnswerSource, 0, len(hits))
	for i, h := range hits {
		doc, err := s.store.GetDocByURL(ctx, h.URL)
		if err != nil {
			continue
		}
		text := doc.Text
		if len(text) > perDocChars {
			text = text[:perDocChars] + "…"
		}
		// Iter 154: include Author when present (same shape as /research synth).
		if doc.Author != "" {
			fmt.Fprintf(&sb, "[%d] Title: %s\nAuthor: %s\nURL: %s\nContent: %s\n\n", i+1, doc.Title, doc.Author, doc.URL, text)
		} else {
			fmt.Fprintf(&sb, "[%d] Title: %s\nURL: %s\nContent: %s\n\n", i+1, doc.Title, doc.URL, text)
		}
		sources = append(sources, AnswerSource{ID: i + 1, URL: doc.URL, Title: doc.Title, Score: h.Score})
	}
	if len(sources) == 0 {
		writeProblem(w, http.StatusNotFound, "no sources matched the query")
		return
	}
	s.enrichSources(ctx, sources)

	userMsg := fmt.Sprintf("Sources:\n%s\nQuestion: %s\n\nAnswer (cite all factual claims with [id]):", sb.String(), q)
	answer, err := s.chat.Chat(ctx, []embed.ChatMsg{
		{Role: "system", Content: answerSystemPrompt},
		{Role: "user", Content: userMsg},
	})
	if err != nil {
		writeProblem(w, http.StatusBadGateway, fmt.Sprintf("chat: %v", err))
		return
	}

	// Iter 178: ?calibrate=true on /answer mirrors iter-164's /search.
	if v := r.URL.Query().Get("calibrate"); v == "true" || v == "1" {
		calibrateSources(sources)
	}

	writeJSON(w, http.StatusOK, AnswerResponse{
		Query:      q,
		Answer:     strings.TrimSpace(answer),
		Sources:    sources,
		Took:       time.Since(start).String(),
		Calibrated: false, // honest: no calibration data yet
	})
}

// streamAnswer is /answer's SSE variant. Same retrieval + synthesis as the
// non-streaming path, but emits events along the way so clients see progress.
// Iter 108 — completes thestreaming surface that iter 98 started
// with /research.
//
// Event types (matches the /research events from iter 16 for client reuse):
//   - "retrieved":    { "urls": [...] }                 (one event; /answer has no per-variant retrieval)
//   - "synthesizing": { "sources": N }
//   - "answer_chunk": { "text": "..." }                 (when chat client supports streaming)
//   - "done":         the full AnswerResponse JSON
//   - "error":        { "detail": "..." }
//
// The stream always ends with exactly one terminal event ("done" or "error").
// Falls back to a single Chat() call if the chat client doesn't implement
// embed.StreamingChatClient — still emits all the structural events, just
// no per-token chunks.
func (s *Server) streamAnswer(w http.ResponseWriter, r *http.Request, q string, k int, start time.Time) {
	emit, bail, ok := sseHandler(w)
	if !ok {
		return
	}

	retriever := "bm25"
	if s.vidx != nil && s.emb != nil {
		retriever = "hybrid"
	}
	// Mirror handleAnswer's expand handling so ?expand=true&stream=true works
	// as expected (otherwise stream-mode silently ignores expand).
	useExpand := s.defaults.Expand
	if v := r.URL.Query().Get("expand"); v != "" {
		useExpand = v == "true" || v == "1"
	}
	if useExpand && s.paraphraser == nil {
		bail("expand requested but no paraphraser configured")
		return
	}

	// Iter 166: ?hyde=true also applies to streaming /answer. Same checks as
	// the non-streaming path.
	// Iter 174: same hydeEnabledKey wiring so expandHits does per-paraphrase
	// HyDE for streaming too.
	ctx := r.Context()
	if v := r.URL.Query().Get("hyde"); v == "true" || v == "1" {
		if s.hyde == nil {
			bail("hyde requested but no chat client configured")
			return
		}
		if s.vidx == nil || s.emb == nil {
			bail("hyde requested but dense/hybrid is not configured (no embedder)")
			return
		}
		passage := s.hyde.Passage(ctx, q)
		if passage != "" && passage != q {
			ctx = context.WithValue(ctx, hydeQueryKey{}, passage)
		}
		ctx = context.WithValue(ctx, hydeEnabledKey{}, true)
	}
	// Iter 168: ?mmr=true on streaming /answer.
	ctx = mmrFromQuery(ctx, r)

	hits, _, _, err := s.runSearch(ctx, retriever, q, k)
	if err != nil {
		bail(fmt.Sprintf("retrieval: %v", err))
		return
	}
	// Iter 170: PRF for streaming /answer — same shape as non-streaming.
	hits, _, _ = s.applyPRFIfRequested(ctx, r, retriever, q, hits, nil, k)
	if useExpand && len(hits) > 0 {
		hits = s.expandHits(ctx, q, retriever, k, hits)
	}

	urls := make([]string, 0, len(hits))
	for _, h := range hits {
		urls = append(urls, h.URL)
	}
	emit("retrieved", map[string]any{"urls": urls})

	const perDocChars = 1200
	var sb strings.Builder
	sources := make([]AnswerSource, 0, len(hits))
	for i, h := range hits {
		doc, err := s.store.GetDocByURL(ctx, h.URL)
		if err != nil {
			continue
		}
		text := doc.Text
		if len(text) > perDocChars {
			text = text[:perDocChars] + "…"
		}
		// Iter 154: include Author when present (same shape as /research + /answer).
		if doc.Author != "" {
			fmt.Fprintf(&sb, "[%d] Title: %s\nAuthor: %s\nURL: %s\nContent: %s\n\n", i+1, doc.Title, doc.Author, doc.URL, text)
		} else {
			fmt.Fprintf(&sb, "[%d] Title: %s\nURL: %s\nContent: %s\n\n", i+1, doc.Title, doc.URL, text)
		}
		sources = append(sources, AnswerSource{ID: i + 1, URL: doc.URL, Title: doc.Title, Score: h.Score})
	}
	if len(sources) == 0 {
		bail("no sources matched the query")
		return
	}
	s.enrichSources(ctx, sources)

	emit("synthesizing", map[string]any{"sources": len(sources)})

	userMsg := fmt.Sprintf("Sources:\n%s\nQuestion: %s\n\nAnswer (cite all factual claims with [id]):", sb.String(), q)
	msgs := []embed.ChatMsg{
		{Role: "system", Content: answerSystemPrompt},
		{Role: "user", Content: userMsg},
	}

	var answer string
	if sc, ok := s.chat.(embed.StreamingChatClient); ok {
		full, streamErr := sc.ChatStream(ctx, msgs, func(chunk string) {
			emit("answer_chunk", map[string]string{"text": chunk})
		})
		if streamErr != nil {
			bail(fmt.Sprintf("chat: %v", streamErr))
			return
		}
		answer = full
	} else {
		full, callErr := s.chat.Chat(ctx, msgs)
		if callErr != nil {
			bail(fmt.Sprintf("chat: %v", callErr))
			return
		}
		answer = full
	}

	// Iter 178: ?calibrate=true on streaming /answer.
	if v := r.URL.Query().Get("calibrate"); v == "true" || v == "1" {
		calibrateSources(sources)
	}

	emit("done", AnswerResponse{
		Query:      q,
		Answer:     strings.TrimSpace(answer),
		Sources:    sources,
		Took:       time.Since(start).String(),
		Calibrated: false,
	})
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func writeProblem(w http.ResponseWriter, status int, detail string) {
	writeJSON(w, status, map[string]any{
		"error":  http.StatusText(status),
		"status": status,
		"detail": detail,
	})
}

// sseHandler establishes an SSE stream on `w` and returns:
//   - emit: writes one `event: <name>\ndata: <json>\n\n` block and flushes
//   - bail: emits an "error" event with the given detail (terminal-by-convention)
//   - ok: false if the underlying writer doesn't support flushing — in that case
//     sseHandler has already written a 500 problem response and the caller should
//     just return
//
// Iter 114 — extracted from iter-16/98 streamResearch, iter-108 streamAnswer,
// and iter-112 handleAdminReembed which all wrote the same ~18-line boilerplate.
// Three callsites is where the duplication starts costing more than the
// abstraction (matches the iter-109 client-side extraction's threshold).
func sseHandler(w http.ResponseWriter) (emit func(event string, data any), bail func(detail string), ok bool) {
	flusher, hasFlusher := w.(http.Flusher)
	if !hasFlusher {
		writeProblem(w, http.StatusInternalServerError, "streaming unsupported by this writer")
		return nil, nil, false
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	emit = func(event string, data any) {
		b, err := json.Marshal(data)
		if err != nil {
			return
		}
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b)
		flusher.Flush()
	}
	bail = func(detail string) { emit("error", map[string]string{"detail": detail}) }
	return emit, bail, true
}

// logMiddleware logs status code + duration AND records the request into
// the metrics store. Promoted to a method so it can reach s.metrics.
func (s *Server) logMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: 200}
		h.ServeHTTP(rec, r)
		d := time.Since(start)
		if s.metrics != nil {
			s.metrics.RecordRequest(r.URL.Path, d)
		}
		log.Printf("%s %s %d %s", r.Method, r.URL.Path, rec.status, d)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Flush passes through to the underlying writer so SSE handlers can stream
// through the log middleware. Without this, w.(http.Flusher) inside handlers
// fails and /research?stream=true 500s.
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// ListenAndServe is a convenience runner for cmd/cosift.
func ListenAndServe(ctx context.Context, addr string, h http.Handler) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutCtx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
