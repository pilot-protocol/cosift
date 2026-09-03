package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"crypto/sha256"
	"encoding/hex"

	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/judge"
	"github.com/pilot-protocol/cosift/internal/rerank"
)

type synthRequest struct {
	Q              string `json:"q"`
	K              int    `json:"k,omitempty"`
	IncludeDomains string `json:"include_domains,omitempty"`
	ExcludeDomains string `json:"exclude_domains,omitempty"`
	Site           string `json:"site,omitempty"`
	Since          string `json:"since,omitempty"`
	Until          string `json:"until,omitempty"`
	IncludeText    bool   `json:"include_text,omitempty"`
	Rerank         bool   `json:"rerank,omitempty"`
	Expand         string `json:"expand,omitempty"`
	Stream         bool   `json:"stream,omitempty"`
}

func (req synthRequest) toValues() url.Values {
	v := url.Values{}
	if req.Q != "" {
		v.Set("q", req.Q)
	}
	if req.K > 0 {
		v.Set("k", strconv.Itoa(req.K))
	}
	if req.IncludeDomains != "" {
		v.Set("include_domains", req.IncludeDomains)
	}
	if req.ExcludeDomains != "" {
		v.Set("exclude_domains", req.ExcludeDomains)
	}
	if req.Site != "" {
		v.Set("site", req.Site)
	}
	if req.Since != "" {
		v.Set("since", req.Since)
	}
	if req.Until != "" {
		v.Set("until", req.Until)
	}
	if req.IncludeText {
		v.Set("include_text", "true")
	}
	if req.Rerank {
		v.Set("rerank", "true")
	}
	if req.Expand != "" {
		v.Set("expand", req.Expand)
	}
	if req.Stream {
		v.Set("stream", "true")
	}
	return v
}

func (s *pebbleHTTP) handleAnswerPOST(w http.ResponseWriter, r *http.Request) {
	var req synthRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	r.URL.RawQuery = req.toValues().Encode()
	s.handleAnswer(w, r)
}

func (s *pebbleHTTP) handleResearchPOST(w http.ResponseWriter, r *http.Request) {
	var req synthRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	r.URL.RawQuery = req.toValues().Encode()
	s.handleResearch(w, r)
}

// /answer — synthesis over BM25 retrieval. Mirrors the SQLite-side
// /answer in spirit (top-k sources → cited synthesis) but stays minimal: no
// streaming, no rerank, no query expansion. Those land in follow-up iters
// once this surface is exercised. Returns 501 when no chat model is
// configured so the absent capability fails loud instead of silent.
const answerSystemPrompt = `You are a research assistant. Answer the user's question using the provided sources.
- Cite sources by their numeric id in square brackets, e.g. [1] or [2,3]. Every factual claim needs a citation.
- Synthesize the answer from what the sources state, including reasonable inferences from adjacent context — sources rarely state every answer verbatim.
- Only fall back to "the sources do not cover X" if NONE of the sources have any relevant material at all. Do not refuse just because the exact formula, number, or phrase isn't quoted verbatim — extract whatever IS there and say so.
- IGNORE sources that are irrelevant to the question — login forms, page-not-found stubs, wiki edit/diff/admin pages, navigation menus, raw image directories, or content about a different topic. Do not cite them. Do not mention them in your answer. Pretend they weren't in the source list.
- Do not invent facts not supported by the sources.
- Keep the answer focused on what the sources actually say; do not pad.`

// HyDE-style query expansion prompt. Borrowed verbatim from
// internal/server/hyde.go so the SQLite and Pebble paths produce comparable
// expansions and operators don't have to learn two prompt shapes.
const hydeSystemPrompt = `Write a brief, factual passage (2-4 sentences) that would directly answer the user's question. Output ONLY the passage — no preamble, no commentary, no apology if you're uncertain. If the question is ambiguous, pick the most plausible interpretation and answer that. The passage doesn't need to be true; it needs to be the SHAPE of what a relevant document would say. Embedding this passage and searching by its vector will find documents that look like real answers, even if the user's original query was just a few keywords.`

// doChat wraps ChatClient.Chat with attempt/failure counters.
// Takes the client as a parameter so it works for both s.chat and the
// StreamingChatClient passed into streamResearch/streamAnswer.
func (s *pebbleHTTP) doChat(ctx context.Context, c embed.ChatClient, msgs []embed.ChatMsg) (string, error) {
	s.chatAttempts.Add(1)
	start := time.Now()
	out, err := c.Chat(ctx, msgs)
	s.chatDurationNanos.Add(time.Since(start).Nanoseconds())
	if err != nil {
		s.chatFailures.Add(1)
	}
	return out, err
}

// doChatStream wraps StreamingChatClient.ChatStream with the same counters.
func (s *pebbleHTTP) doChatStream(ctx context.Context, c embed.StreamingChatClient, msgs []embed.ChatMsg, onChunk func(string)) (string, error) {
	s.chatAttempts.Add(1)
	start := time.Now()
	out, err := c.ChatStream(ctx, msgs, onChunk)
	s.chatDurationNanos.Add(time.Since(start).Nanoseconds())
	if err != nil {
		s.chatFailures.Add(1)
	}
	return out, err
}

type answerSource struct {
	// Matches the [N] tokens the synth prompt produces in the answer
	// text. SQLite-side AnswerSource has had this; pebble's was missing,
	// which caused the CLI to render every source as '[0]' before the
	// i+1 fallback landed.
	ID          int        `json:"id"`
	URL         string     `json:"url"`
	Title       string     `json:"title"`
	Excerpt     string     `json:"excerpt,omitempty"`
	PublishedAt *time.Time `json:"published_at,omitempty"`
	Author      string     `json:"author,omitempty"`
	Text        string     `json:"text,omitempty"`
}

type answerResponse struct {
	Query          string `json:"query"`
	EffectiveQuery string `json:"effective_query,omitempty"`
	Expand         string `json:"expand,omitempty"`
	// retriever label — same vocabulary as /search ("bm25",
	// "dense", "bm25+dense:rrf", "+hyde"/"+paraphrase", "+rerank:<name>").
	Retriever       string         `json:"retriever,omitempty"`
	Answer          string         `json:"answer"`
	Sources         []answerSource `json:"sources"`
	Model           string         `json:"model"`
	Warnings        []string       `json:"warnings,omitempty"`
	TotalCandidates int            `json:"total_candidates,omitempty"`
	Took            string         `json:"took"`
	// when the answer matches a "sources do not contain" pattern,
	// surface a hint URL pointing at /research?strategy=planner. The 60-Q
	// eval showed multi-hop decomposition rescues ~40% of findability +
	// factual-lookup misses. Purely additive — clients decide whether to
	// render a "Try research mode" affordance.
	SuggestEscalation string `json:"suggest_escalation,omitempty"`
}

// answerLooksLikeNoInfo returns true when the answer reads as "sources don't
// cover this" — the canonical /research-escalation trigger. Same phrase set
// the eval uses; kept in sync so eval results predict escalation rate.
func answerLooksLikeNoInfo(s string) bool {
	if len(s) > 800 {
		// Long answers with one disclaimer line aren't bails — only treat
		// short, mostly-disclaimer responses as escalation candidates.
		return false
	}
	low := strings.ToLower(s)
	for _, p := range []string{
		"do not contain", "do not provide", "sources don",
		"not contain the", "no information", "do not include",
		"do not have", "do not cover", "not mention", "no mention",
	} {
		if strings.Contains(low, p) {
			return true
		}
	}
	return false
}

// rerankText is only populated when a reranker ran; an empty excerpt makes the judge drop the candidate.
func judgeExcerpt(rerankText, title, excerpt string) string {
	if rerankText != "" {
		return rerankText
	}
	return title + "\n" + excerpt
}

func (s *pebbleHTTP) handleAnswer(w http.ResponseWriter, r *http.Request) {
	// When clustered + gateway mode +
	// not already serving as a leaf for someone else, fan-out retrieval
	// to peers (with include_text), then run a SINGLE synth here.
	if s.cluster.GatewayMode && s.cluster.IsClustered() && r.URL.Query().Get("cluster_local") != "1" {
		s.handleAnswerGateway(w, r)
		return
	}
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
	// Cache + singleflight on /answer. Key is the full query-string
	// (sorted) so different filter combos don't collide. We capture the
	// rendered body into a buffer; on success the buffer is the cache
	// value. Skip the cache for explicit no-cache requests and for
	// streaming clients — bufferedResponse can't flush, so SSE would
	// silently fall through to the sync JSON path.
	if s.answerCache != nil && r.Header.Get("Cache-Control") != "no-cache" && !wantsSSE(r) {
		key := answerCacheKey(r)
		body, err, shared := s.answerCache.Do(key, func() ([]byte, error) {
			rec := &bufferedResponse{header: http.Header{}, status: 200}
			s.handleAnswerInner(rec, r, start)
			if rec.status != http.StatusOK {
				return nil, errAnswerNotOK
			}
			return rec.buf.Bytes(), nil
		})
		if err == nil {
			if shared {
				w.Header().Set("X-Cache", "hit")
			} else {
				w.Header().Set("X-Cache", "miss")
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
			return
		}
		// On non-OK from the inner handler, fall through to the
		// uncached path so the real status code reaches the client.
	}
	s.handleAnswerInner(w, r, start)
}

// errAnswerNotOK signals that the inner handler wrote a non-2xx
// response — don't cache it, and let the caller re-run uncached so
// the proper status reaches the client.
var errAnswerNotOK = errors.New("answer: non-OK response, not cached")

// answerCacheKey is the deterministic cache key for a given /answer
// request. Sorts query parameters so URL-order doesn't fragment the
// cache; drops parameters that don't affect the response.
func answerCacheKey(r *http.Request) string {
	q := r.URL.Query()
	// Don't key on transport-only or debug-only flags.
	q.Del("cluster_local")
	q.Del("trace")
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		for _, v := range q[k] {
			sb.WriteString(k)
			sb.WriteByte('=')
			sb.WriteString(v)
			sb.WriteByte('\x00')
		}
	}
	sum := sha256.Sum256([]byte(sb.String()))
	return hex.EncodeToString(sum[:16])
}

// bufferedResponse captures a handler's writes into a buffer so we
// can store the rendered JSON in answerCache before flushing to the
// real response writer.
type bufferedResponse struct {
	buf    bytes.Buffer
	header http.Header
	status int
}

func (b *bufferedResponse) Header() http.Header { return b.header }

func (b *bufferedResponse) WriteHeader(statusCode int) { b.status = statusCode }

func (b *bufferedResponse) Write(p []byte) (int, error) {
	if b.status == 0 {
		b.status = http.StatusOK
	}
	return b.buf.Write(p)
}

// handleAnswerInner is the uncached body. Called both via the cache
// path (with a bufferedResponse) and directly when the cache is
// disabled or a non-OK response needs the real writer.
func (s *pebbleHTTP) handleAnswerInner(w http.ResponseWriter, r *http.Request, start time.Time) {
	q := r.URL.Query().Get("q")
	k := 5
	if v := r.URL.Query().Get("k"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 20 {
			k = n
		}
	}
	// /answer respects the same retrieval filters /search does —
	// scoping research to a domain or date window is the common EXA shape.
	include := splitDomainsCSV(r.URL.Query().Get("include_domains"))
	exclude := splitDomainsCSV(r.URL.Query().Get("exclude_domains"))
	sites := parseSiteScopes(r.URL.Query().Get("site"))
	siteFilter := len(sites) > 0
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
	// Default to hybrid + rerank on /answer — the reranker is the proven
	// quality recovery layer; ?retriever=bm25 and ?rerank=false opt out.
	retrieverParam, wantRerank := s.applyLLMEndpointDefaults(
		r.URL.Query().Get("retriever"),
		r.URL.Query().Get("rerank"),
	)
	keepCap := k
	if wantRerank {
		keepCap = s.rerankCandK
		if keepCap < k {
			keepCap = k
		}
	}
	fetchK := keepCap
	if len(include) > 0 || len(exclude) > 0 || dateFilter || siteFilter {
		mult := 5
		if dateFilter {
			mult = 10
		}
		if siteFilter {
			mult = 50 // site= over-fetches aggressively: small site in a large corpus
		}
		fetchK = keepCap * mult
		cap := 200 // /answer caps tighter than /search — full doc text per source is expensive
		if siteFilter && cap < 500 {
			cap = 500
		}
		if fetchK > cap {
			fetchK = cap
		}
	} else if wantRerank {
		fetchK = keepCap * 2
	}
	// SSE opt-in. Open the stream BEFORE retrieve() so phase events can
	// surface backend pipeline progress (retrieve / rerank / judge / mmr /
	// synth_start) instead of just the synth-chunk tail. Opt in via
	// ?stream=true or Accept: text/event-stream. Caps requirement: chat
	// client must implement StreamingChatClient AND the ResponseWriter
	// must support http.Flusher; if either is missing, fall through to
	// sync (today's behavior).
	wantStream := wantsSSE(r)
	var sse *answerSSE
	var streamChat embed.StreamingChatClient
	if wantStream {
		if sc, ok := s.chat.(embed.StreamingChatClient); ok {
			if a := newAnswerSSE(w, start); a != nil {
				sse, streamChat = a, sc
				stopKA := sse.startKeepalive(r.Context(), 7*time.Second)
				defer stopKA()
				sse.warnings(s.warningsFor(r))
				sse.phase("parsed", map[string]any{
					"q": q, "k": k, "fetch_k": fetchK,
					"retriever": retrieverParam, "rerank": wantRerank,
				})
			}
		}
	}

	// expansion dispatch (bare / HyDE / paraphrase+RRF).
	// retriever dispatch (bm25 / dense / hybrid) via shared helper.
	expandMode := r.URL.Query().Get("expand")
	hits, effectiveQuery, err := s.retrieve(r.Context(), q, fetchK, retrieverParam, expandMode)
	if err != nil {
		if sse != nil {
			sse.errorEvt(err.Error())
			return
		}
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	if sse != nil {
		sse.phase("retrieve", map[string]any{
			"hits": len(hits), "effective_query": effectiveQuery,
		})
	}
	type cand struct {
		src        answerSource
		excerpt    string
		rerankText string
		score      float64 // retrieval score, used by time-decay
	}
	cands := make([]cand, 0, keepCap)
	denseDrops := 0
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
		if siteFilter && !matchesAnySite(h.URL, sites) {
			continue
		}
		doc, derr := s.store.GetDocByURL(r.Context(), h.URL)
		if derr != nil || doc == nil {
			s.noteDenseDrop(retrieverParam)
			denseDrops++
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
		c := cand{src: src, excerpt: excerpt, score: h.Score}
		if wantRerank {
			c.rerankText = doc.Title + "\n" + doc.Text
		}
		cands = append(cands, c)
		if len(cands) >= keepCap {
			break
		}
	}
	if sse != nil {
		sse.phase("filter", map[string]any{"candidates": len(cands), "dense_drops": denseDrops})
	}
	// time-decay re-weights then re-sorts BEFORE rerank, mirroring
	// the /search pre-rerank placement so the reranker sees a freshness-
	// aware pool.
	decayHalfLife, decaySet := parseDecayHalfLife(r.URL.Query().Get("decay"))
	if decaySet && len(cands) > 0 {
		now := time.Now()
		for i := range cands {
			cands[i].score *= decayMultiplier(cands[i].src.PublishedAt, now, decayHalfLife)
		}
		sort.SliceStable(cands, func(i, j int) bool { return cands[i].score > cands[j].score })
		if sse != nil {
			sse.phase("decay", map[string]any{"half_life_days": decayHalfLife})
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
		if sse != nil {
			sse.phase("rerank", map[string]any{"reranker": s.reranker.Name(), "candidates": len(cands)})
		}
	}
	// LLM relevance judge — STRICT gate. The reranker re-orders but
	// doesn't drop; without this gate, marginal content (Wikipedia
	// diff/admin pages, dead .edu PDFs, JS-only landing pages with
	// stub HTML) leaks into the synth context and produces "based on
	// the sources, here is some irrelevant trivia" answers.
	//
	// Strict means: if a candidate fails the relevance threshold, it
	// is DROPPED — we do not top up from the rejected pool to reach k.
	// Better to return 2 truly relevant sources than 5 sources where 3
	// are noise. If 0 candidates pass, downstream returns the "no
	// matching sources" response (already handled below).
	//
	// Skip when ?judge=false or no chat client (correctness-preserving).
	if r.URL.Query().Get("judge") != "false" && s.chat != nil && len(cands) > 1 {
		before := len(cands)
		jCands := make([]judge.Candidate, len(cands))
		for i, c := range cands {
			jCands[i] = judge.Candidate{ID: strconv.Itoa(i), Excerpt: judgeExcerpt(c.rerankText, c.src.Title, c.excerpt)}
		}
		verdicts := judge.Judge(r.Context(), s.chat, q, jCands, judge.Options{MinScore: 0.3})
		keep := make([]cand, 0, len(cands))
		for i, c := range cands {
			if i < len(verdicts) && verdicts[i].Keep {
				keep = append(keep, c)
			}
		}
		cands = keep
		if sse != nil {
			sse.phase("judge", map[string]any{
				"before": before, "kept": len(cands), "dropped": before - len(cands),
			})
		}
	}
	// MMR diversification on /answer — same pattern as /search.
	// Synth quality benefits when the top-k sources cover different
	// angles instead of being 5 paraphrases of the same paper.
	mmrLambda, mmrSet := parseMMRLambda(r.URL.Query().Get("mmr"))
	mmrFired := false
	if mmrSet && len(cands) > 1 {
		urls := make([]string, len(cands))
		for i := range cands {
			urls[i] = cands[i].src.URL
		}
		if order := s.applyMMRPermutation(r.Context(), urls, q, mmrLambda); order != nil {
			reordered := make([]cand, len(order))
			for i, idx := range order {
				reordered[i] = cands[idx]
			}
			cands = reordered
			mmrFired = true
		}
		if sse != nil && mmrFired {
			sse.phase("mmr", map[string]any{"lambda": mmrLambda, "candidates": len(cands)})
		}
	}
	if len(cands) > k {
		cands = cands[:k]
	}
	sources := make([]answerSource, 0, len(cands))
	var promptSources strings.Builder
	for i, c := range cands {
		// stamp ID = i+1 so the JSON response matches the [N]
		// citation tokens we emit in the synth prompt below.
		c.src.ID = i + 1
		sources = append(sources, c.src)
		fmt.Fprintf(&promptSources, "[%d] %s\n%s\n%s\n\n", i+1, c.src.Title, c.src.URL, c.excerpt)
	}
	// same retriever label vocabulary as /search.
	denseReady := s.hnsw() != nil && s.embedder != nil
	retrieverLabel := s.buildRetrieverLabel(retrieverParam, expandMode, denseReady, effectiveQuery != q, wantRerank)
	if mmrFired {
		retrieverLabel += fmt.Sprintf("+mmr:%.2f", mmrLambda)
	}
	if decaySet {
		retrieverLabel += fmt.Sprintf("+decay:%gd", decayHalfLife)
	}
	if len(sources) == 0 {
		// Distinguish the two failure modes so the client can tell
		// "we have nothing on this topic" (retrieval returned 0 hits)
		// from "we retrieved candidates but the relevance judge dropped
		// every one of them" (the corpus has noise, not the answer).
		msg := "No matching sources in the index."
		if len(hits) > 0 {
			msg = "Retrieval returned candidates but none were judged relevant to the question. The corpus may not cover this topic in usable detail."
		}
		if sse != nil {
			sse.sources(q, sources, streamChat.Model(), retrieverLabel, len(hits))
			sse.chunk(msg)
			sse.done()
			return
		}
		empty := answerResponse{
			Query: q, Expand: normalizeExpandMode(expandMode), Retriever: retrieverLabel,
			Answer:  msg,
			Sources: sources, Model: s.chat.Model(), TotalCandidates: len(hits), Took: time.Since(start).String(),
		}
		if effectiveQuery != q {
			empty.EffectiveQuery = effectiveQuery
		}
		empty.Warnings = s.warningsFor(r)
		writeJSON(w, http.StatusOK, empty)
		return
	}

	msgs := []embed.ChatMsg{
		{Role: "system", Content: answerSystemPrompt},
		{Role: "user", Content: "Sources:\n\n" + promptSources.String() + "Question: " + q},
	}

	if sse != nil {
		sse.sources(q, sources, streamChat.Model(), retrieverLabel, len(hits))
		sse.phase("synth_start", map[string]any{"sources": len(sources), "model": streamChat.Model()})
		full, cerr := s.doChatStream(r.Context(), streamChat, msgs, sse.chunk)
		if cerr != nil {
			sse.errorEvt(cerr.Error())
			return
		}
		if answerLooksLikeNoInfo(full) {
			sse.suggestEscalation(q)
		}
		sse.done()
		return
	}

	answer, err := s.doChat(r.Context(), s.chat, msgs)
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "chat: "+err.Error())
		return
	}
	resp := answerResponse{
		Query: q, Expand: normalizeExpandMode(expandMode), Retriever: retrieverLabel,
		Answer: answer, Sources: sources,
		Model: s.chat.Model(), TotalCandidates: len(hits), Took: time.Since(start).String(),
	}
	if effectiveQuery != q {
		resp.EffectiveQuery = effectiveQuery
	}
	resp.Warnings = s.warningsFor(r)
	// suggest /research escalation when the model bailed.
	if answerLooksLikeNoInfo(answer) {
		resp.SuggestEscalation = "/research?q=" + url.QueryEscape(q) + "&strategy=planner"
	}
	writeJSON(w, http.StatusOK, resp)
}

// recordingWriter wraps an http.ResponseWriter and captures every byte
// written to it. Used to record SSE streams for the research cache — the
// captured bytes can later be replayed to a new client directly.
type recordingWriter struct {
	http.ResponseWriter
	buf bytes.Buffer
}

func (r *recordingWriter) Write(b []byte) (int, error) {
	r.buf.Write(b)
	return r.ResponseWriter.Write(b)
}

func (r *recordingWriter) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// wantsSSE reports whether the request opts into Server-Sent Events,
// either via ?stream=true or an Accept: text/event-stream header. Both
// /answer, /query, and /research check the same envelope.
func wantsSSE(r *http.Request) bool {
	return r.URL.Query().Get("stream") == "true" ||
		strings.Contains(r.Header.Get("Accept"), "text/event-stream")
}

// answerSSE wraps the SSE writer for /answer streaming. Methods emit the
// well-known event types (phase, sources, answer_chunk, warnings, error,
// suggest_escalation, done). Phase events surface pipeline progress
// (retrieve / rerank / judge / mmr / synth_start) so clients can render a
// timeline of what the backend is doing.
//
// Writes are mu-protected because the keepalive ticker can race with
// the main handler goroutine. The handler does the substantive event
// writes; the ticker emits SSE comment frames (": ka\n\n") to keep
// the HTTP/2 stream alive across long retrieval / rerank / chat calls
// — without them, browsers (Safari especially) close the stream when
// no bytes flow for 10–30 s and the request fails mid-pipeline.
type answerSSE struct {
	w       http.ResponseWriter
	flusher http.Flusher
	start   time.Time
	last    time.Time // last phase emit; used to compute per-phase duration
	mu      sync.Mutex
}

// newAnswerSSE opens an SSE stream. Returns nil if the writer can't flush.
// Caller is responsible for not calling writeProblem after this point —
// the response status (200) and headers are committed here.
//
// Disables the server-wide WriteTimeout (60s) via ResponseController.
// Multi-pass /research can run 90-180s with 3-5 passes against a slow
// chat model; the deadline would kill the connection mid-stream and
// the browser fetch would surface a generic "Load failed". SSE handlers
// flush their own progress, so a stuck handler still gets killed by
// ctx cancellation when the client disconnects.
func newAnswerSSE(w http.ResponseWriter, start time.Time) *answerSSE {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	if rc := http.NewResponseController(w); rc != nil {
		_ = rc.SetWriteDeadline(time.Time{})
	}
	w.WriteHeader(http.StatusOK)
	return &answerSSE{w: w, flusher: flusher, start: start, last: start}
}

func (a *answerSSE) send(payload any) {
	buf, _ := json.Marshal(payload)
	a.mu.Lock()
	defer a.mu.Unlock()
	fmt.Fprintf(a.w, "data: %s\n\n", buf)
	a.flusher.Flush()
}

// startKeepalive launches a goroutine that emits SSE data frames every
// interval, keeping the HTTP/2 stream and any intermediate proxy alive
// across long retrieval / chat calls that emit no real data. Returns a
// stop function the caller defers; stopping is idempotent.
//
// We deliberately emit `data: {"type":"ka", ...}` frames rather than the
// spec's `: comment` form. Safari's CFNetwork stream stack tracks
// "no payload data received" separately from "no bytes received" — a
// long-running stream of nothing but comment frames still trips its
// internal timeout and the browser surfaces it as "Error: Load failed".
// JSON ka frames count as real data; the chat UI ignores `type:"ka"`.
func (a *answerSSE) startKeepalive(ctx context.Context, interval time.Duration) (stop func()) {
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				a.send(map[string]any{
					"type":       "ka",
					"elapsed_ms": time.Since(a.start).Milliseconds(),
				})
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

// phase emits a pipeline-step event with elapsed-since-start and
// duration-since-last-phase, plus any caller-supplied detail fields.
// Holds a.mu for the whole call so the read+write of a.last and the
// write-to-the-wire are all atomic with respect to concurrent phase
// calls and the keepalive ticker.
func (a *answerSSE) phase(name string, details map[string]any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	evt := map[string]any{
		"type":        "phase",
		"name":        name,
		"elapsed_ms":  now.Sub(a.start).Milliseconds(),
		"duration_ms": now.Sub(a.last).Milliseconds(),
	}
	for k, v := range details {
		evt[k] = v
	}
	a.last = now
	buf, _ := json.Marshal(evt)
	fmt.Fprintf(a.w, "data: %s\n\n", buf)
	a.flusher.Flush()
}

func (a *answerSSE) warnings(warns []string) {
	if len(warns) == 0 {
		return
	}
	a.send(map[string]any{"type": "warnings", "warnings": warns})
}

func (a *answerSSE) sources(q string, src []answerSource, model, retrieverLabel string, totalCandidates int) {
	evt := map[string]any{
		"type":             "sources",
		"query":            q,
		"sources":          src,
		"model":            model,
		"total_candidates": totalCandidates,
	}
	if retrieverLabel != "" {
		evt["retriever"] = retrieverLabel
	}
	a.send(evt)
}

func (a *answerSSE) chunk(delta string) {
	a.send(map[string]any{"type": "answer_chunk", "text": delta})
}

func (a *answerSSE) errorEvt(msg string) {
	a.send(map[string]any{"type": "error", "error": msg})
}

func (a *answerSSE) suggestEscalation(q string) {
	a.send(map[string]any{
		"type":               "suggest_escalation",
		"suggest_escalation": "/research?q=" + url.QueryEscape(q) + "&strategy=planner",
	})
}

func (a *answerSSE) done() {
	a.send(map[string]any{"type": "done", "took": time.Since(a.start).String()})
}

// /research — multi-step retrieval + synthesis. LLM decomposes the
// question into 2-3 sub-queries, each sub-query runs BM25, results are deduped
// by URL keeping the best score, top-k feed a cited synthesis. Mirrors the
// SQLite-side /research planner strategy. No streaming, no rerank, no
// paraphrase strategy yet — those follow once this surface is exercised.
const researchPlanPrompt = `Decompose the user's research question into 2-3 short keyword search phrases (3-6 words each, NOT full sentences). Each phrase MUST target a DIFFERENT facet so the phrases do not overlap: one conceptual/definitional (what it is), one procedural/how-to (steps, configuration, usage), and one edge-case/troubleshooting (errors, limits, gotchas, comparisons). Skip a facet only if it does not apply. Do NOT output near-duplicate rephrasings of the same idea. Output ONLY a JSON array of strings — no prose, no markdown. Example: ["consensus algorithm overview", "configure validator node", "leader election failure recovery"]`

// truncateForPromptLite caps text by approximate char count for source
// prompts. Same heuristic the eval-quick path uses.
func truncateForPromptLite(s string, maxChars int) string {
	if len(s) <= maxChars {
		return s
	}
	return s[:maxChars] + "…"
}

const researchSynthPrompt = `You are a research assistant. Synthesize an answer to the original question using ONLY the provided sources.
- Cite sources by their numeric id, e.g. [1] or [2,3]. Every factual claim needs a citation.
- If the sources don't cover something, say so plainly — do not invent.
- Keep the answer focused on what the sources actually say.`

// researchRefineSynthPrompt is the system prompt for pass 2+ of a multi-pass
// /research call. The user message includes the prior draft answer alongside
// the expanded source pool. The instruction shape mirrors the standard synth
// prompt but explicitly directs the model to revise rather than start fresh.
const researchRefineSynthPrompt = `You are a research assistant. You have a draft answer from a prior research pass plus additional sources that were gathered to close gaps. Produce a REVISED answer using all the sources now available.
- Cite sources by their numeric id, e.g. [1] or [2,3]. Every factual claim needs a citation.
- Keep correct claims from the draft. Correct or extend where the new sources change the picture.
- If sources still don't cover something, say so plainly — do not invent.
- Output the FULL revised answer, not a delta.`

// selfEvalPrompt asks the model to judge whether its own draft answer is
// sufficient given the question and sources. JSON-only output keeps parsing
// deterministic. The "missing" + "refine_queries" fields drive the next
// research pass when sufficient=false.
const selfEvalPrompt = `You wrote a research answer. Judge whether it's sufficient given the question and sources.

Output ONLY a JSON object (no markdown, no prose):
{"sufficient": true|false, "missing": "<one-line gap, empty if sufficient>", "refine_queries": ["<query>", ...]}

Rules:
- "sufficient": true ONLY if the answer fully addresses the question with current sources.
- "missing": one short sentence describing what would strengthen the answer (empty string if sufficient).
- "refine_queries": 1-3 specific search queries that would close the gap (empty array if sufficient). Make them concrete — entity + concept + qualifier — not vague paraphrases.
- Be honest. Most first-pass answers benefit from one more pass on a specific gap.`

// selfEval is the parsed shape of the JSON the self-eval LLM call emits.
type selfEval struct {
	Sufficient    bool     `json:"sufficient"`
	Missing       string   `json:"missing"`
	RefineQueries []string `json:"refine_queries"`
}

// parseSelfEval extracts the {sufficient, missing, refine_queries} JSON from
// the LLM response. Tolerates leading/trailing prose and markdown fences. On
// any parse failure returns ok=false so the caller can treat as "stop and
// keep the current answer" instead of crashing the stream.
func parseSelfEval(s string) (selfEval, bool) {
	s = strings.TrimSpace(s)
	s = jsonFenceRE.ReplaceAllString(s, "$1")
	// Find the first '{' and last '}' to skip any preamble.
	i := strings.IndexByte(s, '{')
	j := strings.LastIndexByte(s, '}')
	if i < 0 || j <= i {
		return selfEval{}, false
	}
	var ev selfEval
	if err := json.Unmarshal([]byte(s[i:j+1]), &ev); err != nil {
		return selfEval{}, false
	}
	if len(ev.RefineQueries) > 3 {
		ev.RefineQueries = ev.RefineQueries[:3]
	}
	return ev, true
}

// jsonFenceRE strips ```json fences and bare ``` fences from LLM output. The
// judge package has its own copy with the same intent; not worth pulling into
// a shared package for two callers.
var jsonFenceRE = regexp.MustCompile("(?s)```(?:json)?\\s*(.*?)\\s*```")

// researchMaxPasses parses ?max_passes from the request. Default 3, valid
// range 1-10 (clamped). 1 disables the self-eval loop entirely (single-pass
// /research, today's behavior). The upper cap exists to bound LLM cost +
// wall-clock: each non-final pass costs 1 synth + 1 self-eval call on top
// of the per-sub-query retrieval, and the model can in theory loop forever
// emitting "not yet sufficient" verdicts.
func researchMaxPasses(r *http.Request) int {
	const defaultMax = 3
	const hardCap = 10
	v := r.URL.Query().Get("max_passes")
	if v == "" {
		return defaultMax
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return defaultMax
	}
	if n > hardCap {
		return hardCap
	}
	return n
}

type researchResponse struct {
	Query  string   `json:"query"`
	Plan   []string `json:"plan"`
	Expand string   `json:"expand,omitempty"`
	// retriever label, mirroring /search/answer vocabulary.
	Retriever       string         `json:"retriever,omitempty"`
	Answer          string         `json:"answer"`
	Sources         []answerSource `json:"sources"`
	Model           string         `json:"model"`
	Warnings        []string       `json:"warnings,omitempty"`
	TotalCandidates int            `json:"total_candidates,omitempty"`
	Took            string         `json:"took"`
}

// researchSubFanOut bounds how many sub-queries retrieve concurrently within
// a single /research pass. Shared by the sync (handleResearch) and streaming
// (streamResearch) paths so their latency profiles stay identical. 3 keeps the
// site= budget at 3 sub-queries × fan-out-3 = one concurrent batch.
const researchSubFanOut = 3

func (s *pebbleHTTP) handleResearch(w http.ResponseWriter, r *http.Request) {
	// Planner runs once on gateway,
	// each sub-query scatters to peers, single synth here.
	if s.cluster.GatewayMode && s.cluster.IsClustered() && r.URL.Query().Get("cluster_local") != "1" {
		s.handleResearchGateway(w, r)
		return
	}
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
	// same retrieval filters /search/answer/find_similar accept.
	// Parsed once here so the streaming and sync paths share semantics.
	filt, err := parseRetrievalFilters(r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, err.Error())
		return
	}
	includeText := r.URL.Query().Get("include_text") == "true"
	// SSE streaming for /research. Same trigger as /answer.
	// Emits phase-aware events so the UI can render the plan and source list
	// before the synth call completes — /research often runs 10–30s
	// (2-3 plan→retrieve→synth chat rounds), so phase visibility matters.
	wantStream := wantsSSE(r)
	if wantStream {
		if sc, ok := s.chat.(embed.StreamingChatClient); ok {
			cacheKey := "research|" + q + "|" + r.URL.Query().Get("site") + "|" + strconv.Itoa(k)
			if r.Header.Get("Cache-Control") != "no-cache" {
				if cached, ok := s.researchCache.Get(cacheKey); ok {
					w.Header().Set("Content-Type", "text/event-stream")
					w.Header().Set("Cache-Control", "no-cache")
					w.Header().Set("X-Accel-Buffering", "no")
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write(cached)
					if f, ok2 := w.(http.Flusher); ok2 {
						f.Flush()
					}
					return
				}
			}
			rec := &recordingWriter{ResponseWriter: w}
			s.streamResearch(rec, r, sc, q, k, filt, start)
			if rec.buf.Len() > 0 {
				s.researchCache.Set(cacheKey, rec.buf.Bytes())
			}
			return
		}
	}

	// Plan — include site domain so the LLM generates site-specific sub-queries
	// rather than generic web queries that miss small-site content.
	planQ := q
	if len(filt.sites) > 0 {
		siteHints := make([]string, len(filt.sites))
		for si, ss := range filt.sites {
			siteHints[si] = ss.host
			if ss.path != "" {
				siteHints[si] += ss.path
			}
		}
		planQ = q + "\n\nSite filter: " + strings.Join(siteHints, ", ") + ". Generate sub-queries using specific terminology likely found on this site."
		if titles := s.getSiteTitles(r.Context(), filt.sites); len(titles) > 0 {
			planQ += "\n\nExample pages on this site: " + strings.Join(titles, ", ") + "."
		}
	}
	planRaw, err := s.doChat(r.Context(), s.chat, []embed.ChatMsg{
		{Role: "system", Content: researchPlanPrompt},
		{Role: "user", Content: planQ},
	})
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "plan: "+err.Error())
		return
	}
	subs := parseSubQueries(planRaw, q)
	maxSubs := 5
	// site= scope limits the search to a small domain. 3 sub-queries with
	// fan-out 3 complete in one concurrent batch (~21s each), keeping total
	// retrieval under 25s so LLM synthesis fits within the 60s budget.
	if len(filt.sites) > 0 && maxSubs > 3 {
		maxSubs = 3
	}
	if len(subs) > maxSubs {
		subs = subs[:maxSubs]
	}

	// Retrieve per sub-query, dedupe by URL keeping best score.
	type ranked struct {
		score float64
		hit   index.Hit
	}
	best := make(map[string]ranked, k*len(subs))
	// Build the site boost map first: its length equals the site's doc count,
	// which drives the adaptive perSub below.
	syncBoostIDs := s.getSiteBoostIDs(r.Context(), filt.sites)
	perSub := k * 2
	if perSub > 40 {
		perSub = 40
	}
	if len(filt.sites) > 0 {
		// Fetch deep enough to cover the whole site (×1.2 headroom) so the 50×
		// boost can reorder every site doc into top-k. Floor 200 preserves the
		// MaxScore-firing window for tiny site-key terms (e.g. "pilotctl");
		// ceiling 1000 bounds per-sub BM25 latency. Issue #2's MaxScore
		// pre-check is what keeps the larger fetch affordable.
		perSub = max(200, min(1000, int(math.Ceil(float64(len(syncBoostIDs))*1.2))))
	}
	// each sub-query goes through retrieveWithExpansion, so
	// ?expand=hyde gives per-sub-query HyDE (was's behavior) and
	// ?expand=paraphrase fans out 3 paraphrases × N sub-queries → RRF per
	// sub-query → merge into best{}. The cross-sub-query merge stays
	// score-keep-best (not a second RRF) — that's what specified.
	// retriever dispatch (bm25 / dense / hybrid) applies per
	// sub-query — every sub-query runs through the same retriever.
	expandMode := r.URL.Query().Get("expand")
	// Default to hybrid + rerank on /research — same rationale as /answer.
	retrieverParam, wantRerank := s.applyLLMEndpointDefaults(
		r.URL.Query().Get("retriever"),
		r.URL.Query().Get("rerank"),
	)
	// Sub-queries are independent retrieve calls and the per-call cost dominates
	// total latency, so fan them out with a bounded concurrency identical to the
	// streaming path. bestMu guards the shared best map; hits and syncBoostIDs
	// are goroutine-local / read-only respectively. There is no seenURLs set in
	// the sync path (single pass), so the merge is a plain keep-best-score dedupe.
	var bestMu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, researchSubFanOut)
	for _, sq := range subs {
		sq := sq
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			hits, _, err := s.retrieveForSites(r.Context(), sq, perSub, retrieverParam, expandMode, syncBoostIDs, filt.sites)
			if err != nil {
				// log the specific sub-query so operators can diagnose
				// 'why was this research thin on sources' — previously silent.
				log.Printf("pebble-serve: /research sub-query %q failed: %v", sq, err)
				return
			}
			bestMu.Lock()
			for _, h := range hits {
				if prev, ok := best[h.URL]; !ok || h.Score > prev.score {
					best[h.URL] = ranked{score: h.Score, hit: h}
				}
			}
			bestMu.Unlock()
		}()
	}
	wg.Wait()
	pooled := make([]ranked, 0, len(best))
	for _, v := range best {
		pooled = append(pooled, v)
	}
	sort.Slice(pooled, func(i, j int) bool { return pooled[i].score > pooled[j].score })
	// same rerank wiring as /search and /answer — pool widens to
	// rerankCandK before materialization, rerank reorders the pool, truncate
	// to k after rerank so citation numbers track the final order.
	keepCap := k
	if wantRerank {
		keepCap = s.rerankCandK
		if keepCap < k {
			keepCap = k
		}
	}
	// Truncate the sorted pool to keepCap before materialization. For site=
	// queries the 50x boost (applied in Search before its descending sort)
	// guarantees the site's matched docs occupy the front of pooled, so this
	// cut keeps exactly the boosted docs and bounds the rerank candidate count
	// (was ~138 for site= queries; now keepCap, default rerankCandK=20).
	if len(pooled) > keepCap {
		pooled = pooled[:keepCap]
	}

	// Materialize candidates; defer sources + prompt build until after rerank.
	type cand struct {
		src        answerSource
		excerpt    string
		rerankText string
		score      float64 // pooled score, used by time-decay
	}
	cands := make([]cand, 0, len(pooled))
	for _, p := range pooled {
		doc, derr := s.store.GetDocByURL(r.Context(), p.hit.URL)
		if derr != nil || doc == nil {
			s.noteDenseDrop(retrieverParam)
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
		c := cand{src: src, excerpt: excerpt, score: p.score}
		if wantRerank {
			c.rerankText = doc.Title + "\n" + doc.Text
		}
		cands = append(cands, c)
	}
	// time-decay on /research sync — re-weight pooled cands before
	// rerank. Sub-queries that hit recent docs bubble up first.
	decayHalfLife, decaySet := parseDecayHalfLife(r.URL.Query().Get("decay"))
	if decaySet && len(cands) > 0 {
		now := time.Now()
		for i := range cands {
			cands[i].score *= decayMultiplier(cands[i].src.PublishedAt, now, decayHalfLife)
		}
		sort.SliceStable(cands, func(i, j int) bool { return cands[i].score > cands[j].score })
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
	// MMR diversification on /research sync. /research aggregates
	// across sub-queries — duplicate-ish docs accumulate fast. MMR after
	// rerank, before truncation.
	mmrLambda, mmrSet := parseMMRLambda(r.URL.Query().Get("mmr"))
	mmrFired := false
	if mmrSet && len(cands) > 1 {
		urls := make([]string, len(cands))
		for i := range cands {
			urls[i] = cands[i].src.URL
		}
		if order := s.applyMMRPermutation(r.Context(), urls, q, mmrLambda); order != nil {
			reordered := make([]cand, len(order))
			for i, idx := range order {
				reordered[i] = cands[idx]
			}
			cands = reordered
			mmrFired = true
		}
	}
	if len(cands) > k {
		cands = cands[:k]
	}
	sources := make([]answerSource, 0, len(cands))
	var promptSources strings.Builder
	for i, c := range cands {
		// stamp ID = i+1 so the JSON response matches the [N]
		// citation tokens we emit in the synth prompt below.
		c.src.ID = i + 1
		sources = append(sources, c.src)
		fmt.Fprintf(&promptSources, "[%d] %s\n%s\n%s\n\n", i+1, c.src.Title, c.src.URL, c.excerpt)
	}
	// Sub-queries don't yield a
	// single effectiveQuery, so "expansion fired" is approximated by intent:
	// chat is up and expandMode requested it. Matches what warningsFor() uses
	// to decide whether to flag a silent no-op.
	denseReady := s.hnsw() != nil && s.embedder != nil
	expandFired := expandMode != "" && s.chat != nil
	retrieverLabel := s.buildRetrieverLabel(retrieverParam, expandMode, denseReady, expandFired, wantRerank)
	if mmrFired {
		retrieverLabel += fmt.Sprintf("+mmr:%.2f", mmrLambda)
	}
	if decaySet {
		retrieverLabel += fmt.Sprintf("+decay:%gd", decayHalfLife)
	}
	if len(sources) == 0 {
		writeJSON(w, http.StatusOK, researchResponse{
			Query: q, Plan: subs, Expand: normalizeExpandMode(expandMode), Retriever: retrieverLabel,
			Answer:  "No matching sources for any sub-query.",
			Sources: sources, Model: s.chat.Model(), Warnings: s.warningsFor(r),
			TotalCandidates: len(best), Took: time.Since(start).String(),
		})
		return
	}

	answer, err := s.doChat(r.Context(), s.chat, []embed.ChatMsg{
		{Role: "system", Content: researchSynthPrompt},
		{Role: "user", Content: "Sources:\n\n" + promptSources.String() + "Original question: " + q},
	})
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "synth: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, researchResponse{
		Query: q, Plan: subs, Expand: normalizeExpandMode(expandMode), Retriever: retrieverLabel,
		Answer: answer, Sources: sources,
		Model: s.chat.Model(), Warnings: s.warningsFor(r),
		TotalCandidates: len(best), Took: time.Since(start).String(),
	})
}

// streamResearch is the SSE form of handleResearch. Event sequence:
//
//	plan    — sub-queries returned by the planner
//	sources — deduped + ranked pool fed to the synthesizer
//	chunk   — per chat delta during synth
//	done    — final {"took": "..."}
//	error   — terminal; either plan, retrieval, or synth failed
func (s *pebbleHTTP) streamResearch(w http.ResponseWriter, r *http.Request, sc embed.StreamingChatClient, q string, k int, filt retrievalFilters, start time.Time) {
	sse := newAnswerSSE(w, start)
	if sse == nil {
		writeProblem(w, http.StatusInternalServerError, "streaming requires http.Flusher")
		return
	}
	// /research can sit on a single slow retrieve or chat call for
	// 10–30 s. Without a keepalive, the browser's HTTP/2 stream times
	// out as inactive and the request fails mid-pipeline. 7 s gives
	// generous headroom under Safari's ~30 s idle limit.
	stopKA := sse.startKeepalive(r.Context(), 7*time.Second)
	defer stopKA()
	includeText := r.URL.Query().Get("include_text") == "true"

	// surface silent no-ops upfront so SSE clients can render them
	// while the plan call is still in flight. Fires before plan to give the
	// fastest visual feedback when a misconfigured request arrives.
	sse.warnings(s.warningsFor(r))
	sse.phase("plan_start", map[string]any{"q": q, "model": sc.Model()})

	// site= hint: LLM generates site-specific sub-queries with domain terminology
	// rather than generic queries that miss small-site content in BM25 top-200.
	planQ := q
	if len(filt.sites) > 0 {
		siteHints := make([]string, len(filt.sites))
		for si, ss := range filt.sites {
			siteHints[si] = ss.host
			if ss.path != "" {
				siteHints[si] += ss.path
			}
		}
		planQ = q + "\n\nSite filter: " + strings.Join(siteHints, ", ") + ". Generate sub-queries using specific terminology likely found on this site."
		if titles := s.getSiteTitles(r.Context(), filt.sites); len(titles) > 0 {
			planQ += "\n\nExample pages on this site: " + strings.Join(titles, ", ") + "."
		}
	}
	planRaw, err := s.doChat(r.Context(), sc, []embed.ChatMsg{
		{Role: "system", Content: researchPlanPrompt},
		{Role: "user", Content: planQ},
	})
	if err != nil {
		sse.send(map[string]any{"type": "error", "phase": "plan", "error": err.Error()})
		return
	}
	subs := parseSubQueries(planRaw, q)
	{
		maxSubs := 5
		if len(filt.sites) > 0 && maxSubs > 3 {
			maxSubs = 3 // site= budget: 3 sub-queries × fan-out-3 = one concurrent batch (~21s) + LLM fits in 60s
		}
		if len(subs) > maxSubs {
			subs = subs[:maxSubs]
		}
	}
	// per-sub-query expansion via retrieveWithExpansion.
	// per-sub-query retriever dispatch (bm25 / dense / hybrid).
	// read retriever + wantRerank early so the plan event can
	// surface the full retriever label, not just expand.
	// Default to hybrid + rerank on /research streaming — same rationale as /answer.
	expandMode := r.URL.Query().Get("expand")
	retrieverParam, wantRerank := s.applyLLMEndpointDefaults(
		r.URL.Query().Get("retriever"),
		r.URL.Query().Get("rerank"),
	)
	denseReady := s.hnsw() != nil && s.embedder != nil
	retrieverLabel := s.buildRetrieverLabel(retrieverParam, expandMode, denseReady, expandMode != "", wantRerank)

	// surface the active expansion strategy on the plan event so
	// UIs can render 'using paraphrase' or 'using HyDE' as soon as the plan
	// arrives, before per-sub-query expansion fires.
	planEvent := map[string]any{"type": "plan", "query": q, "plan": subs, "model": sc.Model()}
	if mode := normalizeExpandMode(expandMode); mode != "" {
		planEvent["expand"] = mode
	}
	if retrieverLabel != "" {
		planEvent["retriever"] = retrieverLabel
	}
	sse.send(planEvent)
	// Phase event mirrors /query's shape so the chat UI's timeline strip
	// renders identical rows across all three synth endpoints. Co-exists
	// with the legacy `type: plan` event above, which the CLI client at
	// cmd/cosift/main.go keys on.
	planPhase := map[string]any{"queries": subs, "retriever": retrieverLabel}
	if mode := normalizeExpandMode(expandMode); mode != "" {
		planPhase["expand"] = mode
	}
	sse.phase("plan", planPhase)

	type ranked struct {
		score float64
		hit   index.Hit
	}
	type cand struct {
		src        answerSource
		excerpt    string
		rerankText string
		score      float64
	}

	// Multi-pass research loop. Each pass: retrieve for the current subs →
	// fuse → materialize → rerank → mmr → synth → self-eval. Cumulative URL
	// set across passes; new passes skip URLs already seen so retrieval
	// widens rather than re-fetching the same docs. After synth, the model
	// judges whether its draft is sufficient. If not, we use its
	// refine_queries to drive the next pass. Cap at maxPasses to bound
	// latency + LLM cost.
	maxPasses := researchMaxPasses(r)
	// site= constrains to a small domain: multi-pass BM25 at fetchK=300 costs
	// 21s × passes beyond the 60s budget. Cap to 1 pass — a small site's corpus
	// is fully represented in the first retrieval round anyway.
	if len(filt.sites) > 0 && maxPasses > 1 {
		maxPasses = 1
	}
	decayHalfLife, decaySet := parseDecayHalfLife(r.URL.Query().Get("decay"))
	mmrLambda, mmrSet := parseMMRLambda(r.URL.Query().Get("mmr"))
	// Pre-build site boost map once per request (O(site_docs) famHost scan,
	// cached for repeat queries). Nil when site= is absent or store unavailable.
	// Built before perSub because its length equals the site doc count.
	siteBoostIDs := s.getSiteBoostIDs(r.Context(), filt.sites)
	perSub := k * 2
	if perSub > 40 {
		perSub = 40
	}
	if len(filt.sites) > 0 {
		// Fetch deep enough to cover the whole site (×1.2 headroom); floor 200
		// preserves the MaxScore-firing window for tiny site-key terms, ceiling
		// 1000 bounds per-sub BM25 latency. Mitigated by Issue #2's MaxScore
		// pre-check.
		perSub = max(200, min(1000, int(math.Ceil(float64(len(siteBoostIDs))*1.2))))
	}
	keepCap := k
	if wantRerank {
		keepCap = s.rerankCandK
		if keepCap < k {
			keepCap = k
		}
	}

	seenURLs := make(map[string]bool)
	allCands := make([]cand, 0, k*maxPasses)
	totalCandidates := 0 // counts URL contributions across passes for the sources event
	var lastAnswer string
	var finalRetrieverLabel string

	for pass := 1; pass <= maxPasses; pass++ {
		sse.phase("pass_start", map[string]any{"pass": pass, "queries": subs})

		// Retrieval round for this pass. Sub-queries run concurrently with
		// a bounded fan-out — they're independent calls to s.retrieve and
		// the per-call cost dominates total pass latency. Serial execution
		// of N sub-queries at ~12 s each pushed pass-1+pass-2 wall-clock
		// past 60 s on this corpus, blowing the browser's hardcoded fetch
		// timeout on long-streaming responses. With fan-out the pass
		// shrinks to ~max(sub-query latency) plus the per-goroutine
		// bookkeeping. sse.send is mutex-protected; bestMu protects best.
		// seenURLs is read-only during this loop (last written at the end
		// of the prior pass), so no synchronization is needed for it.
		best := make(map[string]ranked, k*len(subs))
		var bestMu sync.Mutex
		var wg sync.WaitGroup
		sem := make(chan struct{}, researchSubFanOut)
		for _, sq := range subs {
			sq := sq
			wg.Add(1)
			go func() {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				sse.phase("retrieving", map[string]any{"pass": pass, "query": sq})
				hits, _, rerr := s.retrieveForSites(r.Context(), sq, perSub, retrieverParam, expandMode, siteBoostIDs, filt.sites)
				if rerr != nil {
					log.Printf("pebble-serve: /research sub-query %q failed: %v", sq, rerr)
					sse.phase("expand", map[string]any{"pass": pass, "query": sq, "error": rerr.Error()})
					return
				}
				bestMu.Lock()
				for _, h := range hits {
					if seenURLs[h.URL] {
						continue
					}
					if prev, ok := best[h.URL]; !ok || h.Score > prev.score {
						best[h.URL] = ranked{score: h.Score, hit: h}
					}
				}
				bestMu.Unlock()
				sse.phase("expand", map[string]any{"pass": pass, "query": sq, "hits": len(hits)})
			}()
		}
		wg.Wait()
		totalCandidates += len(best)
		pooled := make([]ranked, 0, len(best))
		for _, v := range best {
			pooled = append(pooled, v)
		}
		sort.Slice(pooled, func(i, j int) bool { return pooled[i].score > pooled[j].score })
		sse.phase("fuse", map[string]any{"pass": pass, "unique_urls": len(pooled)})
		// Truncate the sorted pool to keepCap before materialization. For site=
		// queries the 50x boost (applied in Search before its descending sort)
		// puts the site's matched docs at the front of pooled, so this cut keeps
		// the boosted docs and bounds the LLM rerank candidate count to keepCap.
		if len(pooled) > keepCap {
			pooled = pooled[:keepCap]
		}

		// Materialize this pass's new candidates (filt-aware).
		newCands := make([]cand, 0, len(pooled))
		for _, p := range pooled {
			doc, derr := s.store.GetDocByURL(r.Context(), p.hit.URL)
			if derr != nil || doc == nil {
				s.noteDenseDrop(retrieverParam)
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
			c := cand{src: src, excerpt: excerpt, score: p.score}
			if wantRerank {
				c.rerankText = doc.Title + "\n" + doc.Text
			}
			newCands = append(newCands, c)
		}
		sse.phase("materialize", map[string]any{"pass": pass, "candidates": len(newCands)})

		// Optional time-decay on this pass's new candidates.
		passLabel := retrieverLabel
		if decaySet && len(newCands) > 0 {
			now := time.Now()
			for i := range newCands {
				newCands[i].score *= decayMultiplier(newCands[i].src.PublishedAt, now, decayHalfLife)
			}
			sort.SliceStable(newCands, func(i, j int) bool { return newCands[i].score > newCands[j].score })
			passLabel += fmt.Sprintf("+decay:%gd", decayHalfLife)
		}
		if wantRerank && len(newCands) > 1 {
			rc := make([]rerank.Candidate, len(newCands))
			for i := range newCands {
				rc[i] = rerank.Candidate{ID: strconv.Itoa(i), Text: newCands[i].rerankText}
			}
			if order, rerr := s.doRerank(r.Context(), q, rc); rerr == nil && len(order) > 0 {
				reordered := make([]cand, 0, len(newCands))
				seenIdx := make(map[int]bool, len(newCands))
				for _, id := range order {
					if n, perr := strconv.Atoi(id); perr == nil && n >= 0 && n < len(newCands) && !seenIdx[n] {
						reordered = append(reordered, newCands[n])
						seenIdx[n] = true
					}
				}
				for i, c := range newCands {
					if !seenIdx[i] {
						reordered = append(reordered, c)
					}
				}
				newCands = reordered
			}
			sse.phase("rerank", map[string]any{"pass": pass, "reranker": s.reranker.Name(), "candidates": len(newCands)})
		}
		if mmrSet && len(newCands) > 1 {
			urls := make([]string, len(newCands))
			for i := range newCands {
				urls[i] = newCands[i].src.URL
			}
			if order := s.applyMMRPermutation(r.Context(), urls, q, mmrLambda); order != nil {
				reordered := make([]cand, len(order))
				for i, idx := range order {
					reordered[i] = newCands[idx]
				}
				newCands = reordered
				passLabel += fmt.Sprintf("+mmr:%.2f", mmrLambda)
				sse.phase("mmr", map[string]any{"pass": pass, "lambda": mmrLambda, "candidates": len(newCands)})
			}
		}
		if len(newCands) > k {
			newCands = newCands[:k]
		}
		// Promote into the cumulative pool.
		for _, c := range newCands {
			seenURLs[c.src.URL] = true
			allCands = append(allCands, c)
		}
		finalRetrieverLabel = passLabel

		// First-pass-empty path mirrors today's behavior: if pass 1 finds
		// no usable sources at all, bail with empty=true.
		if pass == 1 && len(allCands) == 0 {
			sse.send(map[string]any{"type": "done", "took": time.Since(start).String(), "empty": true})
			return
		}
		// Later-pass empty (no new sources added): synth would just rewrite
		// the prior answer with no new signal. Stop and keep lastAnswer.
		if pass > 1 && len(newCands) == 0 {
			break
		}

		// Build the cumulative sources slice + prompt block, stamping IDs
		// 1..N in the order URLs were promoted.
		// For site= queries trim excerpts to 600 chars — reduces synthesis
		// context from ~9.6k to ~4.8k chars, cutting synthesis latency ~40%.
		synthExcerptLen := 1200
		if len(filt.sites) > 0 {
			synthExcerptLen = 600
		}
		cumulativeSources := make([]answerSource, 0, len(allCands))
		var promptSources strings.Builder
		for i, c := range allCands {
			c.src.ID = i + 1
			cumulativeSources = append(cumulativeSources, c.src)
			ex := c.excerpt
			if len(ex) > synthExcerptLen {
				ex = ex[:synthExcerptLen]
			}
			fmt.Fprintf(&promptSources, "[%d] %s\n%s\n%s\n\n", i+1, c.src.Title, c.src.URL, ex)
		}
		srcEvt := map[string]any{
			"type": "sources", "sources": cumulativeSources,
			"total_candidates": totalCandidates, "pass": pass,
		}
		if finalRetrieverLabel != "" {
			srcEvt["retriever"] = finalRetrieverLabel
		}
		sse.send(srcEvt)

		// Synth. Pass 1 uses the standard prompt; pass 2+ uses the refine
		// prompt with the prior draft injected.
		sse.phase("synth_start", map[string]any{
			"pass": pass, "sources": len(cumulativeSources), "model": sc.Model(),
		})
		var synthMsgs []embed.ChatMsg
		userMsg := "Sources:\n\n" + promptSources.String() + "Original question: " + q
		if pass == 1 {
			synthMsgs = []embed.ChatMsg{
				{Role: "system", Content: researchSynthPrompt},
				{Role: "user", Content: userMsg},
			}
		} else {
			synthMsgs = []embed.ChatMsg{
				{Role: "system", Content: researchRefineSynthPrompt},
				{Role: "user", Content: userMsg + "\n\nYour prior draft answer:\n" + lastAnswer},
			}
		}
		full, serr := s.doChatStream(r.Context(), sc, synthMsgs, sse.chunk)
		if serr != nil {
			sse.send(map[string]any{"type": "error", "phase": "synth", "error": serr.Error()})
			return
		}
		lastAnswer = full

		// On the final pass, skip self-eval — nothing to escalate to.
		if pass == maxPasses {
			break
		}

		// Self-evaluate. The model decides whether to escalate.
		sse.phase("self_eval_start", map[string]any{"pass": pass})
		evalUserMsg := fmt.Sprintf(
			"Question: %s\n\nYour answer:\n%s\n\nSources used (id — title — url):\n%s",
			q, lastAnswer, summarizeSourceList(cumulativeSources),
		)
		evalRaw, eerr := s.doChat(r.Context(), sc, []embed.ChatMsg{
			{Role: "system", Content: selfEvalPrompt},
			{Role: "user", Content: evalUserMsg},
		})
		if eerr != nil {
			log.Printf("pebble-serve: /research self-eval pass %d failed: %v", pass, eerr)
			sse.phase("self_eval", map[string]any{"pass": pass, "error": eerr.Error()})
			break
		}
		ev, ok := parseSelfEval(evalRaw)
		if !ok {
			sse.phase("self_eval", map[string]any{"pass": pass, "parse_failed": true})
			break
		}
		sse.phase("self_eval", map[string]any{
			"pass": pass, "sufficient": ev.Sufficient,
			"missing": ev.Missing, "refine_queries": ev.RefineQueries,
		})
		if ev.Sufficient {
			break
		}
		// Blurb summarizing why we're escalating; UI renders inline.
		blurbText := strings.TrimSpace(ev.Missing)
		if blurbText == "" {
			blurbText = "More depth needed"
		}
		blurbText += " — searching deeper."
		sse.phase("blurb", map[string]any{"pass": pass, "text": blurbText})

		// Drive the next pass with the refined queries the model proposed.
		if len(ev.RefineQueries) == 0 {
			break
		}
		subs = ev.RefineQueries
		if len(subs) > 5 {
			subs = subs[:5]
		}
	}

	sse.done()
}

// summarizeSourceList formats sources into compact "[id] title — url" lines
// for the self-eval user message. Avoids piping the full excerpts back into
// the eval prompt — the eval is about coverage, not re-reading.
func summarizeSourceList(srcs []answerSource) string {
	var sb strings.Builder
	for _, s := range srcs {
		fmt.Fprintf(&sb, "[%d] %s — %s\n", s.ID, s.Title, s.URL)
	}
	return sb.String()
}
