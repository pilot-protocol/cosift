package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/qexpand"
	"github.com/pilot-protocol/cosift/internal/rerank"
	"github.com/pilot-protocol/cosift/internal/store"
)

// forwardURLToPeer POSTs a URL to peer's /admin/crawl-enqueue. Best-effort:
// caller logs failures and the URL is dropped — no retry or persistent queue.
var forwardHTTP = &http.Client{Timeout: 10 * time.Second}

func (s *pebbleHTTP) forwardURLToPeer(ctx context.Context, rawURL, peerAddr string) error {
	body, _ := json.Marshal(crawlEnqueueReq{URL: rawURL})
	// peerAddr is host:port; assume http inside the cluster (mTLS / VPN
	// would be a wrapper concern). Switch to https://... if peers expose TLS.
	endpoint := "http://" + peerAddr + "/admin/crawl-enqueue"
	if strings.HasPrefix(peerAddr, "http://") || strings.HasPrefix(peerAddr, "https://") {
		endpoint = strings.TrimRight(peerAddr, "/") + "/admin/crawl-enqueue"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if t := s.cluster.PeerAuthToken; t != "" {
		req.Header.Set("Authorization", "Bearer "+t)
	}
	resp, err := forwardHTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("peer %s returned %d: %s", peerAddr, resp.StatusCode, b)
	}
	return nil
}

// scatterSearch fans /search out to every peer with the given params,
// collects per-peer hit lists, RRF-merges them, and returns the merged
// top-k. Used by /search, /answer, /research gateway modes.
// includeText forces text inlining on the scatter so callers that need
// passages (synth endpoints) get them in one round trip.
func (s *pebbleHTTP) scatterSearch(ctx context.Context, q string, k int, perPeerK int, params url.Values, includeText bool) (hits []searchHit, warns []string, totalCandidates int) {
	srcQ := url.Values{}
	for kk, vv := range params {
		srcQ[kk] = vv
	}
	srcQ.Set("q", q)
	srcQ.Set("k", strconv.Itoa(perPeerK))
	srcQ.Set("cluster_local", "1")
	if includeText {
		srcQ.Set("include_text", "true")
	}
	queryStr := srcQ.Encode()

	type peerResult struct {
		peerIdx int
		hits    []searchHit
		err     error
	}
	resCh := make(chan peerResult, len(s.cluster.Peers))
	httpClient := &http.Client{Timeout: 15 * time.Second}
	for i, peer := range s.cluster.Peers {
		if peer == "" {
			continue
		}
		i, peer := i, peer
		go func() {
			endpoint := "http://" + peer + "/search?" + queryStr
			if strings.HasPrefix(peer, "http://") || strings.HasPrefix(peer, "https://") {
				endpoint = strings.TrimRight(peer, "/") + "/search?" + queryStr
			}
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
			if t := s.cluster.PeerAuthToken; t != "" {
				req.Header.Set("Authorization", "Bearer "+t)
			}
			resp, err := httpClient.Do(req)
			if err != nil {
				resCh <- peerResult{peerIdx: i, err: err}
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode >= 300 {
				resCh <- peerResult{peerIdx: i, err: fmt.Errorf("peer %s returned %d", peer, resp.StatusCode)}
				return
			}
			var sr struct {
				Hits []searchHit `json:"hits"`
			}
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
			if err := json.Unmarshal(body, &sr); err != nil {
				resCh <- peerResult{peerIdx: i, err: err}
				return
			}
			resCh <- peerResult{peerIdx: i, hits: sr.Hits}
		}()
	}
	lists := [][]index.Hit{}
	hitFull := map[string]searchHit{}
gather:
	for i := 0; i < len(s.cluster.Peers); i++ {
		if s.cluster.Peers[i] == "" {
			continue
		}
		select {
		case res := <-resCh:
			if res.err != nil {
				warns = append(warns, fmt.Sprintf("peer %d (%s) failed: %v", res.peerIdx, s.cluster.Peers[res.peerIdx], res.err))
				continue
			}
			totalCandidates += len(res.hits)
			peerList := make([]index.Hit, len(res.hits))
			for j, h := range res.hits {
				peerList[j] = index.Hit{URL: h.URL, Title: h.Title, Score: h.Score}
				if _, ok := hitFull[h.URL]; !ok {
					hitFull[h.URL] = h
				}
			}
			lists = append(lists, peerList)
		case <-ctx.Done():
			warns = append(warns, "client context cancelled before all peers responded")
			break gather
		}
	}
	fused := rrfFuse(lists)
	if len(fused) > k {
		fused = fused[:k]
	}
	hits = make([]searchHit, 0, len(fused))
	for _, f := range fused {
		full, ok := hitFull[f.URL]
		if !ok {
			full = searchHit{URL: f.URL, Title: f.Title}
		}
		full.Score = f.Score
		hits = append(hits, full)
	}
	return hits, warns, totalCandidates
}

// handleSearchGateway is the scatter-gather entry. It fans out the
// search to every peer's /search (including its own shard via the peers[]
// table), each peer over-fetches k*2 candidates locally, gateway RRF-merges
// the per-peer lists, and returns the top-k. Slow / failing peers are
// included in a warnings[] entry but don't block the response.
func (s *pebbleHTTP) handleSearchGateway(w http.ResponseWriter, r *http.Request) {
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
	perPeerK := k * 2
	if perPeerK < 20 {
		perPeerK = 20
	}
	// delegated to shared scatterSearch helper.
	hits, warns, total := s.scatterSearch(r.Context(), q, k, perPeerK, r.URL.Query(), r.URL.Query().Get("include_text") == "true")
	resp := searchResponse{
		Query:           q,
		Retriever:       fmt.Sprintf("gateway:rrf(%d-shard)", numNonEmpty(s.cluster.Peers)-len(warns)),
		Hits:            hits,
		TotalCandidates: total,
		Warnings:        append(warns, s.warningsFor(r)...),
		Took:            time.Since(start).String(),
	}
	writeJSON(w, http.StatusOK, resp)
}

// numNonEmpty counts non-empty entries in a peer list (peers[i]="" means
// "skip", typically MyShardID).
func numNonEmpty(ps []string) int {
	n := 0
	for _, p := range ps {
		if p != "" {
			n++
		}
	}
	return n
}

// handleFindSimilarGateway scatters /find_similar to every shard. The
// owning shard (URL-mode) or every shard (text-mode) produces real
// neighbors; others return empty. Gateway RRF-merges.
func (s *pebbleHTTP) handleFindSimilarGateway(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	k := 10
	if v := r.URL.Query().Get("k"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 100 {
			k = n
		}
	}
	perPeerK := k * 2
	if perPeerK < 20 {
		perPeerK = 20
	}
	srcQ := url.Values{}
	for kk, vv := range r.URL.Query() {
		srcQ[kk] = vv
	}
	srcQ.Set("k", strconv.Itoa(perPeerK))
	srcQ.Set("cluster_local", "1")
	queryStr := srcQ.Encode()

	type peerResult struct {
		peerIdx int
		hits    []searchHit
		err     error
	}
	resCh := make(chan peerResult, len(s.cluster.Peers))
	httpClient := &http.Client{Timeout: 15 * time.Second}
	for i, peer := range s.cluster.Peers {
		if peer == "" {
			continue
		}
		i, peer := i, peer
		go func() {
			endpoint := "http://" + peer + "/find_similar?" + queryStr
			if strings.HasPrefix(peer, "http://") || strings.HasPrefix(peer, "https://") {
				endpoint = strings.TrimRight(peer, "/") + "/find_similar?" + queryStr
			}
			req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, endpoint, http.NoBody)
			if t := s.cluster.PeerAuthToken; t != "" {
				req.Header.Set("Authorization", "Bearer "+t)
			}
			resp, err := httpClient.Do(req)
			if err != nil {
				resCh <- peerResult{peerIdx: i, err: err}
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode >= 300 {
				resCh <- peerResult{peerIdx: i, err: fmt.Errorf("peer %s returned %d", peer, resp.StatusCode)}
				return
			}
			var sr struct {
				Hits []searchHit `json:"hits"`
			}
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
			if err := json.Unmarshal(body, &sr); err != nil {
				resCh <- peerResult{peerIdx: i, err: err}
				return
			}
			resCh <- peerResult{peerIdx: i, hits: sr.Hits}
		}()
	}
	lists := [][]index.Hit{}
	hitFull := map[string]searchHit{}
	warns := []string{}
	total := 0
gather:
	for i := 0; i < len(s.cluster.Peers); i++ {
		if s.cluster.Peers[i] == "" {
			continue
		}
		select {
		case res := <-resCh:
			if res.err != nil {
				warns = append(warns, fmt.Sprintf("peer %d (%s) failed: %v", res.peerIdx, s.cluster.Peers[res.peerIdx], res.err))
				continue
			}
			total += len(res.hits)
			peerList := make([]index.Hit, len(res.hits))
			for j, h := range res.hits {
				peerList[j] = index.Hit{URL: h.URL, Title: h.Title, Score: h.Score}
				if _, ok := hitFull[h.URL]; !ok {
					hitFull[h.URL] = h
				}
			}
			lists = append(lists, peerList)
		case <-r.Context().Done():
			warns = append(warns, "client context cancelled")
			break gather
		}
	}
	fused := rrfFuse(lists)
	if len(fused) > k {
		fused = fused[:k]
	}
	out := make([]searchHit, 0, len(fused))
	for _, f := range fused {
		full, ok := hitFull[f.URL]
		if !ok {
			full = searchHit{URL: f.URL, Title: f.Title}
		}
		full.Score = f.Score
		out = append(out, full)
	}
	writeJSON(w, http.StatusOK, searchResponse{
		Query:           r.URL.Query().Get("url"),
		Retriever:       fmt.Sprintf("gateway:find_similar:rrf(%d-shard)", numNonEmpty(s.cluster.Peers)-len(warns)),
		Hits:            out,
		TotalCandidates: total,
		Warnings:        append(warns, s.warningsFor(r)...),
		Took:            time.Since(start).String(),
	})
}

// handleResearchGateway runs the planner on the gateway, scatters
// each sub-query to peers (collecting RRF-merged hits with text), then
// runs a single research synth.
func (s *pebbleHTTP) handleResearchGateway(w http.ResponseWriter, r *http.Request) {
	if s.chat == nil {
		writeProblem(w, http.StatusNotImplemented, "/research requires cfg.Chat.Model on the gateway")
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
	// 1. Plan via chat.
	planRaw, err := s.doChat(r.Context(), s.chat, []embed.ChatMsg{
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
	// 2. Scatter per sub-query. Dedup by URL, keep best fused score.
	type ranked struct {
		score float64
		hit   searchHit
	}
	best := make(map[string]ranked, k*len(subs))
	allWarns := []string{}
	perSub := k * 2
	for _, sq := range subs {
		hits, warns, _ := s.scatterSearch(r.Context(), sq, perSub, perSub, r.URL.Query(), true)
		allWarns = append(allWarns, warns...)
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
	if len(pooled) > k {
		pooled = pooled[:k]
	}
	// 3. Build prompt.
	sources := make([]answerSource, 0, len(pooled))
	var promptSources strings.Builder
	for i, p := range pooled {
		src := answerSource{ID: i + 1, URL: p.hit.URL, Title: p.hit.Title, Excerpt: p.hit.Excerpt, Author: p.hit.Author, PublishedAt: p.hit.PublishedAt}
		text := p.hit.Text
		if text == "" {
			text = p.hit.Excerpt
		}
		sources = append(sources, src)
		fmt.Fprintf(&promptSources, "[%d] %s\n%s\n%s\n\n", i+1, p.hit.Title, p.hit.URL, text)
	}
	if len(sources) == 0 {
		writeJSON(w, http.StatusOK, researchResponse{
			Query: q, Plan: subs, Answer: "No matching sources across the cluster.",
			Sources: sources, Model: s.chat.Model(),
			Warnings: allWarns, Took: time.Since(start).String(),
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
		Query: q, Plan: subs, Answer: answer, Sources: sources,
		Model:     s.chat.Model(),
		Retriever: fmt.Sprintf("gateway:rrf(%d-shard,%d-sub)", numNonEmpty(s.cluster.Peers), len(subs)),
		Warnings:  allWarns, Took: time.Since(start).String(),
	})
}

// handleAnswerGateway scatters /search across peers with include_text, runs
// a SINGLE chat synth on the gateway.
func (s *pebbleHTTP) handleAnswerGateway(w http.ResponseWriter, r *http.Request) {
	if s.chat == nil {
		writeProblem(w, http.StatusNotImplemented, "/answer requires cfg.Chat.Model on the gateway")
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
	hits, warns, total := s.scatterSearch(r.Context(), q, k, k*2, r.URL.Query(), true)
	if len(hits) == 0 {
		writeJSON(w, http.StatusOK, answerResponse{
			Query: q, Answer: "No matching sources across the cluster.",
			Sources: []answerSource{}, Model: s.chat.Model(),
			TotalCandidates: total, Warnings: warns, Took: time.Since(start).String(),
		})
		return
	}
	sources := make([]answerSource, 0, len(hits))
	var promptSources strings.Builder
	for i, h := range hits {
		src := answerSource{ID: i + 1, URL: h.URL, Title: h.Title, Excerpt: h.Excerpt, Author: h.Author, PublishedAt: h.PublishedAt}
		text := h.Text
		if text == "" {
			text = h.Excerpt
		}
		sources = append(sources, src)
		fmt.Fprintf(&promptSources, "[%d] %s\n%s\n%s\n\n", i+1, h.Title, h.URL, text)
	}
	answer, err := s.doChat(r.Context(), s.chat, []embed.ChatMsg{
		{Role: "system", Content: answerSystemPrompt},
		{Role: "user", Content: "Sources:\n\n" + promptSources.String() + "Question: " + q},
	})
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "synth: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, answerResponse{
		Query: q, Answer: answer, Sources: sources, Model: s.chat.Model(),
		Retriever:       fmt.Sprintf("gateway:rrf(%d-shard)", numNonEmpty(s.cluster.Peers)-len(warns)),
		TotalCandidates: total, Warnings: warns, Took: time.Since(start).String(),
	})
}

// searchHit is the minimal hit shape returned by pebble-serve's /search.
// Intentionally narrower than the SQLite server's SearchHit — feature
// parity (highlight, excerpt, calibration, paragraph filters) grows as
// follow-up iters port each one through the Pebble side.
type searchHit struct {
	URL         string     `json:"url"`
	Title       string     `json:"title"`
	Score       float64    `json:"score"`
	Excerpt     string     `json:"excerpt,omitempty"`
	PublishedAt *time.Time `json:"published_at,omitempty"`
	Author      string     `json:"author,omitempty"`
	Text        string     `json:"text,omitempty"`
}

type searchResponse struct {
	Query           string      `json:"query"`
	EffectiveQuery  string      `json:"effective_query,omitempty"`
	Expand          string      `json:"expand,omitempty"`
	Retriever       string      `json:"retriever"`
	Hits            []searchHit `json:"hits"`
	TotalCandidates int         `json:"total_candidates,omitempty"`
	Warnings        []string    `json:"warnings,omitempty"`
	Took            string      `json:"took"`
}

// POST /search with JSON body — for callers whose query lists,
// quoted phrases, or filter CSVs are awkward to URL-encode. Decodes into a
// searchRequest, re-encodes as r.URL.RawQuery, then calls handleSearch so
// every param the GET form supports works identically here. The hand-off
// shape (URL.Values) is the cosift contract — easier to keep one parser
// than to fork it across method handlers.
type searchRequest struct {
	Q              string `json:"q"`
	K              int    `json:"k,omitempty"`
	IncludeDomains string `json:"include_domains,omitempty"`
	ExcludeDomains string `json:"exclude_domains,omitempty"`
	Site           string `json:"site,omitempty"`
	Since          string `json:"since,omitempty"`
	Until          string `json:"until,omitempty"`
	Sort           string `json:"sort,omitempty"`
	Enrich         *bool  `json:"enrich,omitempty"`
	IncludeText    bool   `json:"include_text,omitempty"`
	Rerank         bool   `json:"rerank,omitempty"`
	Expand         string `json:"expand,omitempty"`
}

func (s *pebbleHTTP) handleSearchPOST(w http.ResponseWriter, r *http.Request) {
	var req searchRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
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
	if req.Sort != "" {
		v.Set("sort", req.Sort)
	}
	if req.Enrich != nil && !*req.Enrich {
		v.Set("enrich", "false")
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
	r.URL.RawQuery = v.Encode()
	s.handleSearch(w, r)
}

type findSimilarRequest struct {
	URL            string `json:"url,omitempty"`
	Text           string `json:"text,omitempty"`  // content-based MLT, no source URL needed
	Title          string `json:"title,omitempty"` // optional title-boost when using text mode
	K              int    `json:"k,omitempty"`
	Q              string `json:"q,omitempty"`
	IncludeDomains string `json:"include_domains,omitempty"`
	ExcludeDomains string `json:"exclude_domains,omitempty"`
	Since          string `json:"since,omitempty"`
	Until          string `json:"until,omitempty"`
	IncludeText    bool   `json:"include_text,omitempty"`
	Rerank         bool   `json:"rerank,omitempty"`
}

func (s *pebbleHTTP) handleFindSimilarPOST(w http.ResponseWriter, r *http.Request) {
	var req findSimilarRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	v := url.Values{}
	if req.URL != "" {
		v.Set("url", req.URL)
	}
	if req.Text != "" {
		v.Set("text", req.Text)
	}
	if req.Title != "" {
		v.Set("title", req.Title)
	}
	if req.K > 0 {
		v.Set("k", strconv.Itoa(req.K))
	}
	if req.Q != "" {
		v.Set("q", req.Q)
	}
	if req.IncludeDomains != "" {
		v.Set("include_domains", req.IncludeDomains)
	}
	if req.ExcludeDomains != "" {
		v.Set("exclude_domains", req.ExcludeDomains)
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
	r.URL.RawQuery = v.Encode()
	s.handleFindSimilar(w, r)
}

func (s *pebbleHTTP) handleSearch(w http.ResponseWriter, r *http.Request) {
	// When this process is in cluster
	// gateway-mode AND the caller hasn't already set ?cluster_local=1
	// (which is how the gateway tells peers "just do your local search,
	// don't fan out again"), fan out to peers and RRF-merge their results.
	if s.cluster.GatewayMode && s.cluster.IsClustered() && r.URL.Query().Get("cluster_local") != "1" {
		s.handleSearchGateway(w, r)
		return
	}
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
	// include_domains / exclude_domains — mirrors the
	// SQLite-side server semantics (CSV, dot-boundary suffix match).
	// since / until — ISO-date filters on doc.PublishedAt.
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
	// site — scope results to one or more host[/path] sections, e.g.
	// ?site=pilotprotocol.network/docs. Host-suffix + path-prefix match,
	// ANDed with include/exclude. Applied post-retrieval like the domain
	// filters, so it widens the over-fetch below.
	sites := parseSiteScopes(r.URL.Query().Get("site"))
	siteFilter := len(sites) > 0
	// rerank widens both the fetch and the keep-cap before filtering,
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
	if len(include) > 0 || len(exclude) > 0 || dateFilter || siteFilter {
		mult := 5
		if dateFilter {
			mult = 10
		}
		if siteFilter {
			// site= uses a fixed pool regardless of k — fetching proportionally
			// (k*100) hits the 2000-cap and spends 20s+ on BM25 for common terms.
			// 300 candidates is enough to include a small site's relevant pages
			// while keeping latency under ~3s even for high-frequency query terms.
			fetchK = 300
		} else {
			fetchK = keepCap * mult
			if fetchK > 500 {
				fetchK = 500
			}
		}
	} else if wantRerank {
		fetchK = keepCap * 2
	}
	// expansion dispatch (bare / HyDE / paraphrase+RRF).
	// Shared
	// helper used by /answer + /research so all three get the same matrix.
	// Reranker still scores against the original q regardless of strategy.
	expandMode := r.URL.Query().Get("expand")
	retrieverParam := r.URL.Query().Get("retriever")
	denseReady := s.hnsw() != nil && s.embedder != nil
	hits, effectiveQuery, err := s.retrieve(r.Context(), q, fetchK, retrieverParam, expandMode)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	// enrich each surviving hit with Excerpt + PublishedAt + Author
	// via a single GetDocByURL per hit. Cost: k extra Gets — block-cache hot,
	// ~ms-scale at the k≤100 we accept. Opt out with ?enrich=false for callers
	// that only need scoring. The date filter forces a doc fetch
	// even when enrich=false, since PublishedAt lives on the gob.
	// ?include_text=true inlines doc.Text on each hit so research
	// pipelines avoid the N+1 round trip to /contents. Off by default —
	// payload size grows linearly with k and average doc length.
	enrich := r.URL.Query().Get("enrich") != "false"
	if wantRerank {
		enrich = true // rerank needs per-doc text — overrides enrich opt-out
	}
	// time-decay multiplier needs PublishedAt per hit, which lives
	// behind the enrich flag. Force enrich when decay is requested so the
	// signal is available downstream.
	decayHalfLife, decaySet := parseDecayHalfLife(r.URL.Query().Get("decay"))
	if decaySet {
		enrich = true
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
		if siteFilter && !matchesAnySite(h.URL, sites) {
			continue
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
	// Runs BEFORE rerank so the rerank pool
	// reflects both quality and recency — when fetchK is over-fetched, the
	// reranker still sees the freshest-AND-most-relevant top-N. Hits without
	// PublishedAt are left alone (no signal, no penalty). The pool re-sort
	// re-aligns rerankTexts via a URL→text map so both knobs compose.
	if decaySet {
		var textByURL map[string]string
		if wantRerank && len(rerankTexts) == len(out) {
			textByURL = make(map[string]string, len(out))
			for i, h := range out {
				textByURL[h.URL] = rerankTexts[i]
			}
		}
		applyTimeDecay(out, decayHalfLife, time.Now())
		if textByURL != nil {
			rerankTexts = rerankTexts[:0]
			for _, h := range out {
				rerankTexts = append(rerankTexts, textByURL[h.URL])
			}
		}
	}
	// rerank now that we have keepCap candidates with text.
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
	// MMR diversification. ?mmr=<lambda> (0..1) reorders the
	// candidate pool to balance query relevance against diversity from
	// previously-selected hits. Requires HNSW (per-hit vectors) and an
	// embedder when the query vector wasn't already computed by the
	// dense/hybrid path. Composes after rerank — rerank gives quality
	// ordering, MMR diversifies it. warningsFor() handles the silent
	// fall-through when MMR was requested but couldn't fire.
	if mmrLambda, mmrSet := parseMMRLambda(r.URL.Query().Get("mmr")); mmrSet && len(out) > 1 && s.hnsw() != nil {
		var qVec []float32
		// Hybrid/dense path already embedded the query — try to reuse it.
		// For BM25-only path we need to embed q if an embedder is configured.
		if s.embedder != nil {
			if vecs, embErr := s.embedder.Embed(r.Context(), []string{q}); embErr == nil && len(vecs) > 0 {
				qVec = vecs[0]
			}
		}
		if len(qVec) > 0 {
			hitVecs := make([][]float32, len(out))
			for i := range out {
				if v, ok := s.hnsw().LookupVectorByURL(out[i].URL); ok {
					hitVecs[i] = v
				}
			}
			out = mmrSelect(qVec, out, hitVecs, mmrLambda)
		}
	}
	if len(out) > k {
		out = out[:k]
	}
	// Applies to
	// the already-collected top-k pool; raise k to widen the pool before
	// re-sorting if you need more candidates for date ordering.
	switch r.URL.Query().Get("sort") {
	case "date_desc":
		sortHitsByDate(out, false)
	case "date_asc":
		sortHitsByDate(out, true)
	}
	// label centralized in buildRetrieverLabel so /answer
	// and /research report the same vocabulary as /search.
	retrieverLabel := s.buildRetrieverLabel(retrieverParam, expandMode, denseReady, effectiveQuery != q, wantRerank)
	// mmr suffix when diversification actually fired (HNSW loaded
	// + embedder available). Same conditional as the apply site above —
	// the label tracks the real pipeline.
	if mmrLambda, mmrSet := parseMMRLambda(r.URL.Query().Get("mmr")); mmrSet && s.hnsw() != nil && s.embedder != nil && mmrLambda < 1.0 {
		retrieverLabel += fmt.Sprintf("+mmr:%.2f", mmrLambda)
	}
	// decay suffix when time-decay was applied.
	if decaySet {
		retrieverLabel += fmt.Sprintf("+decay:%gd", decayHalfLife)
	}
	resp := searchResponse{
		Query:     q,
		Expand:    normalizeExpandMode(expandMode),
		Retriever: retrieverLabel,
		Hits:      out,
		// total_candidates = BM25 candidates considered before
		// filter (capped at fetchK). Operators tuning over-fetch can see
		// whether their filter is dropping a lot — when out=k but
		// total_candidates is close to fetchK, the filter is restrictive
		// enough that you may want to raise k or relax filters.
		TotalCandidates: len(hits),
		Took:            time.Since(start).String(),
	}
	// surface the post-HyDE query when it actually changed, so callers
	// can debug whether expand=true contributed any extra terms or returned q
	// unchanged (chat down, empty passage, etc).
	if effectiveQuery != q {
		resp.EffectiveQuery = effectiveQuery
	}
	resp.Warnings = s.warningsFor(r)
	writeJSON(w, http.StatusOK, resp)
}

// sortHitsByDate orders hits by PublishedAt. Hits with no PublishedAt sink to
// the end regardless of direction — they have no comparable value. asc=true
// gives oldest-first; asc=false gives newest-first.
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

// BM25-only "more like this". Mirrors EXA's /findSimilar shape but
// stays dependency-free: no embeddings required. Algorithm is Lucene's MLT —
// pick the source doc's top-N terms by tf·idf, build a query from them, run
// the existing BM25 search, drop the source URL itself. With dense vectors
// off the table on the Pebble path (HNSW indexing during crawl is the iter
// follow-up), this is the cheapest credible /find_similar we can ship.
func (s *pebbleHTTP) handleFindSimilar(w http.ResponseWriter, r *http.Request) {
	// /find_similar runs locally with a
	// known URL+vec, but in a cluster the URL's shard owns its vector and
	// only one shard has it. We fan-out to all shards anyway: the owning
	// shard does the real work; others either fall back to text-mode (if
	// text supplied) or return empty.
	if s.cluster.GatewayMode && s.cluster.IsClustered() && r.URL.Query().Get("cluster_local") != "1" {
		s.handleFindSimilarGateway(w, r)
		return
	}
	start := time.Now()
	// accept either ?url= (existing behavior) or ?text= (content-
	// based similarity for unindexed drafts). text path skips the source-URL
	// exclusion since there's no source URL to exclude.
	rawURL := r.URL.Query().Get("url")
	rawText := r.URL.Query().Get("text")
	if rawURL == "" && rawText == "" {
		writeProblem(w, http.StatusBadRequest, "missing url or text parameter")
		return
	}
	var srcTitle, srcText, srcURL string
	if rawURL != "" {
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
		srcTitle, srcText, srcURL = src.Title, src.Text, src.URL
	} else {
		srcText = rawText
		srcTitle = r.URL.Query().Get("title") // optional title boost when text mode
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
	for _, t := range index.Tokenize(srcTitle) {
		tf[t] += 3
	}
	for _, t := range index.Tokenize(srcText) {
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
		// previously referenced `decoded` (only in scope in the
		// url-mode branch) — broken since's text-mode addition.
		// Use srcURL when present, else echo the bare text/title as the
		// source identifier so the empty response still tells the caller
		// what was searched.
		emptyQuery := srcURL
		if emptyQuery == "" {
			emptyQuery = srcTitle
			if emptyQuery == "" {
				emptyQuery = srcText
			}
		}
		writeJSON(w, http.StatusOK, searchResponse{
			Query: emptyQuery, Retriever: "bm25-mlt",
			Hits: []searchHit{}, Took: time.Since(start).String(),
		})
		return
	}
	terms := make([]string, topN)
	for i := 0; i < topN; i++ {
		terms[i] = scored[i].term
	}
	queryStr := strings.Join(terms, " ")
	// optional ?q= augments the auto-derived MLT query so callers
	// can constrain "more like this URL" with an extra concept (e.g.
	// /find_similar?url=...&q=pricing). Appended verbatim — supports the
	// same quoted-phrase shape /search accepts.
	if extra := strings.TrimSpace(r.URL.Query().Get("q")); extra != "" {
		queryStr = queryStr + " " + extra
	}

	// scope MLT with the same retrieval filters /search and /answer
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
	// ?rerank=true closes /find_similar's parity gap with the other
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

	// /find_similar?retriever=dense reuses the source's
	// persisted vector (URL-mode) or embeds the user's text (text-mode) and
	// runs an HNSW cosine search instead of BM25-MLT. ?retriever=hybrid
	// runs BOTH BM25-MLT and dense, then RRF-fuses — the
	// strongest "find similar" signal: lexical precision + semantic recall.
	//
	// Requires COSIFT_LOAD_HNSW=true at server start (for the graph);
	// text-mode additionally needs a configured embedder. URL-mode dense
	// works without an embedder — the source vector is already in the graph
	// from indexing. Missing requirements fall through to BM25-MLT;
	// warningsFor() flags it (carves out the URL-mode-no-embedder
	// case so the warning isn't misleading).
	retrieverParam := r.URL.Query().Get("retriever")
	useDense := retrieverParam == "dense" && s.hnsw() != nil
	useHybrid := retrieverParam == "hybrid" && s.hnsw() != nil
	var (
		hits       []index.Hit
		denseFired bool
		bm25Fired  bool
	)
	// Helper: fetch the query vector (URL-mode lookup, falling back to
	// text-mode embed when allowed). Returns ok=false when neither path
	// produced a usable vector.
	getQueryVec := func() ([]float32, bool) {
		if srcURL != "" {
			if v, ok := s.hnsw().LookupVectorByURL(srcURL); ok {
				return v, true
			}
		}
		if s.embedder != nil && (srcText != "" || srcTitle != "") {
			seed := strings.TrimSpace(srcTitle + " " + srcText)
			if seed != "" {
				vecs, embErr := s.embedder.Embed(r.Context(), []string{seed})
				if embErr == nil && len(vecs) > 0 {
					return vecs[0], true
				}
			}
		}
		return nil, false
	}
	if useDense {
		if queryVec, ok := getQueryVec(); ok {
			vhits := s.hnsw().Search(r.Context(), queryVec, fetchK)
			hits = make([]index.Hit, len(vhits))
			for i, vh := range vhits {
				hits[i] = index.Hit{URL: vh.URL, Title: vh.Title, Score: vh.Score}
			}
			denseFired = true
		}
	} else if useHybrid {
		// Run BM25-MLT and dense in series; RRF-fuse the two ranked lists.
		// Each retriever votes fetchK candidates so the fused top-fetchK has
		// room to balance both signals before filter/enrich/rerank.
		bm25Hits, bm25Err := s.idx.Search(r.Context(), queryStr, fetchK)
		if bm25Err != nil {
			writeProblem(w, http.StatusInternalServerError, bm25Err.Error())
			return
		}
		bm25Fired = true
		if queryVec, ok := getQueryVec(); ok {
			vhits := s.hnsw().Search(r.Context(), queryVec, fetchK)
			denseHits := s.applyAuthorityToDense(vhits)
			hits = rrfFuse([][]index.Hit{bm25Hits, denseHits})
			if len(hits) > fetchK {
				hits = hits[:fetchK]
			}
			denseFired = true
		} else {
			// Hybrid fell through to BM25-only (e.g. text-mode + no embedder).
			// Keep the BM25 hits we already paid for.
			hits = bm25Hits
		}
	}
	if !denseFired && !bm25Fired {
		var err error
		hits, err = s.idx.Search(r.Context(), queryStr, fetchK)
		if err != nil {
			writeProblem(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	type fsCand struct {
		hit        searchHit
		rerankText string
	}
	cands := make([]fsCand, 0, keepCap)
	for _, h := range hits {
		if srcURL != "" && h.URL == srcURL {
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
	// Same pre-rerank placement as
	// /search — re-sort cands by adjusted score so rerank sees a freshness-
	// aware pool. Re-aligns rerankText too via URL→text map.
	decayHalfLife, decaySet := parseDecayHalfLife(r.URL.Query().Get("decay"))
	if decaySet && len(cands) > 0 {
		now := time.Now()
		for i := range cands {
			cands[i].hit.Score *= decayMultiplier(cands[i].hit.PublishedAt, now, decayHalfLife)
		}
		sort.SliceStable(cands, func(i, j int) bool { return cands[i].hit.Score > cands[j].hit.Score })
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
	// MMR diversification on /find_similar. Anchored at the
	// source vector — URL-mode reuses the persisted vec, text-mode embeds
	// the seed via getQueryVec (defined above for dense/hybrid dispatch).
	// Without an explicit user query, the source IS the query — this is
	// the right anchor for "find similar but diverse".
	mmrLambda, mmrSet := parseMMRLambda(r.URL.Query().Get("mmr"))
	mmrFired := false
	if mmrSet && len(cands) > 1 && s.hnsw() != nil {
		if qVec, ok := getQueryVec(); ok {
			hitVecs := make([][]float32, len(cands))
			for i, c := range cands {
				if v, vok := s.hnsw().LookupVectorByURL(c.hit.URL); vok {
					hitVecs[i] = v
				}
			}
			order := mmrOrder(qVec, hitVecs, mmrLambda)
			if order != nil {
				reordered := make([]fsCand, len(order))
				for i, idx := range order {
					reordered[i] = cands[idx]
				}
				cands = reordered
				mmrFired = true
			}
		}
	}
	if len(cands) > k {
		cands = cands[:k]
	}
	out := make([]searchHit, 0, len(cands))
	for _, c := range cands {
		out = append(out, c.hit)
	}
	// label tracks which retrievers actually fired.
	//   bm25-mlt              — default, BM25 only
	//   dense                 — ?retriever=dense fired (graph + vec found)
	//   bm25-mlt+dense:rrf    — ?retriever=hybrid, both BM25 and dense fired
	// If dense was requested but fell through (no graph, no vec), the label
	// stays bm25-mlt and warningsFor() carries the reason.
	retrieverLabel := "bm25-mlt"
	switch {
	case bm25Fired && denseFired:
		retrieverLabel = "bm25-mlt+dense:rrf"
	case denseFired:
		retrieverLabel = "dense"
	}
	if wantRerank {
		retrieverLabel += "+rerank:" + s.reranker.Name()
	}
	if mmrFired {
		retrieverLabel += fmt.Sprintf("+mmr:%.2f", mmrLambda)
	}
	if decaySet {
		retrieverLabel += fmt.Sprintf("+decay:%gd", decayHalfLife)
	}
	writeJSON(w, http.StatusOK, searchResponse{
		Query:           queryStr,
		Retriever:       retrieverLabel,
		Hits:            out,
		TotalCandidates: len(hits),
		Warnings:        s.warningsFor(r),
		Took:            time.Since(start).String(),
	})
}

// doRerank wraps s.reranker.Rerank with attempt/failure counters.
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

// applyBM25EnvOverrides reads COSIFT_BM25_K1 / _B and applies them
// to idx. Shared by runPebbleServe and runQuery so both honor the same env.
// Returns which knobs landed so callers can log selectively.
type bm25EnvResult struct {
	k1Set, bSet bool
	k1Val, bVal float64
	k1Bad, bBad string // non-empty when env was set but unparseable
}

func applyBM25EnvOverrides(idx *index.PebbleBM25) bm25EnvResult {
	var out bm25EnvResult
	if v := os.Getenv("COSIFT_BM25_K1"); v != "" {
		if k1, err := strconv.ParseFloat(v, 64); err == nil && k1 > 0 {
			idx.WithBM25Params(k1, 0)
			out.k1Set, out.k1Val = true, k1
		} else {
			out.k1Bad = v
		}
	}
	if v := os.Getenv("COSIFT_BM25_B"); v != "" {
		if b, err := strconv.ParseFloat(v, 64); err == nil && b > 0 {
			idx.WithBM25Params(0, b)
			out.bSet, out.bVal = true, b
		} else {
			out.bBad = v
		}
	}
	return out
}

// paraphraseQuery returns up to n paraphrases of q via the chat
// client, parses the JSON array shape the SQLite-side paraphraser already
// uses, and falls back to nil on any failure (no chat client, empty / malformed
// reply, parse error). Caller decides what to do with an empty list — the
// downstream RRF strategy treats it as 'no expansion, fall back to single
// query'. No cache yet — keyed on (q, n) would need a different shape than
// the HyDE cache; revisit when workload justifies.
func (s *pebbleHTTP) paraphraseQuery(ctx context.Context, q string, n int) []string {
	if s.chat == nil || n <= 0 {
		return nil
	}
	s.paraMu.RLock()
	if cached, ok := s.paraCache[q]; ok {
		s.paraMu.RUnlock()
		s.paraHits.Add(1)
		return cached
	}
	s.paraMu.RUnlock()
	s.paraMisses.Add(1)
	const sys = `Generate paraphrases of a search query. Each paraphrase preserves the semantic intent but uses different vocabulary — different keywords that a target document might also use. Output ONLY a JSON array of strings.
Example output for "go programming language": ["golang concurrent compiled language", "Google's systems programming language with goroutines"]`
	resp, err := s.doChat(ctx, s.chat, []embed.ChatMsg{
		{Role: "system", Content: sys},
		{Role: "user", Content: fmt.Sprintf("Generate %d paraphrases of: %s", n, q)},
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
	startIdx := strings.Index(raw, "[")
	endIdx := strings.LastIndex(raw, "]")
	if startIdx < 0 || endIdx <= startIdx {
		return nil
	}
	var arr []string
	if err := json.Unmarshal([]byte(raw[startIdx:endIdx+1]), &arr); err != nil {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, p := range arr {
		t := strings.TrimSpace(p)
		if t != "" {
			out = append(out, t)
		}
	}
	if len(out) > 0 {
		s.paraMu.Lock()
		if len(s.paraCache) >= s.paraCacheCap {
			for k := range s.paraCache {
				delete(s.paraCache, k)
				break
			}
		}
		s.paraCache[q] = out
		s.paraMu.Unlock()
	}
	return out
}

// rrfFuse implements Reciprocal Rank Fusion across N ranked lists.
// k=60 is the standard Cormack et al. constant; tweaking rarely changes top-k
// ordering meaningfully. Each list contributes 1/(k + rank+1) to a URL's
// fused score; URLs appearing in more lists at higher ranks rise. Returns
// synthesized Hits ordered by fused score (URL/Title from first encounter,
// Score = fused RRF score).
func rrfFuse(lists [][]index.Hit) []index.Hit {
	const fuseK = 60
	type fused struct {
		hit   index.Hit
		score float64
	}
	scored := make(map[string]*fused, 64)
	for _, list := range lists {
		for rank, h := range list {
			if existing, ok := scored[h.URL]; ok {
				existing.score += 1.0 / float64(fuseK+rank+1)
			} else {
				scored[h.URL] = &fused{hit: h, score: 1.0 / float64(fuseK+rank+1)}
			}
		}
	}
	flat := make([]*fused, 0, len(scored))
	for _, f := range scored {
		flat = append(flat, f)
	}
	sort.Slice(flat, func(i, j int) bool { return flat[i].score > flat[j].score })
	out := make([]index.Hit, len(flat))
	for i, f := range flat {
		out[i] = f.hit
		out[i].Score = f.score
	}
	return out
}

// warningsFor surfaces silent no-ops that callers used to have to
// derive from absent effective_query / retriever fields. Each warning is one
// human-readable sentence; consumers programmatically inspect the slice.
func (s *pebbleHTTP) warningsFor(r *http.Request) []string {
	var w []string
	if mode := r.URL.Query().Get("expand"); mode != "" {
		if s.chat == nil {
			w = append(w, "expand="+mode+" requested but no chat client configured (set cfg.Chat.Model)")
		} else if normalizeExpandMode(mode) == "" {
			// unknown expand value used to be silently ignored.
			w = append(w, "expand="+mode+" is not a known strategy (try: hyde, paraphrase) — treated as no expansion")
		}
	}
	if r.URL.Query().Get("rerank") == "true" && s.reranker == nil {
		w = append(w, "rerank=true requested but no reranker configured (set cfg.Rerank.URL or cfg.Rerank.Enabled)")
	}
	// dense + hybrid retrievers need both a loaded HNSW graph
	// and an embedder. Missing either falls through to BM25 with a warning.
	// /find_similar?url=X URL-mode-dense reads the source vector
	// directly from the graph via LookupVectorByURL — no embed RPC. Skip the
	// embedder warning in that case (would otherwise be misleading: the
	// request succeeded without an embedder).
	// /find_similar falls back to bm25-mlt (not bm25), so the
	// warning says so — caller's retriever label matches the warning text.
	if rv := r.URL.Query().Get("retriever"); rv == "dense" || rv == "hybrid" {
		isFindSimilar := strings.HasSuffix(r.URL.Path, "/find_similar")
		isFindSimilarURLMode := isFindSimilar && r.URL.Query().Get("url") != ""
		fallback := "BM25"
		if isFindSimilar {
			fallback = "BM25-MLT"
		}
		switch {
		case s.hnsw() == nil:
			w = append(w, "retriever="+rv+" requested but HNSW graph not loaded (set COSIFT_LOAD_HNSW=true at server start) — fell back to "+fallback)
		case s.embedder == nil && !isFindSimilarURLMode:
			w = append(w, "retriever="+rv+" requested but no embedder configured (set cfg.Embeddings.Model) — fell back to "+fallback)
		}
	}
	// MMR diversification needs HNSW + embedder. Bad values (non-
	// float, out of [0,1]) fall through silently — flag them. Missing
	// requirements also warn.
	// /find_similar?url=X reuses the source vector from the graph
	// for the MMR anchor — no embedder needed. Same carve-out as
	// for the retriever warning.
	if raw := r.URL.Query().Get("mmr"); raw != "" {
		isFindSimilarURLMode := strings.HasSuffix(r.URL.Path, "/find_similar") && r.URL.Query().Get("url") != ""
		if _, ok := parseMMRLambda(raw); !ok {
			w = append(w, "mmr="+raw+" is not a float in [0,1] — diversification skipped")
		} else if s.hnsw() == nil {
			w = append(w, "mmr requires HNSW graph (set COSIFT_LOAD_HNSW=true at server start) — diversification skipped")
		} else if s.embedder == nil && !isFindSimilarURLMode {
			w = append(w, "mmr requires an embedder to vectorize the query (set cfg.Embeddings.Model) — diversification skipped")
		}
	}
	// invalid decay half-life flags loudly instead of silently
	// being ignored. Empty value is fine (no decay requested).
	if raw := r.URL.Query().Get("decay"); raw != "" {
		if _, ok := parseDecayHalfLife(raw); !ok {
			w = append(w, "decay="+raw+" is not a positive half-life in days (≤ 36500) — time-decay skipped")
		}
	}
	// catch unknown ?sort= values (silently treated as relevance).
	if sortVal := r.URL.Query().Get("sort"); sortVal != "" {
		switch sortVal {
		case "relevance", "date_desc", "date_asc":
			// valid
		default:
			w = append(w, "sort="+sortVal+" is not a known mode (try: relevance, date_desc, date_asc) — treated as relevance")
		}
	}
	// catch obviously-bad ?k= values that silently fell back to the
	// per-endpoint default. Upper-bound clamping varies per endpoint so we
	// flag only the universally-invalid cases (non-integer, zero, negative).
	if kVal := r.URL.Query().Get("k"); kVal != "" {
		if n, err := strconv.Atoi(kVal); err != nil || n <= 0 {
			w = append(w, "k="+kVal+" is not a positive integer — using server default")
		}
	}
	// Users sometimes
	// pass 'https://example.com/foo' when 'example.com' was wanted; the
	// dot-boundary matcher silently drops every result.
	for _, key := range []string{"include_domains", "exclude_domains"} {
		if v := r.URL.Query().Get(key); v != "" {
			for _, d := range strings.Split(v, ",") {
				d = strings.TrimSpace(d)
				if strings.Contains(d, "://") || strings.Contains(d, "/") {
					w = append(w, key+" entry "+d+" looks like a URL — pass a hostname (e.g., example.com), not a URL")
					break // one warning per filter list is enough
				}
			}
		}
	}
	if len(w) > 0 {
		s.warningsEmitted.Add(1)
	}
	return w
}

// normalizeExpandMode returns the canonical strategy name for an `expand`
// param value: "true" → "hyde" (alias), "hyde"/"paraphrase" pass through,
// anything else → "" (so the response field doesn't echo a value that had
// no effect).
func normalizeExpandMode(raw string) string {
	switch raw {
	case "true", "hyde":
		return "hyde"
	case "paraphrase":
		return "paraphrase"
	default:
		return ""
	}
}

// parseDecayHalfLife parses ?decay= as a positive half-life in days. Returns
// (halfLife, true) when valid. Empty / non-positive → (0, false) so caller
// short-circuits. Bounded at 36500 (100 years) to avoid pathological values.
func parseDecayHalfLife(raw string) (float64, bool) {
	if raw == "" {
		// Half-life from COSIFT_DEFAULT_DECAY_DAYS
		// (default 180 days = 6 months). Docs older than 6 months get exponentially
		// less weight in the score; recent docs surface naturally without keyword
		// hacks like the JS regex. Set the env to 0 to disable globally.
		// Explicit ?decay=N still wins.
		def := 180.0
		if v := os.Getenv("COSIFT_DEFAULT_DECAY_DAYS"); v != "" {
			if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 {
				def = f
			}
		}
		if def <= 0 {
			return 0, false
		}
		return def, true
	}
	// explicit decay=0 disables (even if env default is on).
	if raw == "0" {
		return 0, false
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}
	if math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 || v > 36500 {
		return 0, false
	}
	return v, true
}

// decayMultiplier computes the time-decay weight for a single PublishedAt.
// Half-life H: doc from H days ago → 0.5x, 2H → 0.25x. Missing date or
// non-positive halfLife → 1.0 (no decay).
func decayMultiplier(publishedAt *time.Time, now time.Time, halfLifeDays float64) float64 {
	if halfLifeDays <= 0 || publishedAt == nil || publishedAt.IsZero() {
		return 1
	}
	ageDays := now.Sub(*publishedAt).Hours() / 24.0
	if ageDays < 0 {
		ageDays = 0 // future-dated → treated as fresh, never boosted
	}
	return math.Exp(-math.Ln2 * ageDays / halfLifeDays)
}

// applyTimeDecay multiplies each hit's Score by decayMultiplier and resorts.
func applyTimeDecay(hits []searchHit, halfLifeDays float64, now time.Time) {
	if halfLifeDays <= 0 || len(hits) == 0 {
		return
	}
	for i := range hits {
		hits[i].Score *= decayMultiplier(hits[i].PublishedAt, now, halfLifeDays)
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].Score > hits[j].Score })
}

// parseMMRLambda parses ?mmr= as a float in [0, 1]. Returns (lambda, true)
// when valid. Empty value or out-of-range returns (0, false) so the caller
// can short-circuit without firing MMR.
func parseMMRLambda(raw string) (float64, bool) {
	if raw == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}
	if v < 0 || v > 1 {
		return 0, false
	}
	return v, true
}

// mmrOrder computes a permutation of [0..n-1] via Maximal Marginal Relevance
// (Carbonell & Goldstein '98). At each step picks the candidate maximizing
//
//	score = λ · sim(q, c) - (1-λ) · max_{s∈selected} sim(c, s)
//
// Candidates whose vectors are missing (zero-length) score 0 on relevance
// and 0 against any other missing-vec candidate — MMR can only diversify
// what it can compare. lambda=1 → pure relevance (no diversification);
// lambda=0 → pure diversity. 0.5–0.7 is a typical starting point.
//
// Returns the permutation as a []int; never drops anything. The caller
// applies it to whatever URL-keyed slice it has.
func mmrOrder(qVec []float32, hitVecs [][]float32, lambda float64) []int {
	n := len(hitVecs)
	if n == 0 {
		return nil
	}
	selected := make([]bool, n)
	order := make([]int, 0, n)
	rel := make([]float64, n)
	for i := range hitVecs {
		if len(hitVecs[i]) > 0 {
			rel[i] = float64(index.Dot(qVec, hitVecs[i]))
		}
	}
	for len(order) < n {
		bestI := -1
		bestScore := -math.MaxFloat64
		for i := 0; i < n; i++ {
			if selected[i] {
				continue
			}
			var maxSim float64
			for _, j := range order {
				if len(hitVecs[i]) == 0 || len(hitVecs[j]) == 0 {
					continue
				}
				sim := float64(index.Dot(hitVecs[i], hitVecs[j]))
				if sim > maxSim {
					maxSim = sim
				}
			}
			score := lambda*rel[i] - (1.0-lambda)*maxSim
			if score > bestScore {
				bestScore = score
				bestI = i
			}
		}
		if bestI < 0 {
			break
		}
		selected[bestI] = true
		order = append(order, bestI)
	}
	return order
}

// mmrSelect — thin /search wrapper around mmrOrder that operates on []searchHit
// directly.
func mmrSelect(qVec []float32, hits []searchHit, hitVecs [][]float32, lambda float64) []searchHit {
	if len(hits) <= 1 || lambda >= 1.0 {
		return hits
	}
	order := mmrOrder(qVec, hitVecs, lambda)
	out := make([]searchHit, 0, len(hits))
	for _, i := range order {
		out = append(out, hits[i])
	}
	return out
}

// applyMMRPermutation embeds q, looks up per-URL vectors from HNSW, and
// returns an MMR permutation. Returns nil when MMR can't fire (no graph,
// no embedder, embed failed, λ≥1, single-element pool). Shared by /answer
// and /research synth endpoints.
func (s *pebbleHTTP) applyMMRPermutation(ctx context.Context, urls []string, q string, lambda float64) []int {
	if s.hnsw() == nil || s.embedder == nil || len(urls) <= 1 || lambda >= 1.0 {
		return nil
	}
	vecs, err := s.embedder.Embed(ctx, []string{q})
	if err != nil || len(vecs) == 0 {
		return nil
	}
	hitVecs := make([][]float32, len(urls))
	for i, u := range urls {
		if v, ok := s.hnsw().LookupVectorByURL(u); ok {
			hitVecs[i] = v
		}
	}
	return mmrOrder(vecs[0], hitVecs, lambda)
}

// buildRetrieverLabel produces the human-readable retriever string
// surfaced on /search, /answer, /research responses. Mirrors the
// /search inline switch so all three endpoints report the same vocabulary.
// expansionFired is the post-hoc "did expand=hyde/paraphrase actually run"
// signal (effectiveQuery != q for /search & /answer; chat-present for
// /research where sub-queries are individual). wantRerank appends the
// "+rerank:<name>" suffix when rerank fired.
func (s *pebbleHTTP) buildRetrieverLabel(retrieverParam, expandMode string, denseReady, expansionFired, wantRerank bool) string {
	label := "bm25"
	switch {
	case retrieverParam == "dense" && denseReady:
		label = "dense"
	case retrieverParam == "hybrid" && denseReady:
		label = "bm25+dense:rrf"
	default:
		switch expandMode {
		case "paraphrase":
			if expansionFired {
				label = "bm25+paraphrase"
			}
		case "true", "hyde":
			if expansionFired {
				label = "bm25+hyde"
			}
		}
	}
	if wantRerank && s.reranker != nil {
		label += "+rerank:" + s.reranker.Name()
	}
	return label
}

// applyLLMEndpointDefaults sets the retriever-choice and rerank defaults
// the LLM-synthesized endpoints (/answer, /research) want. BM25 alone is
// too slow at multi-million-doc corpora for synchronous LLM use, and the
// reranker is the proven quality recovery layer — so /answer and /research
// default to hybrid + rerank whenever the components are wired, while
// /search keeps its plain-BM25 default for low-latency operator use.
//
// Operators retain full control: ?retriever=bm25|dense|hybrid and
// ?rerank=true|false|1|0 override either default.
func (s *pebbleHTTP) applyLLMEndpointDefaults(retrieverParam, rerankParam string) (string, bool) {
	denseReady := s.hnsw() != nil && s.embedder != nil
	if retrieverParam == "" && denseReady {
		retrieverParam = "hybrid"
	}
	wantRerank := s.reranker != nil
	switch rerankParam {
	case "false", "0":
		wantRerank = false
	case "true", "1":
		wantRerank = s.reranker != nil
	}
	// When the vLLM probe says the backend is queue-deep, silently
	// disable rerank. Operators can override by passing ?rerank=true
	// explicitly — but with COSIFT_LLM_DEGRADE_QUEUE breached, the
	// rerank call would just queue and time out anyway. Better to ship
	// hybrid scores fast than block on a doomed LLM call.
	if wantRerank && rerankParam == "" && s.llmProbe != nil && s.llmProbe.Loaded() {
		wantRerank = false
	}
	return retrieverParam, wantRerank
}

// retrieve dispatches retriever choice (bm25 / dense / hybrid) and
// then expansion (bare / HyDE / paraphrase+RRF) for BM25 paths. Shared by
// /search, /answer, /research so all three endpoints get the same retriever
// matrix. Dense / hybrid require both the loaded HNSW graph and an
// embedder; missing either falls through to BM25 — warningsFor()
// surfaces that to the client.
// applyAuthorityToDense converts HNSW VectorHits into index.Hits with
// the per-host authority multiplier applied to the score, AND re-sorts
// the result by the new score so the rank order reflects authority.
// RRF fusion downstream uses rank position, not raw score, so without
// the re-sort the multiplier was a no-op (observed: 'What is the C10K
// problem?' still cited 3 luanjiao.cfd subdomains after applying the
// multiplier alone). PebbleBM25.Search already multiplies AND sorts
// internally; this helper mirrors that contract for the dense path.
func (s *pebbleHTTP) applyAuthorityToDense(vhits []index.VectorHit) []index.Hit {
	out := make([]index.Hit, len(vhits))
	for i, vh := range vhits {
		score := vh.Score
		if s.authority != nil {
			score *= s.authority.Multiplier(hostFromURL(vh.URL))
		}
		out[i] = index.Hit{URL: vh.URL, Title: vh.Title, Score: score}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	return out
}

func (s *pebbleHTTP) retrieve(ctx context.Context, q string, fetchK int, retrieverParam, expandMode string) ([]index.Hit, string, error) {
	return s.retrieveWith(ctx, s.idx, q, fetchK, retrieverParam, expandMode)
}

// retrieveWith is retrieve parameterized over the BM25 index, so callers that
// need a transformed index (e.g. the site-boosted index from WithBoost) can
// pass it in without shallow-copying the whole pebbleHTTP (which embeds
// mutexes — copying it trips go vet's lock-copy check and is unsafe).
func (s *pebbleHTTP) retrieveWith(ctx context.Context, idx *index.PebbleBM25, q string, fetchK int, retrieverParam, expandMode string) ([]index.Hit, string, error) {
	denseReady := s.hnsw() != nil && s.embedder != nil
	switch {
	case retrieverParam == "dense" && denseReady:
		vecs, err := s.embedder.Embed(ctx, []string{q})
		if err != nil {
			return nil, "", fmt.Errorf("embedder: %w", err)
		}
		vhits := s.hnsw().Search(ctx, vecs[0], fetchK)
		hits := s.applyAuthorityToDense(vhits)
		return hits, q, nil
	case retrieverParam == "hybrid" && denseReady:
		bm25Hits, bm25Eff, bm25Err := s.retrieveWithExpansionIdx(ctx, idx, q, fetchK, expandMode)
		if bm25Err != nil {
			return nil, "", bm25Err
		}
		vecs, embErr := s.embedder.Embed(ctx, []string{q})
		if embErr != nil {
			return nil, "", fmt.Errorf("embedder: %w", embErr)
		}
		denseV := s.hnsw().Search(ctx, vecs[0], fetchK)
		denseHits := s.applyAuthorityToDense(denseV)
		hits := rrfFuse([][]index.Hit{bm25Hits, denseHits})
		if len(hits) > fetchK {
			hits = hits[:fetchK]
		}
		return hits, bm25Eff, nil
	default:
		return s.retrieveWithExpansionIdx(ctx, idx, q, fetchK, expandMode)
	}
}

// getSiteBoostIDs returns a docID→50× multiplier map for all docs belonging
// to the given site scopes. Populated lazily on first call; cached in
// siteBoostCache thereafter. Using the famHost index so the scan is
// O(site_docs) not O(corpus).
func (s *pebbleHTTP) getSiteBoostIDs(ctx context.Context, scopes []siteScope) map[int64]float64 {
	if len(scopes) == 0 || s.store == nil {
		return nil
	}
	key := func() string {
		parts := make([]string, len(scopes))
		for i, sc := range scopes {
			parts[i] = sc.host
			if sc.path != "" {
				parts[i] += sc.path
			}
		}
		sort.Strings(parts)
		return strings.Join(parts, "|")
	}()
	if cached, ok := s.siteBoostCache.Load(key); ok {
		return cached.(map[int64]float64)
	}
	boost := make(map[int64]float64)
	for _, sc := range scopes {
		_ = s.store.IterHostDocIDs(ctx, sc.host, func(id int64) bool {
			boost[id] = 50.0
			return true
		})
	}
	if len(boost) > 0 {
		s.siteBoostCache.Store(key, boost)
	}
	return boost
}

// getSiteTitles returns up to maxSiteTitles real page titles from the given
// site scopes, for injection into the research planner prompt so the LLM sees
// the site's actual vocabulary. Bounded and cheap: a single early-stopping
// famHost scan per scope plus <=10 GetDocMeta side-blob lookups. Cached in
// siteTitleCache thereafter. Returns nil if nothing usable is found.
func (s *pebbleHTTP) getSiteTitles(ctx context.Context, scopes []siteScope) []string {
	if len(scopes) == 0 || s.store == nil {
		return nil
	}
	key := func() string {
		parts := make([]string, len(scopes))
		for i, sc := range scopes {
			parts[i] = sc.host
			if sc.path != "" {
				parts[i] += sc.path
			}
		}
		sort.Strings(parts)
		return strings.Join(parts, "|")
	}()
	if cached, ok := s.siteTitleCache.Load(key); ok {
		return cached.([]string)
	}
	const maxSiteTitles = 10
	titles := make([]string, 0, maxSiteTitles)
	seen := make(map[string]struct{}, maxSiteTitles)
	for _, sc := range scopes {
		if len(titles) >= maxSiteTitles {
			break
		}
		_ = s.store.IterHostDocIDs(ctx, sc.host, func(id int64) bool {
			_, title, ok, err := s.store.GetDocMeta(ctx, id)
			if err != nil {
				return false // stop this scope on error
			}
			if !ok {
				return true
			}
			t := strings.TrimSpace(title)
			if t == "" {
				return true
			}
			if _, dup := seen[t]; dup {
				return true
			}
			seen[t] = struct{}{}
			titles = append(titles, t)
			return len(titles) < maxSiteTitles // stop once we have 10
		})
	}
	if len(titles) > 0 {
		s.siteTitleCache.Store(key, titles)
	}
	return titles
}

// hostPartitionReadEnabled reports whether site= queries should route to the
// 'P'-family host partition via SearchInHost. Gated by
// COSIFT_HOST_PARTITION_READ=1; off → the 50× boost path is used.
func hostPartitionReadEnabled() bool {
	return os.Getenv("COSIFT_HOST_PARTITION_READ") == "1"
}

// retrieveForSites resolves a site-scoped sub-query. When the host partition
// is enabled and the scope is a single bare host, it scans only that host's
// posting partition (O(site_docs)) via SearchInHost — the definitive fix for
// the global-scan latency + recall problem. Otherwise (multi-site, path-scoped,
// flag off, or partition not yet backfilled for the host) it falls back to the
// 50× BM25 boost over the global index.
func (s *pebbleHTTP) retrieveForSites(ctx context.Context, q string, fetchK int, retrieverParam, expandMode string, boostIDs map[int64]float64, scopes []siteScope) ([]index.Hit, string, error) {
	if hostPartitionReadEnabled() && len(scopes) == 1 && scopes[0].path == "" && scopes[0].host != "" {
		hits, err := s.idx.SearchInHost(ctx, q, scopes[0].host, fetchK)
		if err == nil {
			return hits, q, nil
		}
		if !errors.Is(err, index.ErrHostPartitionEmpty) {
			return nil, "", err
		}
		// partition empty for this host → fall through to the boost path.
	}
	if len(boostIDs) == 0 {
		return s.retrieve(ctx, q, fetchK, retrieverParam, expandMode)
	}
	return s.retrieveWith(ctx, s.idx.WithBoost(boostIDs), q, fetchK, retrieverParam, expandMode)
}

// retrieveWithExpansionIdx dispatches the BM25 call across the three
// expansion strategies /search and /answer share — bare, HyDE, paraphrase+RRF.
// Returns (hits, effectiveQuery, err). effectiveQuery == q when no expansion
// fired (bare path, or expansion no-op'd because chat is down). It is
// parameterized over the BM25 index so a transformed index (e.g. the
// site-boosted one from WithBoost) can be passed in without shallow-copying the
// whole pebbleHTTP struct (which embeds mutexes).
func (s *pebbleHTTP) retrieveWithExpansionIdx(ctx context.Context, idx *index.PebbleBM25, q string, fetchK int, expandMode string) ([]index.Hit, string, error) {
	switch expandMode {
	case "paraphrase":
		paras := s.paraphraseQuery(ctx, q, 3)
		if len(paras) == 0 {
			hits, err := idx.Search(ctx, q, fetchK)
			return hits, q, err
		}
		queries := append([]string{q}, paras...)
		lists := make([][]index.Hit, 0, len(queries))
		for _, qq := range queries {
			h, lerr := idx.Search(ctx, qq, fetchK)
			if lerr != nil {
				continue
			}
			lists = append(lists, h)
		}
		hits := rrfFuse(lists)
		if len(hits) > fetchK {
			hits = hits[:fetchK]
		}
		return hits, q + " | " + strings.Join(paras, " | "), nil
	case "true", "hyde":
		eq := s.expandQuery(ctx, q)
		hits, err := idx.Search(ctx, eq, fetchK)
		return hits, eq, err
	case "entity":
		// Rule-based query rewrite for question-form entity lookups
		// ("who created X", "how tall is X", "what is the capital of
		// X"). Strips the question form, appends canonical-attribute
		// keywords to the original, single BM25 call. The earlier
		// RRF-fusion variant regressed legitimate passes because
		// equal-weight fusion across 4 queries pushed the original's
		// top hit out of the reranker's top-30. Concat preserves the
		// original signal while letting the new keywords add positive
		// scoring contributions where biographical pages exist.
		rewrites := qexpand.RewriteEntity(q)
		if len(rewrites) == 0 {
			hits, err := idx.Search(ctx, q, fetchK)
			return hits, q, err
		}
		expanded := q + " " + strings.Join(rewrites, " ")
		hits, err := idx.Search(ctx, expanded, fetchK)
		return hits, expanded, err
	default:
		// Bare path runs entity-expansion when the query matches a
		// question-form pattern. Earlier RRF-fuse experiment regressed
		// 'who created the World Wide Web' (passing → bail) because
		// equal-weight fusion across 4 queries pushed the
		// previously-#1 Berners-Lee bio out of the reranker's top-30.
		// Concat-into-single-query avoids the fusion-weight problem:
		// the rewrites only add positive scoring contributions for
		// pages that contain the canonical-attribute words ("creator,"
		// "inventor"), without diluting the original query's ranking
		// signal. Operators can disable with COSIFT_DISABLE_ENTITY_EXPAND=1.
		if os.Getenv("COSIFT_DISABLE_ENTITY_EXPAND") == "" {
			if rewrites := qexpand.RewriteEntity(q); len(rewrites) > 0 {
				expanded := q + " " + strings.Join(rewrites, " ")
				hits, err := idx.Search(ctx, expanded, fetchK)
				return hits, expanded, err
			}
		}
		hits, err := idx.Search(ctx, q, fetchK)
		return hits, q, err
	}
}

// expandQuery returns q + " " + a HyDE-generated passage when a chat client is
// configured. On any error (no chat client, chat call fails, empty passage),
// returns q unchanged so callers can compose safely without explicit guards.
// added a bounded in-memory cache to skip the chat call on
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
	passage, err := s.doChat(ctx, s.chat, []embed.ChatMsg{
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
	if len(s.hydeCache) >= s.hydeCacheCap {
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

// queryPlanPrompt is the LLM-orchestrator prompt. The model
// analyzes the user's natural-language query, classifies intent, and
// outputs a structured retrieval plan: expanded queries (3 variations
// covering different angles), recency window, retriever choice, and
// optionally a domain allowlist. The plan then drives a broader RRF-
// fused retrieval. JSON schema is strict; sanity-checked on receive.
const queryPlanPrompt = `You analyze a user's natural-language search query and output a JSON retrieval plan.

Output ONLY a JSON object (no markdown, no prose, no commentary) with this exact shape:
{
  "intent": "current_news" | "factual_lookup" | "comparison" | "research" | "findability",
  "queries": ["expanded query 1", "expanded query 2", "expanded query 3"],
  "since_days": null | <positive integer, days lookback from today>,
  "retriever": "bm25" | "dense" | "hybrid",
  "decay_days": <positive integer half-life> | null
}

Guidance:
- "queries": 3 paraphrases/expansions of the original. Include entity names, synonyms, related terms. e.g. "latest news from Romania" → ["Romania political news 2026", "Romanian government coalition PSD AUR", "Bucharest current events"]
- "since_days": only set when the query implies recency (latest/current/recent/today/news). null otherwise.
- "decay_days": half-life in days. For current-events queries use 60. For factual/research queries use null or 365.
- "retriever": hybrid is the safe default. Use "bm25" only for exact-term lookups (model names, error codes). Use "dense" only for purely conceptual queries with no keyword overlap.
- DO NOT invent include_domains. The corpus chooses sources, not you.

Examples:
Input: "latest news from Romania"
{"intent":"current_news","queries":["Romania political news 2026","Romanian PSD AUR coalition government","Bucharest economy and politics current"],"since_days":90,"retriever":"hybrid","decay_days":60}

Input: "what is BM25 ranking"
{"intent":"factual_lookup","queries":["BM25 Okapi ranking function formula","BM25 information retrieval probabilistic","term frequency inverse document frequency BM25"],"since_days":null,"retriever":"hybrid","decay_days":null}

Input: "compare HNSW vs IVF"
{"intent":"comparison","queries":["HNSW vs IVF approximate nearest neighbor","Hierarchical Navigable Small World versus Inverted File","ANN index tradeoffs HNSW IVF performance"],"since_days":null,"retriever":"hybrid","decay_days":730}`

// queryPlan captures the LLM's structured retrieval plan. Sanitized on
// parse — out-of-range values clipped to safe defaults.
type queryPlan struct {
	Intent    string   `json:"intent"`
	Queries   []string `json:"queries"`
	SinceDays *int     `json:"since_days"`
	Retriever string   `json:"retriever"`
	DecayDays *int     `json:"decay_days"`
}

// parseQueryPlan extracts the planner's JSON output and applies guard-rails.
// Returns a plan with sane defaults if the LLM produces garbage — never
// errors at the caller, so /query always succeeds.
func parseQueryPlan(raw, original string) queryPlan {
	defaultPlan := queryPlan{
		Intent:    "research",
		Queries:   []string{original},
		Retriever: "hybrid",
	}
	// Strip code fences if the LLM wrapped output despite the prompt.
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "```") {
		if i := strings.Index(raw, "\n"); i > 0 {
			raw = raw[i+1:]
		}
		raw = strings.TrimSuffix(raw, "```")
	}
	var p queryPlan
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return defaultPlan
	}
	if len(p.Queries) == 0 {
		p.Queries = []string{original}
	}
	if len(p.Queries) > 5 {
		p.Queries = p.Queries[:5]
	}
	switch p.Retriever {
	case "bm25", "dense", "hybrid":
		// ok
	default:
		p.Retriever = "hybrid"
	}
	if p.SinceDays != nil && (*p.SinceDays <= 0 || *p.SinceDays > 3650) {
		p.SinceDays = nil
	}
	if p.DecayDays != nil && (*p.DecayDays <= 0 || *p.DecayDays > 3650) {
		p.DecayDays = nil
	}
	if p.Intent == "" {
		p.Intent = "research"
	}
	return p
}

// handleQuery is the LLM-orchestrated search/research endpoint.
// Flow: (1) LLM planner analyzes intent + emits expanded queries, (2) each
// expansion runs through /search internals (hybrid + decay), (3) RRF-fuse
// the per-expansion result lists, (4) optional rerank, (5) synth with
// citations. Returns the plan in the response so callers can see what was
// done.
//
// Effectively: /research planner-style decomposition, but optimized for
// QUERY EXPANSION not sub-question DECOMPOSITION. The two are different —
// research splits multi-faceted questions ("compare X and Y" → "X facts",
// "Y facts", "X vs Y tradeoffs"); query expands a single intent into
// paraphrases that catch corpus phrasing variation.
func (s *pebbleHTTP) handleQuery(w http.ResponseWriter, r *http.Request) {
	if s.chat == nil {
		writeProblem(w, http.StatusNotImplemented, "/query requires cfg.Chat.Model")
		return
	}
	start := time.Now()
	q := r.URL.Query().Get("q")
	if q == "" {
		writeProblem(w, http.StatusBadRequest, "missing q parameter")
		return
	}

	// SSE opt-in. Stream phase events through the planner → expand → fuse
	// → synth pipeline so operators see the LLM's sub-queries, per-expansion
	// hit counts, fusion result, and synth tokens as they're produced.
	wantStream := wantsSSE(r)
	var sse *answerSSE
	var streamChat embed.StreamingChatClient
	if wantStream {
		if sc, ok := s.chat.(embed.StreamingChatClient); ok {
			if a := newAnswerSSE(w, start); a != nil {
				sse, streamChat = a, sc
				stopKA := sse.startKeepalive(r.Context(), 7*time.Second)
				defer stopKA()
				sse.phase("plan_start", map[string]any{"q": q, "model": s.chat.Model()})
			}
		}
	}

	// Step 1: LLM planner.
	planRaw, err := s.doChat(r.Context(), s.chat, []embed.ChatMsg{
		{Role: "system", Content: queryPlanPrompt},
		{Role: "user", Content: q},
	})
	if err != nil {
		if sse != nil {
			sse.errorEvt("plan: " + err.Error())
			return
		}
		writeProblem(w, http.StatusBadGateway, "plan: "+err.Error())
		return
	}
	plan := parseQueryPlan(planRaw, q)
	if sse != nil {
		details := map[string]any{
			"intent":    plan.Intent,
			"retriever": plan.Retriever,
			"queries":   plan.Queries,
		}
		if plan.SinceDays != nil {
			details["since_days"] = *plan.SinceDays
		}
		if plan.DecayDays != nil {
			details["decay_days"] = *plan.DecayDays
		}
		sse.phase("plan", details)
	}

	// Step 2: run each expanded query through retrieval, dedupe by URL.
	type hitInfo struct {
		hit    index.Hit
		bestRR float64 // best reciprocal rank across expansions
	}
	byURL := make(map[string]*hitInfo, 64)
	fetchK := 30
	for _, sub := range plan.Queries {
		hits, _, err := s.retrieve(r.Context(), sub, fetchK, plan.Retriever, "")
		if err != nil {
			if sse != nil {
				sse.phase("expand", map[string]any{"query": sub, "error": err.Error()})
			}
			continue
		}
		for rank, h := range hits {
			rr := 1.0 / float64(60+rank) // RRF with k=60
			if existing, ok := byURL[h.URL]; ok {
				if rr > existing.bestRR {
					existing.bestRR = rr
				}
			} else {
				byURL[h.URL] = &hitInfo{hit: h, bestRR: rr}
			}
		}
		if sse != nil {
			sse.phase("expand", map[string]any{"query": sub, "hits": len(hits)})
		}
	}

	// Step 3: sort by fused RRF score with optional per-host boosts.
	fused := make([]index.Hit, 0, len(byURL))
	for _, v := range byURL {
		h := v.hit
		h.Score = v.bestRR
		if len(s.hostBoosts) > 0 {
			h.Score *= index.HostBoostFor(h.URL, s.hostBoosts)
		}
		fused = append(fused, h)
	}
	sort.Slice(fused, func(i, j int) bool { return fused[i].Score > fused[j].Score })
	if sse != nil {
		sse.phase("fuse", map[string]any{"unique_urls": len(fused)})
	}

	// Step 4: optional since filter + decay (decay handled by /answer-style
	// path when we materialize). Apply since filter here on PublishedAt.
	keep := 10
	if v := r.URL.Query().Get("k"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 20 {
			keep = n
		}
	}
	var since time.Time
	if plan.SinceDays != nil {
		since = time.Now().AddDate(0, 0, -(*plan.SinceDays))
	}

	// Step 5: enrich + filter by since.
	type cand struct {
		src     answerSource
		excerpt string
		text    string
	}
	cands := make([]cand, 0, keep)
	for _, h := range fused {
		if len(cands) >= keep {
			break
		}
		doc, err := s.store.GetDocByURL(r.Context(), h.URL)
		if err != nil || doc == nil {
			continue
		}
		if !since.IsZero() && (doc.PublishedAt.IsZero() || doc.PublishedAt.Before(since)) {
			continue
		}
		c := cand{
			src: answerSource{
				ID:    len(cands) + 1,
				URL:   h.URL,
				Title: h.Title,
			},
			excerpt: textExcerpt(doc.Text, 320),
			text:    doc.Text,
		}
		if !doc.PublishedAt.IsZero() {
			t := doc.PublishedAt
			c.src.PublishedAt = &t
		}
		cands = append(cands, c)
	}
	if sse != nil {
		sse.phase("materialize", map[string]any{"candidates": len(cands)})
	}

	// Step 6: synth via answerSystemPrompt.
	var promptSrcs strings.Builder
	sources := make([]answerSource, 0, len(cands))
	for i, c := range cands {
		c.src.ID = i + 1
		c.src.Excerpt = c.excerpt
		fmt.Fprintf(&promptSrcs, "[%d] %s\n%s\n\n", c.src.ID, c.src.URL, truncateForPromptLite(c.text, 1200))
		sources = append(sources, c.src)
	}

	if sse != nil {
		sse.sources(q, sources, s.chat.Model(), "query:planner+hybrid+rrf", len(fused))
		if len(cands) == 0 {
			sse.chunk("No sources matched the planner's expanded queries within the chosen time window.")
			sse.done()
			return
		}
		sse.phase("synth_start", map[string]any{"sources": len(sources), "model": streamChat.Model()})
		_, cerr := s.doChatStream(r.Context(), streamChat, []embed.ChatMsg{
			{Role: "system", Content: answerSystemPrompt},
			{Role: "user", Content: "Sources:\n\n" + promptSrcs.String() + "Question: " + q},
		}, sse.chunk)
		if cerr != nil {
			sse.errorEvt("synth: " + cerr.Error())
			return
		}
		sse.done()
		return
	}

	answerText := ""
	if len(cands) > 0 {
		answerText, err = s.doChat(r.Context(), s.chat, []embed.ChatMsg{
			{Role: "system", Content: answerSystemPrompt},
			{Role: "user", Content: "Sources:\n\n" + promptSrcs.String() + "Question: " + q},
		})
		if err != nil {
			writeProblem(w, http.StatusBadGateway, "synth: "+err.Error())
			return
		}
	} else {
		answerText = "No sources matched the planner's expanded queries within the chosen time window."
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"query":   q,
		"plan":    plan,
		"answer":  answerText,
		"sources": sources,
		"model":   s.chat.Model(),
		"took":    time.Since(start).String(),
	})
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

// POST /contents — batch URL → document. Up to 100 URLs in
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
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
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
