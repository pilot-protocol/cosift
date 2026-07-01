package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

// handlePQEncode backfills PQ codes for every node currently missing one,
// against the codebook already loaded on this serve. Order-of-magnitude
// faster than handlePQTrain because it skips k-means; just encode loop.
func (s *pebbleHTTP) handlePQEncode(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
	}
	if s.hnsw == nil {
		writeProblem(w, http.StatusBadRequest, "pq-encode: no HNSW loaded")
		return
	}
	if !s.hnsw.HasPQ() {
		writeProblem(w, http.StatusBadRequest, "pq-encode: no codebook loaded — run /admin/pq-train first")
		return
	}
	t0 := time.Now()
	ids, codes, err := s.hnsw.EncodeMissing()
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "encode: "+err.Error())
		return
	}
	encodeElapsed := time.Since(t0)
	if len(ids) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{
			"encoded":       0,
			"total":         s.hnsw.Len(),
			"already_full":  true,
			"total_elapsed": encodeElapsed.String(),
		})
		return
	}
	// Persist freshly-encoded codes in a single batch.
	pq := s.hnsw.PQStatus()
	cb := &index.PQCodebook{Dim: pq.Dim, M: pq.M, K: pq.K, SubDim: pq.Dim / pq.M}
	entries := make([]store.PQCodeEntry, len(ids))
	for i := range ids {
		entries[i] = store.PQCodeEntry{ID: ids[i], Blob: cb.EncodeCodeBlob(codes[i])}
	}
	persistT0 := time.Now()
	if err := s.store.PutPQCodesBatch(r.Context(), entries); err != nil {
		writeProblem(w, http.StatusInternalServerError, "persist codes: "+err.Error())
		return
	}
	persistElapsed := time.Since(persistT0)
	total := s.hnsw.Len()
	coverage := 100.0 * float64(pq.NodesWithCode+len(ids)) / float64(total)
	totalElapsed := time.Since(t0)
	log.Printf("pq-encode: backfilled %d codes in %s (persist %s, total %s) → coverage %.1f%%",
		len(ids), encodeElapsed-persistElapsed, persistElapsed, totalElapsed, coverage)
	writeJSON(w, http.StatusOK, map[string]any{
		"encoded":         len(ids),
		"total":           total,
		"coverage_pct":    coverage,
		"encode_elapsed":  (encodeElapsed - persistElapsed).String(),
		"persist_elapsed": persistElapsed.String(),
		"total_elapsed":   totalElapsed.String(),
	})
}

// handleCheckpoint creates a Pebble checkpoint (hard-linked, point-in-time)
// at a server-chosen path under COSIFT_CHECKPOINT_DIR (default /tmp). The
// returned path is safe to tar — Pebble's compactor cannot mutate hard-linked
// SSTs. Caller is responsible for deleting the dir after consuming it.
func (s *pebbleHTTP) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
	}
	base := os.Getenv("COSIFT_CHECKPOINT_DIR")
	if base == "" {
		base = "/tmp"
	}
	dest := filepath.Join(base, fmt.Sprintf("cosift-ckpt-%d", time.Now().UnixNano()))
	t0 := time.Now()
	if err := s.store.Checkpoint(dest); err != nil {
		writeProblem(w, http.StatusInternalServerError, "checkpoint: "+err.Error())
		return
	}
	elapsed := time.Since(t0)
	log.Printf("checkpoint: created %q in %s", dest, elapsed)
	writeJSON(w, http.StatusOK, map[string]any{
		"path":    dest,
		"elapsed": elapsed.String(),
	})
}

// handlePQTrain trains a PQ codebook on a sample of the current HNSW
// graph, then encodes ALL nodes and persists codes + codebook to Pebble.
// Synchronous; returns when training completes.
type pqTrainReq struct {
	SampleSize int `json:"sample_size"` // default 50000
	M          int `json:"m"`           // default dim/8 (subspaces of size 8)
	K          int `json:"k"`           // default 256
	Iters      int `json:"iters"`       // default 15
	Parallel   int `json:"parallel"`    // default GOMAXPROCS
}

func (s *pebbleHTTP) handlePQTrain(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
	}
	if s.hnsw == nil || s.hnsw.Len() == 0 {
		writeProblem(w, http.StatusBadRequest, "pq-train requires an in-memory HNSW with nodes")
		return
	}
	var req pqTrainReq
	if r.ContentLength > 0 {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 4<<10))
		_ = json.Unmarshal(body, &req)
	}
	if req.SampleSize <= 0 {
		req.SampleSize = 50000
	}
	if req.K <= 0 {
		req.K = 256
	}
	if req.Iters <= 0 {
		req.Iters = 15
	}
	if req.Parallel <= 0 {
		req.Parallel = 8
	}
	if s.embedder == nil {
		writeProblem(w, http.StatusBadRequest, "pq-train needs an embedder to know the vector dim")
		return
	}
	dim := s.embedder.Dim()
	M := req.M
	if M <= 0 {
		// Default: 1 subspace per 8 dims. For 768d → 96 subspaces.
		M = dim / 8
		if M <= 0 {
			M = 1
		}
	}
	if dim%M != 0 {
		writeProblem(w, http.StatusBadRequest, fmt.Sprintf("dim %d not divisible by M %d", dim, M))
		return
	}
	t0 := time.Now()
	log.Printf("pq-train: sampling %d / %d vectors (dim=%d, M=%d, K=%d, iters=%d, parallel=%d)",
		req.SampleSize, s.hnsw.Len(), dim, M, req.K, req.Iters, req.Parallel)
	sample := s.hnsw.SampleVectors(req.SampleSize, time.Now().UnixNano())
	sampledN := len(sample)
	log.Printf("pq-train: training codebook on %d samples...", sampledN)
	cb, err := index.TrainPQCodebookParallel(sample, dim, M, req.K, req.Iters, req.Parallel, time.Now().UnixNano())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "train: "+err.Error())
		return
	}
	trainElapsed := time.Since(t0)
	log.Printf("pq-train: codebook trained in %s; persisting...", trainElapsed)
	if err := cb.Persist(r.Context(), s.store); err != nil {
		writeProblem(w, http.StatusInternalServerError, "persist codebook: "+err.Error())
		return
	}
	encodeT0 := time.Now()
	log.Printf("pq-train: encoding %d nodes...", s.hnsw.Len())
	ids, codes, err := s.hnsw.EncodeAll(cb)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "encode: "+err.Error())
		return
	}
	entries := make([]store.PQCodeEntry, len(ids))
	for i := range ids {
		// K≤256 → byte-packed for 2x disk savings.
		entries[i] = store.PQCodeEntry{ID: ids[i], Blob: cb.EncodeCodeBlob(codes[i])}
	}
	if err := s.store.PutPQCodesBatch(r.Context(), entries); err != nil {
		writeProblem(w, http.StatusInternalServerError, "put codes: "+err.Error())
		return
	}
	encodeElapsed := time.Since(encodeT0)
	totalElapsed := time.Since(t0)
	log.Printf("pq-train: done in %s (train=%s, encode+persist=%s, %d codes written)",
		totalElapsed, trainElapsed, encodeElapsed, len(ids))
	writeJSON(w, http.StatusOK, map[string]any{
		"sample_size":    sampledN,
		"dim":            dim,
		"M":              M,
		"K":              req.K,
		"iters":          req.Iters,
		"nodes_encoded":  len(ids),
		"train_elapsed":  trainElapsed.String(),
		"encode_elapsed": encodeElapsed.String(),
		"total_elapsed":  totalElapsed.String(),
	})
}

// evalQuickQueries is the canned smoke-eval set. Chosen for breadth:
// 3 definition + 3 technical-howto + 2 comparison + 2 findability. Tracks
// the same answered/no-info/error verdicts the external 60-Q harness uses,
// so a regression here predicts a regression in the bigger eval. Hardcoded
// (no new deps) so any operator can hit /admin/eval-quick and get an
// answer rate without setting up Python or external test infra.
var evalQuickQueries = []string{
	"what is BM25 ranking function",
	"what is HNSW algorithm",
	"what is reciprocal rank fusion",
	"how does retrieval augmented generation work",
	"how does Go goroutines scheduling work",
	"explain CAP theorem in distributed systems",
	"compare BM25 vs dense vector retrieval",
	"compare REST vs GraphQL APIs",
	"what is Pilot Protocol overlay network",
	"what is sentence embedding",
}

// handleEvalQuick runs the 10-query smoke eval against the running cosift
// and returns answered-rate + per-query verdicts. Each query hits /answer
// in-process (re-uses the same chat/retrieval stack as a real /answer call),
// so the rate that comes back matches what users see.
func (s *pebbleHTTP) handleEvalQuick(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
	}
	if s.chat == nil {
		writeProblem(w, http.StatusNotImplemented, "eval-quick requires cfg.Chat.Model")
		return
	}
	// the server-wide WriteTimeout (60s) was killing this handler
	// before 10 sequential /answer calls finished — connection closed mid-write,
	// curl saw "empty reply from server" (exit 52). Disable the deadline for
	// this long-running admin endpoint via ResponseController. Pairs with
	// bounded-parallel dispatch below so the 10-query batch finishes in
	// ~one chat-LLM round-trip instead of ten.
	if rc := http.NewResponseController(w); rc != nil {
		_ = rc.SetWriteDeadline(time.Time{})
	}
	type queryResult struct {
		Query     string `json:"query"`
		Verdict   string `json:"verdict"` // answered | no_info | empty | error
		LatencyMs int    `json:"latency_ms"`
		Sources   int    `json:"sources"`
		Suggest   string `json:"suggest_escalation,omitempty"`
	}
	results := make([]queryResult, len(evalQuickQueries))
	t0 := time.Now()

	// Bounded-parallel dispatch. vLLM batches concurrent requests so 4-wide
	// fan-out finishes in roughly one chat round-trip rather than 10.
	sem := make(chan struct{}, 4)
	var wg sync.WaitGroup
	for i, q := range evalQuickQueries {
		wg.Add(1)
		go func(i int, q string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			qStart := time.Now()
			subReq, _ := http.NewRequestWithContext(r.Context(), http.MethodGet,
				"/answer?q="+url.QueryEscape(q)+"&stream=false&retriever=hybrid", nil)
			rec := newResponseRecorder()
			s.handleAnswer(rec, subReq)
			latencyMs := int(time.Since(qStart) / time.Millisecond)

			var ar answerResponse
			if jerr := json.Unmarshal(rec.body.Bytes(), &ar); jerr != nil || rec.code >= 400 {
				results[i] = queryResult{Query: q, Verdict: "error", LatencyMs: latencyMs}
				return
			}
			var verdict string
			switch {
			case len(ar.Answer) < 30:
				verdict = "empty"
			case answerLooksLikeNoInfo(ar.Answer):
				verdict = "no_info"
			default:
				verdict = "answered"
			}
			results[i] = queryResult{
				Query:     q,
				Verdict:   verdict,
				LatencyMs: latencyMs,
				Sources:   len(ar.Sources),
				Suggest:   ar.SuggestEscalation,
			}
		}(i, q)
	}
	wg.Wait()

	answered, noInfo, errCount, empty, totalMs := 0, 0, 0, 0, 0
	for _, r := range results {
		totalMs += r.LatencyMs
		switch r.Verdict {
		case "answered":
			answered++
		case "no_info":
			noInfo++
		case "empty":
			empty++
		default:
			errCount++
		}
	}
	n := len(evalQuickQueries)
	writeJSON(w, http.StatusOK, map[string]any{
		"queries":         n,
		"answered":        answered,
		"no_info":         noInfo,
		"empty":           empty,
		"errors":          errCount,
		"answer_rate_pct": 100 * answered / n,
		"avg_latency_ms":  totalMs / n,
		"total_elapsed":   time.Since(t0).String(),
		"chat_model":      s.chat.Model(),
		"results":         results,
	})
}

// handleHNSWCompact runs HNSW.Compact() in-place, then clears the persisted
// 'v' family and writes a fresh full snapshot so disk matches the compacted
// in-memory graph. Cheaper than the offline hnsw-rebuild subcommand: Compact
// keeps the existing topology among surviving nodes (O(N + edges)), whereas
// Rebuild re-inserts every node via HNSW search (multiple minutes per million
// passages). Operators run this when stats.zombie_nodes climbs above ~30% of
// nodes_total.
//
// Synchronous; holds the HNSW write lock during the compact step and the
// read lock during the persist step. Dense retrieval and AddPassage calls
// queue for the duration. The server-wide WriteTimeout is disabled here via
// ResponseController because compacting a multi-million-node graph routinely
// runs past 60s. Returns counters so operators can confirm progress.
func (s *pebbleHTTP) handleHNSWCompact(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
	}
	if s.hnsw == nil {
		writeProblem(w, http.StatusNotImplemented, "hnsw-compact requires a loaded HNSW graph")
		return
	}
	if rc := http.NewResponseController(w); rc != nil {
		_ = rc.SetWriteDeadline(time.Time{})
	}
	skipPersist := r.URL.Query().Get("skip_persist") == "1"

	before := s.hnsw.Len()
	t0 := time.Now()
	removed := s.hnsw.Compact()
	compactDur := time.Since(t0)
	after := s.hnsw.Len()

	resp := map[string]any{
		"nodes_before": before,
		"nodes_after":  after,
		"removed":      removed,
		"compact_ms":   compactDur.Milliseconds(),
		"persisted":    false,
	}

	if removed == 0 || skipPersist {
		writeJSON(w, http.StatusOK, resp)
		return
	}

	// Compact remapped node indices; the persisted 'v' family now points at
	// stale slots. Wipe and full-rewrite. PQ codes follow node indices too,
	// so clear 'q' as well — operators must re-run /admin/pq-train if PQ was
	// in use.
	persistT0 := time.Now()
	ctx := r.Context()
	if err := s.store.ClearVectorFamily(ctx); err != nil {
		resp["persist_error"] = "clear vector family: " + err.Error()
		writeJSON(w, http.StatusInternalServerError, resp)
		return
	}
	if err := s.store.ClearPQFamily(ctx); err != nil {
		resp["persist_error"] = "clear pq family: " + err.Error()
		writeJSON(w, http.StatusInternalServerError, resp)
		return
	}
	if err := s.hnsw.Persist(ctx, s.store); err != nil {
		resp["persist_error"] = "persist: " + err.Error()
		writeJSON(w, http.StatusInternalServerError, resp)
		return
	}
	resp["persisted"] = true
	resp["persist_ms"] = time.Since(persistT0).Milliseconds()
	log.Printf("hnsw-compact: removed=%d (%.1f%% zombies) compact=%s persist=%s nodes %d→%d",
		removed, 100*float64(removed)/float64(before),
		compactDur.Round(time.Millisecond), time.Since(persistT0).Round(time.Millisecond),
		before, after)
	writeJSON(w, http.StatusOK, resp)
}

// responseRecorder captures an http.Handler's output for in-process
// dispatch. Minimal — only what handleEvalQuick needs.
type responseRecorder struct {
	hdr  http.Header
	body bytes.Buffer
	code int
}

func newResponseRecorder() *responseRecorder {
	return &responseRecorder{hdr: make(http.Header), code: 200}
}

func (r *responseRecorder) Header() http.Header { return r.hdr }

func (r *responseRecorder) Write(b []byte) (int, error) { return r.body.Write(b) }

func (r *responseRecorder) WriteHeader(c int) { r.code = c }

// handleEmbedBackfill walks famDocMeta, finds docs whose URL has zero
// HNSW vector nodes (lexical-only ingest from WET imports, or any docs
// indexed before an embedder was wired), and embeds them. Pairs with
// /admin/wet-import?lexical_only=true so bulk-loaded content gets dense
// retrieval after the fast lexical pass.
//
// Body: {"limit": 10000, "workers": 4}
//   - limit:   cap on docs processed this call (0 = unlimited)
//   - workers: concurrent embed pipelines (default 4)
//
// Idempotent against concurrent re-runs — re-embedding a doc that
// already has vectors is a no-op because UpsertPassage just adds more
// nodes pointing at the same URL (which the dedup-by-URL in
// HNSW.Search picks the best of anyway). Cleaner: zombie-reclaim kicks
// in to invalidate prior, then fresh vectors take over.
type embedBackfillReq struct {
	Limit   int `json:"limit,omitempty"`
	Workers int `json:"workers,omitempty"`
}

// handleHostBackfill triggers the 'P' host-partition backfill IN-PROCESS, in a
// detached background goroutine (Pebble is single-writer, so this can't run as
// a separate process against a live store). Returns immediately; progress is
// logged. ?host=<host> runs the fast targeted path; omit for the full corpus.
//
// Run this with the COSIFT_HOST_PARTITION write flag OFF so the backfill is the
// sole writer of the 'T'/'next_host_id' families (no race with crawl indexing).
// After it logs "done", set COSIFT_HOST_PARTITION_READ=1 (and =1 for the write
// flag to keep it fresh) and restart.
func (s *pebbleHTTP) handleHostBackfill(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
	}
	if s.store == nil {
		writeProblem(w, http.StatusNotImplemented, "host-backfill requires a PebbleStore")
		return
	}
	host := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("host")))
	// Detached context so the long backfill survives the request returning.
	go func() {
		t0 := time.Now()
		scope := "full corpus"
		if host != "" {
			scope = "host=" + host
		}
		log.Printf("host-backfill: starting (%s)", scope)
		written, err := s.store.BackfillHostPostings(context.Background(), host, func(n int64) {
			log.Printf("host-backfill: %d postings written (%s elapsed)", n, time.Since(t0).Round(time.Second))
		})
		if err != nil {
			log.Printf("host-backfill: FAILED after %d postings: %v", written, err)
			return
		}
		log.Printf("host-backfill: DONE — %d postings in %s. Set COSIFT_HOST_PARTITION_READ=1 and restart.",
			written, time.Since(t0).Round(time.Second))
	}()
	writeJSON(w, http.StatusAccepted, map[string]any{
		"status": "started", "scope": host, "note": "running in background; watch server logs for progress + 'DONE'",
	})
}

func (s *pebbleHTTP) handleEmbedBackfill(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
	}
	if s.embedder == nil || s.hnsw == nil {
		writeProblem(w, http.StatusNotImplemented, "embed backfill requires both an embedder and HNSW graph to be configured")
		return
	}
	var req embedBackfillReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	_ = json.Unmarshal(body, &req)
	if req.Workers <= 0 {
		req.Workers = 4
	}
	if req.Workers > 32 {
		req.Workers = 32
	}

	// Pipeline: iter docs → filter (no-vector) → chunker → embed → write.
	// Bounded channel so we don't materialize the candidate list in memory.
	type cand struct {
		docID int64
		url   string
	}
	candCh := make(chan cand, req.Workers*4)

	t0 := time.Now()
	var scanned, missing, embedded atomic.Int64

	// Find candidates: docs with zero HNSW vectors. Producer.
	go func() {
		defer close(candCh)
		_ = s.store.IterDocsLite(r.Context(), func(docID int64, url string) error {
			scanned.Add(1)
			if req.Limit > 0 && missing.Load() >= int64(req.Limit) {
				return errors.New("limit reached") // sentinel — stops IterDocsLite
			}
			if _, ok := s.hnsw.LookupVectorByURL(url); ok {
				return nil // already has vectors
			}
			missing.Add(1)
			select {
			case candCh <- cand{docID: docID, url: url}:
			case <-r.Context().Done():
				return r.Context().Err()
			}
			return nil
		})
	}()

	// Worker pool: chunk + embed + write passages per doc.
	var wg sync.WaitGroup
	for i := 0; i < req.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range candCh {
				doc, err := s.store.GetDocByID(r.Context(), c.docID)
				if err != nil || doc == nil || doc.Text == "" {
					continue
				}
				// Use index defaults (NewChunker passes 0,0 = default 320/64).
				chunker := index.NewChunker()
				chunks := chunker.Chunk(doc.Text)
				if len(chunks) == 0 {
					continue
				}
				const tokenCap = 1500
				texts := make([]string, len(chunks))
				for j, ch := range chunks {
					texts[j] = truncateForEmbedLite(ch.Text, tokenCap)
				}
				vecs, eErr := s.embedder.Embed(r.Context(), texts)
				if eErr != nil || len(vecs) != len(chunks) {
					continue
				}
				for j, ch := range chunks {
					s.hnsw.AddPassage(doc.URL, doc.Title, ch.Offset, ch.Length, vecs[j])
					_ = j
				}
				embedded.Add(1)
			}
		}()
	}
	wg.Wait()

	log.Printf("embed-backfill: scanned=%d missing=%d embedded=%d in %s",
		scanned.Load(), missing.Load(), embedded.Load(), time.Since(t0).Round(time.Second))
	writeJSON(w, http.StatusOK, map[string]any{
		"scanned":  scanned.Load(),
		"missing":  missing.Load(),
		"embedded": embedded.Load(),
		"elapsed":  time.Since(t0).String(),
	})
}

// truncateForEmbedLite mirrors the crawler's helper without pulling the
// crawler package in. Same heuristic: cap by approximate token count.
func truncateForEmbedLite(s string, tokenCap int) string {
	// Rough: 1 token ≈ 4 chars for ASCII; cap at 4×tokenCap bytes as a
	// fast upper bound. Real chunker is at ~320 words = ~1200 tokens, so
	// 1500-token cap rarely triggers.
	maxBytes := tokenCap * 4
	if len(s) <= maxBytes {
		return s
	}
	return s[:maxBytes]
}
