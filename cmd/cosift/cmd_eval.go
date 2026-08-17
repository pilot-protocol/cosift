package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/crawler"
	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/eval"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/rerank"
	"github.com/pilot-protocol/cosift/internal/server"
	"github.com/pilot-protocol/cosift/internal/store"
)

// bm25Adapter adapts index.BM25 to the eval.Retriever interface (URL-only result list).
type bm25Adapter struct{ inner *index.BM25 }

func (a *bm25Adapter) Search(ctx context.Context, q string, k int) ([]string, error) {
	hits, err := a.inner.Search(ctx, q, k)
	if err != nil {
		return nil, err
	}
	urls := make([]string, len(hits))
	for i, h := range hits {
		urls[i] = h.URL
	}
	return urls, nil
}

// pebbleBM25Adapter adapts index.PebbleBM25 to eval.Retriever, so the golden
// set gates the production (pebble-serve) BM25 path, not just the SQLite twin.
type pebbleBM25Adapter struct{ inner *index.PebbleBM25 }

func (a *pebbleBM25Adapter) Search(ctx context.Context, q string, k int) ([]string, error) {
	hits, err := a.inner.Search(ctx, q, k)
	if err != nil {
		return nil, err
	}
	urls := make([]string, len(hits))
	for i, h := range hits {
		urls[i] = h.URL
	}
	return urls, nil
}

// denseAdapter is a Retriever backed by an in-memory VectorIndex + an Embedder.
// The embedder is consulted once per query at search time.
type denseAdapter struct {
	idx *index.VectorIndex
	emb embed.Embedder
}

func (a *denseAdapter) Search(ctx context.Context, q string, k int) ([]string, error) {
	vecs, err := a.emb.Embed(ctx, []string{q})
	if err != nil {
		return nil, err
	}
	hits := a.idx.Search(ctx, vecs[0], k)
	urls := make([]string, len(hits))
	for i, h := range hits {
		urls[i] = h.URL
	}
	return urls, nil
}

// hybridAdapter fans out to two retrievers and fuses with RRF (k=60).
type hybridAdapter struct {
	a, b eval.Retriever
}

func (h *hybridAdapter) Search(ctx context.Context, q string, k int) ([]string, error) {
	// Each retriever gets to vote on 2k candidates so RRF has room to work.
	cand := k * 2
	if cand < 10 {
		cand = 10
	}
	ra, errA := h.a.Search(ctx, q, cand)
	if errA != nil {
		return nil, errA
	}
	rb, errB := h.b.Search(ctx, q, cand)
	if errB != nil {
		return nil, errB
	}
	return index.RRF([][]string{ra, rb}, k, 60), nil
}

// rerankAdapter wraps any Retriever with a reranker. The inner retriever is
// asked for `candidateK` candidates; the reranker reorders them; the top k come back.
// textByURL provides the passage text the reranker reads.
type rerankAdapter struct {
	inner      eval.Retriever
	reranker   rerank.Reranker
	textByURL  map[string]string
	candidateK int
}

func (a *rerankAdapter) Search(ctx context.Context, q string, k int) ([]string, error) {
	candK := a.candidateK
	if candK <= 0 || candK < k {
		candK = k * 5
	}
	urls, err := a.inner.Search(ctx, q, candK)
	if err != nil {
		return nil, err
	}
	if len(urls) <= 1 {
		return urls, nil
	}
	cands := make([]rerank.Candidate, 0, len(urls))
	for _, u := range urls {
		text := a.textByURL[u]
		if text == "" {
			text = u
		}
		cands = append(cands, rerank.Candidate{ID: u, Text: text})
	}
	reranked, err := a.reranker.Rerank(ctx, q, cands)
	if err != nil {
		return urls, nil // fall back to inner order
	}
	if len(reranked) > k {
		reranked = reranked[:k]
	}
	return reranked, nil
}

func runEval(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("eval", flag.ExitOnError)
	corpusPath := fs.String("corpus", "testdata/eval/corpus.json", "corpus JSON")
	queriesPath := fs.String("queries", "testdata/eval/queries.json", "queries JSON")
	savePath := fs.String("save", "", "save run summary")
	baselinePath := fs.String("baseline", "", "compare against a prior summary")
	retriever := fs.String("retriever", "bm25", "retriever: bm25 | dense | hybrid")
	backend := fs.String("backend", "sqlite", "BM25 index backend: sqlite | pebble (bm25 retriever only — gates production-path changes like the top-k pool)")
	embModel := fs.String("embed-model", "text-embedding-3-small", "embedding model name")
	embURL := fs.String("embed-url", "", "embedding endpoint URL (default OpenAI)")
	embDim := fs.Int("embed-dim", 1536, "embedding dimensionality")
	embCacheDir := fs.String("embed-cache", "./eval-embed-cache", "embedding cache dir (set empty to disable)")
	useRerank := fs.Bool("rerank", false, "wrap retriever with the LLM listwise reranker")
	rerankModel := fs.String("rerank-model", "gpt-4o-mini", "chat model for the reranker")
	candK := fs.Int("rerank-k", 20, "candidates pulled from inner retriever before rerank")
	apiURL := fs.String("api", "", "if set, query the remote cosift HTTP API instead of building a local index")
	apiBearer := fs.String("api-bearer", "", "optional bearer token for the remote API")
	distractors := fs.Int("distractors", 0, "inject N synthetic distractor docs into the index before scoring (tests robustness vs an inflated candidate set)")
	distractorSeed := fs.Int64("distractor-seed", 42, "deterministic seed for distractor generation")
	distractorMode := fs.String("distractor-mode", "diverse", "diverse (90-word neutral pool) | narrow (single-topic 7-word pool)")
	embBatch := fs.Int("embed-batch", 256, "embedding batch size — lower for large-dim models (text-embedding-3-large returns malformed JSON at 1000)")
	chunkSize := fs.Int("chunk-size", 0, "passage chunker target words (0 = index.NewChunker default 320); A/B across runs to compare retrieval at different granularities")
	chunkOverlap := fs.Int("chunk-overlap", 0, "passage chunker overlap words (0 = default 64)")
	autoParaphrase := fs.Bool("auto-paraphrase", false, "generate N paraphrases per query via the chat client at eval time + RRF-fuse results")
	paraphraseN := fs.Int("paraphrase-n", 2, "number of paraphrases per query")
	paraphraseModel := fs.String("paraphrase-model", "gpt-4o-mini", "chat model for paraphrase generation")
	mainWeight := fs.Float64("main-weight", 0, "main-query weight in -auto-paraphrase OR -planner RRF fusion (paraphrases / sub-queries each weight 1.0); 0 = equal-weight (standard RRF); mirrors server-side Defaults.ExpandMainWeight for offline measurement")
	usePlanner := fs.Bool("planner", false, "decompose each query into 2-3 sub-queries via the /research planner prompt + RRF-fuse results (mirror /research retrieval, no synth)")
	plannerModel := fs.String("planner-model", "gpt-4o-mini", "chat model for the planner decomposition")
	if err := fs.Parse(args); err != nil {
		return err
	}

	qs, err := eval.LoadQuerySet(*queriesPath)
	if err != nil {
		return err
	}

	// --api short-circuit: run the queries straight against a deployed cosift's
	// /search endpoint. Skip the local corpus/embed/index work entirely —
	// the deployed instance owns its own index. The corpus.json's relevance
	// labels still apply because they're declared independently of the index.
	if *apiURL != "" {
		ret := &httpAPIRetriever{
			baseURL:   strings.TrimRight(*apiURL, "/"),
			bearer:    *apiBearer,
			retriever: *retriever,
			rerank:    *useRerank,
			http:      &http.Client{Timeout: 30 * time.Second},
		}
		summary, e := eval.Run(ctx, qs, ret)
		if e != nil {
			return e
		}
		summary.Name = fmt.Sprintf("%s/api(%s)", qs.Name, *apiURL)
		fmt.Print(eval.PrintTable(summary))
		if *baselinePath != "" {
			base, e := eval.LoadSummary(*baselinePath)
			if e != nil {
				return fmt.Errorf("baseline: %w", e)
			}
			fmt.Printf("\nvs baseline (%s):\n%s", *baselinePath, eval.Diff(base, summary))
		}
		if *savePath != "" {
			if e := eval.SaveSummary(summary, *savePath); e != nil {
				return fmt.Errorf("save: %w", e)
			}
			fmt.Printf("\nsaved summary to %s\n", *savePath)
		}
		return nil
	}

	corpus, err := eval.LoadCorpus(*corpusPath)
	if err != nil {
		return err
	}

	// Build an ephemeral store in a temp dir — eval doesn't touch the user's index.
	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("cosift-eval-%d", time.Now().UnixNano()))
	s, err := store.Open(tmpDir)
	if err != nil {
		return err
	}
	defer func() {
		s.Close()
		_ = os.RemoveAll(tmpDir)
	}()

	bm := index.NewBM25(s)
	for _, d := range corpus.Docs {
		id, e := s.UpsertDocument(ctx, &store.Document{
			URL: d.URL, Title: d.Title, Text: d.Text,
			Source: "eval", FetchedAt: time.Now(),
		})
		if e != nil {
			return fmt.Errorf("ingest %s: %w", d.URL, e)
		}
		if e := bm.IndexDocument(ctx, id, d.Title, d.Text); e != nil {
			return fmt.Errorf("index %s: %w", d.URL, e)
		}
	}

	// BM25 distractor injection: synthetic noise to stress-test retrieval at
	// higher index sizes than the 20-doc canonical set. Each distractor is 100
	// random tokens from a neutral vocab disjoint from the eval queries' vocab —
	// they should NEVER be relevant, so any showing up in top-K = measurable degradation.
	// Dense distractors are embedded alongside the corpus in the dense block below.
	var distractorTexts []string
	if *distractors > 0 {
		drng := rand.New(rand.NewSource(*distractorSeed))
		neutralVocab := neutralVocabForDistractors()
		// Narrow mode: clamp to the first topic group (pottery — 7 words).
		// Tests the hypothesis that smaller-but-more-similar noise
		// is harder for the reranker than larger-but-diverse noise.
		if *distractorMode == "narrow" {
			neutralVocab = neutralVocab[:7]
		} else if *distractorMode != "diverse" {
			return fmt.Errorf("unknown distractor-mode %q (diverse | narrow)", *distractorMode)
		}
		distractorTexts = make([]string, *distractors)
		for i := 0; i < *distractors; i++ {
			dURL := fmt.Sprintf("https://distractor.test/%d", i)
			dTitle := fmt.Sprintf("Distractor %d", i)
			dText := generateDistractorText(drng, neutralVocab, 100)
			distractorTexts[i] = dText
			id, e := s.UpsertDocument(ctx, &store.Document{
				URL: dURL, Title: dTitle, Text: dText, Source: "distractor", FetchedAt: time.Now(),
			})
			if e != nil {
				return fmt.Errorf("distractor %d: %w", i, e)
			}
			if e := bm.IndexDocument(ctx, id, dTitle, dText); e != nil {
				return fmt.Errorf("index distractor %d: %w", i, e)
			}
		}
		fmt.Printf("injected %d distractor docs into BM25 index\n", *distractors)
	}

	// Build the requested retriever.
	if *backend != "sqlite" && *backend != "pebble" {
		return fmt.Errorf("unknown backend %q (sqlite | pebble)", *backend)
	}
	if *backend == "pebble" && *retriever != "bm25" {
		return fmt.Errorf("-backend pebble only supports -retriever bm25")
	}
	var ret eval.Retriever
	switch *retriever {
	case "bm25":
		if *backend == "pebble" {
			ps, e := store.OpenPebble(filepath.Join(tmpDir, "pebble"))
			if e != nil {
				return e
			}
			defer ps.Close()
			pbm := index.NewPebbleBM25(ps)
			for _, d := range corpus.Docs {
				id, e := ps.UpsertDocument(ctx, &store.Document{
					URL: d.URL, Title: d.Title, Text: d.Text,
					Source: "eval", FetchedAt: time.Now(),
				})
				if e != nil {
					return fmt.Errorf("pebble ingest %s: %w", d.URL, e)
				}
				if e := pbm.IndexDocument(ctx, id, d.Title, d.Text); e != nil {
					return fmt.Errorf("pebble index %s: %w", d.URL, e)
				}
			}
			for i, dText := range distractorTexts {
				dURL := fmt.Sprintf("https://distractor.test/%d", i)
				dTitle := fmt.Sprintf("Distractor %d", i)
				id, e := ps.UpsertDocument(ctx, &store.Document{
					URL: dURL, Title: dTitle, Text: dText, Source: "distractor", FetchedAt: time.Now(),
				})
				if e != nil {
					return fmt.Errorf("pebble distractor %d: %w", i, e)
				}
				if e := pbm.IndexDocument(ctx, id, dTitle, dText); e != nil {
					return fmt.Errorf("pebble index distractor %d: %w", i, e)
				}
			}
			ret = &pebbleBM25Adapter{inner: pbm}
			break
		}
		ret = &bm25Adapter{inner: bm}
	case "dense", "hybrid":
		apiKey := os.Getenv("OPENAI_API_KEY")
		if apiKey == "" {
			apiKey = os.Getenv("OPENAI") // accept the short form too
		}
		if apiKey == "" {
			return errors.New("OPENAI_API_KEY (or OPENAI) not set (put it in .env)")
		}
		oai := embed.NewOpenAIClient(apiKey, *embURL, *embModel, *embDim)
		var emb embed.Embedder = oai
		if *embCacheDir != "" {
			emb = embed.NewCachedEmbedder(oai, *embCacheDir)
		}
		// Chunk corpus into passage windows, then batch-embed.
		// Same chunker the crawler uses → eval and serve behave identically.
		chunker := chunkerWith(*chunkSize, *chunkOverlap)
		type passageRef struct {
			docIdx int
			chunk  index.Chunk
		}
		var allTexts []string
		var refs []passageRef
		for i, d := range corpus.Docs {
			text := d.Title + "\n\n" + d.Text
			for _, c := range chunker.Chunk(text) {
				allTexts = append(allTexts, c.Text)
				refs = append(refs, passageRef{docIdx: i, chunk: c})
			}
		}
		fmt.Printf("embedding %d passages across %d docs with %s (batch=%d)...\n", len(allTexts), len(corpus.Docs), *embModel, *embBatch)
		vecs, e := batchEmbed(ctx, emb, allTexts, *embBatch)
		if e != nil {
			return fmt.Errorf("embed corpus: %w", e)
		}
		vi := index.NewVectorIndex(*embDim)
		for j, v := range vecs {
			d := corpus.Docs[refs[j].docIdx]
			vi.AddPassage(d.URL, d.Title, refs[j].chunk.Offset, refs[j].chunk.Length, v)
		}
		// Embed + add distractor passages into the SAME vector index.
		// batchEmbed splits into ≤1000-input chunks for provider limits.
		if len(distractorTexts) > 0 {
			fmt.Printf("embedding %d distractor passages (batch=%d)...\n", len(distractorTexts), *embBatch)
			dvecs, err := batchEmbed(ctx, emb, distractorTexts, *embBatch)
			if err != nil {
				return fmt.Errorf("embed distractors: %w", err)
			}
			for i, v := range dvecs {
				vi.AddPassage(fmt.Sprintf("https://distractor.test/%d", i), "Distractor", 0, len(distractorTexts[i]), v)
			}
		}
		denseRet := &denseAdapter{idx: vi, emb: emb}
		if *retriever == "dense" {
			ret = denseRet
		} else {
			ret = &hybridAdapter{a: &bm25Adapter{inner: bm}, b: denseRet}
		}
	default:
		return fmt.Errorf("unknown retriever %q", *retriever)
	}

	// Apply paraphrase + rerank wrappers below.
	suffix := *retriever
	if *backend == "pebble" {
		suffix += "-pebble"
	}
	if *useRerank {
		apiKey := os.Getenv("OPENAI_API_KEY")
		if apiKey == "" {
			apiKey = os.Getenv("OPENAI")
		}
		if apiKey == "" {
			return errors.New("rerank: OPENAI_API_KEY (or OPENAI) not set")
		}
		chat := embed.NewOpenAIChat(apiKey, "", *rerankModel)
		rr := rerank.NewLLMReranker(chat)
		textByURL := make(map[string]string, len(corpus.Docs))
		for _, d := range corpus.Docs {
			textByURL[d.URL] = d.Title + "\n\n" + d.Text
		}
		ret = &rerankAdapter{inner: ret, reranker: rr, textByURL: textByURL, candidateK: *candK}
		suffix = fmt.Sprintf("%s+rerank(%s)", *retriever, *rerankModel)
	}

	// Planner wrapper — applied BEFORE paraphrase so a "both" run looks like
	// (rerank ⊂ planner ⊂ paraphrase). Mirrors /research's plan→retrieve pipeline
	// without the synth step, so we measure planner *retrieval coverage* in isolation.
	if *usePlanner {
		apiKey := os.Getenv("OPENAI_API_KEY")
		if apiKey == "" {
			apiKey = os.Getenv("OPENAI")
		}
		if apiKey == "" {
			return errors.New("planner: OPENAI_API_KEY (or OPENAI) not set")
		}
		chat := embed.NewOpenAIChat(apiKey, "", *plannerModel)
		ret = &plannerRetriever{inner: ret, chat: chat, mainWeight: *mainWeight, cache: make(map[string][]string)}
		if *mainWeight > 0 {
			suffix = fmt.Sprintf("%s+planner(%s,mw=%.2f)", suffix, *plannerModel, *mainWeight)
		} else {
			suffix = fmt.Sprintf("%s+planner(%s)", suffix, *plannerModel)
		}
	}

	// Auto-paraphrase wrapper — outermost so paraphrases hit the FULL retriever
	// stack (including rerank, if enabled).
	if *autoParaphrase {
		apiKey := os.Getenv("OPENAI_API_KEY")
		if apiKey == "" {
			apiKey = os.Getenv("OPENAI")
		}
		if apiKey == "" {
			return errors.New("auto-paraphrase: OPENAI_API_KEY (or OPENAI) not set")
		}
		chat := embed.NewOpenAIChat(apiKey, "", *paraphraseModel)
		ret = &paraphraseRetriever{inner: ret, chat: chat, n: *paraphraseN, mainWeight: *mainWeight, cache: make(map[string][]string)}
		if *mainWeight > 0 {
			suffix = fmt.Sprintf("%s+paraphrase(%d/%s,mw=%.2f)", suffix, *paraphraseN, *paraphraseModel, *mainWeight)
		} else {
			suffix = fmt.Sprintf("%s+paraphrase(%d/%s)", suffix, *paraphraseN, *paraphraseModel)
		}
	}

	summary, err := eval.Run(ctx, qs, ret)
	if err != nil {
		return err
	}
	summary.Name = fmt.Sprintf("%s/%s", qs.Name, suffix)
	fmt.Print(eval.PrintTable(summary))

	if *baselinePath != "" {
		base, err := eval.LoadSummary(*baselinePath)
		if err != nil {
			return fmt.Errorf("baseline: %w", err)
		}
		fmt.Printf("\nvs baseline (%s):\n%s", *baselinePath, eval.Diff(base, summary))
	}
	if *savePath != "" {
		if err := eval.SaveSummary(summary, *savePath); err != nil {
			return fmt.Errorf("save: %w", err)
		}
		fmt.Printf("\nsaved summary to %s\n", *savePath)
	}
	return nil
}

// runBench measures search latency under synthetic load. No API calls, no disk
// for the vector path (in-memory only); BM25 uses a temp SQLite.
//
// Use it to decide:
//   - When to swap brute-force kNN for HNSW (when vector p50 > ~50 ms or
//     p99 > ~200 ms at your target N).
//   - When BM25 needs Tantivy or another inverted-index lib (when BM25 p50
//     exceeds a comparable threshold).
//
// benchResult holds one mode's bench output. Each field maps to a JSON key
// when `-json` is set; human-readable output uses the formatHuman method.
type benchResult struct {
	Mode           string  `json:"mode"`
	N              int     `json:"n"`
	Dim            int     `json:"dim,omitempty"`
	Queries        int     `json:"queries,omitempty"`
	P50Micros      int64   `json:"p50_us,omitempty"`
	P95Micros      int64   `json:"p95_us,omitempty"`
	P99Micros      int64   `json:"p99_us,omitempty"`
	QPS            float64 `json:"qps,omitempty"`
	ElapsedMicros  int64   `json:"elapsed_us,omitempty"`
	Docs           int64   `json:"docs,omitempty"`
	PagesPerSec    float64 `json:"pages_per_sec,omitempty"`
	Terms          int64   `json:"terms,omitempty"`
	PerHostDelayMs int     `json:"per_host_delay_ms,omitempty"`
}

func runBench(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	n := fs.Int("n", 10000, "number of passages (for vector/bm25 modes) or pages (for crawl mode)")
	dim := fs.Int("dim", 384, "embedding dimensionality (vector mode)")
	queries := fs.Int("queries", 100, "queries per run (vector/bm25 modes)")
	mode := fs.String("mode", "vector", "vector | bm25 | crawl | all")
	perHostDelay := fs.Int("per-host-delay", 0, "per-host delay in ms for crawl mode (default 0 — localhost throughput; set 100-1000 to measure under realistic politeness)")
	jsonOut := fs.Bool("json", false, "emit one JSON object per mode instead of a human-readable line — useful for CI ingestion + dashboards")
	if err := fs.Parse(args); err != nil {
		return err
	}

	emit := func(r *benchResult) {
		if *jsonOut {
			b, _ := json.Marshal(r)
			fmt.Println(string(b))
		} else {
			fmt.Println(r.formatHuman())
		}
	}

	if *mode == "vector" || *mode == "all" || *mode == "both" {
		emit(benchVector(*n, *dim, *queries))
	}
	if *mode == "bm25" || *mode == "all" || *mode == "both" {
		r, err := benchBM25(ctx, *n, *queries)
		if err != nil {
			return err
		}
		emit(r)
	}
	if *mode == "crawl" || *mode == "all" {
		// Crawl mode uses a smaller default — N=10k pages of an in-process site
		// would slow the bench without proportional signal. 200 is plenty to
		// measure pages/sec accurately on a typical host.
		crawlN := *n
		if crawlN > 1000 {
			crawlN = 200
		}
		r, err := benchCrawl(ctx, crawlN, *perHostDelay)
		if err != nil {
			return err
		}
		emit(r)
	}
	if *mode == "storage" || *mode == "all" {
		// SQLite vs Pebble head-to-head. Same N synthetic docs, same
		// K queries, two backends. Emit one result per backend; consumers diff
		// the two lines (or use cosift bench-compare on the saved JSON).
		sq, err := benchBM25SQLite(ctx, *n, *queries)
		if err != nil {
			return err
		}
		emit(sq)
		pb, err := benchBM25Pebble(ctx, *n, *queries)
		if err != nil {
			return err
		}
		emit(pb)
	}
	return nil
}

// formatHuman renders the result as the one-line format the bench used in iter
// 1–70 — preserves the existing UX when `-json` isn't set.
func (r *benchResult) formatHuman() string {
	switch r.Mode {
	case "vector":
		return fmt.Sprintf("vector  n=%d dim=%d  p50=%-9s p95=%-9s p99=%-9s qps=%.0f",
			r.N, r.Dim,
			time.Duration(r.P50Micros)*time.Microsecond,
			time.Duration(r.P95Micros)*time.Microsecond,
			time.Duration(r.P99Micros)*time.Microsecond,
			r.QPS)
	case "bm25":
		return fmt.Sprintf("bm25    n=%d  p50=%-9s p95=%-9s p99=%-9s qps=%.0f",
			r.N,
			time.Duration(r.P50Micros)*time.Microsecond,
			time.Duration(r.P95Micros)*time.Microsecond,
			time.Duration(r.P99Micros)*time.Microsecond,
			r.QPS)
	case "crawl":
		delay := ""
		if r.PerHostDelayMs > 0 {
			delay = fmt.Sprintf(" per-host-delay=%dms", r.PerHostDelayMs)
		}
		return fmt.Sprintf("crawl   n=%-5d elapsed=%-10s docs=%-5d  pages/sec=%-6.0f  terms=%d%s",
			r.N,
			(time.Duration(r.ElapsedMicros) * time.Microsecond).Round(time.Millisecond),
			r.Docs, r.PagesPerSec, r.Terms, delay)
	}
	return fmt.Sprintf("%+v", *r)
}

// loadBenchReport parses a `cosift bench -json` output file. Format: one
// benchResult JSON object per line (NDJSON). Lines that don't parse as JSON
// are skipped silently — this lets users pipe `bench -json` into a file
// alongside other tool output if they want.
func loadBenchReport(path string) (map[string]*benchResult, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	out := make(map[string]*benchResult)
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || !strings.HasPrefix(line, "{") {
			continue
		}
		var r benchResult
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			continue // skip unparseable lines rather than failing the whole report
		}
		if r.Mode == "" {
			continue
		}
		out[r.Mode] = &r
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s: no parseable bench records (expecting NDJSON from `cosift bench -json`)", path)
	}
	return out, nil
}

// runBenchCompare diffs two saved bench reports side-by-side. Mirrors
// answer-eval-compare for the bench surface — locks the JSON contract
// as a stable interface tools can rely on.
//
// For each mode present in either report, prints the absolute numbers + deltas.
// Reports a mode as "missing in BASELINE" or "missing in NEW" rather than
// silently skipping — operators benefit from knowing the compare wasn't
// fully apples-to-apples.
func runBenchCompare(args []string) error {
	fs := flag.NewFlagSet("bench-compare", flag.ExitOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) != 2 {
		return errors.New("usage: cosift bench-compare BASELINE.json NEW.json")
	}
	a, err := loadBenchReport(rest[0])
	if err != nil {
		return fmt.Errorf("baseline: %w", err)
	}
	b, err := loadBenchReport(rest[1])
	if err != nil {
		return fmt.Errorf("new: %w", err)
	}
	fmt.Printf("BASELINE: %s  (modes: %v)\n", rest[0], sortedModes(a))
	fmt.Printf("NEW:      %s  (modes: %v)\n", rest[1], sortedModes(b))
	fmt.Println()

	// Compare any mode present in either report. Stable order: vector, bm25, crawl.
	allModes := union(a, b)
	for _, mode := range allModes {
		ar := a[mode]
		br := b[mode]
		switch {
		case ar == nil:
			fmt.Printf("%-7s (missing in BASELINE — new in this run)\n", mode)
		case br == nil:
			fmt.Printf("%-7s (missing in NEW — present in BASELINE only)\n", mode)
		default:
			printBenchDelta(mode, ar, br)
		}
	}
	return nil
}

func sortedModes(m map[string]*benchResult) []string {
	order := []string{"vector", "bm25", "crawl"}
	out := make([]string, 0, len(m))
	for _, k := range order {
		if _, ok := m[k]; ok {
			out = append(out, k)
		}
	}
	// Catch any modes outside the known order too.
	for k := range m {
		if k != "vector" && k != "bm25" && k != "crawl" {
			out = append(out, k)
		}
	}
	return out
}

func union(a, b map[string]*benchResult) []string {
	seen := make(map[string]bool)
	order := []string{"vector", "bm25", "crawl"}
	out := []string{}
	for _, k := range order {
		if _, ok := a[k]; ok {
			seen[k] = true
			out = append(out, k)
		}
		if _, ok := b[k]; ok && !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	for _, m := range []map[string]*benchResult{a, b} {
		for k := range m {
			if !seen[k] {
				seen[k] = true
				out = append(out, k)
			}
		}
	}
	return out
}

// printBenchDelta prints one mode's before/after side-by-side. Format
// matches answer-eval-compare's row style.
func printBenchDelta(mode string, a, b *benchResult) {
	switch mode {
	case "vector", "bm25":
		// QPS is the headline; latency percentiles are signal too.
		fmt.Printf("%-7s n=%d→%d  qps %.0f→%.0f (%+.0f)  p50 %dµs→%dµs (%+dµs)  p95 %dµs→%dµs (%+dµs)\n",
			mode, a.N, b.N,
			a.QPS, b.QPS, b.QPS-a.QPS,
			a.P50Micros, b.P50Micros, b.P50Micros-a.P50Micros,
			a.P95Micros, b.P95Micros, b.P95Micros-a.P95Micros,
		)
	case "crawl":
		fmt.Printf("%-7s n=%d→%d  pages/sec %.0f→%.0f (%+.0f)  elapsed %dms→%dms  docs %d→%d\n",
			mode, a.N, b.N,
			a.PagesPerSec, b.PagesPerSec, b.PagesPerSec-a.PagesPerSec,
			a.ElapsedMicros/1000, b.ElapsedMicros/1000,
			a.Docs, b.Docs,
		)
	default:
		fmt.Printf("%-7s (unknown mode — raw: a=%+v b=%+v)\n", mode, *a, *b)
	}
}

func benchVector(n, dim, queries int) *benchResult {
	rng := newSeededRand()
	vi := index.NewVectorIndex(dim)
	for i := 0; i < n; i++ {
		v := randUnit(rng, dim)
		vi.AddPassage(fmt.Sprintf("https://test/p%d", i), "doc", i*100, 100, v)
	}
	// Time `queries` searches.
	q := make([][]float32, queries)
	for i := range q {
		q[i] = randUnit(rng, dim)
	}
	lats := make([]time.Duration, queries)
	for i, qv := range q {
		start := time.Now()
		_ = vi.Search(context.Background(), qv, 10)
		lats[i] = time.Since(start)
	}
	p50, p95, p99 := percentiles(lats)
	return &benchResult{
		Mode: "vector", N: n, Dim: dim, Queries: queries,
		P50Micros: p50.Microseconds(), P95Micros: p95.Microseconds(), P99Micros: p99.Microseconds(),
		QPS: float64(queries) / sumDur(lats).Seconds(),
	}
}

func benchBM25(ctx context.Context, n, queries int) (*benchResult, error) {
	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("cosift-bench-%d", time.Now().UnixNano()))
	s, err := store.Open(tmpDir)
	if err != nil {
		return nil, err
	}
	defer func() {
		s.Close()
		_ = os.RemoveAll(tmpDir)
	}()
	bm := index.NewBM25(s)

	rng := newSeededRand()
	vocab := []string{"alpha", "beta", "gamma", "delta", "epsilon", "go", "rust", "python",
		"distributed", "consensus", "raft", "paxos", "quantum", "entanglement", "cell",
		"mitochondria", "roman", "empire", "pacific", "midway", "tennis", "soccer",
		"bitcoin", "blockchain", "transformer", "attention", "model", "vector", "index"}
	// Build N synthetic docs of ~50 random vocab words each.
	for i := 0; i < n; i++ {
		var sb strings.Builder
		for j := 0; j < 50; j++ {
			if j > 0 {
				sb.WriteByte(' ')
			}
			sb.WriteString(vocab[rng.Intn(len(vocab))])
		}
		id, err := s.UpsertDocument(ctx, &store.Document{
			URL: fmt.Sprintf("https://test/d%d", i), Title: vocab[rng.Intn(len(vocab))],
			Text: sb.String(), Source: "bench", FetchedAt: time.Now(),
		})
		if err != nil {
			return nil, err
		}
		if err := bm.IndexDocument(ctx, id, "", sb.String()); err != nil {
			return nil, err
		}
	}

	// Random queries of 2-4 vocab words.
	qstrs := make([]string, queries)
	for i := range qstrs {
		k := 2 + rng.Intn(3)
		var sb strings.Builder
		for j := 0; j < k; j++ {
			if j > 0 {
				sb.WriteByte(' ')
			}
			sb.WriteString(vocab[rng.Intn(len(vocab))])
		}
		qstrs[i] = sb.String()
	}

	lats := make([]time.Duration, queries)
	for i, q := range qstrs {
		start := time.Now()
		_, err := bm.Search(ctx, q, 10)
		if err != nil {
			return nil, err
		}
		lats[i] = time.Since(start)
	}
	p50, p95, p99 := percentiles(lats)
	return &benchResult{
		Mode: "bm25", N: n, Queries: queries,
		P50Micros: p50.Microseconds(), P95Micros: p95.Microseconds(), P99Micros: p99.Microseconds(),
		QPS: float64(queries) / sumDur(lats).Seconds(),
	}, nil
}

// benchBM25SQLite is an alias of benchBM25 with an explicit "bm25-sqlite" Mode
// label so storage-mode output is unambiguous when both backends are emitted.
func benchBM25SQLite(ctx context.Context, n, queries int) (*benchResult, error) {
	r, err := benchBM25(ctx, n, queries)
	if err != nil {
		return nil, err
	}
	r.Mode = "bm25-sqlite"
	return r, nil
}

// benchBM25Pebble mirrors benchBM25 but runs against PebbleStore + PebbleBM25.
// Deterministic vocab + RNG seed so SQLite and Pebble runs produce comparable
// numbers (same N docs, same K queries, same query distribution).
// empirical validation of the path-2 storage rework.
func benchBM25Pebble(ctx context.Context, n, queries int) (*benchResult, error) {
	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("cosift-pebble-bench-%d", time.Now().UnixNano()))
	ps, err := store.OpenPebble(tmpDir)
	if err != nil {
		return nil, err
	}
	defer func() {
		ps.Close()
		_ = os.RemoveAll(tmpDir)
	}()
	bm := index.NewPebbleBM25(ps)

	rng := newSeededRand()
	vocab := []string{"alpha", "beta", "gamma", "delta", "epsilon", "go", "rust", "python",
		"distributed", "consensus", "raft", "paxos", "quantum", "entanglement", "cell",
		"mitochondria", "roman", "empire", "pacific", "midway", "tennis", "soccer",
		"bitcoin", "blockchain", "transformer", "attention", "model", "vector", "index"}
	for i := 0; i < n; i++ {
		var sb strings.Builder
		for j := 0; j < 50; j++ {
			if j > 0 {
				sb.WriteByte(' ')
			}
			sb.WriteString(vocab[rng.Intn(len(vocab))])
		}
		id, err := ps.UpsertDocument(ctx, &store.Document{
			URL: fmt.Sprintf("https://test/d%d", i), Title: vocab[rng.Intn(len(vocab))],
			Text: sb.String(), Source: "bench", FetchedAt: time.Now(),
		})
		if err != nil {
			return nil, err
		}
		if err := bm.IndexDocument(ctx, id, "", sb.String()); err != nil {
			return nil, err
		}
	}

	qstrs := make([]string, queries)
	for i := range qstrs {
		k := 2 + rng.Intn(3)
		var sb strings.Builder
		for j := 0; j < k; j++ {
			if j > 0 {
				sb.WriteByte(' ')
			}
			sb.WriteString(vocab[rng.Intn(len(vocab))])
		}
		qstrs[i] = sb.String()
	}

	lats := make([]time.Duration, queries)
	for i, q := range qstrs {
		start := time.Now()
		_, err := bm.Search(ctx, q, 10)
		if err != nil {
			return nil, err
		}
		lats[i] = time.Since(start)
	}
	p50, p95, p99 := percentiles(lats)
	return &benchResult{
		Mode: "bm25-pebble", N: n, Queries: queries,
		P50Micros: p50.Microseconds(), P95Micros: p95.Microseconds(), P99Micros: p99.Microseconds(),
		QPS: float64(queries) / sumDur(lats).Seconds(),
	}, nil
}

func newSeededRand() *rand.Rand {
	return rand.New(rand.NewSource(42)) // deterministic so runs are comparable
}

func randUnit(rng *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	var norm float64
	for i := range v {
		v[i] = float32(rng.NormFloat64())
		norm += float64(v[i]) * float64(v[i])
	}
	if norm == 0 {
		v[0] = 1
		return v
	}
	inv := float32(1.0 / sqrt(norm))
	for i := range v {
		v[i] *= inv
	}
	return v
}

func sqrt(x float64) float64 {
	// stdlib math import already pulled through other packages; just do it here.
	z := x / 2
	for i := 0; i < 20; i++ {
		if z == 0 {
			break
		}
		z = (z + x/z) / 2
	}
	return z
}

// benchCrawl measures end-to-end crawler throughput against an in-process
// website with N linked synthetic pages. Backs up the project directive's
// "lightweight, on-par with modern crawlers" claim with measured pages/sec.
//
// The fixture: an httptest server serving N pages, each linking to the next
// few via <a href="/pN+1">. The crawler is configured with per_host_delay=0
// (we're hitting localhost), max_concurrent=8, robots disabled (would add a
// constant ~1 fetch of /robots.txt unrelated to the throughput question).
func benchCrawl(ctx context.Context, n, perHostDelayMs int) (*benchResult, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Parse "/pN" from request path.
		if !strings.HasPrefix(r.URL.Path, "/p") {
			http.NotFound(w, r)
			return
		}
		idx, err := strconv.Atoi(r.URL.Path[2:])
		if err != nil || idx < 0 || idx >= n {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, `<!doctype html><html><head><title>Page %d</title></head><body><h1>Page %d</h1>`, idx, idx)
		fmt.Fprintf(w, `<p>Synthetic content for page %d. The crawler follows links from this page to nearby pages, building up the index.</p>`, idx)
		// Link to next 3 pages so the crawler has a graph to walk.
		for j := idx + 1; j < idx+4 && j < n; j++ {
			fmt.Fprintf(w, `<a href="/p%d">page %d</a> `, j, j)
		}
		fmt.Fprint(w, `</body></html>`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("cosift-bench-crawl-%d", time.Now().UnixNano()))
	s, err := store.Open(tmpDir)
	if err != nil {
		return nil, err
	}
	defer func() {
		s.Close()
		_ = os.RemoveAll(tmpDir)
	}()

	cfg := config.Default().Crawler
	// -per-host-delay propagates from the bench CLI flag. Default 0
	// (localhost throughput); set to 100–1000 to measure under realistic politeness.
	cfg.PerHostDelayMs = perHostDelayMs
	// Lower concurrency than the default 8 — bench output gets noisy with
	// SQLite BUSY contention messages at high parallelism, and the bench
	// metric is meant to be repeatable. Real deployments crawling many hosts
	// at once should keep the default 8.
	cfg.MaxConcurrent = 4
	cfg.MaxDepth = 100 // ensure no depth ceiling cuts the crawl short
	cfg.RespectRobots = false
	cfg.IncludeDomains = nil // accept the httptest host (port-bound)

	c := crawler.New(cfg, s)
	// Seed ALL pages — measures pure fetch+parse+index throughput, not the
	// crawler's link-discovery rate. Avoids the 1.5s "frontier empty"
	// terminator firing before link parsing has populated more work.
	for i := 0; i < n; i++ {
		if e := c.Seed(fmt.Sprintf("%s/p%d", srv.URL, i)); e != nil {
			return nil, fmt.Errorf("seed p%d: %w", i, e)
		}
	}

	start := time.Now()
	// Cap the run at 60s in case something goes wrong — bench should never hang.
	runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	if err := c.Run(runCtx); err != nil {
		return nil, fmt.Errorf("run: %w", err)
	}
	elapsed := time.Since(start)

	st, err := s.Stats(ctx)
	if err != nil {
		return nil, err
	}
	return &benchResult{
		Mode: "crawl", N: n,
		ElapsedMicros:  elapsed.Microseconds(),
		Docs:           st.Documents,
		PagesPerSec:    float64(st.Documents) / elapsed.Seconds(),
		Terms:          st.Terms,
		PerHostDelayMs: perHostDelayMs,
	}, nil
	// Note: the crawler's termination detector adds ~1.5s of polling at the
	// end of every run. For small N this dominates elapsed and the pages/sec
	// figure understates steady-state throughput. Larger N is more representative.
}

func percentiles(d []time.Duration) (p50, p95, p99 time.Duration) {
	if len(d) == 0 {
		return 0, 0, 0
	}
	sorted := make([]time.Duration, len(d))
	copy(sorted, d)
	// Simple sort — durations as int64 nanos.
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j] < sorted[j-1]; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	q := func(p float64) time.Duration {
		idx := int(float64(len(sorted)-1) * p)
		return sorted[idx]
	}
	return q(0.5), q(0.95), q(0.99)
}

func sumDur(d []time.Duration) time.Duration {
	var s time.Duration
	for _, x := range d {
		s += x
	}
	return s
}

// paraphraseRetriever wraps any Retriever with on-the-fly query expansion via
// a chat client. For each search, asks the LLM for N paraphrases, runs all of
// them through the inner retriever, RRF-fuses the result lists.
//
// measured that hand-written paraphrases recover 0.02 nDCG at 10k
// distractors. This is the auto-generated version — removes the hand-writing
// requirement and makes the prescription deployable. Per-query overhead:
// one extra chat call (~$0.0001 at gpt-4o-mini) + N extra inner-retriever calls.
//
// In-memory cache by exact query text — eval re-runs hit it; production /search
// calls don't (different process). For production, the cache would move into
// the SQLite store or a small Redis-shaped layer.
type paraphraseRetriever struct {
	inner      eval.Retriever
	chat       embed.ChatClient
	n          int
	mainWeight float64 // 0 = equal-weight RRF (pre); >0 = main query gets this weight, paraphrases each 1.0
	cache      map[string][]string
	mu         sync.Mutex
}

const paraphraseSystem = `Generate paraphrases of a search query. Each paraphrase preserves the semantic intent but uses different vocabulary — different keywords that a target document might also use. Output ONLY a JSON array of strings.
Example output for "go programming language": ["golang concurrent compiled language", "Google's systems programming language with goroutines"]`

func (p *paraphraseRetriever) generateParaphrases(ctx context.Context, q string) []string {
	p.mu.Lock()
	if cached, ok := p.cache[q]; ok {
		p.mu.Unlock()
		return cached
	}
	p.mu.Unlock()

	resp, err := p.chat.Chat(ctx, []embed.ChatMsg{
		{Role: "system", Content: paraphraseSystem},
		{Role: "user", Content: fmt.Sprintf("Generate %d paraphrases of: %s", p.n, q)},
	})
	if err != nil {
		return nil
	}
	// Same robust JSON-array parsing as the research planner.
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
	p.cache[q] = arr
	p.mu.Unlock()
	return arr
}

func (p *paraphraseRetriever) Search(ctx context.Context, q string, k int) ([]string, error) {
	main, err := p.inner.Search(ctx, q, k)
	if err != nil {
		return nil, err
	}
	paras := p.generateParaphrases(ctx, q)
	if len(paras) == 0 {
		return main, nil
	}
	lists := make([][]string, 0, 1+len(paras))
	lists = append(lists, main)
	for _, p2 := range paras {
		extra, err := p.inner.Search(ctx, p2, k)
		if err != nil {
			continue
		}
		lists = append(lists, extra)
	}
	// Matches the server expand
	// path's behavior so eval-time measurements use the same fusion
	// math as production.
	var weights []float64
	if p.mainWeight > 0 {
		weights = make([]float64, len(lists))
		weights[0] = p.mainWeight
		for i := 1; i < len(weights); i++ {
			weights[i] = 1.0
		}
	}
	return index.RRFWeighted(lists, weights, k, 60), nil
}

// plannerRetriever wraps any Retriever with the /research planner's
// decomposition step: ask the LLM for 2-3 focused sub-queries, run each through
// the inner retriever, RRF-fuse the results. Mirrors /research minus the synth.
//
// This exists so we can measure whether the planner's decomposition
// already subsumes paraphrase expansion — that claim was asserted in
// when we chose not to add ?expand=true to /research, but never measured.
const plannerSystemPrompt = `Decompose the user's research question into 2-3 focused sub-queries that, taken together, would cover the answer. Output ONLY a JSON array of strings — no prose, no markdown. Example: ["sub-query 1", "sub-query 2"]`

type plannerRetriever struct {
	inner      eval.Retriever
	chat       embed.ChatClient
	mainWeight float64 // 0 = equal-weight RRF; >0 = main query gets this weight, sub-queries each 1.0
	cache      map[string][]string
	mu         sync.Mutex
}

func (p *plannerRetriever) plan(ctx context.Context, q string) []string {
	p.mu.Lock()
	if cached, ok := p.cache[q]; ok {
		p.mu.Unlock()
		return cached
	}
	p.mu.Unlock()

	resp, err := p.chat.Chat(ctx, []embed.ChatMsg{
		{Role: "system", Content: plannerSystemPrompt},
		{Role: "user", Content: q},
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
	if len(arr) > 5 {
		arr = arr[:5] // hard cap matches /research planner
	}
	p.mu.Lock()
	p.cache[q] = arr
	p.mu.Unlock()
	return arr
}

func (p *plannerRetriever) Search(ctx context.Context, q string, k int) ([]string, error) {
	main, err := p.inner.Search(ctx, q, k)
	if err != nil {
		return nil, err
	}
	subs := p.plan(ctx, q)
	if len(subs) == 0 {
		return main, nil
	}
	lists := make([][]string, 0, 1+len(subs))
	lists = append(lists, main)
	for _, sq := range subs {
		extra, err := p.inner.Search(ctx, sq, k)
		if err != nil {
			continue
		}
		lists = append(lists, extra)
	}
	// Same shape as
	// paraphraseRetriever; main query gets mainWeight, sub-queries each 1.0.
	var weights []float64
	if p.mainWeight > 0 {
		weights = make([]float64, len(lists))
		weights[0] = p.mainWeight
		for i := 1; i < len(weights); i++ {
			weights[i] = 1.0
		}
	}
	return index.RRFWeighted(lists, weights, k, 60), nil
}

// httpAPIRetriever queries a deployed cosift's /search endpoint and adapts the
// JSON response to eval.Retriever. Lets `cosift eval -api <url>` validate a
// remote deployment with the same metric harness used for local development.
type httpAPIRetriever struct {
	baseURL   string
	bearer    string
	retriever string
	rerank    bool
	http      *http.Client
}

func (r *httpAPIRetriever) Search(ctx context.Context, q string, k int) ([]string, error) {
	v := url.Values{}
	v.Set("q", q)
	v.Set("k", strconv.Itoa(k))
	if r.retriever != "" {
		v.Set("retriever", r.retriever)
	}
	if r.rerank {
		v.Set("rerank", "true")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.baseURL+"/search?"+v.Encode(), http.NoBody)
	if err != nil {
		return nil, err
	}
	if r.bearer != "" {
		req.Header.Set("Authorization", "Bearer "+r.bearer)
	}
	resp, err := r.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("api %d: %s", resp.StatusCode, body)
	}
	var sr struct {
		Hits []struct {
			URL string `json:"url"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	urls := make([]string, len(sr.Hits))
	for i, h := range sr.Hits {
		urls[i] = h.URL
	}
	return urls, nil
}

// neutralVocabForDistractors returns words that don't appear in the eval queries
// or relevant docs, so any distractor returning in top-K is unambiguous noise.
// Picked from common-but-eval-disjoint domains (pottery, lighthouses, knitting,
// dentistry, beekeeping, etc.).
func neutralVocabForDistractors() []string {
	return []string{
		"kiln", "pottery", "glaze", "ceramic", "wheel", "stoneware", "earthenware",
		"lighthouse", "beacon", "keeper", "lantern", "rocky", "shoal", "fog",
		"yarn", "knit", "purl", "stitch", "skein", "needle", "cable",
		"dentistry", "molar", "incisor", "filling", "enamel", "gingival",
		"beekeeping", "hive", "queen", "drone", "wax", "pollen", "swarm",
		"bookbinding", "spine", "endpaper", "cloth", "headband", "signature",
		"masonry", "mortar", "trowel", "course", "wythe", "lintel", "soffit",
		"falconry", "raptor", "perch", "lure", "jess", "creance", "mews",
		"weaving", "warp", "weft", "shuttle", "heddle", "treadle", "selvedge",
		"smithing", "forge", "anvil", "tongs", "quench", "temper", "billet",
		"glassblowing", "punty", "marver", "blowpipe", "annealing", "cane",
		"watchmaking", "balance", "escapement", "mainspring", "barrel", "jewel",
		"taxidermy", "tan", "mount", "armature", "habitat", "diorama",
		"calligraphy", "nib", "ink", "stroke", "kerning", "ascender", "descender",
		"thatching", "reed", "spar", "ridge", "rafter", "eaves",
	}
}

// generateDistractorText assembles a short text out of the neutral vocab.
func generateDistractorText(rng *rand.Rand, vocab []string, words int) string {
	parts := make([]string, words)
	for i := range parts {
		parts[i] = vocab[rng.Intn(len(vocab))]
	}
	return strings.Join(parts, " ")
}

// runAnswerEval is's measurement of /research answer quality across
// the two retrieval strategies (planner vs paraphrase). Iters 52-53 measured
// *retrieval coverage* via nDCG and found paraphrase wins; added
// ?strategy=paraphrase but kept planner as the default *because we hadn't
// measured synthesis quality*. This subcommand fills that gap.
//
// Flow per query:
//  1. Build an in-memory cosift HTTP server with corpus ingested, dense index
//     built, chat + paraphraser configured.
//  2. Call /research?strategy=planner and /research?strategy=paraphrase.
//  3. Send (question, answer, sources) to a judge LLM with a different model.
//  4. Parse two scores: coverage (1-5) and grounding (1-5). Comments are kept
//     for the per-query report.
//  5. Aggregate means per strategy and print a comparison table.
//
// Cost note: ~$0.005 per query at gpt-4o-mini synth + gpt-4o judge for the
// 10 multi-faceted queries → roughly $0.10 total per run. Manageable but not
// free; the -dry-run flag prints the plan without LLM calls.
func runAnswerEval(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("answer-eval", flag.ExitOnError)
	corpusPath := fs.String("corpus", "testdata/eval/corpus.json", "corpus JSON")
	queriesPath := fs.String("queries", "testdata/eval/queries-multifaceted.json", "queries JSON (multi-faceted by default — these stress synthesis the most)")
	synthModel := fs.String("synth-model", "gpt-4o-mini", "chat model for /research synthesis + planner/paraphraser")
	judgeModel := fs.String("judge-model", "gpt-4o", "chat model for the judge (use a different + stronger model than synth to avoid self-preference)")
	savePath := fs.String("save", "", "save full per-query report as JSON")
	embModel := fs.String("embed-model", "text-embedding-3-small", "embedding model name")
	embDim := fs.Int("embed-dim", 1536, "embedding dimensionality")
	embCacheDir := fs.String("embed-cache", "./eval-embed-cache", "embedding cache dir (set empty to disable)")
	embURL := fs.String("embed-url", "", "embedding endpoint URL (leave empty for OpenAI default)")
	chatURL := fs.String("chat-url", "", "chat endpoint URL for synth+judge models")
	answerChunkSize := fs.Int("chunk-size", 0, "passage chunker target words (0 = default 320)")
	answerChunkOverlap := fs.Int("chunk-overlap", 0, "passage chunker overlap words (0 = default 64)")
	dryRun := fs.Bool("dry-run", false, "build the harness + print the plan, but issue NO LLM calls")
	useRerank := fs.Bool("rerank", false, "wire the LLM listwise reranker into the in-process server; tests whether rerank cuts the grounding=1 cases on single-doc")
	rerankModel := fs.String("rerank-model", "gpt-4o-mini", "chat model for the reranker (kept default = synth-model so cost stays predictable)")
	synthK := fs.Int("synth-k", 0, "cap synth-source count via WithDefaults({ResearchSynthK: N}); 0 = preserve built-in default (10). tests whether K=5 trades coverage for grounding on single-doc workloads.")
	serverURL := fs.String("server", "", "live server base URL (e.g. https://cosift.pilotprotocol.network); skips in-process build, corpus+embed flags ignored")
	if err := fs.Parse(args); err != nil {
		return err
	}

	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		apiKey = os.Getenv("OPENAI")
	}
	if apiKey == "" && *embURL == "" && *chatURL == "" && !*dryRun {
		return errors.New("OPENAI_API_KEY (or OPENAI) not set; pass -embed-url/-chat-url for a local endpoint, or -dry-run to inspect")
	}
	if apiKey == "" {
		apiKey = "local"
	}

	qs, err := eval.LoadQuerySet(*queriesPath)
	if err != nil {
		return fmt.Errorf("load queries: %w", err)
	}

	liveMode := *serverURL != ""
	fmt.Printf("answer-eval: %d queries × 2 strategies × judge calls\n", len(qs.Queries))
	if liveMode {
		fmt.Printf("server = %s   judge model = %s   dry-run = %v\n", *serverURL, *judgeModel, *dryRun)
	} else {
		fmt.Printf("synth model = %s   judge model = %s   rerank = %v   synth_k = %d   dry-run = %v\n", *synthModel, *judgeModel, *useRerank, *synthK, *dryRun)
	}
	if *dryRun {
		for _, q := range qs.Queries {
			fmt.Printf("  - %s  (relevant: %d docs)\n", q.Text, len(q.Relevant))
		}
		return nil
	}

	// researchBaseURL is the base for /research calls — either live server or httptest.
	var researchBaseURL string
	judge := embed.NewOpenAIChat(apiKey, *chatURL, *judgeModel)

	if liveMode {
		researchBaseURL = strings.TrimRight(*serverURL, "/")
	} else {
		corpus, err := eval.LoadCorpus(*corpusPath)
		if err != nil {
			return fmt.Errorf("load corpus: %w", err)
		}

		// Build an ephemeral store + indexes (in-memory style, but SQLite needs a dir).
		tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("cosift-answer-eval-%d", time.Now().UnixNano()))
		st, err := store.Open(tmpDir)
		if err != nil {
			return err
		}
		defer func() {
			st.Close()
			_ = os.RemoveAll(tmpDir)
		}()

		bm := index.NewBM25(st)
		oai := embed.NewOpenAIClient(apiKey, *embURL, *embModel, *embDim)
		var emb embed.Embedder = oai
		if *embCacheDir != "" {
			emb = embed.NewCachedEmbedder(oai, *embCacheDir)
		}
		chunker := chunkerWith(*answerChunkSize, *answerChunkOverlap)
		var allTexts []string
		for _, d := range corpus.Docs {
			id, err := st.UpsertDocument(ctx, &store.Document{
				URL: d.URL, Title: d.Title, Text: d.Text, Source: "answer-eval", FetchedAt: time.Now(),
			})
			if err != nil {
				return err
			}
			if err := bm.IndexDocument(ctx, id, d.Title, d.Text); err != nil {
				return err
			}
			text := d.Title + "\n\n" + d.Text
			for _, c := range chunker.Chunk(text) {
				allTexts = append(allTexts, c.Text)
			}
		}
		fmt.Printf("embedding %d passages across %d docs...\n", len(allTexts), len(corpus.Docs))
		vecs, err := batchEmbed(ctx, emb, allTexts, 256)
		if err != nil {
			return fmt.Errorf("embed: %w", err)
		}
		vi := index.NewVectorIndex(*embDim)
		idx := 0
		for _, d := range corpus.Docs {
			text := d.Title + "\n\n" + d.Text
			for _, c := range chunker.Chunk(text) {
				if idx >= len(vecs) {
					break
				}
				vi.AddPassage(d.URL, d.Title, c.Offset, c.Length, vecs[idx])
				idx++
			}
		}

		chat := embed.NewOpenAIChat(apiKey, *chatURL, *synthModel)

		srv := server.New(st).
			WithVector(vi, emb).
			WithChat(chat).
			WithParaphraser(chat, 2).
			WithLLMLimiter(0, 0) // disable rate limiter — this is an in-process eval, not a public endpoint. caught this when the 10/min default 429'd after 5 queries.
		if *useRerank {
			rerankChat := chat
			if *rerankModel != *synthModel {
				rerankChat = embed.NewOpenAIChat(apiKey, "", *rerankModel)
			}
			srv = srv.WithReranker(rerank.NewLLMReranker(rerankChat), 20)
		}
		if *synthK > 0 {
			srv = srv.WithDefaults(server.Defaults{ResearchSynthK: *synthK})
		}
		httpSrv := httptest.NewServer(srv.Handler())
		defer httpSrv.Close()
		researchBaseURL = httpSrv.URL
	}

	type strategyReport struct {
		Strategy     string   `json:"strategy"`
		Answer       string   `json:"answer"`
		SourceURLs   []string `json:"source_urls"`
		Coverage     int      `json:"coverage"`
		Grounding    int      `json:"grounding"`
		JudgeComment string   `json:"judge_comment"`
	}
	type queryReport struct {
		Query   string           `json:"query"`
		Gold    []string         `json:"gold_relevant"`
		Reports []strategyReport `json:"reports"`
	}

	all := make([]queryReport, 0, len(qs.Queries))
	httpClient := &http.Client{Timeout: 120 * time.Second}

	for qi, q := range qs.Queries {
		fmt.Printf("\n[%d/%d] %s\n", qi+1, len(qs.Queries), q.Text)
		qr := queryReport{Query: q.Text, Gold: q.Relevant}

		for _, strategy := range []string{"planner", "paraphrase"} {
			// /research call.
			u := researchBaseURL + "/research?strategy=" + strategy + "&q=" + url.QueryEscape(q.Text)
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, http.NoBody)
			resp, err := httpClient.Do(req)
			if err != nil {
				fmt.Printf("  %s: research call failed: %v\n", strategy, err)
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				fmt.Printf("  %s: research returned %d: %s\n", strategy, resp.StatusCode, string(body))
				continue
			}
			var rr struct {
				Query    string   `json:"query"`
				Strategy string   `json:"strategy"`
				Plan     []string `json:"plan"`
				Answer   string   `json:"answer"`
				Sources  []struct {
					ID    int    `json:"id"`
					URL   string `json:"url"`
					Title string `json:"title"`
				} `json:"sources"`
			}
			if err := json.Unmarshal(body, &rr); err != nil {
				fmt.Printf("  %s: parse research: %v\n", strategy, err)
				continue
			}
			sourceURLs := make([]string, len(rr.Sources))
			for i, src := range rr.Sources {
				sourceURLs[i] = src.URL
			}
			fmt.Printf("  %-10s answer=%dchars sources=%d  plan=%v\n", strategy, len(rr.Answer), len(rr.Sources), rr.Plan)

			// Judge call.
			score, err := judgeAnswer(ctx, judge, q.Text, rr.Answer, rr.Sources)
			if err != nil {
				fmt.Printf("    judge failed: %v\n", err)
				continue
			}
			fmt.Printf("    judge: coverage=%d grounding=%d  — %s\n", score.Coverage, score.Grounding, score.Comment)
			qr.Reports = append(qr.Reports, strategyReport{
				Strategy:     strategy,
				Answer:       rr.Answer,
				SourceURLs:   sourceURLs,
				Coverage:     score.Coverage,
				Grounding:    score.Grounding,
				JudgeComment: score.Comment,
			})
		}
		all = append(all, qr)
	}

	// Aggregate. Exported field names matter — saved unexported agg
	// fields which JSON-marshalled to `{}`. Now the summary in the saved JSON
	// is actually inspectable by jq.
	type strategySummary struct {
		N             int     `json:"n"`
		MeanCoverage  float64 `json:"mean_coverage"`
		MeanGrounding float64 `json:"mean_grounding"`
		Combined      float64 `json:"combined"`
	}
	rawAcc := map[string]struct{ cov, grd, n int }{"planner": {}, "paraphrase": {}}
	for _, q := range all {
		for _, sr := range q.Reports {
			a, ok := rawAcc[sr.Strategy]
			if !ok {
				continue
			}
			a.cov += sr.Coverage
			a.grd += sr.Grounding
			a.n++
			rawAcc[sr.Strategy] = a
		}
	}
	summary := map[string]strategySummary{}
	for _, name := range []string{"planner", "paraphrase"} {
		a := rawAcc[name]
		s := strategySummary{N: a.n}
		if a.n > 0 {
			s.MeanCoverage = float64(a.cov) / float64(a.n)
			s.MeanGrounding = float64(a.grd) / float64(a.n)
			s.Combined = s.MeanCoverage + s.MeanGrounding
		}
		summary[name] = s
	}

	fmt.Println("\n=== summary ===")
	fmt.Printf("%-12s  N   mean coverage   mean grounding   combined\n", "strategy")
	for _, name := range []string{"planner", "paraphrase"} {
		s := summary[name]
		if s.N == 0 {
			fmt.Printf("%-12s  0   —                —                —\n", name)
			continue
		}
		fmt.Printf("%-12s  %d   %.2f            %.2f             %.2f\n", name, s.N, s.MeanCoverage, s.MeanGrounding, s.Combined)
	}

	if *savePath != "" {
		buf, _ := json.MarshalIndent(map[string]any{
			"synth_model": *synthModel,
			"judge_model": *judgeModel,
			"queries":     *queriesPath,
			"rerank":      *useRerank,
			"synth_k":     *synthK,
			"reports":     all,
			"summary":     summary,
			"when":        time.Now().UTC().Format(time.RFC3339),
		}, "", "  ")
		if err := os.WriteFile(*savePath, buf, 0o644); err != nil {
			return fmt.Errorf("save report: %w", err)
		}
		fmt.Printf("\nsaved full report to %s\n", *savePath)
	}
	return nil
}

// judgeScore is what we parse out of the judge LLM's JSON response.
type judgeScore struct {
	Coverage  int    `json:"coverage"`
	Grounding int    `json:"grounding"`
	Comment   string `json:"comment"`
}

// judgeSystemPrompt was rewritten in after the trace revealed
// the prior judge was evaluating the full source LIST relevance instead of the
// CITED sources' accuracy. That bias deflated grounding scores in proportion
// to how many tangential sources were in the list (paraphrase: 10, planner: 4-6).
// The new prompt is explicit + the harness pre-filters sources to only those
// the answer actually cites via [N].
const judgeSystemPrompt = `You are an expert judge evaluating a research answer's quality. Score on TWO dimensions, each on a 1-5 integer scale:

  - coverage: does the answer address every aspect of the question? (1 = misses major aspects, 5 = covers everything the question asked)
  - grounding: do the sources the answer CITES via [N] actually support the claims those citations are attached to? (1 = cited sources don't support the claims they're attached to, 5 = every cited source genuinely supports the claim it appears next to)

CRITICAL: Only consider sources the answer actually cites via [N]. Sources are listed by the IDs the answer used; uncited sources are not shown. Do not penalize the answer for not citing additional material.

Output ONLY a single JSON object with this exact shape:
{"coverage": <int 1-5>, "grounding": <int 1-5>, "comment": "<one sentence explaining the lower of the two scores>"}

Do not output anything else — no preamble, no code fences, no markdown.`

// citedSourceIDs extracts the integer ids referenced by [N] or [N,M,K] in the
// answer text. fix for the judge-bias bug — we now pass only the
// actually-cited sources to the judge, not the full source list.
func citedSourceIDs(answer string) map[int]bool {
	ids := make(map[int]bool)
	// Match [1] or [1,2] or [1, 2, 3] inside square brackets.
	for _, m := range citationRE.FindAllStringSubmatch(answer, -1) {
		// m[1] is the contents inside the brackets.
		for _, tok := range strings.Split(m[1], ",") {
			n, err := strconv.Atoi(strings.TrimSpace(tok))
			if err == nil {
				ids[n] = true
			}
		}
	}
	return ids
}

var citationRE = regexp.MustCompile(`\[([0-9, ]+)\]`)

// judgeAnswer asks the judge LLM to score a single (question, answer, sources)
// triple. pre-filters sources to only those cited in the answer, so
// the judge can't be confused by uncited tangential material in the list.
// Returns a judgeScore or an error.
func judgeAnswer(ctx context.Context, judge embed.ChatClient, question, answer string, sources []struct {
	ID    int    `json:"id"`
	URL   string `json:"url"`
	Title string `json:"title"`
}) (judgeScore, error) {
	cited := citedSourceIDs(answer)
	var sb strings.Builder
	for _, src := range sources {
		if !cited[src.ID] {
			continue // skip uncited sources — judge only evaluates citation accuracy
		}
		fmt.Fprintf(&sb, "[%d] %s (%s)\n", src.ID, src.Title, src.URL)
	}
	citedBlock := sb.String()
	if citedBlock == "" {
		citedBlock = "(the answer did not cite any sources via [N])\n"
	}
	userMsg := fmt.Sprintf("Question: %s\n\nSources actually cited by the answer:\n%s\nAnswer:\n%s\n\nScore this answer.", question, citedBlock, answer)
	raw, err := judge.Chat(ctx, []embed.ChatMsg{
		{Role: "system", Content: judgeSystemPrompt},
		{Role: "user", Content: userMsg},
	})
	if err != nil {
		return judgeScore{}, fmt.Errorf("judge call: %w", err)
	}
	raw = strings.TrimSpace(raw)
	for _, fence := range []string{"```json", "```"} {
		raw = strings.TrimPrefix(raw, fence)
		raw = strings.TrimSuffix(raw, "```")
	}
	raw = strings.TrimSpace(raw)
	startIdx := strings.Index(raw, "{")
	endIdx := strings.LastIndex(raw, "}")
	if startIdx < 0 || endIdx <= startIdx {
		return judgeScore{}, fmt.Errorf("judge: no JSON object in response: %q", raw)
	}
	var sc judgeScore
	if err := json.Unmarshal([]byte(raw[startIdx:endIdx+1]), &sc); err != nil {
		return judgeScore{}, fmt.Errorf("judge: parse JSON: %w (raw=%q)", err, raw)
	}
	if sc.Coverage < 1 || sc.Coverage > 5 || sc.Grounding < 1 || sc.Grounding > 5 {
		return judgeScore{}, fmt.Errorf("judge: scores out of 1-5 range: %+v", sc)
	}
	return sc, nil
}

// savedAnswerEvalReport mirrors the structure runAnswerEval writes. Used by
// runAnswerEvalCompare to load and diff two prior reports without re-running
// the eval. The shape is the same map[string]any savedJSON shape; this struct
// carries only the fields we need for comparison.
type savedAnswerEvalReport struct {
	SynthModel string `json:"synth_model"`
	JudgeModel string `json:"judge_model"`
	Queries    string `json:"queries"`
	Rerank     bool   `json:"rerank"`
	SynthK     int    `json:"synth_k"`
	When       string `json:"when"`
	Summary    map[string]struct {
		N             int     `json:"n"`
		MeanCoverage  float64 `json:"mean_coverage"`
		MeanGrounding float64 `json:"mean_grounding"`
		Combined      float64 `json:"combined"`
	} `json:"summary"`
	Reports []struct {
		Query   string `json:"query"`
		Reports []struct {
			Strategy     string `json:"strategy"`
			Coverage     int    `json:"coverage"`
			Grounding    int    `json:"grounding"`
			JudgeComment string `json:"judge_comment"`
		} `json:"reports"`
	} `json:"reports"`
}

func loadAnswerEvalReport(path string) (*savedAnswerEvalReport, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var r savedAnswerEvalReport
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	// Old reports had an empty summary because the agg struct used
	// unexported fields. Recompute the summary on the fly for those.
	if len(r.Summary) == 0 || r.Summary["planner"].N == 0 {
		r.Summary = recomputeSummary(r)
	}
	return &r, nil
}

func recomputeSummary(r savedAnswerEvalReport) map[string]struct {
	N             int     `json:"n"`
	MeanCoverage  float64 `json:"mean_coverage"`
	MeanGrounding float64 `json:"mean_grounding"`
	Combined      float64 `json:"combined"`
} {
	out := make(map[string]struct {
		N             int     `json:"n"`
		MeanCoverage  float64 `json:"mean_coverage"`
		MeanGrounding float64 `json:"mean_grounding"`
		Combined      float64 `json:"combined"`
	})
	for _, strategy := range []string{"planner", "paraphrase"} {
		var covSum, grdSum, n int
		for _, q := range r.Reports {
			for _, sr := range q.Reports {
				if sr.Strategy == strategy {
					covSum += sr.Coverage
					grdSum += sr.Grounding
					n++
				}
			}
		}
		s := out[strategy]
		s.N = n
		if n > 0 {
			s.MeanCoverage = float64(covSum) / float64(n)
			s.MeanGrounding = float64(grdSum) / float64(n)
			s.Combined = s.MeanCoverage + s.MeanGrounding
		}
		out[strategy] = s
	}
	return out
}

// runAnswerEvalCompare loads two saved answer-eval JSON reports and prints
// side-by-side coverage/grounding/combined deltas + per-query moves above a
// configurable threshold. No LLM calls; pure file-to-file diff —
// first-class replacement for the ad-hoc python one-liners operators
// previously chained for the same report.
func runAnswerEvalCompare(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("answer-eval-compare", flag.ExitOnError)
	threshold := fs.Int("query-threshold", 2, "report per-query moves of |Δgrounding| or |Δcoverage| ≥ this value (1-5 scale)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) != 2 {
		return errors.New("usage: cosift answer-eval-compare [-query-threshold N] BASELINE.json NEW.json")
	}
	a, err := loadAnswerEvalReport(rest[0])
	if err != nil {
		return fmt.Errorf("baseline: %w", err)
	}
	b, err := loadAnswerEvalReport(rest[1])
	if err != nil {
		return fmt.Errorf("new: %w", err)
	}
	// Header — record what we're comparing.
	fmt.Printf("BASELINE: %s\n", rest[0])
	fmt.Printf("  synth=%s judge=%s rerank=%v synth_k=%d queries=%s\n", a.SynthModel, a.JudgeModel, a.Rerank, a.SynthK, a.Queries)
	fmt.Printf("  when: %s\n", a.When)
	fmt.Printf("NEW:      %s\n", rest[1])
	fmt.Printf("  synth=%s judge=%s rerank=%v synth_k=%d queries=%s\n", b.SynthModel, b.JudgeModel, b.Rerank, b.SynthK, b.Queries)
	fmt.Printf("  when: %s\n", b.When)
	fmt.Println()

	// Summary deltas.
	fmt.Printf("%-12s   N     cov     grnd    comb   |   Δcov    Δgrnd   Δcomb\n", "strategy")
	fmt.Println("-----------------------------------------------------------------------")
	for _, strategy := range []string{"planner", "paraphrase"} {
		ab, bb := a.Summary[strategy], b.Summary[strategy]
		if ab.N == 0 && bb.N == 0 {
			fmt.Printf("%-12s   (not present in either report)\n", strategy)
			continue
		}
		fmt.Printf("%-12s %3d→%-3d %.2f→%.2f %.2f→%.2f %.2f→%.2f | %+.2f  %+.2f  %+.2f\n",
			strategy, ab.N, bb.N,
			ab.MeanCoverage, bb.MeanCoverage,
			ab.MeanGrounding, bb.MeanGrounding,
			ab.Combined, bb.Combined,
			bb.MeanCoverage-ab.MeanCoverage,
			bb.MeanGrounding-ab.MeanGrounding,
			bb.Combined-ab.Combined,
		)
	}

	// Per-query moves.
	if *threshold > 0 {
		// Index by (query, strategy) for fast lookup.
		aIdx := make(map[string]struct{ cov, grd int })
		for _, q := range a.Reports {
			for _, sr := range q.Reports {
				aIdx[q.Query+"\x00"+sr.Strategy] = struct{ cov, grd int }{sr.Coverage, sr.Grounding}
			}
		}
		type move struct {
			query, strategy string
			oldCov, oldGrd  int
			newCov, newGrd  int
		}
		var moves []move
		for _, q := range b.Reports {
			for _, sr := range q.Reports {
				ab, ok := aIdx[q.Query+"\x00"+sr.Strategy]
				if !ok {
					continue
				}
				dc := abs(sr.Coverage - ab.cov)
				dg := abs(sr.Grounding - ab.grd)
				if dc >= *threshold || dg >= *threshold {
					moves = append(moves, move{q.Query, sr.Strategy, ab.cov, ab.grd, sr.Coverage, sr.Grounding})
				}
			}
		}
		if len(moves) > 0 {
			fmt.Printf("\nPer-query moves with |Δ| ≥ %d:\n", *threshold)
			fmt.Printf("  %-10s  %-50s  cov   grnd\n", "strategy", "query")
			for _, m := range moves {
				fmt.Printf("  %-10s  %-50s  %d→%d  %d→%d\n",
					m.strategy, truncate(m.query, 50),
					m.oldCov, m.newCov,
					m.oldGrd, m.newGrd,
				)
			}
		} else {
			fmt.Printf("\nNo per-query moves with |Δ| ≥ %d.\n", *threshold)
		}
	}
	return nil
}
