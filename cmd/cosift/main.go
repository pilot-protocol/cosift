package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/calinteodor/cosift/internal/config"
	"github.com/calinteodor/cosift/internal/crawler"
	"github.com/calinteodor/cosift/internal/embed"
	"github.com/calinteodor/cosift/internal/eval"
	"github.com/calinteodor/cosift/internal/index"
	"github.com/calinteodor/cosift/internal/rerank"
	"github.com/calinteodor/cosift/internal/server"
	"github.com/calinteodor/cosift/internal/store"
)

const usage = `cosift — self-hostable search + research

usage:
  cosift init               write a sensible default cosift.json to ./
  cosift init -site URL     same, with include_domains pre-populated
  cosift serve              run the HTTP API (port from config)
  cosift crawl <url...>     one-shot crawl of seed URLs
  cosift check-robots <url...>   report whether each URL is crawlable per the site's robots.txt
  cosift crawl-errors [-limit N] list recently-errored frontier URLs with their failure reason
  cosift ingest -corpus P   ingest a pre-built corpus.json into the local index
  cosift query <text>       run a BM25 query against the local index
  cosift search <text>      hit a running cosift server's /search with the full pipeline
  cosift research <text>    hit a running cosift server's /research (LLM-synthesized answer + cited sources)
  cosift find-similar <url> hit a running cosift server's /find_similar (dense neighbors of an indexed URL)
  cosift contents <url...>  hit a running cosift server's /contents (single GET or batch POST when N>1 or -file given)
  cosift answer <text>      hit a running cosift server's /answer (single-question LLM answer with cited sources)
  cosift admin <stats|config|recrawl|recrawl-domain|reembed>   admin-token-protected operator endpoints (token via -token or COSIFT_ADMIN_TOKEN env)
  cosift stats              print index stats
  cosift eval [flags]       run the eval set against a chosen retriever
  cosift answer-eval [flags]  LLM-judged /research answer quality, planner vs paraphrase
  cosift answer-eval-compare A.json B.json   diff two saved answer-eval reports
  cosift bench [flags]      synthetic micro-benchmarks (vector + BM25 + crawl)
  cosift bench-compare A.json B.json   diff two saved bench JSON outputs (NDJSON, one record per mode)
  cosift version            print version

eval flags:
  -corpus <path>          path to corpus.json   (default: testdata/eval/corpus.json)
  -queries <path>         path to queries.json  (default: testdata/eval/queries.json)
  -save <path>            save run summary as JSON for future diffs
  -baseline <path>        load a prior summary and print a side-by-side diff
  -retriever <name>       bm25 | dense | hybrid (default bm25)
  -rerank                 wrap retriever with the LLM listwise reranker

flags:
  -config <path>          config file (default: ./cosift.json)
`

var version = "0.0.1-dev"

// chunkerWith is a thin shim around iter-147's index.NewChunkerWith — kept as
// a local alias to avoid churning the 4 CLI callsites, and so the iter-142/145
// "where chunker config gets resolved" path remains discoverable from this file.
func chunkerWith(size, overlap int) *index.Chunker {
	return index.NewChunkerWith(size, overlap)
}

func main() {
	cfgPath := flag.String("config", "cosift.json", "path to config file")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(2)
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	switch cmd := flag.Arg(0); cmd {
	case "version":
		fmt.Println(version)
	case "init":
		if err := runInit(*cfgPath, flag.Args()[1:]); err != nil {
			log.Fatalf("init: %v", err)
		}
	case "crawl":
		args := flag.Args()[1:]
		if err := runCrawl(ctx, cfg, args); err != nil {
			log.Fatalf("crawl: %v", err)
		}
	case "query":
		// Iter 89: query text is the FIRST positional arg, then optional
		// flags. Old: `cosift query "text"` (still works). New: `cosift
		// query "text" -k 20 -json`.
		if flag.NArg() < 2 {
			log.Fatal("query: text required (usage: cosift query <text> [-k N] [-json])")
		}
		if err := runQuery(ctx, cfg, flag.Arg(1), flag.Args()[2:]); err != nil {
			log.Fatalf("query: %v", err)
		}
	case "search":
		// Iter 90: HTTP-via-server search. Same positional+flags pattern as `query`.
		// Distinct from `query` (BM25 local-only); `search` exercises the full
		// pipeline of a running cosift instance.
		if flag.NArg() < 2 {
			log.Fatal("search: text required (usage: cosift search <text> [-server URL] [-k N] [-retriever ...] [-json])")
		}
		if err := runSearchCLI(ctx, cfg, flag.Arg(1), flag.Args()[2:]); err != nil {
			log.Fatalf("search: %v", err)
		}
	case "research":
		// Iter 91: HTTP-via-server research. Sibling to `search` but hits the
		// /research endpoint — LLM synthesis over retrieved sources.
		// Non-streaming for now; SSE could be a follow-up iter.
		if flag.NArg() < 2 {
			log.Fatal("research: text required (usage: cosift research <text> [-server URL] [-strategy planner|paraphrase] [-json])")
		}
		if err := runResearchCLI(ctx, cfg, flag.Arg(1), flag.Args()[2:]); err != nil {
			log.Fatalf("research: %v", err)
		}
	case "find-similar":
		// Iter 92: HTTP-via-server find-similar. URL was positional-required.
		// Iter 300: relax to accept either positional URL OR -text/-text-file
		// (iter 298 content-based MLT). Treat first non-flag arg as URL.
		fsArgs := flag.Args()[1:]
		var sourceURL string
		if len(fsArgs) > 0 && !strings.HasPrefix(fsArgs[0], "-") {
			sourceURL = fsArgs[0]
			fsArgs = fsArgs[1:]
		}
		if err := runFindSimilarCLI(ctx, cfg, sourceURL, fsArgs); err != nil {
			log.Fatalf("find-similar: %v", err)
		}
	case "contents":
		// Iter 93: GET /contents (single URL) or POST /contents (batch — multiple
		// positional URLs OR -file).content fetching from the CLI.
		// Required-args validation happens inside runContentsCLI after flag
		// parsing because URLs can come from positional args OR -file.
		if err := runContentsCLI(ctx, cfg, flag.Args()[1:]); err != nil {
			log.Fatalf("contents: %v", err)
		}
	case "answer":
		// Iter 94: HTTP-via-server /answer — single-question LLM answer with
		// cited sources. Sibling to `research` but no plan/expansion strategy
		// surface (just answer the question, k retrieved sources).
		if flag.NArg() < 2 {
			log.Fatal("answer: question required (usage: cosift answer <text> [-server URL] [-k N] [-expand] [-json])")
		}
		if err := runAnswerCLI(ctx, cfg, flag.Arg(1), flag.Args()[2:]); err != nil {
			log.Fatalf("answer: %v", err)
		}
	case "admin":
		// Iter 99: Admin CLI subcommands — bearer-auth /admin/* endpoints.
		// Iter 100 added `recrawl` (destructive POST, requires -y).
		// Iter 111 added `recrawl-domain` (bulk by domain, requires -y or -dry-run).
		// Iter 121: multi-line help when invoked without a subcommand.
		if flag.NArg() < 2 {
			fmt.Fprint(os.Stderr, adminUsageError())
			os.Exit(2)
		}
		if err := runAdmin(ctx, cfg, flag.Arg(1), flag.Args()[2:]); err != nil {
			log.Fatalf("admin: %v", err)
		}
	case "stats":
		if err := runStats(ctx, cfg, flag.Args()[1:]); err != nil {
			log.Fatalf("stats: %v", err)
		}
	case "status-file":
		if err := runStatusFile(ctx, cfg, flag.Args()[1:]); err != nil {
			log.Fatalf("status-file: %v", err)
		}
	case "crawl-status":
		if err := runCrawlStatus(ctx, cfg, flag.Args()[1:]); err != nil {
			log.Fatalf("crawl-status: %v", err)
		}
	case "eval":
		if err := runEval(ctx, flag.Args()[1:]); err != nil {
			log.Fatalf("eval: %v", err)
		}
	case "answer-eval":
		if err := runAnswerEval(ctx, flag.Args()[1:]); err != nil {
			log.Fatalf("answer-eval: %v", err)
		}
	case "answer-eval-compare":
		if err := runAnswerEvalCompare(ctx, flag.Args()[1:]); err != nil {
			log.Fatalf("answer-eval-compare: %v", err)
		}
	case "bench-compare":
		if err := runBenchCompare(flag.Args()[1:]); err != nil {
			log.Fatalf("bench-compare: %v", err)
		}
	case "ingest":
		if err := runIngest(ctx, cfg, flag.Args()[1:]); err != nil {
			log.Fatalf("ingest: %v", err)
		}
	case "export":
		if err := runExport(ctx, cfg, flag.Args()[1:]); err != nil {
			log.Fatalf("export: %v", err)
		}
	case "migrate-to-pebble":
		if err := runMigrateToPebble(ctx, cfg, flag.Args()[1:]); err != nil {
			log.Fatalf("migrate-to-pebble: %v", err)
		}
	case "gc":
		if err := runGC(ctx, cfg, flag.Args()[1:]); err != nil {
			log.Fatalf("gc: %v", err)
		}
	case "outcomes":
		if err := runOutcomes(ctx, cfg, flag.Args()[1:]); err != nil {
			log.Fatalf("outcomes: %v", err)
		}
	case "doctor":
		// Iter 102 extended doctor with optional remote checks via -server / -token.
		if err := runDoctor(ctx, cfg, flag.Args()[1:]); err != nil {
			os.Exit(1)
		}
	case "check-robots":
		if err := runCheckRobots(ctx, cfg, flag.Args()[1:]); err != nil {
			log.Fatalf("check-robots: %v", err)
		}
	case "crawl-errors":
		if err := runCrawlErrors(ctx, cfg, flag.Args()[1:]); err != nil {
			log.Fatalf("crawl-errors: %v", err)
		}
	case "reembed":
		if err := runReembed(ctx, cfg, flag.Args()[1:]); err != nil {
			log.Fatalf("reembed: %v", err)
		}
	case "compact-index":
		if err := runCompactIndex(ctx, cfg, flag.Args()[1:]); err != nil {
			log.Fatalf("compact-index: %v", err)
		}
	case "bench":
		if err := runBench(ctx, flag.Args()[1:]); err != nil {
			log.Fatalf("bench: %v", err)
		}
	case "refresh-due":
		if err := runRefreshDue(ctx, cfg, flag.Args()[1:]); err != nil {
			log.Fatalf("refresh-due: %v", err)
		}
	case "serve":
		if err := runServe(ctx, cfg); err != nil {
			log.Fatalf("serve: %v", err)
		}
	case "pebble-serve":
		if err := runPebbleServe(ctx, cfg, flag.Args()[1:]); err != nil {
			log.Fatalf("pebble-serve: %v", err)
		}
	case "pebble-info":
		if err := runPebbleInfo(ctx, cfg, flag.Args()[1:]); err != nil {
			log.Fatalf("pebble-info: %v", err)
		}
	case "verify":
		if err := runVerifyPebble(ctx, cfg, flag.Args()[1:]); err != nil {
			log.Fatalf("verify: %v", err)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", cmd)
		flag.Usage()
		os.Exit(2)
	}
}

func runCrawl(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("crawl", flag.ExitOnError)
	refresh := fs.Bool("refresh", false, "force re-crawl of URLs already in the frontier")
	sitemap := fs.String("sitemap", "", "URL of a sitemap.xml (or sitemap index) to seed from")
	backend := fs.String("backend", "sqlite", "storage backend: sqlite (default) | pebble")
	duration := fs.Duration("duration", 0, "iter 223: stop the crawl cleanly after this much time (0 = run until frontier empty or SIGTERM)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	urls := fs.Args()
	if len(urls) == 0 && *sitemap == "" {
		return errors.New("crawl: at least one URL or -sitemap is required")
	}

	// Iter 223: bounded crawl via -duration. Wraps the caller's ctx with a
	// timeout; workers see ctx.Err() != nil and exit cleanly. Pebble flushes
	// on Close (via the deferred ps.Close() below) so durability is preserved.
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
		log.Printf("crawler: bounded run, will stop after %s", *duration)
	}

	var c *crawler.Crawler
	switch *backend {
	case "sqlite", "":
		s, err := store.Open(cfg.DataDir)
		if err != nil {
			return err
		}
		defer s.Close()
		c = crawler.New(cfg.Crawler, s)
	case "pebble":
		// Iter 213: route through the iter-200..212 Pebble path. The
		// data dir layout under cfg.DataDir for Pebble: a sibling "pebble"
		// subdir so SQLite and Pebble stores can coexist during migration.
		pebbleDir := filepath.Join(cfg.DataDir, "pebble")
		ps, err := store.OpenPebble(pebbleDir)
		if err != nil {
			return fmt.Errorf("open pebble at %s: %w", pebbleDir, err)
		}
		defer ps.Close()
		c = crawler.NewWithBackend(cfg.Crawler, ps, index.NewPebbleBM25(ps))
		log.Printf("crawler: pebble backend at %s", pebbleDir)
	default:
		return fmt.Errorf("crawl: unknown -backend %q (want: sqlite | pebble)", *backend)
	}

	// Iter 186 / 213: auto-wire embedder when configured. Works for both
	// backends; PebbleStore's vector-write path is currently no-op until a
	// PassageWriter bridge to HNSW is provided.
	if cfg.Embeddings.Model != "" {
		apiKey := os.Getenv("OPENAI_API_KEY")
		if apiKey == "" {
			apiKey = os.Getenv("OPENAI")
		}
		if apiKey != "" {
			dim := cfg.Embeddings.Dim
			if dim == 0 {
				dim = 1536
			}
			emb := embed.NewOpenAIClient(apiKey, cfg.Embeddings.URL, cfg.Embeddings.Model, dim)
			c = c.WithEmbedder(emb)
			log.Printf("crawler: dense embeddings enabled (model=%s, dim=%d)", cfg.Embeddings.Model, dim)
		} else {
			log.Printf("warning: embeddings configured but no OPENAI_API_KEY in env; crawling BM25 only")
		}
	}
	if *sitemap != "" {
		n, err := c.SeedSitemap(ctx, *sitemap)
		if err != nil {
			return fmt.Errorf("sitemap %s: %w", *sitemap, err)
		}
		log.Printf("seeded %d URLs from sitemap %s", n, *sitemap)
	}
	for _, u := range urls {
		if *refresh {
			if err := c.Recrawl(ctx, u); err != nil {
				return fmt.Errorf("recrawl %s: %w", u, err)
			}
		} else {
			if err := c.Seed(u); err != nil {
				return fmt.Errorf("seed %s: %w", u, err)
			}
		}
	}
	return c.Run(ctx)
}

func runQuery(ctx context.Context, cfg *config.Config, q string, args []string) error {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	k := fs.Int("k", 10, "max results")
	jsonOut := fs.Bool("json", false, "emit JSON array instead of human-readable list — for shell pipelines")
	backend := fs.String("backend", "sqlite", "storage backend: sqlite (default) | pebble")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *k <= 0 || *k > 100 {
		return errors.New("k must be in [1, 100]")
	}

	// Iter 216: -backend flag mirrors iter-213 on crawl + stats. Both
	// BM25 implementations expose the same Search(ctx, q, k) signature so
	// the rest of runQuery is backend-agnostic.
	type searcher interface {
		Search(ctx context.Context, q string, k int) ([]index.Hit, error)
	}
	var idx searcher
	switch *backend {
	case "sqlite", "":
		s, err := store.Open(cfg.DataDir)
		if err != nil {
			return err
		}
		defer s.Close()
		idx = index.NewBM25(s)
	case "pebble":
		pebbleDir := filepath.Join(cfg.DataDir, "pebble")
		ps, err := store.OpenPebble(pebbleDir)
		if err != nil {
			return fmt.Errorf("open pebble: %w", err)
		}
		defer ps.Close()
		// Iter 301: honor COSIFT_BM25_K1 / _B here too. runQuery previously
		// always used defaults; with env overrides set, score values diverged
		// silently from pebble-serve's. Now they line up.
		pidx := index.NewPebbleBM25(ps)
		applyBM25EnvOverrides(pidx)
		idx = pidx
	default:
		return fmt.Errorf("query: unknown -backend %q (want: sqlite | pebble)", *backend)
	}
	hits, err := idx.Search(ctx, q, *k)
	if err != nil {
		return err
	}

	if *jsonOut {
		// Wire format matches the public /search response's per-hit shape
		// (sans Highlight / Domain / Excerpt — those need a store lookup
		// and `cosift query` is the no-server path).
		type jsonHit struct {
			URL   string  `json:"url"`
			Title string  `json:"title"`
			Score float64 `json:"score"`
		}
		out := make([]jsonHit, len(hits))
		for i, h := range hits {
			out[i] = jsonHit{URL: h.URL, Title: h.Title, Score: h.Score}
		}
		b, err := json.Marshal(out)
		if err != nil {
			return err
		}
		fmt.Println(string(b))
		return nil
	}
	for i, h := range hits {
		fmt.Printf("%2d. [%.3f] %s\n    %s\n", i+1, h.Score, h.Title, h.URL)
	}
	return nil
}

// runSearchCLI hits a running cosift server's /search endpoint over HTTP.
// Unlike runQuery (local BM25 only), this exercises the full pipeline:
// dense/hybrid retrieval, LLM rerank, paraphrase expansion, date/domain
// filters, custom sort. Useful from operator shells without curl + jq.
func runSearchCLI(ctx context.Context, cfg *config.Config, q string, args []string) error {
	fs := flag.NewFlagSet("search", flag.ExitOnError)
	defaultServer := "http://" + cfg.Server.Addr
	// 0.0.0.0 is a bind-only sentinel; clients must dial loopback.
	defaultServer = strings.Replace(defaultServer, "0.0.0.0", "127.0.0.1", 1)
	serverURL := fs.String("server", defaultServer, "cosift server URL")
	k := fs.Int("k", 10, "max results")
	retriever := fs.String("retriever", "", "bm25 | dense | hybrid (server default if empty)")
	rerankFlag := fs.Bool("rerank", false, "wrap retrieval with LLM listwise reranker (server must have it configured)")
	expand := fs.Bool("expand", false, "LLM paraphrase + RRF fusion (server must have a paraphraser)")
	since := fs.String("since", "", "ISO date — only results published on or after")
	until := fs.String("until", "", "ISO date — only results published on or before")
	includeDomains := fs.String("include-domains", "", "comma-separated allowlist of result domains")
	excludeDomains := fs.String("exclude-domains", "", "comma-separated denylist of result domains")
	sortMode := fs.String("sort", "", "relevance | date_desc | date_asc (server default if empty)")
	format := fs.String("format", "text", "human-output format: text | markdown (or md). Iter 96.")
	jsonOut := fs.Bool("json", false, "emit raw JSON response instead of human-readable list")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *k <= 0 || *k > 100 {
		return errors.New("k must be in [1, 100]")
	}
	if err := validateFormat(*format); err != nil {
		return err
	}

	v := url.Values{}
	v.Set("q", q)
	v.Set("k", strconv.Itoa(*k))
	if *retriever != "" {
		v.Set("retriever", *retriever)
	}
	if *rerankFlag {
		v.Set("rerank", "true")
	}
	if *expand {
		v.Set("expand", "true")
	}
	if *since != "" {
		v.Set("since", *since)
	}
	if *until != "" {
		v.Set("until", *until)
	}
	if *includeDomains != "" {
		v.Set("include_domains", *includeDomains)
	}
	if *excludeDomains != "" {
		v.Set("exclude_domains", *excludeDomains)
	}
	if *sortMode != "" {
		v.Set("sort", *sortMode)
	}

	endpoint := strings.TrimRight(*serverURL, "/") + "/search?" + v.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("get %s: %w", *serverURL, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server %d: %s", resp.StatusCode, body)
	}

	if *jsonOut {
		// Pass the server's JSON straight through — same shape as /search.
		fmt.Println(string(body))
		return nil
	}
	var sr server.SearchResponse
	if err := json.Unmarshal(body, &sr); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if *format == "md" || *format == "markdown" {
		fmt.Print(renderRankedMarkdown("Results: "+sr.Query, sr.Hits))
		return nil
	}
	if len(sr.Hits) == 0 {
		fmt.Println("(no results)")
		return nil
	}
	for i, h := range sr.Hits {
		date := ""
		if h.PublishedAt != nil {
			date = " " + h.PublishedAt.Format("2006-01-02")
		}
		domain := ""
		if h.Domain != "" {
			domain = " [" + h.Domain + "]"
		}
		fmt.Printf("%2d. [%.3f]%s%s %s\n    %s\n", i+1, h.Score, domain, date, h.Title, h.URL)
		if h.Highlight != nil && h.Highlight.Text != "" {
			fmt.Printf("    > %s\n", truncate(h.Highlight.Text, 200))
		} else if h.Excerpt != "" {
			fmt.Printf("    > %s\n", truncate(h.Excerpt, 200))
		}
	}
	return nil
}

// runResearchCLI hits a running cosift server's /research endpoint over HTTP.
// Sibling to runSearchCLI (iter 90): same -server / -json affordance, but pulls
// an LLM-synthesized answer with cited sources rather than a ranked URL list.
// Non-streaming — the SSE mode (?stream=true) is a separate code path; this
// returns the same JSON the non-streaming endpoint produces.
// renderAnswerMarkdown formats a /research or /answer response as markdown,
// suitable for piping into LLM contexts or markdown viewers. Used by both
// runResearchCLI and runAnswerCLI via the shared `-format md|markdown` flag.
// strategy/plan are research-only; pass "" / nil from /answer's callsite.
// Iter 95.
func renderAnswerMarkdown(query, strategy string, plan []string, answer string, sources []server.AnswerSource) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n", query)
	if strategy != "" {
		fmt.Fprintf(&b, "> Strategy: `%s`", strategy)
		if len(plan) > 0 {
			fmt.Fprintf(&b, " (plan: %s)", strings.Join(plan, " | "))
		}
		b.WriteString("\n\n")
	}
	b.WriteString(answer)
	if !strings.HasSuffix(answer, "\n") {
		b.WriteString("\n")
	}
	if len(sources) > 0 {
		b.WriteString("\n## Sources\n\n")
		for _, s := range sources {
			// `N. [Title](URL)` — the leading N matches the citation IDs the
			// LLM emits inline (e.g. `[1]` in the answer references source 1).
			fmt.Fprintf(&b, "%d. [%s](%s)", s.ID, s.Title, s.URL)
			var trailing []string
			if s.Domain != "" {
				trailing = append(trailing, s.Domain)
			}
			if s.PublishedAt != nil {
				trailing = append(trailing, s.PublishedAt.Format("2006-01-02"))
			}
			if len(trailing) > 0 {
				fmt.Fprintf(&b, " — %s", strings.Join(trailing, ", "))
			}
			b.WriteString("\n")
		}
	}
	return b.String()
}

// renderRankedMarkdown formats a ranked-hits response (search, find-similar)
// as markdown for LLM piping or markdown viewers. `title` is the H1 text —
// callers pass "Results: <query>" for /search or "Similar to: <url>" for
// /find_similar. Hit-list shape is the same in both cases. Iter 97 generalized
// from iter 96's renderSearchMarkdown.
func renderRankedMarkdown(title string, hits []server.SearchHit) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n", title)
	if len(hits) == 0 {
		b.WriteString("_No results._\n")
		return b.String()
	}
	for i, h := range hits {
		fmt.Fprintf(&b, "## %d. [%s](%s)\n\n", i+1, h.Title, h.URL)
		var meta []string
		meta = append(meta, fmt.Sprintf("Score: %.3f", h.Score))
		if h.Domain != "" {
			meta = append(meta, h.Domain)
		}
		if h.PublishedAt != nil {
			meta = append(meta, h.PublishedAt.Format("2006-01-02"))
		}
		fmt.Fprintf(&b, "_%s_\n", strings.Join(meta, " · "))
		// Excerpt OR highlight as a blockquote. Highlight wins when both are
		// present (it's the matched passage, more relevant than the prefix).
		var excerpt string
		if h.Highlight != nil && h.Highlight.Text != "" {
			excerpt = h.Highlight.Text
		} else if h.Excerpt != "" {
			excerpt = h.Excerpt
		}
		if excerpt != "" {
			b.WriteString("\n> ")
			b.WriteString(truncate(excerpt, 240))
			b.WriteString("\n")
		}
		// Trailing blank line between hits, but not after the last one.
		if i < len(hits)-1 {
			b.WriteString("\n")
		}
	}
	return b.String()
}

// forEachSSEEvent reads an SSE response body and invokes handle(event, data)
// for each `event: <name>\ndata: <json>\n\n` block. The handler can return
// errSSEDone to signal terminal completion (the scanner returns nil), or any
// other error to abort. Multi-line `data:` lines are concatenated with `\n`
// per the SSE spec. Iter 109 — extracted from iter-98's consumeResearchSSE so
// both research and answer CLI consumers share the framing logic.
func forEachSSEEvent(body io.Reader, handle func(event, data string) error) error {
	scanner := bufio.NewScanner(body)
	// SSE events can be larger than the default 64KB scanner buffer (the
	// terminal `done` blob in /research carries the full ResearchResponse).
	scanner.Buffer(make([]byte, 0, 64<<10), 4<<20)

	var event, data string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			// Blank line terminates an event block — dispatch.
			if event != "" {
				if err := handle(event, data); err != nil {
					if err == errSSEDone {
						return nil
					}
					return err
				}
			}
			event, data = "", ""
			continue
		}
		switch {
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			d := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if data == "" {
				data = d
			} else {
				data = data + "\n" + d
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read stream: %w", err)
	}
	// Stream ended without a terminal event — caller's responsibility to flag
	// (this function just reports the natural end-of-stream as success).
	return nil
}

// consumeResearchSSE reads a /research?stream=true response body and renders
// each event as it arrives. Terminal events: "done" (full response) emits a
// final newline + Sources section; "error" emits the detail and returns an
// error. Iter 98 — first streaming CLI in cosift. Refactored in iter 109 to
// use the generic forEachSSEEvent scanner.
func consumeResearchSSE(body io.Reader, format string) error {
	useMarkdown := format == "md" || format == "markdown"
	var answerStarted, sawDone bool

	err := forEachSSEEvent(body, func(event, data string) error {
		switch event {
		case "plan":
			var p struct {
				Strategy string   `json:"strategy"`
				Variants []string `json:"variants"`
			}
			if err := json.Unmarshal([]byte(data), &p); err != nil {
				return nil // tolerate malformed mid-stream event
			}
			if useMarkdown {
				fmt.Printf("> Strategy: `%s`", p.Strategy)
				if len(p.Variants) > 0 {
					fmt.Printf(" (plan: %s)", strings.Join(p.Variants, " | "))
				}
				fmt.Println()
				fmt.Println()
			} else {
				fmt.Printf("Strategy: %s", p.Strategy)
				if len(p.Variants) > 0 {
					fmt.Printf(" (plan: %s)", strings.Join(p.Variants, " | "))
				}
				fmt.Println()
			}
		case "retrieved":
			var r struct {
				Variant string   `json:"variant"`
				URLs    []string `json:"urls"`
			}
			if err := json.Unmarshal([]byte(data), &r); err != nil {
				return nil
			}
			fmt.Printf("[retrieved %d url(s) for %q]\n", len(r.URLs), r.Variant)
		case "synthesizing":
			var s struct {
				Sources int `json:"sources"`
			}
			if err := json.Unmarshal([]byte(data), &s); err != nil {
				return nil
			}
			fmt.Printf("[synthesizing answer over %d source(s)]\n\n", s.Sources)
		case "answer_chunk":
			var c struct {
				Text string `json:"text"`
			}
			if err := json.Unmarshal([]byte(data), &c); err != nil {
				return nil
			}
			fmt.Print(c.Text)
			answerStarted = true
		case "done":
			sawDone = true
			if answerStarted {
				fmt.Println()
			}
			var rr server.ResearchResponse
			if err := json.Unmarshal([]byte(data), &rr); err != nil {
				return fmt.Errorf("parse done event: %w", err)
			}
			renderStreamingSources(rr.Sources, useMarkdown)
			return errSSEDone
		case "error":
			var e struct {
				Detail string `json:"detail"`
			}
			_ = json.Unmarshal([]byte(data), &e)
			return fmt.Errorf("server stream error: %s", e.Detail)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !sawDone {
		// Stream ended without a terminal event — flag for operators but
		// don't error (matches iter-98 behavior).
		fmt.Fprintln(os.Stderr, "(stream ended without `done` event)")
	}
	return nil
}

// renderStreamingSources writes the terminal Sources section for both the
// research and answer SSE consumers. Format matches the non-stream paths from
// iter-91 (research) and iter-94 (answer) so operators get consistent output
// regardless of whether they used -stream. Iter 109.
func renderStreamingSources(sources []server.AnswerSource, useMarkdown bool) {
	if len(sources) == 0 {
		return
	}
	if useMarkdown {
		fmt.Println()
		fmt.Println("## Sources")
		fmt.Println()
		for _, s := range sources {
			fmt.Fprintf(os.Stdout, "%d. [%s](%s)", s.ID, s.Title, s.URL)
			var trailing []string
			if s.Domain != "" {
				trailing = append(trailing, s.Domain)
			}
			if s.PublishedAt != nil {
				trailing = append(trailing, s.PublishedAt.Format("2006-01-02"))
			}
			if len(trailing) > 0 {
				fmt.Printf(" — %s", strings.Join(trailing, ", "))
			}
			fmt.Println()
		}
		return
	}
	fmt.Println()
	fmt.Println("Sources:")
	for _, s := range sources {
		date := ""
		if s.PublishedAt != nil {
			date = " " + s.PublishedAt.Format("2006-01-02")
		}
		domain := ""
		if s.Domain != "" {
			domain = " [" + s.Domain + "]"
		}
		fmt.Printf("  [%d]%s%s %s\n      %s\n", s.ID, domain, date, s.Title, s.URL)
	}
}

// consumeAnswerSSE reads an /answer?stream=true response and renders events as
// they arrive. Same event vocabulary as consumeResearchSSE minus the `plan`
// event (/answer has no expansion phase). Iter 109.
func consumeAnswerSSE(body io.Reader, format string) error {
	useMarkdown := format == "md" || format == "markdown"
	var answerStarted, sawDone bool

	err := forEachSSEEvent(body, func(event, data string) error {
		switch event {
		case "retrieved":
			// /answer emits a single retrieved event with the k URLs (vs
			// /research's one-per-variant).
			var r struct {
				URLs []string `json:"urls"`
			}
			if err := json.Unmarshal([]byte(data), &r); err != nil {
				return nil
			}
			fmt.Printf("[retrieved %d source(s)]\n", len(r.URLs))
		case "synthesizing":
			var s struct {
				Sources int `json:"sources"`
			}
			if err := json.Unmarshal([]byte(data), &s); err != nil {
				return nil
			}
			fmt.Printf("[synthesizing answer over %d source(s)]\n\n", s.Sources)
		case "answer_chunk":
			var c struct {
				Text string `json:"text"`
			}
			if err := json.Unmarshal([]byte(data), &c); err != nil {
				return nil
			}
			fmt.Print(c.Text)
			answerStarted = true
		case "done":
			sawDone = true
			if answerStarted {
				fmt.Println()
			}
			var ar server.AnswerResponse
			if err := json.Unmarshal([]byte(data), &ar); err != nil {
				return fmt.Errorf("parse done event: %w", err)
			}
			renderStreamingSources(ar.Sources, useMarkdown)
			return errSSEDone
		case "error":
			var e struct {
				Detail string `json:"detail"`
			}
			_ = json.Unmarshal([]byte(data), &e)
			return fmt.Errorf("server stream error: %s", e.Detail)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !sawDone {
		fmt.Fprintln(os.Stderr, "(stream ended without `done` event)")
	}
	return nil
}

// errSSEDone signals consumeResearchSSE's outer loop that the terminal "done"
// event was processed and the read loop should exit cleanly. Sentinel error,
// not surfaced to the caller. Iter 98.
var errSSEDone = errors.New("sse-done")

// validateFormat returns nil if v is one of the accepted output-format values
// for the synth CLIs (`-format`). "" is rejected — flag.String defaults to ""
// when the user passes `-format` without a value, and we want that to fail
// loudly rather than silently behave as text. Iter 95.
func validateFormat(v string) error {
	switch v {
	case "text", "md", "markdown":
		return nil
	default:
		return fmt.Errorf("format must be one of: text, markdown (got %q)", v)
	}
}

func runResearchCLI(ctx context.Context, cfg *config.Config, q string, args []string) error {
	fs := flag.NewFlagSet("research", flag.ExitOnError)
	defaultServer := "http://" + cfg.Server.Addr
	defaultServer = strings.Replace(defaultServer, "0.0.0.0", "127.0.0.1", 1)
	serverURL := fs.String("server", defaultServer, "cosift server URL")
	strategy := fs.String("strategy", "", "planner | paraphrase (server default if empty)")
	format := fs.String("format", "text", "human-output format: text | markdown (or md). Iter 95.")
	stream := fs.Bool("stream", false, "stream progress + token-by-token answer over SSE. Iter 98.")
	jsonOut := fs.Bool("json", false, "emit raw JSON response instead of human-readable answer+sources")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := validateFormat(*format); err != nil {
		return err
	}
	if *stream && *jsonOut {
		return errors.New("-stream and -json are mutually exclusive (stream is event-by-event, json is the final blob)")
	}

	v := url.Values{}
	v.Set("q", q)
	if *strategy != "" {
		v.Set("strategy", *strategy)
	}
	if *stream {
		v.Set("stream", "true")
	}

	endpoint := strings.TrimRight(*serverURL, "/") + "/research?" + v.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	if *stream {
		req.Header.Set("Accept", "text/event-stream")
	}
	// /research issues multiple LLM calls (expand + retrieve + synth); bump the
	// client timeout from search's 30s so slow chat backends don't get cut off.
	// In stream mode the timeout has to cover the whole response (multiple LLM
	// chunks emitted incrementally), so it's still 120s — there's no per-event
	// deadline; if needed in future, a custom transport timeout would do that.
	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("get %s: %w", *serverURL, err)
	}
	defer resp.Body.Close()
	if *stream {
		if resp.StatusCode != http.StatusOK {
			// Server may emit a JSON problem doc on non-200 even with SSE Accept.
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
			return fmt.Errorf("server %d: %s", resp.StatusCode, b)
		}
		return consumeResearchSSE(resp.Body, *format)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server %d: %s", resp.StatusCode, body)
	}

	if *jsonOut {
		fmt.Println(string(body))
		return nil
	}
	var rr server.ResearchResponse
	if err := json.Unmarshal(body, &rr); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if *format == "md" || *format == "markdown" {
		fmt.Print(renderAnswerMarkdown(rr.Query, rr.Strategy, rr.Plan, rr.Answer, rr.Sources))
		return nil
	}
	fmt.Printf("Q: %s\n", rr.Query)
	if rr.Strategy != "" {
		fmt.Printf("Strategy: %s", rr.Strategy)
		if len(rr.Plan) > 0 {
			fmt.Printf(" (plan: %s)", strings.Join(rr.Plan, " | "))
		}
		fmt.Println()
	}
	fmt.Println()
	fmt.Println(rr.Answer)
	if len(rr.Sources) > 0 {
		fmt.Println()
		fmt.Println("Sources:")
		for _, s := range rr.Sources {
			date := ""
			if s.PublishedAt != nil {
				date = " " + s.PublishedAt.Format("2006-01-02")
			}
			domain := ""
			if s.Domain != "" {
				domain = " [" + s.Domain + "]"
			}
			fmt.Printf("  [%d]%s%s %s\n      %s\n", s.ID, domain, date, s.Title, s.URL)
		}
	}
	return nil
}

// runFindSimilarCLI hits a running cosift server's /find_similar endpoint.
// Completes the iter-89..iter-92 CLI matrix:
//   - query        local BM25 only
//   - search       /search (full retrieval pipeline)
//   - research     /research (LLM synthesis)
//   - find-similar /find_similar (dense neighbors of an indexed URL)
func runFindSimilarCLI(ctx context.Context, cfg *config.Config, sourceURL string, args []string) error {
	fs := flag.NewFlagSet("find-similar", flag.ExitOnError)
	defaultServer := "http://" + cfg.Server.Addr
	defaultServer = strings.Replace(defaultServer, "0.0.0.0", "127.0.0.1", 1)
	serverURL := fs.String("server", defaultServer, "cosift server URL")
	k := fs.Int("k", 10, "max results")
	format := fs.String("format", "text", "human-output format: text | markdown (or md). Iter 97.")
	jsonOut := fs.Bool("json", false, "emit raw JSON response instead of human-readable list")
	// Iter 300: alternative inputs — feed arbitrary text (or a file of text)
	// instead of an indexed source URL. Mirrors iter-298 /find_similar text mode.
	textInput := fs.String("text", "", "arbitrary text for content-based MLT (no positional URL needed)")
	textFile := fs.String("text-file", "", "read MLT source text from FILE")
	textTitle := fs.String("text-title", "", "optional title (×3 boost) when using -text or -text-file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *k <= 0 || *k > 100 {
		return errors.New("k must be in [1, 100]")
	}
	if err := validateFormat(*format); err != nil {
		return err
	}

	if *textFile != "" {
		buf, err := os.ReadFile(*textFile)
		if err != nil {
			return fmt.Errorf("read -text-file %s: %w", *textFile, err)
		}
		*textInput = string(buf)
	}
	if sourceURL == "" && *textInput == "" {
		return errors.New("find-similar: positional URL or -text/-text-file required")
	}

	v := url.Values{}
	if sourceURL != "" {
		v.Set("url", sourceURL)
	}
	if *textInput != "" {
		v.Set("text", *textInput)
	}
	if *textTitle != "" {
		v.Set("title", *textTitle)
	}
	v.Set("k", strconv.Itoa(*k))

	// Iter 300: switch GET → POST when -text is large (URL params have practical
	// limits ~8 KB). The POST endpoint accepts the same flag set as a JSON body.
	useMethod := http.MethodGet
	endpoint := strings.TrimRight(*serverURL, "/") + "/find_similar?" + v.Encode()
	var bodyBuf io.Reader
	if len(*textInput) > 4096 {
		useMethod = http.MethodPost
		body := map[string]any{"k": *k}
		if sourceURL != "" {
			body["url"] = sourceURL
		}
		if *textInput != "" {
			body["text"] = *textInput
		}
		if *textTitle != "" {
			body["title"] = *textTitle
		}
		jb, _ := json.Marshal(body)
		bodyBuf = bytes.NewReader(jb)
		endpoint = strings.TrimRight(*serverURL, "/") + "/find_similar"
	}
	req, err := http.NewRequestWithContext(ctx, useMethod, endpoint, bodyBuf)
	if err != nil {
		return err
	}
	if useMethod == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	// /find_similar embeds the source doc (one embedder call) and does a vector
	// search — 30s mirrors search's timeout.
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("get %s: %w", *serverURL, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server %d: %s", resp.StatusCode, body)
	}

	if *jsonOut {
		fmt.Println(string(body))
		return nil
	}
	var fr server.FindSimilarResponse
	if err := json.Unmarshal(body, &fr); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if *format == "md" || *format == "markdown" {
		fmt.Print(renderRankedMarkdown("Similar to: "+fr.URL, fr.Hits))
		return nil
	}
	if len(fr.Hits) == 0 {
		fmt.Println("(no similar documents)")
		return nil
	}
	fmt.Printf("Similar to: %s\n\n", fr.URL)
	for i, h := range fr.Hits {
		date := ""
		if h.PublishedAt != nil {
			date = " " + h.PublishedAt.Format("2006-01-02")
		}
		domain := ""
		if h.Domain != "" {
			domain = " [" + h.Domain + "]"
		}
		fmt.Printf("%2d. [%.3f]%s%s %s\n    %s\n", i+1, h.Score, domain, date, h.Title, h.URL)
	}
	return nil
}

// runContentsCLI hits a running cosift server's /contents endpoint.
// Single URL → GET /contents?url=<url>. Multiple URLs (positional or via
// -file) → POST /contents with {urls: [...]} body (iter-88 batch API).
// Iter 93.
func runContentsCLI(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("contents", flag.ExitOnError)
	defaultServer := "http://" + cfg.Server.Addr
	defaultServer = strings.Replace(defaultServer, "0.0.0.0", "127.0.0.1", 1)
	serverURL := fs.String("server", defaultServer, "cosift server URL")
	filePath := fs.String("file", "", "read URLs from FILE (one per line, # comments allowed)")
	textOnly := fs.Bool("text", false, "print only the document text (no metadata) — useful for piping into LLMs")
	jsonOut := fs.Bool("json", false, "emit raw JSON response instead of human-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	urls := fs.Args()
	if *filePath != "" {
		extra, err := readURLsFromFile(*filePath)
		if err != nil {
			return fmt.Errorf("read -file: %w", err)
		}
		urls = append(urls, extra...)
	}
	if len(urls) == 0 {
		return errors.New("at least one URL or -file required")
	}

	base := strings.TrimRight(*serverURL, "/")
	client := &http.Client{Timeout: 60 * time.Second}

	if len(urls) == 1 {
		// Single — GET /contents?url=<url>.
		v := url.Values{}
		v.Set("url", urls[0])
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/contents?"+v.Encode(), nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("get %s: %w", *serverURL, err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
		if err != nil {
			return fmt.Errorf("read response: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("server %d: %s", resp.StatusCode, body)
		}
		if *jsonOut {
			fmt.Println(string(body))
			return nil
		}
		var cr server.ContentsResponse
		if err := json.Unmarshal(body, &cr); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
		if *textOnly {
			fmt.Println(cr.Text)
			return nil
		}
		fmt.Printf("URL:        %s\n", cr.URL)
		fmt.Printf("Title:      %s\n", cr.Title)
		fmt.Printf("Cached:     %t\n", cr.Cached)
		if !cr.FetchedAt.IsZero() {
			fmt.Printf("FetchedAt:  %s\n", cr.FetchedAt.Format(time.RFC3339))
		}
		if cr.Lang != "" {
			fmt.Printf("Lang:       %s\n", cr.Lang)
		}
		fmt.Println()
		fmt.Println(cr.Text)
		return nil
	}

	// Batch — POST /contents with {urls: [...]}.
	reqBody, err := json.Marshal(server.ContentsBatchRequest{URLs: urls})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/contents", bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("post %s: %w", *serverURL, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server %d: %s", resp.StatusCode, body)
	}
	if *jsonOut {
		fmt.Println(string(body))
		return nil
	}
	var br server.ContentsBatchResponse
	if err := json.Unmarshal(body, &br); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if *textOnly {
		// Separator between docs lets downstream tools chunk by `---` if they want.
		for i, item := range br.Results {
			if i > 0 {
				fmt.Println("---")
			}
			if !item.Found {
				continue
			}
			fmt.Println(item.Text)
		}
		return nil
	}
	for _, item := range br.Results {
		fmt.Printf("URL: %s\n", item.URL)
		if !item.Found {
			fmt.Printf("  (not found: %s)\n\n", item.Error)
			continue
		}
		status := "fetched"
		if item.Cached {
			status = "cached"
		}
		fmt.Printf("  Title:      %s\n", item.Title)
		fmt.Printf("  Status:     %s\n", status)
		if !item.FetchedAt.IsZero() {
			fmt.Printf("  FetchedAt:  %s\n", item.FetchedAt.Format(time.RFC3339))
		}
		if item.Lang != "" {
			fmt.Printf("  Lang:       %s\n", item.Lang)
		}
		fmt.Printf("  Text:       %s\n\n", truncate(item.Text, 240))
	}
	return nil
}

// readURLsFromFile parses one URL per line. Blank lines and `#`-prefixed
// comments are ignored. Used by `cosift contents -file`. Iter 93.
func readURLsFromFile(path string) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var urls []string
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		urls = append(urls, line)
	}
	return urls, nil
}

// runAnswerCLI hits a running cosift server's /answer endpoint. Sibling to
// runResearchCLI (iter 91) but no planner/paraphrase strategy surface —
// /answer just retrieves k sources and synthesizes a cited answer. Smaller K
// cap [1,20] mirrors the server's bound (vs /search's [1,100]). Iter 94.
func runAnswerCLI(ctx context.Context, cfg *config.Config, q string, args []string) error {
	fs := flag.NewFlagSet("answer", flag.ExitOnError)
	defaultServer := "http://" + cfg.Server.Addr
	defaultServer = strings.Replace(defaultServer, "0.0.0.0", "127.0.0.1", 1)
	serverURL := fs.String("server", defaultServer, "cosift server URL")
	k := fs.Int("k", 5, "number of sources to retrieve (1-20)")
	expand := fs.Bool("expand", false, "LLM paraphrase + RRF fusion of retrieval inputs (server must have a paraphraser)")
	format := fs.String("format", "text", "human-output format: text | markdown (or md). Iter 95.")
	stream := fs.Bool("stream", false, "stream progress + token-by-token answer over SSE. Iter 109.")
	jsonOut := fs.Bool("json", false, "emit raw JSON response instead of human-readable answer+sources")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *k <= 0 || *k > 20 {
		return errors.New("k must be in [1, 20]")
	}
	if err := validateFormat(*format); err != nil {
		return err
	}
	if *stream && *jsonOut {
		return errors.New("-stream and -json are mutually exclusive (stream is event-by-event, json is the final blob)")
	}

	v := url.Values{}
	v.Set("q", q)
	v.Set("k", strconv.Itoa(*k))
	if *expand {
		v.Set("expand", "true")
	}
	if *stream {
		v.Set("stream", "true")
	}

	endpoint := strings.TrimRight(*serverURL, "/") + "/answer?" + v.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	if *stream {
		req.Header.Set("Accept", "text/event-stream")
	}
	// /answer issues retrieval + 1 synth LLM call; 120s mirrors /research.
	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("get %s: %w", *serverURL, err)
	}
	defer resp.Body.Close()
	if *stream {
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
			return fmt.Errorf("server %d: %s", resp.StatusCode, b)
		}
		return consumeAnswerSSE(resp.Body, *format)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server %d: %s", resp.StatusCode, body)
	}

	if *jsonOut {
		fmt.Println(string(body))
		return nil
	}
	var ar server.AnswerResponse
	if err := json.Unmarshal(body, &ar); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if *format == "md" || *format == "markdown" {
		// /answer has no strategy or plan — pass empty/nil so the shared
		// renderer just skips the strategy line.
		fmt.Print(renderAnswerMarkdown(ar.Query, "", nil, ar.Answer, ar.Sources))
		return nil
	}
	fmt.Printf("Q: %s\n\n", ar.Query)
	fmt.Println(ar.Answer)
	if len(ar.Sources) > 0 {
		fmt.Println()
		fmt.Println("Sources:")
		for _, s := range ar.Sources {
			date := ""
			if s.PublishedAt != nil {
				date = " " + s.PublishedAt.Format("2006-01-02")
			}
			domain := ""
			if s.Domain != "" {
				domain = " [" + s.Domain + "]"
			}
			fmt.Printf("  [%d]%s%s %s\n      %s\n", s.ID, domain, date, s.Title, s.URL)
		}
	}
	return nil
}

// runAdmin dispatches to a sub-operation under `cosift admin <op>`. Sub-ops
// share bearer-token resolution: -token flag wins, then COSIFT_ADMIN_TOKEN env.
// Empty token still hits the server (and gets a clear 401) — the CLI doesn't
// pre-fail because the operator might be running an instance without admin
// auth enabled (rare, but the failure path is the server's to own). Iter 99.
// adminUsageError returns the multi-line help message printed when the
// `cosift admin` parent is invoked without a subcommand. Listed alphabetically
// by subcommand name so the output is stable across iterations. Iter 121.
//
// Extracted as a function (rather than a const) so tests can assert on the
// listing's structure — every admin subcommand must appear here, in the same
// alphabetic order it appears in runAdmin's dispatch switch.
func adminUsageError() string {
	return `admin: subcommand required.

Available subcommands:
  config                   Retriever defaults and capability flags.
  recrawl <url...>         Re-enqueue specific URLs into the crawl frontier. Requires -y.
  recrawl-domain <pattern> Bulk re-enqueue every doc matching the domain pattern. Requires -y or -dry-run.
  reembed                  Re-embed every doc with the configured model (streams progress).
                           Requires -y. Supports -drop-old and -since DATE.
  stats                    Index counts, frontier breakdown, top domains, capability flags.
                           Supports -summary for one-line output (monitoring / shell prompts).

Common flags:
  -server URL              cosift server URL (default: from local cosift.json)
  -token TOKEN             admin bearer token (or COSIFT_ADMIN_TOKEN env)
  -json                    raw JSON output instead of human-readable

Usage: cosift admin <subcommand> [flags]
`
}

func runAdmin(ctx context.Context, cfg *config.Config, op string, args []string) error {
	switch op {
	case "stats":
		return runAdminStats(ctx, cfg, args)
	case "config":
		return runAdminConfig(ctx, cfg, args)
	case "recrawl":
		return runAdminRecrawl(ctx, cfg, args)
	case "recrawl-domain":
		return runAdminRecrawlDomain(ctx, cfg, args)
	case "reembed":
		return runAdminReembedCLI(ctx, cfg, args)
	default:
		return fmt.Errorf("unknown admin subcommand %q (want: stats | config | recrawl | recrawl-domain | reembed)", op)
	}
}

// adminCommonFlags parses the -server/-token/-json flags shared by every admin
// sub-operation. Returns parsed values + the leftover positional args (none for
// stats/config; future ops like recrawl will have positional URL args).
type adminCommonOpts struct {
	server string
	token  string
	json   bool
}

func parseAdminCommon(name string, cfg *config.Config, args []string) (adminCommonOpts, []string, error) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	defaultServer := "http://" + cfg.Server.Addr
	defaultServer = strings.Replace(defaultServer, "0.0.0.0", "127.0.0.1", 1)
	server := fs.String("server", defaultServer, "cosift server URL")
	// Token: -token flag overrides env. Env is the secure default — keeps the
	// token out of shell history / process listings.
	token := fs.String("token", "", "admin bearer token (defaults to COSIFT_ADMIN_TOKEN env)")
	jsonOut := fs.Bool("json", false, "emit raw JSON response instead of human-readable summary")
	if err := fs.Parse(args); err != nil {
		return adminCommonOpts{}, nil, err
	}
	resolvedToken := *token
	if resolvedToken == "" {
		resolvedToken = os.Getenv("COSIFT_ADMIN_TOKEN")
	}
	return adminCommonOpts{server: *server, token: resolvedToken, json: *jsonOut}, fs.Args(), nil
}

// adminRequest builds an authenticated request to the given /admin/* path. The
// Authorization header is omitted when token is empty (server returns 401,
// which is the right error to surface). Iter 99.
func adminRequest(ctx context.Context, method, server, path, token string, body io.Reader) (*http.Request, error) {
	endpoint := strings.TrimRight(server, "/") + path
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req, nil
}

func runAdminStats(ctx context.Context, cfg *config.Config, args []string) error {
	// Iter 122: inline flag parsing (vs parseAdminCommon) so we can add the
	// -summary flag. Same shape as iter-100 runAdminRecrawl's flag set —
	// admin sub-ops with op-specific flags bypass the common helper.
	fs := flag.NewFlagSet("admin stats", flag.ExitOnError)
	defaultServer := "http://" + cfg.Server.Addr
	defaultServer = strings.Replace(defaultServer, "0.0.0.0", "127.0.0.1", 1)
	serverURL := fs.String("server", defaultServer, "cosift server URL")
	token := fs.String("token", "", "admin bearer token (defaults to COSIFT_ADMIN_TOKEN env)")
	jsonOut := fs.Bool("json", false, "emit raw JSON response")
	summary := fs.Bool("summary", false, "one-line compact output for monitoring / shell prompts")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *summary && *jsonOut {
		return errors.New("-summary and -json are mutually exclusive")
	}
	resolvedToken := *token
	if resolvedToken == "" {
		resolvedToken = os.Getenv("COSIFT_ADMIN_TOKEN")
	}

	req, err := adminRequest(ctx, http.MethodGet, *serverURL, "/admin/stats", resolvedToken, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("get %s: %w", *serverURL, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server %d: %s", resp.StatusCode, body)
	}
	if *jsonOut {
		fmt.Println(string(body))
		return nil
	}
	var st server.AdminStatsResponse
	if err := json.Unmarshal(body, &st); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if *summary {
		// Compact one-line format designed for: shell prompts, cron, status
		// scripts. Always-present fields (docs/queued/errored). Embedder named
		// only when configured (omitted with " · " to keep separator clean).
		parts := []string{
			fmt.Sprintf("%d docs", st.Documents),
			fmt.Sprintf("%d queued", st.Frontier.Queued),
			fmt.Sprintf("%d errored", st.Frontier.Errored),
		}
		if st.EmbedderModel != "" {
			parts = append(parts, "embedder="+st.EmbedderModel)
		}
		fmt.Println(strings.Join(parts, " · "))
		return nil
	}
	fmt.Println("=== Index ===")
	fmt.Printf("  Documents:           %d\n", st.Documents)
	fmt.Printf("  Terms:               %d\n", st.Terms)
	fmt.Printf("  Passages:            %d\n", st.Passages)
	if st.DocsWithPublishedAt > 0 {
		pct := float64(st.DocsWithPublishedAt) / float64(st.Documents) * 100
		fmt.Printf("  Docs w/ published_at: %d (%.1f%%)\n", st.DocsWithPublishedAt, pct)
	}
	// Iter 171: LLM-cache sizes. Show only when populated — a fresh
	// deployment with no LLM cache yet shouldn't bloat the dashboard.
	if st.Paraphrases > 0 || st.HyDECache > 0 {
		fmt.Println()
		fmt.Println("=== LLM caches ===")
		if st.Paraphrases > 0 {
			fmt.Printf("  Paraphrases:         %d\n", st.Paraphrases)
		}
		if st.HyDECache > 0 {
			fmt.Printf("  HyDE passages:       %d\n", st.HyDECache)
		}
	}
	fmt.Println()
	fmt.Println("=== Frontier ===")
	fmt.Printf("  Queued:    %d\n", st.Frontier.Queued)
	fmt.Printf("  In-flight: %d\n", st.Frontier.InFlight)
	fmt.Printf("  Done:      %d\n", st.Frontier.Done)
	fmt.Printf("  Errored:   %d\n", st.Frontier.Errored)
	fmt.Println()
	fmt.Println("=== Capabilities ===")
	fmt.Printf("  Dense:    %t\n", st.DenseEnabled)
	fmt.Printf("  Answer:   %t\n", st.AnswerEnabled)
	fmt.Printf("  Fetcher:  %t\n", st.FetcherEnabled)
	if st.EmbedderModel != "" {
		fmt.Printf("  Embedder: %s\n", st.EmbedderModel)
	}
	if st.ChatModel != "" {
		fmt.Printf("  Chat:     %s\n", st.ChatModel)
	}
	if st.RerankerName != "" {
		fmt.Printf("  Reranker: %s\n", st.RerankerName)
	}
	if len(st.TopDomains) > 0 {
		fmt.Println()
		fmt.Println("=== Top Domains ===")
		// Map iteration order is random; sort by count desc, then domain asc.
		type kv struct {
			k string
			v int64
		}
		pairs := make([]kv, 0, len(st.TopDomains))
		for k, v := range st.TopDomains {
			pairs = append(pairs, kv{k, v})
		}
		sort.Slice(pairs, func(i, j int) bool {
			if pairs[i].v != pairs[j].v {
				return pairs[i].v > pairs[j].v
			}
			return pairs[i].k < pairs[j].k
		})
		for _, p := range pairs {
			fmt.Printf("  %-32s %d\n", p.k, p.v)
		}
	}
	return nil
}

func runAdminConfig(ctx context.Context, cfg *config.Config, args []string) error {
	opts, _, err := parseAdminCommon("admin config", cfg, args)
	if err != nil {
		return err
	}
	req, err := adminRequest(ctx, http.MethodGet, opts.server, "/admin/config", opts.token, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("get %s: %w", opts.server, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server %d: %s", resp.StatusCode, body)
	}
	if opts.json {
		fmt.Println(string(body))
		return nil
	}
	var cfgResp server.AdminConfigResponse
	if err := json.Unmarshal(body, &cfgResp); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	fmt.Printf("Cosift version: %s\n\n", cfgResp.Version)
	fmt.Println("=== Defaults ===")
	d := cfgResp.Defaults
	fmt.Printf("  Retriever: %s\n", d.Retriever)
	fmt.Printf("  Expand:    %t\n", d.Expand)
	fmt.Println()
	fmt.Println("=== Capabilities ===")
	c := cfgResp.Capabilities
	fmt.Printf("  Dense:        %t\n", c.DenseEnabled)
	fmt.Printf("  Chat:         %t\n", c.ChatEnabled)
	fmt.Printf("  Rerank:       %t\n", c.RerankEnabled)
	fmt.Printf("  Paraphraser:  %t\n", c.ParaphraserEnabled)
	fmt.Printf("  Fetcher:      %t\n", c.FetcherEnabled)
	fmt.Printf("  Admin:        %t\n", c.AdminEnabled)
	if c.EmbedderModel != "" {
		fmt.Printf("  Embedder model:  %s\n", c.EmbedderModel)
	}
	if c.ChatModel != "" {
		fmt.Printf("  Chat model:      %s\n", c.ChatModel)
	}
	if c.RerankerName != "" {
		fmt.Printf("  Reranker name:   %s\n", c.RerankerName)
	}
	return nil
}

// runAdminRecrawl re-enqueues the given URLs into the crawler frontier. First
// destructive admin operation — requires `-y` to confirm. URLs come from
// positional args, `-file`, or both (mirrors iter-93's `contents` CLI pattern).
// Iter 100.
func runAdminRecrawl(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("admin recrawl", flag.ExitOnError)
	defaultServer := "http://" + cfg.Server.Addr
	defaultServer = strings.Replace(defaultServer, "0.0.0.0", "127.0.0.1", 1)
	serverURL := fs.String("server", defaultServer, "cosift server URL")
	token := fs.String("token", "", "admin bearer token (defaults to COSIFT_ADMIN_TOKEN env)")
	filePath := fs.String("file", "", "read URLs from FILE (one per line, # comments allowed)")
	confirm := fs.Bool("y", false, "confirm the destructive operation (recrawl re-enqueues URLs into the frontier)")
	jsonOut := fs.Bool("json", false, "emit raw JSON response instead of human-readable summary")
	if err := fs.Parse(args); err != nil {
		return err
	}

	urls := fs.Args()
	if *filePath != "" {
		extra, err := readURLsFromFile(*filePath)
		if err != nil {
			return fmt.Errorf("read -file: %w", err)
		}
		urls = append(urls, extra...)
	}
	if len(urls) == 0 {
		return errors.New("at least one URL or -file required")
	}
	if !*confirm {
		return fmt.Errorf("admin recrawl is destructive (re-enqueues %d URL(s) into the crawl frontier). Add -y to confirm", len(urls))
	}

	resolvedToken := *token
	if resolvedToken == "" {
		resolvedToken = os.Getenv("COSIFT_ADMIN_TOKEN")
	}

	reqBody, err := json.Marshal(server.AdminRecrawlRequest{URLs: urls})
	if err != nil {
		return err
	}
	req, err := adminRequest(ctx, http.MethodPost, *serverURL, "/admin/recrawl", resolvedToken, bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("post %s: %w", *serverURL, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server %d: %s", resp.StatusCode, body)
	}
	if *jsonOut {
		fmt.Println(string(body))
		return nil
	}
	var rr server.AdminRecrawlResponse
	if err := json.Unmarshal(body, &rr); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	fmt.Printf("Queued %d URL(s) for recrawl.\n", len(rr.Queued))
	if len(rr.Errors) > 0 {
		fmt.Printf("\n%d URL(s) failed to enqueue:\n", len(rr.Errors))
		// Sort error URLs for stable output across runs.
		keys := make([]string, 0, len(rr.Errors))
		for k := range rr.Errors {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, u := range keys {
			fmt.Printf("  %s: %s\n", u, rr.Errors[u])
		}
	}
	return nil
}

// runAdminRecrawlDomain bulk-recrawls every doc whose domain matches `pattern`
// (positional arg). Wraps iter-110's /admin/recrawl-by-domain endpoint. Requires
// `-y` to commit or `-dry-run` to preview the matched count without queuing.
// If both flags are set, `-dry-run` wins (safer-on-conflict). Iter 111.
func runAdminRecrawlDomain(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("admin recrawl-domain", flag.ExitOnError)
	defaultServer := "http://" + cfg.Server.Addr
	defaultServer = strings.Replace(defaultServer, "0.0.0.0", "127.0.0.1", 1)
	serverURL := fs.String("server", defaultServer, "cosift server URL")
	token := fs.String("token", "", "admin bearer token (defaults to COSIFT_ADMIN_TOKEN env)")
	confirm := fs.Bool("y", false, "confirm the destructive operation (recrawl every matching URL)")
	dryRun := fs.Bool("dry-run", false, "enumerate matched URLs without queuing — preview the blast radius")
	// Iter 124: cap dry-run URL listing to avoid console spam on large matches.
	// 0 = no list (count only — iter-111 behavior). 20 = default safe cap.
	// -1 = unlimited (operator explicitly wants the full list).
	listLimit := fs.Int("limit-list", 20, "in dry-run mode, max URLs to print (0 = count only, -1 = unlimited)")
	jsonOut := fs.Bool("json", false, "emit raw JSON response instead of human-readable summary")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return errors.New("domain pattern required (positional arg)")
	}
	if fs.NArg() > 1 {
		return errors.New("only one domain pattern supported per call")
	}
	pattern := fs.Arg(0)

	if !*confirm && !*dryRun {
		return fmt.Errorf("admin recrawl-domain is destructive (would re-enqueue every URL matching %q). Add -y to commit or -dry-run to preview", pattern)
	}

	resolvedToken := *token
	if resolvedToken == "" {
		resolvedToken = os.Getenv("COSIFT_ADMIN_TOKEN")
	}

	// dry-run wins when both flags are set. Operator who typed both probably
	// meant the safer option; the destructive interpretation requires they
	// explicitly drop -dry-run.
	effectiveDryRun := *dryRun

	reqBody, err := json.Marshal(server.AdminRecrawlByDomainRequest{
		Domain: pattern,
		DryRun: effectiveDryRun,
	})
	if err != nil {
		return err
	}
	req, err := adminRequest(ctx, http.MethodPost, *serverURL, "/admin/recrawl-by-domain", resolvedToken, bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("post %s: %w", *serverURL, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server %d: %s", resp.StatusCode, body)
	}
	if *jsonOut {
		fmt.Println(string(body))
		return nil
	}
	var rr server.AdminRecrawlByDomainResponse
	if err := json.Unmarshal(body, &rr); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if effectiveDryRun {
		fmt.Printf("Dry-run: %d URL(s) match %q. Re-run with -y (not -dry-run) to queue them.\n", rr.Matched, rr.Domain)
		// Iter 124: render the iter-123 URLs field. -limit-list 0 → count only
		// (iter-111 behavior, preserved by explicit opt-in). -limit-list -1 →
		// unlimited (operator wants the full list, accepts console spam).
		// Otherwise: print up to listLimit and append a "... (N more)" suffix.
		if *listLimit != 0 && len(rr.URLs) > 0 {
			max := *listLimit
			if max < 0 || max > len(rr.URLs) {
				max = len(rr.URLs)
			}
			for _, u := range rr.URLs[:max] {
				fmt.Printf("  %s\n", u)
			}
			if remaining := len(rr.URLs) - max; remaining > 0 {
				fmt.Printf("  ... (%d more — use -limit-list -1 to see all)\n", remaining)
			}
		}
		return nil
	}
	fmt.Printf("Queued %d/%d URL(s) for recrawl matching %q.\n", rr.Queued, rr.Matched, rr.Domain)
	if len(rr.Errors) > 0 {
		fmt.Printf("\n%d URL(s) failed to enqueue:\n", len(rr.Errors))
		keys := make([]string, 0, len(rr.Errors))
		for k := range rr.Errors {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, u := range keys {
			fmt.Printf("  %s: %s\n", u, rr.Errors[u])
		}
	}
	return nil
}

// runAdminReembedCLI triggers a server-side reembed via the iter-112
// `/admin/reembed` SSE endpoint. Closes the iter 112/113 server-then-CLI arc.
// Requires `-y` to commit (reembed costs LLM credits; same destructive-op
// guard pattern as iter-100/111). `-drop-old` propagates to the server's
// `DropPassagesNotModel` step. Iter 113.
func runAdminReembedCLI(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("admin reembed", flag.ExitOnError)
	defaultServer := "http://" + cfg.Server.Addr
	defaultServer = strings.Replace(defaultServer, "0.0.0.0", "127.0.0.1", 1)
	serverURL := fs.String("server", defaultServer, "cosift server URL")
	token := fs.String("token", "", "admin bearer token (defaults to COSIFT_ADMIN_TOKEN env)")
	dropOld := fs.Bool("drop-old", false, "also delete passages from other models after re-embedding")
	// Iter 117: -since restricts reembed to docs published >= DATE. Client-side
	// validation catches malformed dates before the HTTP call (saves a round-trip
	// for the common typo case).
	since := fs.String("since", "", "ISO date — only re-embed docs published >= this. Drops undated docs when set")
	confirm := fs.Bool("y", false, "confirm the operation (reembed re-runs every embedding call; counts as LLM spend)")
	// Iter 126: -dry-run shows the would-be-processed count without spending
	// LLM credits. Either -y OR -dry-run is required (iter-111 pattern).
	dryRun := fs.Bool("dry-run", false, "preview the count of docs that would be re-embedded, without actually embedding")
	jsonOut := fs.Bool("json", false, "emit raw SSE response instead of human-readable progress lines")
	if err := fs.Parse(args); err != nil {
		return err
	}
	// Validate -since client-side before checking -y, so operators see "bad
	// date" before they've committed with -y (clearer iteration).
	if _, err := parseExportDate(*since); err != nil {
		return fmt.Errorf("-since: %w", err)
	}
	// Iter 126: dry-run wins when both flags set (safer-on-conflict, matches
	// iter-111 pattern on /admin/recrawl-by-domain).
	effectiveDryRun := *dryRun
	if !*confirm && !*dryRun {
		guard := "every doc"
		if *dropOld {
			guard = "every doc AND drop other-model passages"
		}
		if *since != "" {
			guard = "every doc published >= " + *since
			if *dropOld {
				guard += " AND drop other-model passages"
			}
		}
		return fmt.Errorf("admin reembed will re-embed %s (counts as LLM spend). Add -y to commit or -dry-run to preview", guard)
	}

	resolvedToken := *token
	if resolvedToken == "" {
		resolvedToken = os.Getenv("COSIFT_ADMIN_TOKEN")
	}

	reqBody, err := json.Marshal(server.AdminReembedRequest{
		DropOld: *dropOld,
		Since:   *since,
		DryRun:  effectiveDryRun,
	})
	if err != nil {
		return err
	}
	req, err := adminRequest(ctx, http.MethodPost, *serverURL, "/admin/reembed", resolvedToken, bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	// Reembed can run for tens of minutes on large corpora. Bump the HTTP
	// client timeout well past the synth CLIs; the server emits keep-alive
	// progress events every ~2s so the connection stays warm.
	client := &http.Client{Timeout: 60 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("post %s: %w", *serverURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("server %d: %s", resp.StatusCode, b)
	}
	if *jsonOut {
		// In JSON mode, just stream the raw SSE bytes through to stdout. Useful
		// for piping into a custom parser or log file. Different shape from the
		// other admin CLIs' -json because reembed has no terminal JSON blob —
		// the event stream IS the response.
		_, err := io.Copy(os.Stdout, resp.Body)
		return err
	}
	return consumeReembedSSE(resp.Body)
}

// consumeReembedSSE reads /admin/reembed events and renders progress lines.
// Iter 113 — third SSE consumer in the CLI (after iter-98 research and
// iter-109 answer), reusing the iter-109 forEachSSEEvent scanner.
func consumeReembedSSE(body io.Reader) error {
	var sawDone bool
	// Iter 126: track the started event's total_docs and target_model so the
	// dry-run done-event branch can reference them (the dry-run done payload
	// reports zeros for processed/passages, but the meaningful count is in started).
	var startedTotal int
	var startedTarget string

	err := forEachSSEEvent(body, func(event, data string) error {
		switch event {
		case "started":
			var s struct {
				TotalDocs   int    `json:"total_docs"`
				TargetModel string `json:"target_model"`
			}
			if err := json.Unmarshal([]byte(data), &s); err != nil {
				return nil // tolerate malformed mid-stream event
			}
			startedTotal = s.TotalDocs
			startedTarget = s.TargetModel
			fmt.Printf("[reembed started: %d docs, target=%s]\n", s.TotalDocs, s.TargetModel)
		case "progress":
			var p struct {
				DocsProcessed   int `json:"docs_processed"`
				PassagesWritten int `json:"passages_written"`
			}
			if err := json.Unmarshal([]byte(data), &p); err != nil {
				return nil
			}
			fmt.Printf("[progress: %d docs, %d passages]\n", p.DocsProcessed, p.PassagesWritten)
		case "done":
			sawDone = true
			var d struct {
				DocsProcessed   int    `json:"docs_processed"`
				PassagesWritten int    `json:"passages_written"`
				DroppedOld      int64  `json:"dropped_old"`
				DryRun          bool   `json:"dry_run,omitempty"`
				Took            string `json:"took"`
			}
			if err := json.Unmarshal([]byte(data), &d); err != nil {
				return fmt.Errorf("parse done event: %w", err)
			}
			if d.DryRun {
				// Iter 126: dry-run summary — reference the started event's
				// count (the meaningful "would-be-processed" number).
				fmt.Printf("Dry-run: %d docs would be re-embedded with target=%s. Re-run without -dry-run (or with -y) to actually do it.\n",
					startedTotal, startedTarget)
			} else {
				fmt.Printf("Done: %d docs reembedded, %d passages written, %d dropped, took %s.\n",
					d.DocsProcessed, d.PassagesWritten, d.DroppedOld, d.Took)
			}
			return errSSEDone
		case "error":
			var e struct {
				Detail string `json:"detail"`
			}
			_ = json.Unmarshal([]byte(data), &e)
			return fmt.Errorf("server stream error: %s", e.Detail)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !sawDone {
		fmt.Fprintln(os.Stderr, "(stream ended without `done` event)")
	}
	return nil
}

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
	chunkSize := fs.Int("chunk-size", 0, "iter-145: passage chunker target words (0 = index.NewChunker default 320); A/B across runs to compare retrieval at different granularities")
	chunkOverlap := fs.Int("chunk-overlap", 0, "iter-145: passage chunker overlap words (0 = default 64)")
	autoParaphrase := fs.Bool("auto-paraphrase", false, "generate N paraphrases per query via the chat client at eval time + RRF-fuse results")
	paraphraseN := fs.Int("paraphrase-n", 2, "number of paraphrases per query")
	paraphraseModel := fs.String("paraphrase-model", "gpt-4o-mini", "chat model for paraphrase generation")
	mainWeight := fs.Float64("main-weight", 0, "iter-139/140: main-query weight in -auto-paraphrase OR -planner RRF fusion (paraphrases / sub-queries each weight 1.0); 0 = equal-weight (standard RRF); mirrors server-side Defaults.ExpandMainWeight for offline measurement")
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
			baseURL: strings.TrimRight(*apiURL, "/"),
			bearer:  *apiBearer,
			retriever: *retriever,
			rerank: *useRerank,
			http:   &http.Client{Timeout: 30 * time.Second},
		}
		summary, err := eval.Run(ctx, qs, ret)
		if err != nil {
			return err
		}
		summary.Name = fmt.Sprintf("%s/api(%s)", qs.Name, *apiURL)
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
		id, err := s.UpsertDocument(ctx, &store.Document{
			URL: d.URL, Title: d.Title, Text: d.Text,
			Source: "eval", FetchedAt: time.Now(),
		})
		if err != nil {
			return fmt.Errorf("ingest %s: %w", d.URL, err)
		}
		if err := bm.IndexDocument(ctx, id, d.Title, d.Text); err != nil {
			return fmt.Errorf("index %s: %w", d.URL, err)
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
		// Tests the iter-40 hypothesis that smaller-but-more-similar noise
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
			id, err := s.UpsertDocument(ctx, &store.Document{
				URL: dURL, Title: dTitle, Text: dText, Source: "distractor", FetchedAt: time.Now(),
			})
			if err != nil {
				return fmt.Errorf("distractor %d: %w", i, err)
			}
			if err := bm.IndexDocument(ctx, id, dTitle, dText); err != nil {
				return fmt.Errorf("index distractor %d: %w", i, err)
			}
		}
		fmt.Printf("injected %d distractor docs into BM25 index\n", *distractors)
	}

	// Build the requested retriever.
	var ret eval.Retriever
	switch *retriever {
	case "bm25":
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
		vecs, err := batchEmbed(ctx, emb, allTexts, *embBatch)
		if err != nil {
			return fmt.Errorf("embed corpus: %w", err)
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

// firstEnv returns the first non-empty value among the named environment vars.
func firstEnv(names ...string) string {
	for _, n := range names {
		if v := os.Getenv(n); v != "" {
			return v
		}
	}
	return ""
}

func runServe(ctx context.Context, cfg *config.Config) error {
	s, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer s.Close()

	srv := server.New(s)
	// Instance-wide retrieval defaults from cfg.Defaults. Per-request query
	// params still override; this is just what the handler picks up when the
	// caller doesn't specify.
	srv = srv.WithDefaults(server.Defaults{
		Retriever:         cfg.Defaults.Retriever,
		Expand:            cfg.Defaults.Expand,
		ResearchStrategy:  cfg.Defaults.ResearchStrategy,
		ResearchSynthK:    cfg.Defaults.ResearchSynthK,
		ExpandMainWeight:  cfg.Defaults.ExpandMainWeight,
		HybridDenseWeight: cfg.Defaults.HybridDenseWeight,
	})
	if cfg.Defaults.Retriever != "" || cfg.Defaults.Expand || cfg.Defaults.ResearchStrategy != "" || cfg.Defaults.ResearchSynthK > 0 || cfg.Defaults.ExpandMainWeight > 0 || cfg.Defaults.HybridDenseWeight > 0 {
		log.Printf("defaults: retriever=%q expand=%v research_strategy=%q research_synth_k=%d expand_main_weight=%v hybrid_dense_weight=%v",
			cfg.Defaults.Retriever, cfg.Defaults.Expand, cfg.Defaults.ResearchStrategy, cfg.Defaults.ResearchSynthK, cfg.Defaults.ExpandMainWeight, cfg.Defaults.HybridDenseWeight)
	}
	// Iter 145: thread chunker config to /admin/reembed handler so re-embed
	// produces passages with the same shape as crawl-time indexing.
	if cfg.Crawler.ChunkSize > 0 || cfg.Crawler.ChunkOverlap > 0 {
		srv = srv.WithChunker(cfg.Crawler.ChunkSize, cfg.Crawler.ChunkOverlap)
		log.Printf("chunker: size=%d overlap=%d", cfg.Crawler.ChunkSize, cfg.Crawler.ChunkOverlap)
	}
	if tok := cfg.Server.AdminToken; tok != "" {
		srv = srv.WithAdminToken(tok)
		log.Printf("/admin/* endpoints enabled (bearer-auth)")
	}
	if len(cfg.Server.TrustedProxies) > 0 {
		s2, err := srv.WithTrustedProxies(cfg.Server.TrustedProxies)
		if err != nil {
			return fmt.Errorf("trusted_proxies: %w", err)
		}
		srv = s2
		log.Printf("X-Forwarded-For trusted from %v", cfg.Server.TrustedProxies)
	}

	// Auto-wire embeddings if model is set and an API key is in env.
	if cfg.Embeddings.Model != "" {
		apiKey := os.Getenv("OPENAI_API_KEY")
		if apiKey == "" {
			apiKey = os.Getenv("OPENAI")
		}
		if apiKey == "" {
			log.Printf("warning: embeddings configured but no OPENAI_API_KEY in env; dense/hybrid disabled")
		} else {
			model := cfg.Embeddings.Model
			dim := cfg.Embeddings.Dim
			if dim == 0 {
				dim = 1536
			}
			emb := embed.NewOpenAIClient(apiKey, cfg.Embeddings.URL, model, dim)
			vi, err := index.LoadVectorIndex(ctx, s, model, dim)
			if err != nil {
				return fmt.Errorf("load vector index: %w", err)
			}
			srv = srv.WithVector(vi, emb)
			log.Printf("vector index loaded: %d passages, model=%s dim=%d", vi.Len(), model, dim)

			// Chat is opt-in via cfg.Chat.Model. Reuses the same API key.
			if cfg.Chat.Model != "" {
				chat := embed.NewOpenAIChat(apiKey, cfg.Chat.URL, cfg.Chat.Model)
				srv = srv.WithChat(chat)
				log.Printf("/answer enabled with chat model=%s", cfg.Chat.Model)
				// Auto-enable /search?expand=true via the same chat client.
				// Iter-45 measured +0.02 nDCG at 10k distractors for $0.004 per run.
				srv = srv.WithParaphraser(chat, 2)
				log.Printf("/search?expand=true enabled (auto-paraphrase via %s)", cfg.Chat.Model)
			}

			// Reranker: HTTP backend wins if URL is set; otherwise LLM backend
			// (only if chat is configured). Either is opt-out via cfg.Rerank.Enabled=false
			// even when configured.
			var rr rerank.Reranker
			rerankWanted := cfg.Rerank.URL != "" || cfg.Chat.Model != ""
			if !cfg.Rerank.Enabled && rerankWanted {
				// Default ON when wired; flip cfg.Rerank.Enabled false to skip.
				cfg.Rerank.Enabled = true
			}
			if cfg.Rerank.Enabled && cfg.Rerank.URL != "" {
				rerankKey := cfg.Rerank.APIKey
				if rerankKey == "" {
					rerankKey = firstEnv("COHERE_API_KEY", "VOYAGE_API_KEY")
				}
				rr = rerank.NewHTTPReranker(cfg.Rerank.URL, rerankKey, cfg.Rerank.Model)
				log.Printf("rerank enabled with http endpoint=%s model=%s", cfg.Rerank.URL, cfg.Rerank.Model)
			} else if cfg.Rerank.Enabled && cfg.Chat.Model != "" {
				rerankModel := cfg.Rerank.Model
				if rerankModel == "" {
					rerankModel = cfg.Chat.Model
				}
				rerankChat := embed.NewOpenAIChat(apiKey, cfg.Chat.URL, rerankModel)
				rr = rerank.NewLLMReranker(rerankChat)
				log.Printf("rerank enabled with LLM model=%s (fallback path; consider an HTTP reranker for cost)", rerankModel)
			}
			if rr != nil {
				srv = srv.WithReranker(rr, cfg.Rerank.CandidateK)
			}
		}
	}

	// On-demand /contents: use the crawler's FetchOne. Doesn't share the worker
	// pool's rate gate or robots cache — callers should set a sensible UA + body cap.
	srv = srv.WithFetcher(func(ctx context.Context, u string) (string, string, string, error) {
		r, err := crawler.FetchOne(ctx, nil, cfg.Crawler.UserAgent, u, cfg.Crawler.MaxBodyBytes)
		if err != nil {
			return "", "", "", err
		}
		return r.Title, r.Text, r.Lang, nil
	})

	log.Printf("cosift serving on %s", cfg.Server.Addr)
	return server.ListenAndServe(ctx, cfg.Server.Addr, srv.Handler())
}

// runIngest loads a corpus.json directly into the local store + indexes —
// useful for evaluation, vertical deployments, and users with a pre-curated
// dataset who don't need a crawler.
//
// The corpus shape matches eval.Corpus: {"docs": [{"url","title","text"}...]}.
// If embeddings are configured, every doc is chunked + embedded + persisted
// to the passages table the same way the crawler does.
// progressReporter logs progress for a long-running CLI loop at most once per
// configured interval. Time-based (vs every-N-iterations) so the cadence is
// consistent regardless of per-item cost variance. Set interval to 0 to
// disable. Iter 106 — designed for ingest but generic enough for other loops.
type progressReporter struct {
	label    string
	total    int
	interval time.Duration
	last     time.Time
}

func newProgressReporter(label string, total int, interval time.Duration) *progressReporter {
	return &progressReporter{
		label:    label,
		total:    total,
		interval: interval,
		// Initialize `last` to now so the first call after construction
		// doesn't immediately log (avoids a noisy first-iter "0/N" line).
		last: time.Now(),
	}
}

// maybeLog emits a progress line only if `interval` has elapsed since the last
// emission. Callers pass the 1-indexed current item count. No-op when interval
// is <= 0 or when total is 0 (avoids divide-by-zero in the percentage).
func (p *progressReporter) maybeLog(current int) {
	if p.interval <= 0 || p.total <= 0 {
		return
	}
	if time.Since(p.last) < p.interval {
		return
	}
	pct := float64(current) / float64(p.total) * 100
	log.Printf("%s: %d/%d (%.1f%%)", p.label, current, p.total, pct)
	p.last = time.Now()
}

// loadCorpusJSONL reads a JSONL file (one eval.CorpusDoc per line) into a
// Corpus. Empty lines are skipped (some tools emit trailing blanks). Pairs
// with iter-103's `export -format jsonl` to close the round-trip for ML
// pipelines. Iter 105.
func loadCorpusJSONL(path string) (*eval.Corpus, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	// JSONL lines can be large (full doc text); raise the scanner buffer cap
	// well above the default 64KB. 4MB matches iter-98's SSE parser ceiling.
	scanner.Buffer(make([]byte, 0, 64<<10), 4<<20)
	c := &eval.Corpus{}
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" {
			continue
		}
		var d eval.CorpusDoc
		if err := json.Unmarshal([]byte(raw), &d); err != nil {
			return nil, fmt.Errorf("parse %s line %d: %w", path, lineNum, err)
		}
		c.Docs = append(c.Docs, d)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return c, nil
}

func runIngest(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("ingest", flag.ExitOnError)
	corpusPath := fs.String("corpus", "", "path to corpus file (required)")
	source := fs.String("source", "ingest", "source tag for documents")
	// Iter 105: -format selects the loader. Default "auto" infers from file
	// extension (.jsonl → jsonl, anything else → json). Explicit override
	// avoids ambiguity when files have non-standard extensions.
	format := fs.String("format", "auto", "input format: auto (default — infer from extension) | json | jsonl")
	// Iter 106: progress reporting. Operators ingesting large corpora need
	// some signal that work is happening. 0 disables.
	progressInterval := fs.Duration("progress", 5*time.Second, "log per-doc/per-passage progress every N (0 disables)")
	// Iter 149: CLI override for chunker config (mirrors iter-145 on eval/answer-eval).
	// 0 → fall through to cfg.Crawler.{ChunkSize,ChunkOverlap}, then to NewChunker defaults.
	ingestChunkSize := fs.Int("chunk-size", 0, "iter-149: passage chunker target words (0 = use cfg.Crawler.ChunkSize, then NewChunker default 320)")
	ingestChunkOverlap := fs.Int("chunk-overlap", 0, "iter-149: passage chunker overlap words (0 = use cfg.Crawler.ChunkOverlap, then default 64)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *corpusPath == "" {
		return errors.New("ingest: -corpus is required")
	}
	resolvedFormat := *format
	if resolvedFormat == "auto" {
		if strings.HasSuffix(strings.ToLower(*corpusPath), ".jsonl") {
			resolvedFormat = "jsonl"
		} else {
			resolvedFormat = "json"
		}
	}
	var corpus *eval.Corpus
	var err error
	switch resolvedFormat {
	case "json":
		corpus, err = eval.LoadCorpus(*corpusPath)
	case "jsonl":
		corpus, err = loadCorpusJSONL(*corpusPath)
	default:
		return fmt.Errorf("unknown -format %q (want: auto | json | jsonl)", *format)
	}
	if err != nil {
		return err
	}

	s, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer s.Close()
	bm := index.NewBM25(s)

	// Optional embedder — same auto-wire as runServe.
	var emb embed.Embedder
	if cfg.Embeddings.Model != "" {
		apiKey := os.Getenv("OPENAI_API_KEY")
		if apiKey == "" {
			apiKey = os.Getenv("OPENAI")
		}
		if apiKey != "" {
			dim := cfg.Embeddings.Dim
			if dim == 0 {
				dim = 1536
			}
			emb = embed.NewOpenAIClient(apiKey, cfg.Embeddings.URL, cfg.Embeddings.Model, dim)
		} else {
			log.Printf("warning: embeddings configured but no API key; ingesting BM25 only")
		}
	}

	// Pre-compute chunks for the whole corpus so we can batch-embed in one call.
	// Iter 149: CLI flag wins over cfg.Crawler.ChunkSize (only when flag is set).
	chunkSize := cfg.Crawler.ChunkSize
	if *ingestChunkSize > 0 {
		chunkSize = *ingestChunkSize
	}
	chunkOverlap := cfg.Crawler.ChunkOverlap
	if *ingestChunkOverlap > 0 {
		chunkOverlap = *ingestChunkOverlap
	}
	chunker := chunkerWith(chunkSize, chunkOverlap)
	type passageRef struct {
		docIdx int
		chunk  index.Chunk
	}
	var allTexts []string
	var refs []passageRef
	if emb != nil {
		for i, d := range corpus.Docs {
			for _, c := range chunker.Chunk(d.Title + "\n\n" + d.Text) {
				allTexts = append(allTexts, c.Text)
				refs = append(refs, passageRef{docIdx: i, chunk: c})
			}
		}
		log.Printf("ingest: embedding %d passages across %d docs with %s", len(allTexts), len(corpus.Docs), cfg.Embeddings.Model)
	}

	var vecs [][]float32
	if emb != nil && len(allTexts) > 0 {
		vecs, err = emb.Embed(ctx, allTexts)
		if err != nil {
			return fmt.Errorf("embed: %w", err)
		}
	}

	// Map docIdx → docID for the passage writes.
	docIDs := make([]int64, len(corpus.Docs))
	docProgress := newProgressReporter("ingest docs", len(corpus.Docs), *progressInterval)
	for i, d := range corpus.Docs {
		sha := sha256.Sum256([]byte(d.Text))
		id, err := s.UpsertDocument(ctx, &store.Document{
			URL: d.URL, Title: d.Title, Text: d.Text,
			Source: *source, Quality: 0.7, FetchedAt: time.Now(),
			ContentSHA: sha[:],
		})
		if err != nil {
			return fmt.Errorf("upsert %s: %w", d.URL, err)
		}
		if err := bm.IndexDocument(ctx, id, d.Title, d.Text); err != nil {
			return fmt.Errorf("index %s: %w", d.URL, err)
		}
		docIDs[i] = id
		docProgress.maybeLog(i + 1)
	}

	// Write passages.
	if emb != nil && len(vecs) == len(refs) {
		passageProgress := newProgressReporter("ingest passages", len(vecs), *progressInterval)
		for j, v := range vecs {
			ref := refs[j]
			p := &store.Passage{
				DocID:     docIDs[ref.docIdx],
				Offset:    ref.chunk.Offset,
				Length:    ref.chunk.Length,
				Model:     emb.Model(),
				Embedding: v,
			}
			if err := s.UpsertPassage(ctx, p); err != nil {
				return fmt.Errorf("passage %s offset=%d: %w", corpus.Docs[ref.docIdx].URL, ref.chunk.Offset, err)
			}
			passageProgress.maybeLog(j + 1)
		}
	}

	log.Printf("ingest: %d docs, %d passages", len(corpus.Docs), len(vecs))
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
		r, err := benchVector(*n, *dim, *queries)
		if err != nil {
			return err
		}
		emit(r)
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
		// Iter 206: SQLite vs Pebble head-to-head. Same N synthetic docs, same
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

// runBenchCompare diffs two saved bench reports side-by-side. Mirrors iter-65
// answer-eval-compare for the bench surface — locks the iter-71 JSON contract
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
// matches iter-65 answer-eval-compare's row style.
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

func benchVector(n, dim, queries int) (*benchResult, error) {
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
	}, nil
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
// Iter 206.
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
// Iter 206 — empirical validation of the path-2 storage rework.
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
	// iter 71: -per-host-delay propagates from the bench CLI flag. Default 0
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
		if err := c.Seed(fmt.Sprintf("%s/p%d", srv.URL, i)); err != nil {
			return nil, fmt.Errorf("seed p%d: %w", i, err)
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

// runRefreshDue re-queues URLs whose adaptive interval has elapsed.
// Combined with conditional GET, a refresh-due pass over a stable corpus
// mostly costs 304 round-trips — no body, no parse, no embed.
//
// One-shot by default. Pass `-interval` to loop forever — useful when running
// as a systemd service or inside the Docker image (no extra cron needed).
func runRefreshDue(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("refresh-due", flag.ExitOnError)
	minH := fs.Duration("min", 1*time.Hour, "minimum re-crawl interval")
	maxH := fs.Duration("max", 30*24*time.Hour, "maximum re-crawl interval")
	limit := fs.Int("limit", 100, "max URLs to enqueue per pass")
	dryRun := fs.Bool("dry-run", false, "print URLs that would be refreshed, don't enqueue")
	interval := fs.Duration("interval", 0, "loop with this delay between passes (0 = one-shot)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	s, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer s.Close()

	pass := func() error {
		due, err := s.DueForRefresh(ctx, *minH, *maxH, *limit)
		if err != nil {
			return err
		}
		if len(due) == 0 {
			log.Printf("refresh-due: 0 URLs due")
			return nil
		}
		log.Printf("refresh-due: %d URLs due (limit %d, min=%s max=%s)", len(due), *limit, *minH, *maxH)
		if *dryRun {
			for _, u := range due {
				fmt.Println(u)
			}
			return nil
		}
		c := crawler.New(cfg.Crawler, s)
		for _, u := range due {
			if err := c.Recrawl(ctx, u); err != nil {
				log.Printf("recrawl %s: %v", u, err)
			}
		}
		return c.Run(ctx)
	}

	if *interval <= 0 {
		return pass()
	}
	// Daemon mode: pass, sleep, pass, ... until ctx cancelled.
	for {
		if err := pass(); err != nil {
			log.Printf("refresh-due pass: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(*interval):
		}
	}
}

// runExport writes the local store's documents to a portable corpus.json,
// shape-compatible with `cosift ingest`. Round-trips cleanly:
//   cosift export -output corpus.json
//   cosift -config other.json ingest -corpus corpus.json
//
// Documents only — passages (embeddings) aren't exported because the receiver
// likely uses a different embedding model.
// runMigrateToPebble copies a SQLite-backed cosift data directory into a
// fresh Pebble store. Iter 204 — fifth piece of the path-2 storage rework.
//
// Migrates:
//   - documents (URL, title, text, metadata) via PebbleStore.UpsertDocument
//   - BM25 postings (re-tokenized + re-indexed to preserve iter-197 title
//     boost; the SQLite postings table is NOT copied directly because
//     re-indexing through PebbleBM25 is the same code path that production
//     uses going forward — eliminates the divergence risk of two
//     posting-write paths)
//
// Does NOT migrate (deferred): frontier rows, query_outcomes feedback,
// paraphrase/HyDE caches, vector embeddings. Operators starting from a
// migrated Pebble store get a working BM25 index immediately; dense + LLM
// caches rebuild from scratch on the new instance.
//
// The destination directory must be empty; the migration refuses to
// overwrite existing data so a mistyped path can't clobber a working store.
func runMigrateToPebble(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("migrate-to-pebble", flag.ExitOnError)
	output := fs.String("output", "", "output directory for the Pebble store (required)")
	progress := fs.Duration("progress", 5*time.Second, "log progress every N (0 disables)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *output == "" {
		return errors.New("migrate-to-pebble: -output is required")
	}
	// Refuse to write into a directory that already contains a Pebble store.
	if entries, err := os.ReadDir(*output); err == nil && len(entries) > 0 {
		return fmt.Errorf("migrate-to-pebble: -output %s is non-empty; refusing to overwrite (move or remove first)", *output)
	}

	src, err := store.Open(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("open source SQLite store at %s: %w", cfg.DataDir, err)
	}
	defer src.Close()

	dst, err := store.OpenPebble(*output)
	if err != nil {
		return fmt.Errorf("open destination Pebble store at %s: %w", *output, err)
	}
	defer dst.Close()

	pidx := index.NewPebbleBM25(dst)

	docs, err := src.ListDocuments(ctx, 0)
	if err != nil {
		return fmt.Errorf("list source documents: %w", err)
	}
	log.Printf("migrate-to-pebble: %d documents to copy (source: %s → destination: %s)",
		len(docs), cfg.DataDir, *output)

	reporter := newProgressReporter("migrate docs", len(docs), *progress)
	for i, d := range docs {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Upsert the doc into Pebble — assigns a fresh Pebble-side ID.
		newID, err := dst.UpsertDocument(ctx, d)
		if err != nil {
			return fmt.Errorf("upsert doc %s: %w", d.URL, err)
		}
		// Re-index in Pebble BM25. Uses the same Tokenize + TitleBoost that
		// production reads through, so behavior of the migrated index matches
		// what a fresh crawl would produce.
		if err := pidx.IndexDocument(ctx, newID, d.Title, d.Text); err != nil {
			return fmt.Errorf("index doc %s: %w", d.URL, err)
		}
		reporter.maybeLog(i + 1)
	}

	stats, err := dst.Stats(ctx)
	if err != nil {
		return err
	}
	log.Printf("migrate-to-pebble: %d documents indexed into Pebble at %s", stats.Documents, *output)
	return nil
}

func runExport(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	output := fs.String("output", "", "output path (default depends on -format: corpus-export.{json,jsonl,txt,md})")
	limit := fs.Int("limit", 0, "max docs to export (0 = all, applied AFTER filters)")
	// Iter 103: -format selects the on-disk shape.
	//   json (default — eval.Corpus pretty-printed; backward-compatible with iter-1)
	//   jsonl — one {url,title,text} per line; common for ML fine-tuning pipelines
	//   text  — Title/URL header + body + --- separator; for RAG corpora / grep
	//   md    — `# Title` + URL line + body + horizontal rule; for LLM prompt piping
	format := fs.String("format", "json", "output format: json | jsonl | text | md")
	// Iter 104: filters. All applied client-side after ListDocuments.
	includeDomains := fs.String("include-domains", "", "comma-separated allowlist of domains (suffix match on dot boundary)")
	excludeDomains := fs.String("exclude-domains", "", "comma-separated denylist of domains")
	since := fs.String("since", "", "ISO date — only docs with published_at on or after (drops undated docs)")
	until := fs.String("until", "", "ISO date — only docs with published_at on or before (drops undated docs)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := validateExportFormat(*format); err != nil {
		return err
	}
	if *output == "" {
		*output = "corpus-export." + exportExtension(*format)
	}

	sinceT, err := parseExportDate(*since)
	if err != nil {
		return fmt.Errorf("-since: %w", err)
	}
	untilT, err := parseExportDate(*until)
	if err != nil {
		return fmt.Errorf("-until: %w", err)
	}
	includeList := splitDomainCSV(*includeDomains)
	excludeList := splitDomainCSV(*excludeDomains)
	hasDateFilter := !sinceT.IsZero() || !untilT.IsZero()

	s, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer s.Close()

	// Iter-1 limit applies AFTER filters now — operators expect "give me 100
	// docs from example.com" to mean 100 example.com docs, not 100 docs total
	// of which some are from example.com. Pass 0 (no limit) to ListDocuments
	// and apply the limit client-side after filtering.
	rawLimit := 0
	if *limit > 0 && !hasDateFilter && len(includeList) == 0 && len(excludeList) == 0 {
		// No filters → push the limit down to ListDocuments (faster, less memory).
		rawLimit = *limit
	}
	docs, err := s.ListDocuments(ctx, rawLimit)
	if err != nil {
		return err
	}

	docs = filterExportDocs(docs, includeList, excludeList, sinceT, untilT)
	if *limit > 0 && len(docs) > *limit {
		docs = docs[:*limit]
	}

	f, err := os.Create(*output)
	if err != nil {
		return err
	}
	defer f.Close()

	switch *format {
	case "json":
		// Iter-1 wire shape: eval.Corpus pretty-printed. Preserved as default.
		corpus := eval.Corpus{Docs: make([]eval.CorpusDoc, 0, len(docs))}
		for _, d := range docs {
			corpus.Docs = append(corpus.Docs, eval.CorpusDoc{URL: d.URL, Title: d.Title, Text: d.Text})
		}
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(corpus); err != nil {
			return fmt.Errorf("encode: %w", err)
		}
	case "jsonl":
		// One {url,title,text} per line. No outer array — caller streams line-by-line.
		enc := json.NewEncoder(f)
		for _, d := range docs {
			if err := enc.Encode(eval.CorpusDoc{URL: d.URL, Title: d.Title, Text: d.Text}); err != nil {
				return fmt.Errorf("encode jsonl: %w", err)
			}
		}
	case "text":
		// Title:/URL: headers + blank + body + --- separator. Matches iter-93
		// `contents -text` batch separator convention (LLMs interpret `---` as
		// section breaks).
		for i, d := range docs {
			if i > 0 {
				fmt.Fprintln(f, "---")
				fmt.Fprintln(f)
			}
			if d.Title != "" {
				fmt.Fprintf(f, "Title: %s\n", d.Title)
			}
			fmt.Fprintf(f, "URL: %s\n\n", d.URL)
			fmt.Fprintln(f, d.Text)
		}
	case "md":
		// `# Title` heading + italic URL line + body + horizontal rule. For
		// piping into LLM prompt contexts that render markdown.
		for i, d := range docs {
			if i > 0 {
				fmt.Fprintln(f)
				fmt.Fprintln(f, "---")
				fmt.Fprintln(f)
			}
			if d.Title != "" {
				fmt.Fprintf(f, "# %s\n\n", d.Title)
			}
			fmt.Fprintf(f, "_%s_\n\n", d.URL)
			fmt.Fprintln(f, d.Text)
		}
	}
	log.Printf("export: %d docs → %s (%s)", len(docs), *output, *format)
	return nil
}

// parseExportDate accepts YYYY-MM-DD or full RFC3339 (matches the iter-77
// /search?since=/?until= conventions). Empty input returns zero time + nil
// error (no filter). Iter 104.
func parseExportDate(s string) (time.Time, error) {
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

// splitDomainCSV parses a comma-separated list of domains into lowercased
// non-empty entries. Iter 104 (mirrors internal/server's unexported splitCSV).
func splitDomainCSV(s string) []string {
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

// matchesDomainPattern is the strict suffix-on-dot-boundary match the iter-79
// /search filter uses. `example.com` matches `blog.example.com` but NOT
// `evilexample.com`. Iter 104.
func matchesDomainPattern(host string, patterns []string) bool {
	host = strings.ToLower(host)
	for _, p := range patterns {
		if host == p || strings.HasSuffix(host, "."+p) {
			return true
		}
	}
	return false
}

// hostFromURL extracts the lowercased host without port from a URL. Returns
// empty string when parsing fails so callers can treat unparseable URLs
// uniformly (typically: excluded from include-filtered exports). Iter 104.
func hostFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
}

// filterExportDocs applies the iter-104 export filters to a doc slice
// in-memory. Returns the filtered slice (in input order — preserves whatever
// ordering ListDocuments returned). Iter 104.
func filterExportDocs(docs []*store.Document, include, exclude []string, since, until time.Time) []*store.Document {
	hasDateFilter := !since.IsZero() || !until.IsZero()
	hasDomainFilter := len(include) > 0 || len(exclude) > 0
	if !hasDateFilter && !hasDomainFilter {
		return docs
	}
	out := make([]*store.Document, 0, len(docs))
	for _, d := range docs {
		// Date filter: undated docs are excluded when ANY date bound is set
		// (matches iter-77 /search?since=/?until= semantics).
		if hasDateFilter {
			if d.PublishedAt.IsZero() {
				continue
			}
			if !since.IsZero() && d.PublishedAt.Before(since) {
				continue
			}
			if !until.IsZero() && d.PublishedAt.After(until) {
				continue
			}
		}
		if hasDomainFilter {
			host := hostFromURL(d.URL)
			if len(include) > 0 && !matchesDomainPattern(host, include) {
				continue
			}
			if len(exclude) > 0 && matchesDomainPattern(host, exclude) {
				continue
			}
		}
		out = append(out, d)
	}
	return out
}

// validateExportFormat is the export-side counterpart to iter-95's
// validateFormat. Different accept-list because export supports json/jsonl
// (file-shaped) which the synth-output CLIs don't, and synth CLIs support
// markdown aliases which export doesn't need (single canonical name reduces
// confusion when picking a file extension). Iter 103.
func validateExportFormat(v string) error {
	switch v {
	case "json", "jsonl", "text", "md":
		return nil
	default:
		return fmt.Errorf("export format must be one of: json, jsonl, text, md (got %q)", v)
	}
}

// exportExtension picks a conventional file extension for each export format.
// Used to derive a default -output path when the operator doesn't specify one.
// Iter 103.
func exportExtension(format string) string {
	switch format {
	case "jsonl":
		return "jsonl"
	case "text":
		return "txt"
	case "md":
		return "md"
	default: // json
		return "json"
	}
}

// runGC drops persistently-errored frontier rows and (optionally) VACUUMs
// the SQLite file. Designed for long-lived deployments where frontier garbage
// would otherwise accumulate.
func runGC(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("gc", flag.ExitOnError)
	minAttempts := fs.Int("min-attempts", 5, "drop frontier rows with status='error' AND attempts >= N")
	paraphraseTTL := fs.Duration("paraphrase-ttl", 0, "drop query_paraphrases rows older than this (0 = skip)")
	vacuum := fs.Bool("vacuum", true, "run VACUUM after cleanup to reclaim disk")
	if err := fs.Parse(args); err != nil {
		return err
	}
	s, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer s.Close()

	n, err := s.GCErroredFrontier(ctx, *minAttempts)
	if err != nil {
		return fmt.Errorf("gc frontier: %w", err)
	}
	log.Printf("gc: removed %d errored frontier rows (min_attempts=%d)", n, *minAttempts)

	if *paraphraseTTL > 0 {
		pn, err := s.PruneStaleParaphrases(ctx, *paraphraseTTL)
		if err != nil {
			return fmt.Errorf("gc paraphrases: %w", err)
		}
		log.Printf("gc: pruned %d stale paraphrases (ttl=%s)", pn, *paraphraseTTL)
	}

	if *vacuum {
		log.Printf("gc: vacuuming...")
		if err := s.Vacuum(ctx); err != nil {
			return fmt.Errorf("vacuum: %w", err)
		}
		log.Printf("gc: vacuum complete")
	}
	return nil
}

// runOutcomes exports query_outcomes for offline calibration work. JSON for
// programmatic consumers, CSV for spreadsheet / pandas notebooks.
func runOutcomes(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("outcomes", flag.ExitOnError)
	output := fs.String("output", "outcomes.json", "output path")
	format := fs.String("format", "json", "json | csv")
	limit := fs.Int("limit", 0, "max rows (0 = all)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *format != "json" && *format != "csv" {
		return fmt.Errorf("unsupported format %q (use json or csv)", *format)
	}
	s, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer s.Close()

	rows, err := s.ListOutcomes(ctx, *limit)
	if err != nil {
		return err
	}
	f, err := os.Create(*output)
	if err != nil {
		return err
	}
	defer f.Close()

	switch *format {
	case "json":
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rows); err != nil {
			return err
		}
	case "csv":
		w := csv.NewWriter(f)
		_ = w.Write([]string{"id", "query", "url", "score", "useful", "source", "recorded_at"})
		for _, r := range rows {
			useful := "0"
			if r.Useful {
				useful = "1"
			}
			_ = w.Write([]string{
				strconv.FormatInt(r.ID, 10),
				r.Query, r.URL,
				strconv.FormatFloat(r.Score, 'f', -1, 64),
				useful, r.Source,
				r.RecordedAt.Format(time.RFC3339),
			})
		}
		w.Flush()
		if err := w.Error(); err != nil {
			return err
		}
	}
	log.Printf("outcomes: %d rows → %s (%s)", len(rows), *output, *format)
	return nil
}

// runDoctor checks the local install: data dir writable, schema open,
// fixtures present, env recognized. Pure local checks — no network calls,
// no API key spending. Use `cosift eval` for an end-to-end smoke against
// real services.
//
// Exit code: 0 if everything is PASS or WARN; 1 if any FAIL.
// doctorCheck is one row of the doctor report. Promoted from a locally-typed
// struct so the iter-58 defaults check can be tested without stdout capture.
type doctorCheck struct {
	Name   string
	Status string // "PASS" | "WARN" | "FAIL"
	Detail string
}

// doctorDefaultsChecks cross-checks the iter-55 defaults block against
// configured capabilities. Returns the rows that would be appended to the
// doctor report. Pure function — no IO, no stdout, no environment reads.
// hasEmbed/hasChat reflect "is the capability available at all" (config
// fields populated or env keys present); the caller resolves those before
// calling.
func doctorDefaultsChecks(d config.Defaults, hasEmbed, hasChat bool) []doctorCheck {
	if d.Retriever == "" && !d.Expand && d.ResearchStrategy == "" && d.ResearchSynthK == 0 {
		return []doctorCheck{{
			Name:   "defaults",
			Status: "PASS",
			Detail: "no defaults set (per-request params win; planner is the /research default)",
		}}
	}
	checks := []doctorCheck{{
		Name:   "defaults",
		Status: "PASS",
		Detail: fmt.Sprintf("retriever=%q expand=%v research_strategy=%q research_synth_k=%d", d.Retriever, d.Expand, d.ResearchStrategy, d.ResearchSynthK),
	}}
	if d.ResearchSynthK < 0 {
		checks = append(checks, doctorCheck{
			Name:   "defaults.research_synth_k",
			Status: "FAIL",
			Detail: fmt.Sprintf("research_synth_k must be ≥ 0 (got %d). 0 = use built-in default (10); positive = cap", d.ResearchSynthK),
		})
	}
	switch d.Retriever {
	case "dense", "hybrid":
		if !hasEmbed {
			checks = append(checks, doctorCheck{
				Name:   "defaults.retriever",
				Status: "WARN",
				Detail: fmt.Sprintf("%q default needs embeddings model — set cfg.Embeddings.Model or OPENAI_API_KEY", d.Retriever),
			})
		}
	case "", "bm25":
		// no-op; bm25 needs no capability beyond the SQLite index.
	default:
		checks = append(checks, doctorCheck{
			Name:   "defaults.retriever",
			Status: "FAIL",
			Detail: fmt.Sprintf("unknown retriever %q (valid: bm25, dense, hybrid)", d.Retriever),
		})
	}
	if d.Expand && !hasChat {
		checks = append(checks, doctorCheck{
			Name:   "defaults.expand",
			Status: "WARN",
			Detail: "expand=true needs a chat model for paraphrasing — set cfg.Chat.Model or OPENAI_API_KEY",
		})
	}
	switch d.ResearchStrategy {
	case "paraphrase":
		if !hasChat {
			checks = append(checks, doctorCheck{
				Name:   "defaults.research_strategy",
				Status: "WARN",
				Detail: "paraphrase needs a chat model for the paraphraser — set cfg.Chat.Model or OPENAI_API_KEY",
			})
		}
	case "", "planner":
		// no-op; planner is the historic default.
	default:
		checks = append(checks, doctorCheck{
			Name:   "defaults.research_strategy",
			Status: "FAIL",
			Detail: fmt.Sprintf("unknown strategy %q (valid: planner, paraphrase)", d.ResearchStrategy),
		})
	}
	return checks
}

// runInit writes a sensible default cosift.json to the requested path. By
// default it refuses to overwrite an existing file; -force opts in.
//
// The optional -site URL pre-populates `crawler.include_domains` so the user
// can immediately `cosift crawl <url>` against a target site without editing
// the config first. Lowers the onboarding cost for the self-hostable use case.
func runInit(cfgPath string, args []string) error {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	force := fs.Bool("force", false, "overwrite an existing config file")
	site := fs.String("site", "", "URL of a site to pre-populate crawler.include_domains with (e.g. https://docs.example.com)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if _, err := os.Stat(cfgPath); err == nil && !*force {
		return fmt.Errorf("%s already exists; pass -force to overwrite", cfgPath)
	}

	cfg := config.Default()
	if *site != "" {
		host, err := siteToHost(*site)
		if err != nil {
			return fmt.Errorf("-site: %w", err)
		}
		cfg.Crawler.IncludeDomains = []string{host}
	}

	buf, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := os.WriteFile(cfgPath, buf, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", cfgPath, err)
	}

	fmt.Printf("wrote %s\n", cfgPath)
	if cfg.Crawler.IncludeDomains != nil {
		fmt.Printf("  crawler.include_domains: %v\n", cfg.Crawler.IncludeDomains)
	}
	fmt.Println()
	fmt.Println("next steps:")
	if *site != "" {
		fmt.Printf("  1. cosift crawl %s\n", *site)
	} else {
		fmt.Println("  1. cosift crawl <url...>")
	}
	fmt.Println("  2. cosift serve")
	fmt.Println()
	fmt.Println("optional: set OPENAI_API_KEY in your environment (or .env) to enable")
	fmt.Println("  dense/hybrid retrieval, /answer, /research, and ?expand=true.")
	return nil
}

// siteToHost extracts the hostname from a URL or returns the input verbatim if
// it's already a bare host (e.g. "docs.example.com" vs "https://docs.example.com").
// Used by `cosift init -site` to populate include_domains.
func siteToHost(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", errors.New("empty site")
	}
	// If no scheme, treat as bare host and validate.
	if !strings.Contains(s, "://") {
		// reject if it looks like a path or has whitespace
		if strings.ContainsAny(s, " /\t\n") {
			return "", fmt.Errorf("not a hostname: %q", s)
		}
		return s, nil
	}
	u, err := url.Parse(s)
	if err != nil {
		return "", err
	}
	if u.Host == "" {
		return "", fmt.Errorf("could not extract host from %q", s)
	}
	return u.Host, nil
}

// runCheckRobots reports whether each URL is crawlable under the site's
// robots.txt, plus any Crawl-delay. Lets operators plan crawls without
// hand-inspecting the site's robots.txt. Iter 74.
//
// Wraps the existing internal/crawler.Robots so the CLI uses exactly the
// same enforcement logic the real crawler would. No drift.
func runCheckRobots(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("check-robots", flag.ExitOnError)
	userAgent := fs.String("user-agent", cfg.Crawler.UserAgent, "User-Agent the check pretends to be (defaults to cfg.crawler.user_agent)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	urls := fs.Args()
	if len(urls) == 0 {
		return errors.New("usage: cosift check-robots [-user-agent UA] <url...>")
	}
	if *userAgent == "" {
		*userAgent = "CosiftBot/0.0 (+https://github.com/calinteodor/cosift)"
	}
	httpClient := &http.Client{Timeout: 15 * time.Second}
	r := crawler.NewRobots(httpClient, *userAgent)

	fmt.Printf("user-agent: %s\n", *userAgent)

	// Probe robots.txt for each unique host before running per-URL checks.
	// Robots.Allowed returns (true, ...) when robots.txt is unreachable
	// (graceful degradation suits the crawler), but the operator running
	// check-robots wants to see "this host has no reachable robots.txt"
	// explicitly. Group URLs by host so we only probe each host once.
	hosts := uniqueHosts(urls)
	fmt.Println()
	for _, h := range hosts {
		probeURL := h + "/robots.txt"
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
		req.Header.Set("User-Agent", *userAgent)
		resp, err := httpClient.Do(req)
		if err != nil {
			fmt.Printf("robots.txt   %s — UNREACHABLE: %v (crawler will assume ALLOWED for everything on this host)\n", probeURL, err)
			continue
		}
		_ = resp.Body.Close()
		switch {
		case resp.StatusCode == 200:
			fmt.Printf("robots.txt   %s — OK (status 200)\n", probeURL)
		case resp.StatusCode == 404:
			fmt.Printf("robots.txt   %s — 404, no rules (crawler will assume ALLOWED)\n", probeURL)
		default:
			fmt.Printf("robots.txt   %s — status %d (crawler will assume ALLOWED on non-2xx/404)\n", probeURL, resp.StatusCode)
		}
		// Iter 131: surface Sitemap: directives from robots.txt. Modern
		// crawlers auto-discover sitemaps this way; this output lets operators
		// see what `cosift crawl -sitemap <URL>` could seed from.
		if sitemaps := r.Sitemaps(ctx, h); len(sitemaps) > 0 {
			for _, sm := range sitemaps {
				fmt.Printf("  Sitemap:   %s\n", sm)
			}
		}
	}
	fmt.Println()
	for _, u := range urls {
		allowed, crawlDelay, err := r.Allowed(ctx, u)
		if err != nil {
			fmt.Printf("[ERR ]  %s — %v\n", u, err)
			continue
		}
		mark := "[OK  ]"
		if !allowed {
			mark = "[DENY]"
		}
		extra := ""
		if crawlDelay > 0 {
			extra = fmt.Sprintf("  crawl-delay=%s", crawlDelay)
		}
		fmt.Printf("%s  %s%s\n", mark, u, extra)
	}
	return nil
}

// uniqueHosts extracts the scheme+host part from each URL, deduped + in order.
// Used by check-robots to probe each host's robots.txt exactly once.
func uniqueHosts(urls []string) []string {
	seen := make(map[string]bool)
	out := []string{}
	for _, raw := range urls {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" {
			continue
		}
		key := u.Scheme + "://" + u.Host
		if !seen[key] {
			seen[key] = true
			out = append(out, key)
		}
	}
	return out
}

// runCrawlErrors lists recently-errored frontier URLs with their failure reason.
// Iter 85 — operator diagnostic for "why are 142 URLs in error state?" without
// requiring SQLite shell access. Pure read-only; no side effects on the index.
//
// The error column is iter-85's addition to the frontier schema; pre-iter-85
// errored URLs will have an empty reason.
func runCrawlErrors(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("crawl-errors", flag.ExitOnError)
	limit := fs.Int("limit", 50, "max number of errored URLs to list (most recent first)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	s, err := store.Open(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer s.Close()

	errs, err := s.ListErroredFrontier(ctx, *limit)
	if err != nil {
		return fmt.Errorf("list errored: %w", err)
	}
	if len(errs) == 0 {
		fmt.Println("no errored frontier entries.")
		return nil
	}
	fmt.Printf("%d errored frontier entries (most recent first):\n", len(errs))
	fmt.Println()
	for _, e := range errs {
		reason := e.LastError
		if reason == "" {
			reason = "(no reason recorded — entry predates iter 85)"
		}
		fmt.Printf("  attempts=%d  %s\n    → %s\n", e.Attempts, e.URL, reason)
	}
	return nil
}

func runDoctor(ctx context.Context, cfg *config.Config, args []string) error {
	// Iter 102: optional remote checks. When -server is empty, doctor stays
	// purely local (iter-39 behavior). When -server is set, doctor also pings
	// the server's /healthz and /stats; if -token is also set (or env), it
	// validates the token by hitting /admin/config.
	fs := flag.NewFlagSet("doctor", flag.ExitOnError)
	remoteServer := fs.String("server", "", "remote cosift URL to additionally probe (empty = local checks only)")
	remoteToken := fs.String("token", "", "admin bearer token for -server admin-endpoint check (defaults to COSIFT_ADMIN_TOKEN env)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var checks []doctorCheck
	add := func(name, status, detail string) { checks = append(checks, doctorCheck{name, status, detail}) }

	// 1. Data dir writable.
	probe := filepath.Join(cfg.DataDir, ".doctor.probe")
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		add("data_dir writable", "FAIL", fmt.Sprintf("mkdir %s: %v", cfg.DataDir, err))
	} else if err := os.WriteFile(probe, []byte("ok"), 0o600); err != nil {
		add("data_dir writable", "FAIL", fmt.Sprintf("write %s: %v", probe, err))
	} else {
		_ = os.Remove(probe)
		add("data_dir writable", "PASS", cfg.DataDir)
	}

	// 2. SQLite open + schema migrated.
	s, err := store.Open(cfg.DataDir)
	if err != nil {
		add("sqlite open + schema", "FAIL", err.Error())
	} else {
		st, _ := s.Stats(ctx)
		_ = s.Close()
		add("sqlite open + schema", "PASS", fmt.Sprintf("%d docs, %d terms", st.Documents, st.Terms))
	}

	// 2b. Pebble open + counters (iter 287). Pebble is optional; "absent" is
	// not a failure, just a SKIP. When present, verify open + read counters.
	pebbleDir := filepath.Join(cfg.DataDir, "pebble")
	if _, statErr := os.Stat(pebbleDir); statErr != nil {
		add("pebble store", "SKIP", "no pebble dir (sqlite-only deployment)")
	} else if ps, err := store.OpenPebble(pebbleDir); err != nil {
		add("pebble store", "FAIL", err.Error())
	} else {
		_, n, _ := ps.CorpusStats(ctx)
		_ = ps.Close()
		add("pebble store", "PASS", fmt.Sprintf("%d indexed docs", n))
	}

	// 2c. COSIFT_* env vars (iter 296). Lists which path-2 env overrides are
	// set so operators can confirm 'is my override actually live'. Silent
	// typos previously hid behind 'env unset → keep default' branches.
	cosiftEnvs := []string{
		"COSIFT_PEBBLE_CACHE_MB", "COSIFT_PEBBLE_MEMTABLE_MB", "COSIFT_PEBBLE_MEMTABLES", "COSIFT_PEBBLE_SYNC",
		"COSIFT_BM25_K1", "COSIFT_BM25_B",
		"COSIFT_HYDE_CACHE_SIZE", "COSIFT_PARA_CACHE_SIZE",
	}
	var setEnvs []string
	for _, name := range cosiftEnvs {
		if v := os.Getenv(name); v != "" {
			setEnvs = append(setEnvs, name+"="+v)
		}
	}
	if len(setEnvs) == 0 {
		add("COSIFT_* env", "INFO", "no path-2 overrides set (using defaults)")
	} else {
		add("COSIFT_* env", "INFO", strings.Join(setEnvs, ", "))
	}

	// 3. Config recognized.
	add("config", "PASS", fmt.Sprintf("addr=%s, data_dir=%s", cfg.Server.Addr, cfg.DataDir))

	// 4. API key env (warn — actually pinging is `cosift eval` territory).
	hasOpenAI := os.Getenv("OPENAI_API_KEY") != "" || os.Getenv("OPENAI") != ""
	if hasOpenAI {
		add("OPENAI key", "PASS", "OPENAI_API_KEY (or OPENAI) present")
	} else if cfg.Embeddings.Model != "" || cfg.Chat.Model != "" {
		add("OPENAI key", "WARN", "embeddings/chat configured but no OPENAI key in env — dense/answer disabled")
	} else {
		add("OPENAI key", "WARN", "no key in env (fine for bm25-only)")
	}

	// 5. Eval fixtures.
	corpusPath := "testdata/eval/corpus.json"
	queriesPath := "testdata/eval/queries.json"
	if _, err := os.Stat(corpusPath); err != nil {
		add("eval fixtures", "WARN", "testdata/eval/corpus.json missing (eval won't run)")
	} else if _, err := os.Stat(queriesPath); err != nil {
		add("eval fixtures", "WARN", "testdata/eval/queries.json missing")
	} else {
		add("eval fixtures", "PASS", "corpus.json + queries.json present")
	}

	// 6. Admin token configured if /admin/* is to be used.
	if cfg.Server.AdminToken != "" {
		add("admin_token", "PASS", "set (length redacted)")
	} else {
		add("admin_token", "WARN", "unset — /admin/* will return 403")
	}

	// 7. Defaults vs capabilities. The iter-55 defaults block lets operators
	// pre-configure retrieval behavior, but a default that requires capability
	// the server isn't configured for (e.g. retriever=hybrid without an
	// embedder) will silently fail at request time — 400 on every call. Catch
	// the mismatch HERE instead of at first traffic.
	hasEmbed := cfg.Embeddings.Model != "" || os.Getenv("OPENAI_API_KEY") != "" || os.Getenv("OPENAI") != ""
	hasChat := cfg.Chat.Model != "" || os.Getenv("OPENAI_API_KEY") != "" || os.Getenv("OPENAI") != ""
	checks = append(checks, doctorDefaultsChecks(cfg.Defaults, hasEmbed, hasChat)...)

	// 8 (iter 102). Remote checks — only when -server is set. Token resolution
	// mirrors iter-99 admin CLIs: -token flag wins, then env.
	if *remoteServer != "" {
		resolvedToken := *remoteToken
		if resolvedToken == "" {
			resolvedToken = os.Getenv("COSIFT_ADMIN_TOKEN")
		}
		checks = append(checks, doctorRemoteChecks(ctx, *remoteServer, resolvedToken)...)
	}

	fail := 0
	for _, c := range checks {
		fmt.Printf("%-6s %s — %s\n", "["+c.Status+"]", c.Name, c.Detail)
		if c.Status == "FAIL" {
			fail++
		}
	}
	fmt.Println()
	if fail > 0 {
		fmt.Printf("%d FAIL — fix the issues above before running `cosift serve`\n", fail)
		return errors.New("doctor failures")
	}
	fmt.Println("ready.")
	return nil
}

// doctorRemoteChecks probes a remote cosift server: /healthz reachable, /stats
// returns a sensible shape, and (when token non-empty) /admin/config validates
// the bearer token. Returns a slice of doctorCheck rows for the main runDoctor
// loop to render. Iter 102.
func doctorRemoteChecks(ctx context.Context, serverURL, token string) []doctorCheck {
	var out []doctorCheck
	base := strings.TrimRight(strings.Replace(serverURL, "0.0.0.0", "127.0.0.1", 1), "/")
	client := &http.Client{Timeout: 10 * time.Second}

	// /healthz
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/healthz", nil)
	resp, err := client.Do(req)
	if err != nil {
		out = append(out, doctorCheck{"remote /healthz", "FAIL", fmt.Sprintf("%s: %v", serverURL, err)})
		// If /healthz is unreachable, downstream checks will all fail too —
		// emit one row and skip the cascade.
		return out
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		out = append(out, doctorCheck{"remote /healthz", "PASS", base})
	} else {
		out = append(out, doctorCheck{"remote /healthz", "FAIL", fmt.Sprintf("%s returned %d", base, resp.StatusCode)})
	}

	// /stats
	req, _ = http.NewRequestWithContext(ctx, http.MethodGet, base+"/stats", nil)
	resp, err = client.Do(req)
	if err != nil {
		out = append(out, doctorCheck{"remote /stats", "FAIL", err.Error()})
	} else {
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			out = append(out, doctorCheck{"remote /stats", "FAIL", fmt.Sprintf("returned %d", resp.StatusCode)})
		} else {
			var st server.StatsResponse
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
			if err := json.Unmarshal(body, &st); err != nil {
				out = append(out, doctorCheck{"remote /stats", "WARN", fmt.Sprintf("decode: %v", err)})
			} else {
				out = append(out, doctorCheck{"remote /stats", "PASS", fmt.Sprintf("%d docs", st.Documents)})
			}
		}
	}

	// /admin/config — only when token is provided. No token → skip silently
	// (not a failure; operator may not have admin auth enabled).
	if token != "" {
		req, _ = http.NewRequestWithContext(ctx, http.MethodGet, base+"/admin/config", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err = client.Do(req)
		if err != nil {
			out = append(out, doctorCheck{"remote admin token", "FAIL", err.Error()})
			return out
		}
		defer resp.Body.Close()
		switch resp.StatusCode {
		case http.StatusOK:
			out = append(out, doctorCheck{"remote admin token", "PASS", "/admin/config accepted the token"})
		case http.StatusUnauthorized, http.StatusForbidden:
			out = append(out, doctorCheck{"remote admin token", "FAIL", fmt.Sprintf("token rejected (HTTP %d)", resp.StatusCode)})
		default:
			out = append(out, doctorCheck{"remote admin token", "WARN", fmt.Sprintf("unexpected status %d", resp.StatusCode)})
		}
	}
	return out
}

// runReembed walks every document, chunks it, embeds the chunks with the
// configured model, and writes them as new passages. The new rows coexist with
// the old (different model_id) until `-drop-old` is passed.
//
// Recommended sequence:
//
//	cosift reembed                       # write new passages alongside old
//	# verify search quality on new model …
//	cosift reembed -drop-old             # drop the others
//
// Or just `cosift reembed -drop-old` if you're confident.
func runReembed(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("reembed", flag.ExitOnError)
	dropOld := fs.Bool("drop-old", false, "after re-embed, delete passages with a different model_id")
	batchSize := fs.Int("batch", 256, "embeddings per API call (provider-dependent cap)")
	// Iter 107: time-based progress reporting (shared helper from iter 106).
	progressInterval := fs.Duration("progress", 5*time.Second, "log re-embed progress every N (0 disables)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if cfg.Embeddings.Model == "" {
		return errors.New("reembed: cfg.Embeddings.Model must be set")
	}
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		apiKey = os.Getenv("OPENAI")
	}
	if apiKey == "" {
		return errors.New("reembed: no OPENAI key in env")
	}
	dim := cfg.Embeddings.Dim
	if dim == 0 {
		dim = 1536
	}
	emb := embed.NewOpenAIClient(apiKey, cfg.Embeddings.URL, cfg.Embeddings.Model, dim)

	s, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer s.Close()

	docs, err := s.ListDocuments(ctx, 0)
	if err != nil {
		return err
	}
	if len(docs) == 0 {
		log.Printf("reembed: 0 docs in store — nothing to do")
		return nil
	}

	// Pre-existing models (informational; useful for the operator).
	models, _ := s.PassageModels(ctx)
	log.Printf("reembed: %d docs · target model=%s · existing models=%v · batch=%d",
		len(docs), cfg.Embeddings.Model, models, *batchSize)

	chunker := chunkerWith(cfg.Crawler.ChunkSize, cfg.Crawler.ChunkOverlap)
	type ref struct {
		docID int64
		chunk index.Chunk
	}
	var (
		texts    []string
		refs     []ref
		total    int
		flush    func() error
		written  int
	)
	flush = func() error {
		if len(texts) == 0 {
			return nil
		}
		vecs, err := emb.Embed(ctx, texts)
		if err != nil {
			return fmt.Errorf("embed batch (%d texts): %w", len(texts), err)
		}
		if len(vecs) != len(texts) {
			return fmt.Errorf("embed count mismatch: %d vs %d", len(vecs), len(texts))
		}
		for i, v := range vecs {
			if err := s.UpsertPassage(ctx, &store.Passage{
				DocID:     refs[i].docID,
				Offset:    refs[i].chunk.Offset,
				Length:    refs[i].chunk.Length,
				Model:     emb.Model(),
				Embedding: v,
			}); err != nil {
				return fmt.Errorf("write passage: %w", err)
			}
			written++
		}
		texts = texts[:0]
		refs = refs[:0]
		return nil
	}

	docProgress := newProgressReporter("reembed docs", len(docs), *progressInterval)
	for docIdx, d := range docs {
		for _, c := range chunker.Chunk(d.Title + "\n\n" + d.Text) {
			texts = append(texts, c.Text)
			refs = append(refs, ref{docID: d.ID, chunk: c})
			total++
			if len(texts) >= *batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		docProgress.maybeLog(docIdx + 1)
	}
	if err := flush(); err != nil {
		return err
	}
	log.Printf("reembed: wrote %d passages with model=%s", written, cfg.Embeddings.Model)

	if *dropOld {
		n, err := s.DropPassagesNotModel(ctx, cfg.Embeddings.Model)
		if err != nil {
			return fmt.Errorf("drop old: %w", err)
		}
		log.Printf("reembed: dropped %d passages from other models", n)
	} else if len(models) > 0 && !contains(models, cfg.Embeddings.Model) {
		log.Printf("reembed: %d other-model passages remain — run with -drop-old to remove", len(models))
	}
	return nil
}

func contains(ss []string, x string) bool {
	for _, s := range ss {
		if s == x {
			return true
		}
	}
	return false
}

// runCompactIndex drops orphaned passages and terms, then VACUUMs.
// Companion to `cosift gc` — gc cleans the frontier; compact-index cleans
// the index proper.
func runCompactIndex(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("compact-index", flag.ExitOnError)
	vacuum := fs.Bool("vacuum", true, "run VACUUM after deletes to reclaim disk")
	if err := fs.Parse(args); err != nil {
		return err
	}
	s, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer s.Close()

	dropped, err := s.DropOrphanedPassages(ctx)
	if err != nil {
		return fmt.Errorf("drop orphan passages: %w", err)
	}
	log.Printf("compact-index: dropped %d orphan passages", dropped)

	terms, err := s.DropOrphanedTerms(ctx)
	if err != nil {
		return fmt.Errorf("drop orphan terms: %w", err)
	}
	log.Printf("compact-index: dropped %d orphan terms", terms)

	if *vacuum {
		log.Printf("compact-index: vacuuming...")
		if err := s.Vacuum(ctx); err != nil {
			return fmt.Errorf("vacuum: %w", err)
		}
		log.Printf("compact-index: vacuum complete")
	}
	return nil
}

// batchEmbed chunks a large text slice into provider-safe batches before
// calling the embedder. OpenAI's text-embedding-3-* caps at 2048 inputs OR
// 300k tokens per request; 1000 is a safe default that respects both at
// our typical chunk sizes. Returns vectors in the input order.
//
// Between batches we sleep 200ms — keeps multi-batch runs under OpenAI's
// 1M-TPM org limit for text-embedding-3-small, which iter 39 hit at ~50k inputs.
// Provider rate-limit errors get one bounded retry with the suggested delay.
func batchEmbed(ctx context.Context, e embed.Embedder, texts []string, batchSize int) ([][]float32, error) {
	if batchSize <= 0 {
		batchSize = 256
	}
	// 1000 is the published OpenAI per-call cap, but large-dim models
	// (text-embedding-3-large @ 3072) return malformed JSON well before that
	// in practice. 256 is safe across the model lineup.
	if batchSize > 1000 {
		batchSize = 1000
	}
	if len(texts) <= batchSize {
		return e.Embed(ctx, texts)
	}
	out := make([][]float32, 0, len(texts))
	for i := 0; i < len(texts); i += batchSize {
		end := i + batchSize
		if end > len(texts) {
			end = len(texts)
		}
		vecs, err := embedWithRetry(ctx, e, texts[i:end])
		if err != nil {
			return nil, fmt.Errorf("batch %d-%d: %w", i, end, err)
		}
		if len(vecs) != end-i {
			return nil, fmt.Errorf("batch %d-%d: %d returned, expected %d", i, end, len(vecs), end-i)
		}
		out = append(out, vecs...)
		if end < len(texts) {
			select {
			case <-ctx.Done():
				return out, ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	return out, nil
}

// embedWithRetry retries once on rate-limit errors with a fixed backoff.
// Crude but effective at our scale — keeps full distractor runs from failing
// when a single batch slips past the TPM bucket. Production deployments with
// higher request volumes want exponential backoff + provider-supplied delay
// parsing; deferred until traffic warrants.
func embedWithRetry(ctx context.Context, e embed.Embedder, texts []string) ([][]float32, error) {
	vecs, err := e.Embed(ctx, texts)
	if err == nil {
		return vecs, nil
	}
	msg := err.Error()
	if !strings.Contains(msg, "rate_limit") && !strings.Contains(msg, "429") {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(2 * time.Second):
	}
	return e.Embed(ctx, texts)
}

// paraphraseRetriever wraps any Retriever with on-the-fly query expansion via
// a chat client. For each search, asks the LLM for N paraphrases, runs all of
// them through the inner retriever, RRF-fuses the result lists.
//
// Iter-44 measured that hand-written paraphrases recover 0.02 nDCG at 10k
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
	mainWeight float64 // Iter 139: 0 = equal-weight RRF (pre-iter-139); >0 = main query gets this weight, paraphrases each 1.0
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
	// Iter 139: weighted RRF when mainWeight > 0. Matches the server expand
	// path's iter-136 behavior so eval-time measurements use the same fusion
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
// This exists so we can measure (iter 52) whether the planner's decomposition
// already subsumes paraphrase expansion — that claim was asserted in iter 48
// when we chose not to add ?expand=true to /research, but never measured.
const plannerSystemPrompt = `Decompose the user's research question into 2-3 focused sub-queries that, taken together, would cover the answer. Output ONLY a JSON array of strings — no prose, no markdown. Example: ["sub-query 1", "sub-query 2"]`

type plannerRetriever struct {
	inner      eval.Retriever
	chat       embed.ChatClient
	mainWeight float64 // Iter 140: 0 = equal-weight RRF; >0 = main query gets this weight, sub-queries each 1.0
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
	// Iter 140: weighted RRF when mainWeight > 0. Same shape as iter-139
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.baseURL+"/search?"+v.Encode(), nil)
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

// runAnswerEval is iter-56's measurement of /research answer quality across
// the two retrieval strategies (planner vs paraphrase). Iters 52-53 measured
// *retrieval coverage* via nDCG and found paraphrase wins; iter 54 added
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
	answerChunkSize := fs.Int("chunk-size", 0, "iter-145: passage chunker target words (0 = default 320)")
	answerChunkOverlap := fs.Int("chunk-overlap", 0, "iter-145: passage chunker overlap words (0 = default 64)")
	dryRun := fs.Bool("dry-run", false, "build the harness + print the plan, but issue NO LLM calls")
	useRerank := fs.Bool("rerank", false, "wire the LLM listwise reranker into the in-process server; tests whether rerank cuts the iter-58 grounding=1 cases on single-doc")
	rerankModel := fs.String("rerank-model", "gpt-4o-mini", "chat model for the reranker (kept default = synth-model so cost stays predictable)")
	synthK := fs.Int("synth-k", 0, "cap synth-source count via WithDefaults({ResearchSynthK: N}); 0 = preserve built-in default (10). Iter-62 tests whether K=5 trades coverage for grounding on single-doc workloads.")
	if err := fs.Parse(args); err != nil {
		return err
	}

	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		apiKey = os.Getenv("OPENAI")
	}
	if apiKey == "" && !*dryRun {
		return errors.New("OPENAI_API_KEY (or OPENAI) not set; pass -dry-run to inspect the plan without spending")
	}

	corpus, err := eval.LoadCorpus(*corpusPath)
	if err != nil {
		return fmt.Errorf("load corpus: %w", err)
	}
	qs, err := eval.LoadQuerySet(*queriesPath)
	if err != nil {
		return fmt.Errorf("load queries: %w", err)
	}

	fmt.Printf("answer-eval: %d queries × 2 strategies × judge calls\n", len(qs.Queries))
	fmt.Printf("synth model = %s   judge model = %s   rerank = %v   synth_k = %d   dry-run = %v\n", *synthModel, *judgeModel, *useRerank, *synthK, *dryRun)
	if *dryRun {
		for _, q := range qs.Queries {
			fmt.Printf("  - %s  (relevant: %d docs)\n", q.Text, len(q.Relevant))
		}
		return nil
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
	oai := embed.NewOpenAIClient(apiKey, "", *embModel, *embDim)
	var emb embed.Embedder = oai
	if *embCacheDir != "" {
		emb = embed.NewCachedEmbedder(oai, *embCacheDir)
	}
	chunker := chunkerWith(*answerChunkSize, *answerChunkOverlap)

	type passageRef struct {
		docIdx int
		chunk  index.Chunk
	}
	var allTexts []string
	var refs []passageRef
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
			refs = append(refs, passageRef{docIdx: len(corpus.Docs) - 1, chunk: c})
		}
	}
	fmt.Printf("embedding %d passages across %d docs...\n", len(allTexts), len(corpus.Docs))
	vecs, err := batchEmbed(ctx, emb, allTexts, 256)
	if err != nil {
		return fmt.Errorf("embed: %w", err)
	}
	vi := index.NewVectorIndex(*embDim)
	// Iterate corpus.Docs again to get the right doc for each chunk — refs is
	// only used to remember chunk offset/length per passage.
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

	chat := embed.NewOpenAIChat(apiKey, "", *synthModel)
	judge := embed.NewOpenAIChat(apiKey, "", *judgeModel)

	srv := server.New(st).
		WithVector(vi, emb).
		WithChat(chat).
		WithParaphraser(chat, 2).
		WithLLMLimiter(0, 0) // disable rate limiter — this is an in-process eval, not a public endpoint. Iter 58 caught this when the 10/min default 429'd after 5 queries.
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

	type strategyReport struct {
		Strategy      string  `json:"strategy"`
		Answer        string  `json:"answer"`
		SourceURLs    []string `json:"source_urls"`
		Coverage      int     `json:"coverage"`
		Grounding     int     `json:"grounding"`
		JudgeComment  string  `json:"judge_comment"`
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
			u := httpSrv.URL + "/research?strategy=" + strategy + "&q=" + url.QueryEscape(q.Text)
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
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
				Query    string `json:"query"`
				Strategy string `json:"strategy"`
				Plan     []string `json:"plan"`
				Answer   string `json:"answer"`
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

	// Aggregate. Exported field names matter — iter 57 saved unexported agg
	// fields which JSON-marshalled to `{}`. Now the summary in the saved JSON
	// is actually inspectable by jq.
	type strategySummary struct {
		N            int     `json:"n"`
		MeanCoverage float64 `json:"mean_coverage"`
		MeanGrounding float64 `json:"mean_grounding"`
		Combined     float64 `json:"combined"`
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

// judgeSystemPrompt was rewritten in iter 64 after the iter-58 trace revealed
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
// answer text. Iter-64 fix for the judge-bias bug — we now pass only the
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
// triple. Iter-64 pre-filters sources to only those cited in the answer, so
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
		Query string `json:"query"`
		Reports []struct {
			Strategy     string  `json:"strategy"`
			Coverage     int     `json:"coverage"`
			Grounding    int     `json:"grounding"`
			JudgeComment string  `json:"judge_comment"`
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
	// Old reports (iter 56) had an empty summary because the agg struct used
	// unexported fields. Recompute the summary on the fly for those.
	if r.Summary == nil || len(r.Summary) == 0 || r.Summary["planner"].N == 0 {
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
	for _, strat := range []string{"planner", "paraphrase"} {
		var covSum, grdSum, n int
		for _, q := range r.Reports {
			for _, sr := range q.Reports {
				if sr.Strategy == strat {
					covSum += sr.Coverage
					grdSum += sr.Grounding
					n++
				}
			}
		}
		s := out[strat]
		s.N = n
		if n > 0 {
			s.MeanCoverage = float64(covSum) / float64(n)
			s.MeanGrounding = float64(grdSum) / float64(n)
			s.Combined = s.MeanCoverage + s.MeanGrounding
		}
		out[strat] = s
	}
	return out
}

// runAnswerEvalCompare loads two saved answer-eval JSON reports and prints
// side-by-side coverage/grounding/combined deltas + per-query moves above a
// configurable threshold. No LLM calls; pure file-to-file diff. The pattern
// emerged across iters 58/60/62/63/64 of ad-hoc python one-liners; this iter
// makes it first-class.
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
	for _, strat := range []string{"planner", "paraphrase"} {
		ab, bb := a.Summary[strat], b.Summary[strat]
		if ab.N == 0 && bb.N == 0 {
			fmt.Printf("%-12s   (not present in either report)\n", strat)
			continue
		}
		fmt.Printf("%-12s %3d→%-3d %.2f→%.2f %.2f→%.2f %.2f→%.2f | %+.2f  %+.2f  %+.2f\n",
			strat, ab.N, bb.N,
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

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-1] + "…"
}

func runStats(ctx context.Context, cfg *config.Config, args []string) error {
	// Iter 213: -backend flag (-backend=pebble reads the Pebble subdir).
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	backend := fs.String("backend", "sqlite", "storage backend: sqlite (default) | pebble")
	if err := fs.Parse(args); err != nil {
		return err
	}
	switch *backend {
	case "sqlite", "":
		s, err := store.Open(cfg.DataDir)
		if err != nil {
			return err
		}
		defer s.Close()
		stats, err := s.Stats(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("documents: %d\nterms: %d\ndata_dir: %s\nbackend: sqlite\n",
			stats.Documents, stats.Terms, cfg.DataDir)
	case "pebble":
		pebbleDir := filepath.Join(cfg.DataDir, "pebble")
		ps, err := store.OpenPebble(pebbleDir)
		if err != nil {
			return fmt.Errorf("open pebble: %w", err)
		}
		defer ps.Close()
		stats, err := ps.Stats(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("documents: %d\ndata_dir: %s\nbackend: pebble\n",
			stats.Documents, pebbleDir)
	default:
		return fmt.Errorf("stats: unknown -backend %q (want: sqlite | pebble)", *backend)
	}
	return nil
}

// runCrawlStatus prints an operator-friendly snapshot of an ongoing crawl:
// counts, frontier breakdown, top hosts by indexed-doc count, recent error
// classes, and rolling-window doc rates with a 1M-doc ETA. Safe to run
// concurrently with a live `cosift crawl` — SQLite WAL mode allows readers
// alongside the writer. Iter 193.
// runStatusFile reads the iter-224 crawl-status.json (the file the live
// crawler writes every 10s) and pretty-prints it. Useful when an operator
// can't run `cosift stats -backend=pebble` because Pebble's single-writer
// lock is held by the crawl process. Iter 225.
//
// Default path: <cfg.DataDir>/crawl-status.json. -file flag overrides.
func runStatusFile(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("status-file", flag.ExitOnError)
	path := fs.String("file", "", "path to crawl-status.json (defaults to <data_dir>/crawl-status.json)")
	asJSON := fs.Bool("json", false, "emit raw status JSON plus a derived 'age_seconds' field; suitable for jq / Prometheus")
	target := fs.Int64("target", 0, "doc count target — show indexed/target progress percent (default 0 = no target line)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	p := *path
	if p == "" {
		p = filepath.Join(cfg.DataDir, "crawl-status.json")
	}
	buf, err := os.ReadFile(p)
	if err != nil {
		return fmt.Errorf("read %s: %w", p, err)
	}
	var d struct {
		Queued             int64     `json:"frontier_queued"`
		InFlight           int64     `json:"frontier_in_flight"`
		Done               int64     `json:"frontier_done"`
		Errored            int64     `json:"frontier_errored"`
		IndexedDocs        int64     `json:"indexed_docs,omitempty"`
		IndexedDocsAtStart int64     `json:"indexed_docs_at_start,omitempty"`
		AvgDocLen          float64   `json:"avg_doc_len,omitempty"`
		StartedAt          time.Time `json:"started_at,omitempty"`
		WrittenAt          time.Time `json:"written_at"`
	}
	if err := json.Unmarshal(buf, &d); err != nil {
		return fmt.Errorf("decode %s: %w", p, err)
	}
	age := time.Since(d.WrittenAt).Round(time.Second)
	if *asJSON {
		out := map[string]any{
			"path":                  p,
			"frontier_queued":       d.Queued,
			"frontier_in_flight":    d.InFlight,
			"frontier_done":         d.Done,
			"frontier_errored":      d.Errored,
			"indexed_docs":          d.IndexedDocs,
			"indexed_docs_at_start": d.IndexedDocsAtStart,
			"avg_doc_len":           d.AvgDocLen,
			"started_at":            d.StartedAt,
			"written_at":            d.WrittenAt,
			"age_seconds":           int64(age.Seconds()),
			"stale":                 age > 30*time.Second,
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	fmt.Printf("status file: %s  (written %s ago)\n\n", p, age)
	fmt.Printf("  queued:     %d\n", d.Queued)
	fmt.Printf("  in_flight:  %d\n", d.InFlight)
	fmt.Printf("  done:       %d\n", d.Done)
	fmt.Printf("  errored:    %d\n", d.Errored)
	if d.IndexedDocs > 0 {
		fmt.Printf("  indexed:    %d (avg doc length: %.0f tokens)\n", d.IndexedDocs, d.AvgDocLen)
	}
	total := d.Queued + d.InFlight + d.Done + d.Errored
	if total > 0 {
		processed := d.Done + d.Errored
		fmt.Printf("  processed:  %d / %d (%.1f%%)\n",
			processed, total, float64(processed)/float64(total)*100)
	}
	// Iter 270: ?-target N → 'indexed/target (pct)' line for long crawls toward
	// a known doc-count goal. No-op when target is unset or already met.
	// Iter 271: ETA from iter-271 started_at + indexed_docs_at_start fields.
	// Rate is averaged since the dumper's first poll, not instantaneous.
	if *target > 0 && d.IndexedDocs > 0 {
		pct := float64(d.IndexedDocs) / float64(*target) * 100
		fmt.Printf("  target:     %d / %d (%.1f%%)\n", d.IndexedDocs, *target, pct)
		gained := d.IndexedDocs - d.IndexedDocsAtStart
		elapsed := d.WrittenAt.Sub(d.StartedAt)
		if gained > 0 && elapsed > time.Second && d.IndexedDocs < *target {
			rate := float64(gained) / elapsed.Seconds()
			remaining := float64(*target - d.IndexedDocs)
			eta := time.Duration(remaining/rate) * time.Second
			fmt.Printf("  rate:       %.1f docs/sec  eta: %s\n", rate, eta.Round(time.Second))
		}
	}
	if age > 30*time.Second {
		fmt.Printf("\n  WARNING: status is %s old; crawler may have stopped\n", age)
	}
	return nil
}

func runCrawlStatus(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("crawl-status", flag.ExitOnError)
	hostsN := fs.Int("hosts", 10, "show top N hosts by indexed-doc count")
	errsN := fs.Int("errors", 8, "show top N distinct error classes")
	target := fs.Int64("target", 1_000_000, "doc count target for ETA projection")
	if err := fs.Parse(args); err != nil {
		return err
	}
	s, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer s.Close()

	r, err := s.CrawlStatus(ctx, *hostsN, *errsN)
	if err != nil {
		return err
	}

	fmt.Printf("crawl status — %s\n\n", cfg.DataDir)
	fmt.Printf("  documents:        %d\n", r.Documents)
	fmt.Printf("  terms:            %d\n", r.Terms)
	fmt.Printf("  unique hosts:     %d\n", r.UniqueHosts)
	fmt.Println()
	fmt.Println("  frontier:")
	for _, fs := range r.FrontierByStatus {
		fmt.Printf("    %-12s %d\n", fs.Status, fs.Count)
	}
	fmt.Println()
	fmt.Println("  doc rate (rolling, by fetched_at):")
	for _, win := range r.RateWindows {
		ratePerMin := float64(win.Count) / float64(win.WindowSec) * 60.0
		fmt.Printf("    last %2dmin: %5d docs (%7.0f /min)\n", win.WindowSec/60, win.Count, ratePerMin)
	}
	if len(r.RateWindows) > 0 {
		// Use the longest available rolling window for ETA — most stable signal.
		win := r.RateWindows[len(r.RateWindows)-1]
		ratePerMin := float64(win.Count) / float64(win.WindowSec) * 60.0
		if ratePerMin > 0 {
			remaining := *target - r.Documents
			if remaining < 0 {
				remaining = 0
			}
			etaHours := float64(remaining) / ratePerMin / 60.0
			fmt.Printf("    → ETA to %d docs: %.1f hours = %.2f days (at the %dmin rate)\n",
				*target, etaHours, etaHours/24.0, win.WindowSec/60)
		}
	}
	if len(r.TopHosts) > 0 {
		fmt.Printf("\n  top %d hosts (by indexed doc count):\n", len(r.TopHosts))
		for _, h := range r.TopHosts {
			fmt.Printf("    %-50s %d\n", truncStr(h.Host, 50), h.Count)
		}
	}
	if len(r.ErrorClasses) > 0 {
		fmt.Printf("\n  top %d error classes:\n", len(r.ErrorClasses))
		for _, e := range r.ErrorClasses {
			fmt.Printf("    %-60s %d\n", truncStr(e.LastError, 60), e.Count)
		}
	}
	return nil
}

func truncStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
