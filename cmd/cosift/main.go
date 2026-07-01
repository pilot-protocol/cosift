package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/crawler"
	"github.com/pilot-protocol/cosift/internal/index"
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
  cosift doctor [-json] [-server URL] [-token T]  pre-flight check (data dir, sqlite, pebble, HNSW, env; -json for CI)

  Path-2 (Pebble) commands — see docs/PEBBLE.md and docs/API.md:
  cosift pebble-serve -dir D            HTTP server backed by PebbleStore (search/find_similar/answer/query/research/contents/healthz/stats/metrics/verify)
  cosift pebble-info -dir D [-json]     dump corpus counters + pebble.Metrics for an offline store (-json = jq-friendly shape, no pebble.Metrics)
  cosift pebble-compact -dir D [-range R]  force-compact a Pebble key range to collapse tombstones (R=all|f|d|l|v; default all) — service must be stopped
  cosift domain-audit -dir D [-tranco csv] [-majestic csv] [-out path] [-top N] [-min-count N]   single-scan host inventory + authority scoring (JSONL)
  cosift migrate-to-pebble -output D    copy a SQLite cosift data dir into a fresh Pebble store
  cosift verify [-json] [-server URL]   compare counters vs 'l' family scan (non-zero exit on drift; -server routes through HTTP /verify when the writer lock is held)
  cosift status-file [-target N] [-json]  read crawl-status.json (lock-free; works during a live crawl)
  cosift crawl-status                   richer stats (SQLite-backed; runs against the open store)
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

retrieval/synth flags (search, answer, research):
  -server <URL>           cosift server URL (default: http://<cfg.Server.Addr>)
  -k <int>                top-k results / sources
  -retriever <name>       bm25 | dense | hybrid (server default if empty; dense/hybrid need COSIFT_LOAD_HNSW=true + embedder)
  -mmr <lambda>           MMR diversification λ ∈ [0,1] (needs HNSW + embedder server-side)
  -rerank                 reorder the candidate pool with the configured reranker
  -expand <mode>          hyde | paraphrase | true (answer treats -expand as bool)
  -since / -until <date>  filter by doc PublishedAt (ISO-8601)
  -include-domains <CSV>  dot-boundary suffix allowlist
  -exclude-domains <CSV>  dot-boundary suffix denylist
  -stream                 (answer, research) consume SSE — token-by-token answer + plan/sources events
  -json                   emit raw JSON instead of human-readable output

flags:
  -config <path>          config file (default: ./cosift.json)
`

var version = "0.0.1-dev"

// chunkerWith is a thin alias for index.NewChunkerWith; keeps the four
// CLI call sites short and grep-discoverable.
func chunkerWith(size, overlap int) *index.Chunker {
	return index.NewChunkerWith(size, overlap)
}

// usageError signals that flag.Usage (or a per-command usage line) was
// printed and the process should exit with status 2.
type usageError struct{ msg string }

func (u *usageError) Error() string { return u.msg }

func main() {
	cfgPath := flag.String("config", "cosift.json", "path to config file")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(2)
	}

	err := run(*cfgPath)
	if err == nil {
		return
	}
	var ue *usageError
	if errors.As(err, &ue) {
		if ue.msg != "" {
			fmt.Fprintln(os.Stderr, ue.msg)
		}
		os.Exit(2)
	}
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func run(cfgPath string) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	switch cmd := flag.Arg(0); cmd {
	case "version":
		fmt.Println(version)
	case "init":
		if err := runInit(cfgPath, flag.Args()[1:]); err != nil {
			return fmt.Errorf("init: %w", err)
		}
	case "crawl":
		if err := runCrawl(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("crawl: %w", err)
		}
	case "query":
		if flag.NArg() < 2 {
			return &usageError{msg: "query: text required (usage: cosift query <text> [-k N] [-json])"}
		}
		if err := runQuery(ctx, cfg, flag.Arg(1), flag.Args()[2:]); err != nil {
			return fmt.Errorf("query: %w", err)
		}
	case "search":
		// HTTP-via-server search; exercises the full pipeline of a running
		// cosift instance, unlike `query` which is BM25 local-only.
		if flag.NArg() < 2 {
			return &usageError{msg: "search: text required (usage: cosift search <text> [-server URL] [-k N] [-retriever ...] [-json])"}
		}
		if err := runSearchCLI(ctx, cfg, flag.Arg(1), flag.Args()[2:]); err != nil {
			return fmt.Errorf("search: %w", err)
		}
	case "research":
		// HTTP-via-server research. Sibling to `search` but hits the
		// /research endpoint — LLM synthesis over retrieved sources.
		if flag.NArg() < 2 {
			return &usageError{msg: "research: text required (usage: cosift research <text> [-server URL] [-strategy planner|paraphrase] [-json])"}
		}
		if err := runResearchCLI(ctx, cfg, flag.Arg(1), flag.Args()[2:]); err != nil {
			return fmt.Errorf("research: %w", err)
		}
	case "find-similar":
		// Accepts either a positional URL or -text / -text-file. The first
		// non-flag arg is treated as a URL.
		fsArgs := flag.Args()[1:]
		var sourceURL string
		if len(fsArgs) > 0 && !strings.HasPrefix(fsArgs[0], "-") {
			sourceURL = fsArgs[0]
			fsArgs = fsArgs[1:]
		}
		if err := runFindSimilarCLI(ctx, cfg, sourceURL, fsArgs); err != nil {
			return fmt.Errorf("find-similar: %w", err)
		}
	case "contents":
		// GET /contents (single URL) or POST /contents (batch). Required-args
		// validation happens inside runContentsCLI after flag parsing because
		// URLs can come from positional args OR -file.
		if err := runContentsCLI(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("contents: %w", err)
		}
	case "answer":
		// HTTP-via-server /answer — single-question LLM answer with cited
		// sources. Sibling to `research` but no plan/expansion surface.
		if flag.NArg() < 2 {
			return &usageError{msg: "answer: question required (usage: cosift answer <text> [-server URL] [-k N] [-expand] [-json])"}
		}
		if err := runAnswerCLI(ctx, cfg, flag.Arg(1), flag.Args()[2:]); err != nil {
			return fmt.Errorf("answer: %w", err)
		}
	case "admin":
		if flag.NArg() < 2 {
			return &usageError{msg: adminUsageError()}
		}
		if err := runAdmin(ctx, cfg, flag.Arg(1), flag.Args()[2:]); err != nil {
			return fmt.Errorf("admin: %w", err)
		}
	case "stats":
		if err := runStats(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("stats: %w", err)
		}
	case "status-file":
		if err := runStatusFile(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("status-file: %w", err)
		}
	case "crawl-status":
		if err := runCrawlStatus(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("crawl-status: %w", err)
		}
	case "eval":
		if err := runEval(ctx, flag.Args()[1:]); err != nil {
			return fmt.Errorf("eval: %w", err)
		}
	case "answer-eval":
		if err := runAnswerEval(ctx, flag.Args()[1:]); err != nil {
			return fmt.Errorf("answer-eval: %w", err)
		}
	case "answer-eval-compare":
		if err := runAnswerEvalCompare(ctx, flag.Args()[1:]); err != nil {
			return fmt.Errorf("answer-eval-compare: %w", err)
		}
	case "bench-compare":
		if err := runBenchCompare(flag.Args()[1:]); err != nil {
			return fmt.Errorf("bench-compare: %w", err)
		}
	case "ingest":
		if err := runIngest(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("ingest: %w", err)
		}
	case "export":
		if err := runExport(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("export: %w", err)
		}
	case "migrate-to-pebble":
		if err := runMigrateToPebble(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("migrate-to-pebble: %w", err)
		}
	case "gc":
		if err := runGC(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("gc: %w", err)
		}
	case "outcomes":
		if err := runOutcomes(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("outcomes: %w", err)
		}
	case "doctor":
		// runDoctor already prints its own report; non-nil err means at
		// least one check failed (exit 1) with no extra wrapping.
		if err := runDoctor(ctx, cfg, flag.Args()[1:]); err != nil {
			return err
		}
	case "check-robots":
		if err := runCheckRobots(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("check-robots: %w", err)
		}
	case "crawl-errors":
		if err := runCrawlErrors(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("crawl-errors: %w", err)
		}
	case "reembed":
		if err := runReembed(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("reembed: %w", err)
		}
	case "compact-index":
		if err := runCompactIndex(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("compact-index: %w", err)
		}
	case "bench":
		if err := runBench(ctx, flag.Args()[1:]); err != nil {
			return fmt.Errorf("bench: %w", err)
		}
	case "bench-pq":
		if err := runBenchPQ(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("bench-pq: %w", err)
		}
	case "parse-pdf":
		// Stdin = pdf bytes, stdout = JSON. Sets a soft memory limit so the
		// kernel can kill us via OOM if the pdf library allocates without
		// bound — parent survives.
		debug.SetMemoryLimit(500 << 20)
		crawler.ParsePDFChild(os.Stdin, os.Stdout)
	case "hnsw-rebuild":
		if err := runHNSWRebuild(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("hnsw-rebuild: %w", err)
		}
	case "refresh-due":
		if err := runRefreshDue(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("refresh-due: %w", err)
		}
	case "serve":
		if err := runServe(ctx, cfg); err != nil {
			return fmt.Errorf("serve: %w", err)
		}
	case "pebble-serve":
		if err := runPebbleServe(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("pebble-serve: %w", err)
		}
	case "pebble-info":
		if err := runPebbleInfo(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("pebble-info: %w", err)
		}
	case "pebble-compact":
		if err := runPebbleCompact(ctx, flag.Args()[1:]); err != nil {
			return fmt.Errorf("pebble-compact: %w", err)
		}
	case "frontier-clear":
		if err := runFrontierClear(ctx, flag.Args()[1:]); err != nil {
			return fmt.Errorf("frontier-clear: %w", err)
		}
	case "frontier-trim":
		if err := runFrontierTrim(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("frontier-trim: %w", err)
		}
	case "seed-tranco":
		if err := runSeedTranco(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("seed-tranco: %w", err)
		}
	case "domain-audit":
		if err := runDomainAudit(ctx, flag.Args()[1:]); err != nil {
			return fmt.Errorf("domain-audit: %w", err)
		}
	case "purge-adult":
		if err := runPurgeAdult(ctx, flag.Args()[1:]); err != nil {
			return fmt.Errorf("purge-adult: %w", err)
		}
	case "purge-domain":
		if err := runPurgeDomain(ctx, flag.Args()[1:]); err != nil {
			return fmt.Errorf("purge-domain: %w", err)
		}
	case "backfill-host-postings":
		if err := runBackfillHostPostings(ctx, flag.Args()[1:]); err != nil {
			return fmt.Errorf("backfill-host-postings: %w", err)
		}
	case "verify":
		if err := runVerifyPebble(ctx, cfg, flag.Args()[1:]); err != nil {
			return fmt.Errorf("verify: %w", err)
		}
	default:
		flag.Usage()
		return &usageError{msg: "unknown command: " + cmd}
	}
	return nil
}

// resolveAPIKey returns the first non-empty API key from a slot-specific env
// var, falling back to OPENAI_API_KEY / OPENAI. Lets operators using a
// non-OpenAI embedder or chat endpoint name keys for what they
// actually are. Empty result is "use anonymously" — valid for local
// self-hosted endpoints.
//
// slot: "embed" → checks COSIFT_EMBED_API_KEY first
//
//	"chat"  → checks COSIFT_CHAT_API_KEY first
func resolveAPIKey(slot string) string {
	var first string
	switch slot {
	case "embed":
		first = "COSIFT_EMBED_API_KEY"
	case "chat":
		first = "COSIFT_CHAT_API_KEY"
	}
	if first != "" {
		if v := os.Getenv(first); v != "" {
			return v
		}
	}
	for _, k := range []string{"OPENAI_API_KEY", "OPENAI"} {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

// resolveEmbedAPIKey kept for back-compat with the call site. New
// callers should use resolveAPIKey("embed").
func resolveEmbedAPIKey() string { return resolveAPIKey("embed") }

// firstEnv returns the first non-empty value among the named environment vars.
func firstEnv(names ...string) string {
	for _, n := range names {
		if v := os.Getenv(n); v != "" {
			return v
		}
	}
	return ""
}

func contains(ss []string, x string) bool {
	for _, s := range ss {
		if s == x {
			return true
		}
	}
	return false
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func truncStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
