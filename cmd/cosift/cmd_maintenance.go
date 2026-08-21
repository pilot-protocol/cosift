package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/server"
	"github.com/pilot-protocol/cosift/internal/store"
)

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

// runFrontierTrim deletes completed (status='done') frontier rows older than a
// configurable age and optionally VACUUMs the database.
//
// Background: the GC command (runGC) only drops status='error' rows. Completed
// rows accumulate indefinitely — a 500k-URL crawl leaves 500k done rows that
// serve no purpose once the URL is in the documents table. Over months of
// continuous crawling this can reach tens of millions of rows, bloating the DB
// by several GB and slowing ClaimFrontier's ORDER BY scan even with an index.
//
// Default -older-than of 168h (7 days) is safe for the default refresh cycle
// (refresh-due at 24h): a URL that was crawled 7 days ago and whose done row
// is deleted will re-enter the frontier on the next refresh-due pass, which is
// the correct behavior. Operators with longer refresh cycles should raise this
// value accordingly (e.g. -older-than=720h for monthly refreshes).
func runFrontierTrim(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("frontier-trim", flag.ExitOnError)
	olderThan := fs.Duration("older-than", 7*24*time.Hour, "delete done rows enqueued more than this long ago (e.g. 168h, 720h)")
	dryRun := fs.Bool("dry-run", false, "print what would be deleted without actually deleting")
	vacuum := fs.Bool("vacuum", true, "run VACUUM after deletion to reclaim disk space")
	if err := fs.Parse(args); err != nil {
		return err
	}
	s, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer s.Close()

	stats, serr := s.GetFrontierStats(ctx)
	if serr != nil {
		return fmt.Errorf("frontier-trim: stats: %w", serr)
	}
	log.Printf("frontier-trim: before — queued=%d in_flight=%d done=%d error=%d",
		stats.Queued, stats.InFlight, stats.Done, stats.Errored)

	if *dryRun {
		log.Printf("frontier-trim: dry-run — would delete done rows older than %s (skipping actual delete)", *olderThan)
		return nil
	}

	n, derr := s.TrimDoneFrontier(ctx, *olderThan)
	if derr != nil {
		return fmt.Errorf("frontier-trim: %w", derr)
	}
	log.Printf("frontier-trim: removed %d done rows older than %s", n, *olderThan)

	if *vacuum && n > 0 {
		log.Printf("frontier-trim: vacuuming (this may take a while on large databases)…")
		if verr := s.Vacuum(ctx); verr != nil {
			return fmt.Errorf("frontier-trim: vacuum: %w", verr)
		}
		log.Printf("frontier-trim: vacuum complete")
	}
	return nil
}

// runSeedTranco reads a Tranco top-1M CSV and pushes the top-N root URLs into
// the frontier as depth-0 seeds. This is the fastest way to bootstrap a broad
// crawl from a high-quality, authority-ranked URL set without having to curate
// a manual seeds file.
//
// Each Tranco entry becomes https://<domain>/ with a priority derived from its
// rank: rank 1 → priority 1.0, rank 1M → priority 0.1, log-scale in between.
// The INSERT OR IGNORE semantics of PushFrontier mean this is safe to run
// repeatedly — it only adds URLs not already in the frontier (regardless of
// their current status).
//
// Download the latest Tranco CSV from https://tranco-list.eu/ (updated weekly,
// ~7 MB compressed). The format is "rank,hostname" with no header.
//
// Example:
//
//	cosift seed-tranco -tranco tranco-top1m.csv -top 10000
func runSeedTranco(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("seed-tranco", flag.ExitOnError)
	trancoPath := fs.String("tranco", "", "path to Tranco top-1M CSV (required)")
	topN := fs.Int("top", 10000, "seed only the top-N domains (0 = all)")
	scheme := fs.String("scheme", "https", "URL scheme to prepend (https or http)")
	dryRun := fs.Bool("dry-run", false, "print URLs that would be seeded without writing to the frontier")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *trancoPath == "" {
		return errors.New("seed-tranco: -tranco is required")
	}
	if *scheme != "https" && *scheme != "http" {
		return fmt.Errorf("seed-tranco: -scheme must be https or http, got %q", *scheme)
	}

	f, ferr := os.Open(*trancoPath)
	if ferr != nil {
		return fmt.Errorf("seed-tranco: open %s: %w", *trancoPath, ferr)
	}
	defer f.Close()

	// Parse the CSV into (rank, host) pairs up to topN.
	type entry struct {
		rank int
		host string
	}
	entries := make([]entry, 0, 10000)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 4096), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var rankStr, host string
		if i := strings.IndexAny(line, ",\t"); i > 0 {
			rankStr = strings.TrimSpace(line[:i])
			host = strings.ToLower(strings.TrimSpace(line[i+1:]))
		} else {
			continue
		}
		rank, err := strconv.Atoi(rankStr)
		if err != nil || rank <= 0 {
			continue
		}
		host = strings.TrimPrefix(host, "www.")
		if host == "" {
			continue
		}
		if *topN > 0 && rank > *topN {
			break
		}
		entries = append(entries, entry{rank: rank, host: host})
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("seed-tranco: scan: %w", err)
	}
	log.Printf("seed-tranco: parsed %d entries from %s (top %d)", len(entries), *trancoPath, *topN)

	if *dryRun {
		limit := len(entries)
		if limit > 20 {
			limit = 20
		}
		for _, e := range entries[:limit] {
			log.Printf("  [dry-run] rank=%d  %s://%s/", e.rank, *scheme, e.host)
		}
		if len(entries) > 20 {
			log.Printf("  … and %d more", len(entries)-20)
		}
		return nil
	}

	s, serr := store.Open(cfg.DataDir)
	if serr != nil {
		return serr
	}
	defer s.Close()

	// trancoPriority converts a Tranco rank to a frontier priority [0.1, 1.0].
	// Uses decimal-digit counting as a cheap log10 proxy — good enough for
	// priority bucketing. rank=1→1.0, rank=10→0.83, rank=100→0.67,
	// rank=1k→0.50, rank=10k→0.33, rank=100k→0.17, rank=1M→0.10.
	trancoPriority := func(rank int) float64 {
		if rank <= 0 {
			return 0.5
		}
		digits := 0
		r := rank
		for r >= 10 {
			r /= 10
			digits++
		}
		p := 1.0 - float64(digits)/6.0
		if p < 0.1 {
			p = 0.1
		}
		return p
	}

	pushed := 0
	for _, e := range entries {
		rawURL := fmt.Sprintf("%s://%s/", *scheme, e.host)
		priority := trancoPriority(e.rank)
		if err := s.PushFrontier(ctx, rawURL, 0, priority); err != nil {
			log.Printf("seed-tranco: push %s: %v", rawURL, err)
			continue
		}
		pushed++
	}
	log.Printf("seed-tranco: pushed %d URLs into frontier (INSERT OR IGNORE — already-present URLs unchanged)", pushed)
	return nil
}

// runOutcomes exports query_outcomes for offline calibration work. JSON for
// programmatic consumers, CSV for spreadsheet / pandas notebooks.
func runOutcomes(ctx context.Context, cfg *config.Config, args []string) (err error) {
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
	f, cerr := os.Create(*output)
	if cerr != nil {
		return cerr
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close output: %w", closeErr)
		}
	}()

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
// struct so the defaults check can be tested without stdout capture.
type doctorCheck struct {
	Name   string `json:"name"`
	Status string `json:"status"` // "PASS" | "WARN" | "FAIL" | "INFO" | "SKIP"
	Detail string `json:"detail"`
}

// doctorDefaultsChecks cross-checks the defaults block against
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
	// cosift.json carries Server.AdminToken and may be the sink for
	// LoadDotEnv-sourced secrets (OPENAI_API_KEY, etc.). Owner-only.
	if err := os.WriteFile(cfgPath, buf, 0o600); err != nil {
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

func runDoctor(ctx context.Context, cfg *config.Config, args []string) error {
	// When -server is empty, doctor stays
	// purely local. When -server is set, doctor also pings
	// the server's /healthz and /stats; if -token is also set (or env), it
	// validates the token by hitting /admin/config.
	fs := flag.NewFlagSet("doctor", flag.ExitOnError)
	remoteServer := fs.String("server", "", "remote cosift URL to additionally probe (empty = local checks only)")
	remoteToken := fs.String("token", "", "admin bearer token for -server admin-endpoint check (defaults to COSIFT_ADMIN_TOKEN env)")
	// Same checks, same
	// non-zero exit on FAIL, just JSON instead of '[PASS] name — detail'.
	asJSON := fs.Bool("json", false, "emit JSON instead of human-readable text (suitable for jq / CI)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var checks []doctorCheck
	add := func(name, status, detail string) { checks = append(checks, doctorCheck{name, status, detail}) }

	// 1. Data dir writable.
	probe := filepath.Join(cfg.DataDir, ".doctor.probe")
	// Match store.Open's 0o700 — MkdirAll's perm only applies to NEW dirs,
	// but doctor running first must not silently leave a looser mode behind.
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
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

	// 2b. Pebble open + counters. Pebble is optional; "absent" is
	// not a failure, just a SKIP. When present, verify open + read counters.
	// the open can fail because pebble-serve / a crawl holds the
	// single-writer lock — that's a healthy state, not a FAIL. Distinguish
	// the lock error from real corruption.
	pebbleDir := filepath.Join(cfg.DataDir, "pebble")
	if _, statErr := os.Stat(pebbleDir); statErr != nil {
		add("pebble store", "SKIP", "no pebble dir (sqlite-only deployment)")
	} else if ps, err := store.OpenPebble(pebbleDir); err != nil {
		msg := err.Error()
		if strings.Contains(msg, "lock") || strings.Contains(msg, "resource temporarily unavailable") {
			add("pebble store", "INFO", "another process holds the writer lock (pebble-serve / crawl in flight)")
		} else {
			add("pebble store", "FAIL", msg)
		}
	} else {
		_, n, _ := ps.CorpusStats(ctx)
		// also report HNSW availability so operators can tell from
		// `cosift doctor` whether dense / hybrid retrievers will work without
		// firing pebble-info or pebble-serve /stats. Cheap — same 20-byte
		// meta blob read pebble-info already does.
		meta, hnswOK, _ := index.LoadHNSWMeta(ctx, ps)
		_ = ps.Close()
		add("pebble store", "PASS", fmt.Sprintf("%d indexed docs", n))
		if hnswOK {
			add("HNSW graph", "PASS", fmt.Sprintf("%d nodes, dim=%d — dense/hybrid available (load with COSIFT_LOAD_HNSW=true)", meta.NodeCount, meta.Dim))
		} else {
			add("HNSW graph", "INFO", "no vectors persisted — only bm25 / bm25-mlt retrievers (set cfg.Embeddings.Model and re-crawl to add dense)")
		}
	}

	// 2c. COSIFT_* env vars. Lists which path-2 env overrides are
	// set so operators can confirm 'is my override actually live'. Silent
	// typos previously hid behind 'env unset → keep default' branches.
	cosiftEnvs := []string{
		"COSIFT_PEBBLE_CACHE_MB", "COSIFT_PEBBLE_MEMTABLE_MB", "COSIFT_PEBBLE_MEMTABLES", "COSIFT_PEBBLE_SYNC",
		"COSIFT_BM25_K1", "COSIFT_BM25_B",
		"COSIFT_BM25_TOPK_POOL_FACTOR", "COSIFT_BM25_DISABLE_TOPK_POOL",
		"COSIFT_HYDE_CACHE_SIZE", "COSIFT_PARA_CACHE_SIZE",
		"COSIFT_LOAD_HNSW", //
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
	// a custom embedder/chat URL (Ollama / vLLM / TEI) doesn't
	// need a key; PASS when URL is set even without a key. Hosted defaults
	// (empty URL) still require a key for the configured capability.
	embedKey := resolveEmbedAPIKey()
	chatKey := os.Getenv("OPENAI_API_KEY")
	if chatKey == "" {
		chatKey = os.Getenv("OPENAI")
	}
	embedConfigured := cfg.Embeddings.Model != ""
	chatConfigured := cfg.Chat.Model != ""
	embedLocal := cfg.Embeddings.URL != ""
	chatLocal := cfg.Chat.URL != ""
	switch {
	case !embedConfigured && !chatConfigured:
		add("embed/chat auth", "INFO", "no embed/chat models configured (fine for bm25-only)")
	case (embedConfigured && (embedKey != "" || embedLocal)) && (!chatConfigured || chatKey != "" || chatLocal):
		add("embed/chat auth", "PASS", fmt.Sprintf("embed=%s chat=%s", authStatus(embedConfigured, embedKey, embedLocal), authStatus(chatConfigured, chatKey, chatLocal)))
	default:
		var missing []string
		if embedConfigured && embedKey == "" && !embedLocal {
			missing = append(missing, "embed (no URL, no key)")
		}
		if chatConfigured && chatKey == "" && !chatLocal {
			missing = append(missing, "chat (no URL, no key)")
		}
		add("embed/chat auth", "WARN", strings.Join(missing, ", ")+" — set COSIFT_EMBED_API_KEY / OPENAI_API_KEY, or point Embeddings.URL / Chat.URL at a local server")
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

	// 7. Defaults vs capabilities. The defaults block lets operators
	// pre-configure retrieval behavior, but a default that requires capability
	// the server isn't configured for (e.g. retriever=hybrid without an
	// embedder) will silently fail at request time — 400 on every call. Catch
	// the mismatch HERE instead of at first traffic.
	hasEmbed := cfg.Embeddings.Model != "" || os.Getenv("OPENAI_API_KEY") != "" || os.Getenv("OPENAI") != ""
	hasChat := cfg.Chat.Model != "" || os.Getenv("OPENAI_API_KEY") != "" || os.Getenv("OPENAI") != ""
	checks = append(checks, doctorDefaultsChecks(cfg.Defaults, hasEmbed, hasChat)...)

	// 8. Remote checks — only when -server is set. Token resolution
	// mirrors admin CLIs: -token flag wins, then env.
	if *remoteServer != "" {
		resolvedToken := *remoteToken
		if resolvedToken == "" {
			resolvedToken = os.Getenv("COSIFT_ADMIN_TOKEN")
		}
		checks = append(checks, doctorRemoteChecks(ctx, *remoteServer, resolvedToken)...)
	}

	fail := 0
	for _, c := range checks {
		if c.Status == "FAIL" {
			fail++
		}
	}
	if *asJSON {
		out := map[string]any{
			"checks": checks,
			"fail":   fail,
			"ready":  fail == 0,
		}
		blob, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(blob))
		if fail > 0 {
			return errors.New("doctor failures")
		}
		return nil
	}
	for _, c := range checks {
		fmt.Printf("%-6s %s — %s\n", "["+c.Status+"]", c.Name, c.Detail)
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
// loop to render.
func doctorRemoteChecks(ctx context.Context, serverURL, token string) []doctorCheck {
	var out []doctorCheck
	base := strings.TrimRight(strings.Replace(serverURL, "0.0.0.0", "127.0.0.1", 1), "/")
	client := &http.Client{Timeout: 10 * time.Second}

	// /healthz
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/healthz", http.NoBody)
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
	req, _ = http.NewRequestWithContext(ctx, http.MethodGet, base+"/stats", http.NoBody)
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
		req, _ = http.NewRequestWithContext(ctx, http.MethodGet, base+"/admin/config", http.NoBody)
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
