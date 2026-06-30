package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/server"
	"github.com/pilot-protocol/cosift/internal/store"
)

// runAdmin dispatches to a sub-operation under `cosift admin <op>`. Sub-ops
// share bearer-token resolution: -token flag wins, then COSIFT_ADMIN_TOKEN env.
// Empty token still hits the server (and gets a clear 401) — the CLI doesn't
// pre-fail because the operator might be running an instance without admin
// auth enabled (rare, but the failure path is the server's to own).
// adminUsageError returns the multi-line help message printed when the
// `cosift admin` parent is invoked without a subcommand. Listed alphabetically
// by subcommand name so the output is stable across iterations.
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
// which is the right error to surface).
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
	// inline flag parsing (vs parseAdminCommon) so we can add the
	// -summary flag. Same shape as runAdminRecrawl's flag set —
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
	// LLM-cache sizes. Show only when populated — a fresh
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
// positional args, `-file`, or both (mirrors's `contents` CLI pattern).
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
// (positional arg). Wraps's /admin/recrawl-by-domain endpoint. Requires
// `-y` to commit or `-dry-run` to preview the matched count without queuing.
// If both flags are set, `-dry-run` wins (safer-on-conflict).
func runAdminRecrawlDomain(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("admin recrawl-domain", flag.ExitOnError)
	defaultServer := "http://" + cfg.Server.Addr
	defaultServer = strings.Replace(defaultServer, "0.0.0.0", "127.0.0.1", 1)
	serverURL := fs.String("server", defaultServer, "cosift server URL")
	token := fs.String("token", "", "admin bearer token (defaults to COSIFT_ADMIN_TOKEN env)")
	confirm := fs.Bool("y", false, "confirm the destructive operation (recrawl every matching URL)")
	dryRun := fs.Bool("dry-run", false, "enumerate matched URLs without queuing — preview the blast radius")
	// cap dry-run URL listing to avoid console spam on large matches.
	// 0 = no list (count only — behavior). 20 = default safe cap.
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
		// -limit-list 0 → count only. -limit-list -1 → unlimited (operator
		// wants the full list, accepts console spam). Otherwise: print up
		// to listLimit and append a "... (N more)" suffix.
		if *listLimit != 0 && len(rr.URLs) > 0 {
			n := *listLimit
			if n < 0 || n > len(rr.URLs) {
				n = len(rr.URLs)
			}
			for _, u := range rr.URLs[:n] {
				fmt.Printf("  %s\n", u)
			}
			if remaining := len(rr.URLs) - n; remaining > 0 {
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

// runAdminReembedCLI triggers a server-side reembed via the
// `/admin/reembed` SSE endpoint. Requires `-y` to commit (reembed costs
// LLM credits; same destructive-op guard pattern as other admin verbs).
// `-drop-old` propagates to the server's `DropPassagesNotModel` step.
func runAdminReembedCLI(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("admin reembed", flag.ExitOnError)
	defaultServer := "http://" + cfg.Server.Addr
	defaultServer = strings.Replace(defaultServer, "0.0.0.0", "127.0.0.1", 1)
	serverURL := fs.String("server", defaultServer, "cosift server URL")
	token := fs.String("token", "", "admin bearer token (defaults to COSIFT_ADMIN_TOKEN env)")
	dropOld := fs.Bool("drop-old", false, "also delete passages from other models after re-embedding")
	// -since restricts reembed to docs published >= DATE. Client-side
	// validation catches malformed dates before the HTTP call (saves a round-trip
	// for the common typo case).
	since := fs.String("since", "", "ISO date — only re-embed docs published >= this. Drops undated docs when set")
	confirm := fs.Bool("y", false, "confirm the operation (reembed re-runs every embedding call; counts as LLM spend)")
	// -dry-run shows the would-be-processed count without spending
	// LLM credits. Either -y OR -dry-run is required.
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
	// dry-run wins when both flags set (safer-on-conflict, matches
	// pattern on /admin/recrawl-by-domain).
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
// third SSE consumer in the CLI (after research and
// answer), reusing the forEachSSEEvent scanner.
func consumeReembedSSE(body io.Reader) error {
	var sawDone bool
	// track the started event's total_docs and target_model so the
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
				// dry-run summary — reference the started event's
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

// peekWarnings extracts the warnings field from a server
// response body without committing to a full typed decode. When the CLI hits
// pebble-serve, the response carries a warnings[] slice naming silent no-ops
// (unknown expand, rerank without reranker, malformed include_domains, etc).
// Without surfacing these on stderr the human CLI mode swallows them.
func peekWarnings(body []byte) []string {
	var probe struct {
		Warnings []string `json:"warnings"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return nil
	}
	return probe.Warnings
}

// emitWarnings prints each warning to stderr with a 'cosift:' prefix.
func emitWarnings(body []byte) {
	for _, w := range peekWarnings(body) {
		fmt.Fprintln(os.Stderr, "cosift: warning:", w)
	}
}

func runStats(ctx context.Context, cfg *config.Config, args []string) error {
	// -backend flag (-backend=pebble reads the Pebble subdir).
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
		ps, err := openPebbleOrFriendlyErr(pebbleDir)
		if err != nil {
			return err
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
// alongside the writer.
// runStatusFile reads the crawl-status.json (the file the live
// crawler writes every 10s) and pretty-prints it. Useful when an operator
// can't run `cosift stats -backend=pebble` because Pebble's single-writer
// lock is held by the crawl process.
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
	// ?-target N → 'indexed/target (pct)' line for long crawls toward
	// a known doc-count goal. No-op when target is unset or already met.
	// ETA from started_at + indexed_docs_at_start fields.
	// Rate is averaged since the dumper's first poll, not instantaneous.
	if *target > 0 && d.IndexedDocs > 0 {
		pct := float64(d.IndexedDocs) / float64(*target) * 100
		// cap pct display at 100% and append a 'reached' marker when
		// the crawl has met or exceeded the goal — operators watching a long
		// crawl see the win line instead of '237.4%' growing confusingly.
		switch {
		case d.IndexedDocs >= *target:
			fmt.Printf("  target:     %d / %d (100.0%%, reached)\n", d.IndexedDocs, *target)
		default:
			fmt.Printf("  target:     %d / %d (%.1f%%)\n", d.IndexedDocs, *target, pct)
		}
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
