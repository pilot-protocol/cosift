package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

	// -backend flag mirrors on crawl + stats. Both
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
		ps, err := openPebbleOrFriendlyErr(pebbleDir)
		if err != nil {
			return err
		}
		defer ps.Close()
		// runQuery previously
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
	// MMR lambda passthrough for /search.
	mmr := fs.String("mmr", "", "MMR diversification lambda in [0,1] (needs HNSW + embedder server-side)")
	rerankFlag := fs.Bool("rerank", false, "wrap retrieval with LLM listwise reranker (server must have it configured)")
	expand := fs.Bool("expand", false, "LLM paraphrase + RRF fusion (server must have a paraphraser)")
	since := fs.String("since", "", "ISO date — only results published on or after")
	until := fs.String("until", "", "ISO date — only results published on or before")
	includeDomains := fs.String("include-domains", "", "comma-separated allowlist of result domains")
	excludeDomains := fs.String("exclude-domains", "", "comma-separated denylist of result domains")
	sortMode := fs.String("sort", "", "relevance | date_desc | date_asc (server default if empty)")
	format := fs.String("format", "text", "human-output format: text | markdown (or md).")
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
	if *mmr != "" {
		v.Set("mmr", *mmr)
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
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
	emitWarnings(body)
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
// Sibling to runSearchCLI: same -server / -json affordance, but pulls
// an LLM-synthesized answer with cited sources rather than a ranked URL list.
// Non-streaming — the SSE mode (?stream=true) is a separate code path; this
// returns the same JSON the non-streaming endpoint produces.
// renderAnswerMarkdown formats a /research or /answer response as markdown,
// suitable for piping into LLM contexts or markdown viewers. Used by both
// runResearchCLI and runAnswerCLI via the shared `-format md|markdown` flag.
// strategy/plan are research-only; pass "" / nil from /answer's callsite.
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
		for i, s := range sources {
			// `N. [Title](URL)` — the leading N matches the citation IDs the
			// LLM emits inline (e.g. `[1]` in the answer references source 1).
			fmt.Fprintf(&b, "%d. [%s](%s)", sourceIDOf(i, s), s.Title, s.URL)
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
// renderRankedMarkdown renders a ranked hit list as markdown. Callers
// pass "Results: <query>" for /search or "Similar to: <url>" for
// /find_similar; the hit-list shape is the same in both cases.
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
// per the SSE spec. Extracted from's consumeResearchSSE so
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
					if errors.Is(err, errSSEDone) {
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
// error. First streaming CLI in cosift. Refactored in to
// use the generic forEachSSEEvent scanner.
func consumeResearchSSE(body io.Reader, format string) error {
	useMarkdown := format == "md" || format == "markdown"
	var answerStarted, sawDone bool
	// capture pebble-serve's sources event payload so we can render
	// the final Sources section when pebble's minimal done event arrives.
	var pebbleSources []server.AnswerSource

	err := forEachSSEEvent(body, func(event, data string) error {
		switch event {
		case "warnings":
			// pebble-serve emits this event when the
			// request had silent no-ops. Surface to stderr like the sync CLI.
			var w struct {
				Warnings []string `json:"warnings"`
			}
			if err := json.Unmarshal([]byte(data), &w); err == nil {
				for _, msg := range w.Warnings {
					fmt.Fprintln(os.Stderr, "cosift: warning:", msg)
				}
			}
		case "plan":
			var p struct {
				Strategy string   `json:"strategy"`
				Variants []string `json:"variants"`
				// pebble-serve uses 'plan' for the sub-query list and
				// emits an 'expand' label instead of 'strategy'. Decode both
				// shapes; fall back to "planner" when only plan is set.
				Plan   []string `json:"plan"`
				Expand string   `json:"expand"`
			}
			if err := json.Unmarshal([]byte(data), &p); err != nil {
				return nil // tolerate malformed mid-stream event
			}
			strategy := p.Strategy
			variants := p.Variants
			if strategy == "" && len(p.Plan) > 0 {
				strategy = "planner"
				if p.Expand != "" {
					strategy = "planner+" + p.Expand
				}
			}
			if len(variants) == 0 {
				variants = p.Plan
			}
			if useMarkdown {
				fmt.Printf("> Strategy: `%s`", strategy)
				if len(variants) > 0 {
					fmt.Printf(" (plan: %s)", strings.Join(variants, " | "))
				}
				fmt.Println()
				fmt.Println()
			} else {
				fmt.Printf("Strategy: %s", strategy)
				if len(variants) > 0 {
					fmt.Printf(" (plan: %s)", strings.Join(variants, " | "))
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
		case "sources":
			// pebble-serve emits one 'sources' event after retrieval
			// that combines what the SQLite server splits across
			// retrieved+synthesizing. Render the same progress line.
			// also capture the sources slice so the final 'done'
			// event (pebble's is minimal) can still render the Sources block.
			var s struct {
				Sources []server.AnswerSource `json:"sources"`
			}
			if err := json.Unmarshal([]byte(data), &s); err != nil {
				return nil
			}
			pebbleSources = s.Sources
			fmt.Printf("[synthesizing answer over %d source(s)]\n\n", len(s.Sources))
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
			if err := json.Unmarshal([]byte(data), &rr); err != nil || len(rr.Sources) == 0 {
				// pebble's done event has a minimal payload —
				// fall back to the sources captured from the event.
				renderStreamingSources(pebbleSources, useMarkdown)
				return errSSEDone
			}
			renderStreamingSources(rr.Sources, useMarkdown)
			return errSSEDone
		case "error":
			var e struct {
				Detail string `json:"detail"`
				// pebble-serve uses 'error' field instead of 'detail'.
				Error string `json:"error"`
				// pebble-serve tags errors with the phase that failed.
				Phase string `json:"phase"`
			}
			_ = json.Unmarshal([]byte(data), &e)
			msg := e.Detail
			if msg == "" {
				msg = e.Error
			}
			if e.Phase != "" {
				return fmt.Errorf("server stream error (phase=%s): %s", e.Phase, msg)
			}
			return fmt.Errorf("server stream error: %s", msg)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !sawDone {
		// Stream ended without a terminal event — flag for operators but
		// don't error (matches behavior).
		fmt.Fprintln(os.Stderr, "(stream ended without `done` event)")
	}
	return nil
}

// renderStreamingSources writes the terminal Sources section for both the
// research and answer SSE consumers. Format matches the non-stream paths so
// operators get consistent output regardless of whether they used -stream.
// position-based ID fallback for source rendering when the
// server didn't emit an id field (pebble-serve's source payload). SQLite-side
// servers always emit non-zero IDs, so their output is unchanged.
func sourceIDOf(i int, s server.AnswerSource) int {
	if s.ID > 0 {
		return s.ID
	}
	return i + 1
}

func renderStreamingSources(sources []server.AnswerSource, useMarkdown bool) {
	if len(sources) == 0 {
		return
	}
	idOf := sourceIDOf
	if useMarkdown {
		fmt.Println()
		fmt.Println("## Sources")
		fmt.Println()
		for i, s := range sources {
			fmt.Fprintf(os.Stdout, "%d. [%s](%s)", idOf(i, s), s.Title, s.URL)
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
	for i, s := range sources {
		date := ""
		if s.PublishedAt != nil {
			date = " " + s.PublishedAt.Format("2006-01-02")
		}
		domain := ""
		if s.Domain != "" {
			domain = " [" + s.Domain + "]"
		}
		fmt.Printf("  [%d]%s%s %s\n      %s\n", idOf(i, s), domain, date, s.Title, s.URL)
	}
}

// consumeAnswerSSE reads an /answer?stream=true response and renders events as
// they arrive. Same event vocabulary as consumeResearchSSE minus the `plan`
// event (/answer has no expansion phase).
func consumeAnswerSSE(body io.Reader, format string) error {
	useMarkdown := format == "md" || format == "markdown"
	var answerStarted, sawDone bool
	var pebbleSources []server.AnswerSource //

	err := forEachSSEEvent(body, func(event, data string) error {
		switch event {
		case "warnings":
			// pebble-serve emits this event when the
			// request had silent no-ops. Surface to stderr.
			var w struct {
				Warnings []string `json:"warnings"`
			}
			if err := json.Unmarshal([]byte(data), &w); err == nil {
				for _, msg := range w.Warnings {
					fmt.Fprintln(os.Stderr, "cosift: warning:", msg)
				}
			}
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
		case "sources":
			// pebble-serve emits 'sources' as the combined
			// retrieved+synthesizing signal. Render the synthesizing line.
			// capture for the final Sources block.
			var s struct {
				Sources []server.AnswerSource `json:"sources"`
			}
			if err := json.Unmarshal([]byte(data), &s); err != nil {
				return nil
			}
			pebbleSources = s.Sources
			fmt.Printf("[synthesizing answer over %d source(s)]\n\n", len(s.Sources))
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
			if err := json.Unmarshal([]byte(data), &ar); err != nil || len(ar.Sources) == 0 {
				// tolerate pebble's minimal done payload; fall
				// back to the sources captured from the sources event.
				renderStreamingSources(pebbleSources, useMarkdown)
				return errSSEDone
			}
			renderStreamingSources(ar.Sources, useMarkdown)
			return errSSEDone
		case "error":
			var e struct {
				Detail string `json:"detail"`
				// pebble-serve uses 'error' field instead of 'detail'.
				Error string `json:"error"`
				// pebble-serve tags errors with the phase that failed.
				Phase string `json:"phase"`
			}
			_ = json.Unmarshal([]byte(data), &e)
			msg := e.Detail
			if msg == "" {
				msg = e.Error
			}
			if e.Phase != "" {
				return fmt.Errorf("server stream error (phase=%s): %s", e.Phase, msg)
			}
			return fmt.Errorf("server stream error: %s", msg)
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
// not surfaced to the caller.
var errSSEDone = errors.New("sse-done")

// validateFormat returns nil if v is one of the accepted output-format values
// for the synth CLIs (`-format`). "" is rejected — flag.String defaults to ""
// when the user passes `-format` without a value, and we want that to fail
// loudly rather than silently behave as text.
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
	format := fs.String("format", "text", "human-output format: text | markdown (or md).")
	stream := fs.Bool("stream", false, "stream progress + token-by-token answer over SSE.")
	jsonOut := fs.Bool("json", false, "emit raw JSON response instead of human-readable answer+sources")
	// Expose the quality + scope flags on the CLI.
	k := fs.Int("k", 0, "number of sources fed to synth (1-20, server default if 0)")
	expand := fs.String("expand", "", "retrieval expansion: hyde | paraphrase (empty = no expansion)")
	rerank := fs.Bool("rerank", false, "rerank retrieved sources before synth")
	// per-sub-query retriever choice — applies to every sub-query
	// the planner emits. Server uses bm25 by default if this is empty.
	retriever := fs.String("retriever", "", "retriever: bm25 | dense | hybrid (server must have HNSW + embedder for dense/hybrid)")
	// MMR lambda passthrough for /research.
	mmr := fs.String("mmr", "", "MMR diversification lambda in [0,1] (needs HNSW + embedder server-side)")
	since := fs.String("since", "", "ISO date — only sources published on or after")
	until := fs.String("until", "", "ISO date — only sources published on or before")
	includeDomains := fs.String("include-domains", "", "CSV allowlist of source domains")
	excludeDomains := fs.String("exclude-domains", "", "CSV denylist of source domains")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := validateFormat(*format); err != nil {
		return err
	}
	if *stream && *jsonOut {
		return errors.New("-stream and -json are mutually exclusive (stream is event-by-event, json is the final blob)")
	}
	if *k < 0 || *k > 20 {
		return errors.New("k must be in [1, 20] or 0 to use server default")
	}

	v := url.Values{}
	v.Set("q", q)
	if *strategy != "" {
		v.Set("strategy", *strategy)
	}
	if *k > 0 {
		v.Set("k", strconv.Itoa(*k))
	}
	if *expand != "" {
		v.Set("expand", *expand)
	}
	if *rerank {
		v.Set("rerank", "true")
	}
	if *retriever != "" {
		v.Set("retriever", *retriever)
	}
	if *mmr != "" {
		v.Set("mmr", *mmr)
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
	if *stream {
		v.Set("stream", "true")
	}

	endpoint := strings.TrimRight(*serverURL, "/") + "/research?" + v.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
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
	emitWarnings(body)
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
		for i, s := range rr.Sources {
			date := ""
			if s.PublishedAt != nil {
				date = " " + s.PublishedAt.Format("2006-01-02")
			}
			domain := ""
			if s.Domain != "" {
				domain = " [" + s.Domain + "]"
			}
			fmt.Printf("  [%d]%s%s %s\n      %s\n", sourceIDOf(i, s), domain, date, s.Title, s.URL)
		}
	}
	return nil
}

// runFindSimilarCLI hits a running cosift server's /find_similar endpoint.
// Completes the  CLI matrix:
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
	format := fs.String("format", "text", "human-output format: text | markdown (or md).")
	jsonOut := fs.Bool("json", false, "emit raw JSON response instead of human-readable list")
	// alternative inputs — feed arbitrary text (or a file of text)
	// instead of an indexed source URL. Mirrors /find_similar text mode.
	textInput := fs.String("text", "", "arbitrary text for content-based MLT (no positional URL needed)")
	textFile := fs.String("text-file", "", "read MLT source text from FILE")
	textTitle := fs.String("text-title", "", "optional title (×3 boost) when using -text or -text-file")
	// surface the filter + rerank flags on the CLI.
	rerank := fs.Bool("rerank", false, "rerank neighbors against the MLT query (server must have rerank configured)")
	// retriever choice — bm25-mlt (default) | dense | hybrid.
	// Dense reads source vector from HNSW for URL-mode; hybrid RRF-fuses
	// BM25-MLT and dense. Both require COSIFT_LOAD_HNSW=true server-side.
	retriever := fs.String("retriever", "", "retriever: bm25 (BM25-MLT) | dense | hybrid (server must have HNSW; text-mode dense/hybrid also needs embedder)")
	// MMR diversification for /find_similar.
	mmr := fs.String("mmr", "", "MMR diversification lambda in [0,1] (URL-mode reuses graph vector — no embedder needed)")
	since := fs.String("since", "", "ISO date — only neighbors published on or after")
	until := fs.String("until", "", "ISO date — only neighbors published on or before")
	includeDomains := fs.String("include-domains", "", "CSV allowlist of neighbor domains")
	excludeDomains := fs.String("exclude-domains", "", "CSV denylist of neighbor domains")
	qExtra := fs.String("q", "", "extra query terms appended to the auto-derived MLT query")
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
	// filter + rerank passthroughs.
	if *rerank {
		v.Set("rerank", "true")
	}
	if *retriever != "" {
		v.Set("retriever", *retriever)
	}
	if *mmr != "" {
		v.Set("mmr", *mmr)
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
	if *qExtra != "" {
		v.Set("q", *qExtra)
	}

	// switch GET → POST when -text is large (URL params have practical
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
		if *rerank {
			body["rerank"] = true
		}
		if *retriever != "" {
			body["retriever"] = *retriever
		}
		if *mmr != "" {
			body["mmr"] = *mmr
		}
		if *since != "" {
			body["since"] = *since
		}
		if *until != "" {
			body["until"] = *until
		}
		if *includeDomains != "" {
			body["include_domains"] = *includeDomains
		}
		if *excludeDomains != "" {
			body["exclude_domains"] = *excludeDomains
		}
		if *qExtra != "" {
			body["q"] = *qExtra
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
	emitWarnings(body)
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
// -file) → POST /contents with {urls: [...]} body.
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
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/contents?"+v.Encode(), http.NoBody)
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
// comments are ignored. Used by `cosift contents -file`.
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
// runResearchCLI but no planner/paraphrase strategy surface —
// /answer just retrieves k sources and synthesizes a cited answer. Smaller K
// cap [1,20] mirrors the server's bound (vs /search's [1,100]).
func runAnswerCLI(ctx context.Context, cfg *config.Config, q string, args []string) error {
	fs := flag.NewFlagSet("answer", flag.ExitOnError)
	defaultServer := "http://" + cfg.Server.Addr
	defaultServer = strings.Replace(defaultServer, "0.0.0.0", "127.0.0.1", 1)
	serverURL := fs.String("server", defaultServer, "cosift server URL")
	k := fs.Int("k", 5, "number of sources to retrieve (1-20)")
	expand := fs.Bool("expand", false, "LLM paraphrase + RRF fusion of retrieval inputs (server must have a paraphraser)")
	format := fs.String("format", "text", "human-output format: text | markdown (or md).")
	stream := fs.Bool("stream", false, "stream progress + token-by-token answer over SSE.")
	jsonOut := fs.Bool("json", false, "emit raw JSON response instead of human-readable answer+sources")
	// Expose the quality + scope flags on the CLI.
	rerank := fs.Bool("rerank", false, "rerank retrieved sources before synth (server must have rerank configured)")
	// Empty
	// passes nothing → server uses its default (bm25).
	retriever := fs.String("retriever", "", "retriever: bm25 | dense | hybrid (server must have HNSW + embedder for dense/hybrid)")
	// MMR lambda passthrough. Empty → no diversification.
	mmr := fs.String("mmr", "", "MMR diversification lambda in [0,1] (0.7 = mostly relevance with some diversity; needs HNSW + embedder server-side)")
	since := fs.String("since", "", "ISO date — only sources published on or after")
	until := fs.String("until", "", "ISO date — only sources published on or before")
	includeDomains := fs.String("include-domains", "", "CSV allowlist of source domains")
	excludeDomains := fs.String("exclude-domains", "", "CSV denylist of source domains")
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
	if *rerank {
		v.Set("rerank", "true")
	}
	if *retriever != "" {
		v.Set("retriever", *retriever)
	}
	if *mmr != "" {
		v.Set("mmr", *mmr)
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
	if *stream {
		v.Set("stream", "true")
	}

	endpoint := strings.TrimRight(*serverURL, "/") + "/answer?" + v.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
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
	emitWarnings(body)
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
		for i, s := range ar.Sources {
			date := ""
			if s.PublishedAt != nil {
				date = " " + s.PublishedAt.Format("2006-01-02")
			}
			domain := ""
			if s.Domain != "" {
				domain = " [" + s.Domain + "]"
			}
			fmt.Printf("  [%d]%s%s %s\n      %s\n", sourceIDOf(i, s), domain, date, s.Title, s.URL)
		}
	}
	return nil
}
