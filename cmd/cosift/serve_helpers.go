package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

// retrievalFilters bundles the four post-retrieval predicates /search,
// /answer, /find_similar, and /research all share. Centralized in
// so /research's sync + stream paths stay in lockstep.
type retrievalFilters struct {
	include    []string
	exclude    []string
	sites      []siteScope
	since      time.Time
	until      time.Time
	dateActive bool
}

func parseRetrievalFilters(r *http.Request) (retrievalFilters, error) {
	f := retrievalFilters{
		include: splitDomainsCSV(r.URL.Query().Get("include_domains")),
		exclude: splitDomainsCSV(r.URL.Query().Get("exclude_domains")),
		sites:   parseSiteScopes(r.URL.Query().Get("site")),
	}
	since, err := parseDateBound(r.URL.Query().Get("since"))
	if err != nil {
		return f, fmt.Errorf("since: %w", err)
	}
	until, err := parseDateBound(r.URL.Query().Get("until"))
	if err != nil {
		return f, fmt.Errorf("until: %w", err)
	}
	f.since = since
	f.until = until
	f.dateActive = !since.IsZero() || !until.IsZero()
	return f, nil
}

// allow reports whether (url, publishedAt) clears the filter. publishedAt
// can be the zero Time when the caller doesn't have it loaded; zero is
// treated as "unknown" and dropped under any date filter.
func (f retrievalFilters) allow(rawURL string, publishedAt time.Time) bool {
	if len(f.include) > 0 || len(f.exclude) > 0 {
		host := hostOf(rawURL)
		if len(f.include) > 0 && !matchesAnyDomain(host, f.include) {
			return false
		}
		if len(f.exclude) > 0 && matchesAnyDomain(host, f.exclude) {
			return false
		}
	}
	if len(f.sites) > 0 && !matchesAnySite(rawURL, f.sites) {
		return false
	}
	if f.dateActive {
		if publishedAt.IsZero() {
			return false
		}
		if !f.since.IsZero() && publishedAt.Before(f.since) {
			return false
		}
		if !f.until.IsZero() && publishedAt.After(f.until) {
			return false
		}
	}
	return true
}

func parseSubQueries(raw, fallback string) []string {
	raw = strings.TrimSpace(raw)
	for _, fence := range []string{"```json", "```"} {
		raw = strings.TrimPrefix(raw, fence)
		raw = strings.TrimSuffix(raw, "```")
	}
	raw = strings.TrimSpace(raw)
	startIdx := strings.Index(raw, "[")
	endIdx := strings.LastIndex(raw, "]")
	if startIdx >= 0 && endIdx > startIdx {
		var arr []string
		if err := json.Unmarshal([]byte(raw[startIdx:endIdx+1]), &arr); err == nil && len(arr) > 0 {
			return arr
		}
	}
	return []string{fallback}
}

func splitDomainsCSV(s string) []string {
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

func matchesAnyDomain(host string, patterns []string) bool {
	host = strings.ToLower(host)
	for _, p := range patterns {
		if host == p || strings.HasSuffix(host, "."+p) {
			return true
		}
	}
	return false
}

func hostOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// siteScope is one entry of the `site` search filter: a host (suffix-matched
// on dot boundaries, same as include_domains) plus an optional URL path
// prefix. A zero path matches the whole host; a non-empty path scopes results
// to a section of the site (e.g. host "pilotprotocol.network" + path "/docs").
type siteScope struct {
	host string
	path string // normalized, no trailing slash; "" = any path
}

// parseSiteScopes parses the `site` parameter — a CSV of host or host/path
// (or full-URL) entries — into scopes. Examples of a single entry:
//
//	pilotprotocol.network              → whole host (and subdomains)
//	pilotprotocol.network/docs         → only URLs under /docs
//	https://pilotprotocol.network/docs → same (scheme tolerated)
func parseSiteScopes(csv string) []siteScope {
	if csv == "" {
		return nil
	}
	var out []siteScope
	for _, raw := range strings.Split(csv, ",") {
		t := strings.TrimSpace(raw)
		if t == "" {
			continue
		}
		t = strings.TrimPrefix(t, "https://")
		t = strings.TrimPrefix(t, "http://")
		host, path := t, ""
		if i := strings.IndexByte(t, '/'); i >= 0 {
			host, path = t[:i], t[i:]
		}
		host = strings.ToLower(strings.TrimSpace(host))
		path = strings.TrimRight(strings.TrimSpace(path), "/")
		if host == "" {
			continue
		}
		out = append(out, siteScope{host: host, path: path})
	}
	return out
}

// matchesAnySite reports whether rawURL falls within any of the scopes. Host
// matching reuses include_domains semantics (exact or dot-boundary suffix);
// path matching is a segment-boundary prefix so "/docs" matches "/docs" and
// "/docs/x" but not "/docsearch".
func matchesAnySite(rawURL string, scopes []siteScope) bool {
	if len(scopes) == 0 {
		return true
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	p := u.Path
	for _, sc := range scopes {
		if host != sc.host && !strings.HasSuffix(host, "."+sc.host) {
			continue
		}
		if sc.path == "" || p == sc.path || strings.HasPrefix(p, sc.path+"/") {
			return true
		}
	}
	return false
}

// parseDateBound accepts the same forms as the SQLite-side server: empty
// (zero time), RFC3339 ("2026-01-15T00:00:00Z"), or a bare date
// ("2026-01-15", treated as UTC midnight).
func parseDateBound(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("expected YYYY-MM-DD or RFC3339, got %q", s)
}

// textExcerpt returns a body-prefix excerpt no longer than maxBytes, cut at a
// word boundary if one is available within the trailing 16 bytes.
// Static prefix (not query-aware) — matches the SQLite-side DocMeta.Excerpt
// shape; a query-aware passage extractor is the path on the index
// hot path and lives there, not in the HTTP handler.
func textExcerpt(text string, maxBytes int) string {
	if len(text) <= maxBytes {
		return text
	}
	cut := text[:maxBytes]
	for i := len(cut) - 1; i >= len(cut)-16 && i > 0; i-- {
		if cut[i] == ' ' || cut[i] == '\n' || cut[i] == '\t' {
			cut = cut[:i]
			break
		}
	}
	return strings.TrimSpace(cut) + "…"
}

// runPebbleInfo prints Pebble's built-in metrics for the store at -dir.
// openPebbleOrFriendlyErr wraps store.OpenPebble with a more
// helpful error message when the underlying failure is lock contention (a
// pebble-serve / live crawl is holding the single-writer lock). Callers
// using offline CLI subcommands (pebble-info, verify) hit this when run
// during an active deployment.
func openPebbleOrFriendlyErr(d string) (*store.PebbleStore, error) {
	ps, err := store.OpenPebble(d)
	if err != nil {
		msg := err.Error()
		if strings.Contains(msg, "lock") || strings.Contains(msg, "resource temporarily unavailable") {
			return nil, fmt.Errorf("open pebble at %s: writer lock is held by another process (pebble-serve / crawl in flight); stop the running service first", d)
		}
		return nil, fmt.Errorf("open pebble at %s: %w", d, err)
	}
	return ps, nil
}

// runVerifyViaServer GETs pebble-serve's /verify endpoint and
// renders the JSON body in the same shape runVerifyPebble's local path emits.
// Lets `cosift verify -server URL` work against a running pebble-serve while
// the writer lock is held by the crawl / serve process.
func runVerifyViaServer(ctx context.Context, serverURL string, asJSON bool) error {
	endpoint := strings.TrimRight(serverURL, "/") + "/verify"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if asJSON {
		fmt.Println(string(body))
	} else {
		var d struct {
			OK                 bool  `json:"ok"`
			IndexedDocsCounter int64 `json:"indexed_docs_counter"`
			IndexedDocsScan    int64 `json:"indexed_docs_scan"`
			IndexedDocsDrift   int64 `json:"indexed_docs_drift"`
			SumDocLenCounter   int64 `json:"sum_doc_len_counter"`
			SumDocLenScan      int64 `json:"sum_doc_len_scan"`
			SumDocLenDrift     int64 `json:"sum_doc_len_drift"`
		}
		if err := json.Unmarshal(body, &d); err != nil {
			return fmt.Errorf("decode %s: %w", endpoint, err)
		}
		fmt.Printf("pebble-serve: %s\n\n", serverURL)
		fmt.Printf("  indexed_docs (counter): %d\n", d.IndexedDocsCounter)
		fmt.Printf("  indexed_docs (scan):    %d\n", d.IndexedDocsScan)
		fmt.Printf("  sum_doc_len  (counter): %d\n", d.SumDocLenCounter)
		fmt.Printf("  sum_doc_len  (scan):    %d\n", d.SumDocLenScan)
		if d.OK {
			fmt.Println("\nOK: counters match the 'l' family scan.")
		} else {
			fmt.Printf("\nDRIFT: indexed_docs Δ=%+d, sum_doc_len Δ=%+d\n", d.IndexedDocsDrift, d.SumDocLenDrift)
		}
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("counter drift detected (server returned %d)", resp.StatusCode)
	}
	return nil
}

// pebbleInfoJSON shapes the -json output of `cosift pebble-info`. Extracted
// in so the shape can be unit-tested without spawning a subprocess.
// Mirrors the /stats response — same jq filters compose against either source.
func pebbleInfoJSON(path string, st store.Stats, sumLen, indexedDocs int64, meta index.HNSWMeta, hnswOK bool) map[string]any {
	retrievers := []string{"bm25", "bm25-mlt"}
	if hnswOK {
		retrievers = append(retrievers, "dense", "hybrid")
	}
	out := map[string]any{
		"path":         path,
		"documents":    st.Documents,
		"indexed_docs": indexedDocs,
		"sum_doc_len":  sumLen,
		"hnsw_loaded":  hnswOK,
		"retrievers":   retrievers,
	}
	if indexedDocs > 0 {
		out["avg_doc_len"] = float64(sumLen) / float64(indexedDocs)
	}
	if hnswOK {
		out["vector_nodes"] = meta.NodeCount
		out["vector_dim"] = meta.Dim
	}
	return out
}

// operator visibility into LSM levels, WAL state, on-disk
// size, and compaction queue, surfaced via pebble.Metrics().String().
func runPebbleInfo(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("pebble-info", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (defaults to <cfg.DataDir>/pebble)")
	// Mirrors
	// the shape /stats returns ( retrievers list,'s
	// vector meta) so the same jq filters compose against either source.
	asJSON := fs.Bool("json", false, "emit JSON instead of human-readable text (no pebble.Metrics dump)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	d := *dir
	if d == "" {
		d = filepath.Join(cfg.DataDir, "pebble")
	}
	ps, err := openPebbleOrFriendlyErr(d)
	if err != nil {
		return err
	}
	defer ps.Close()

	st, err := ps.Stats(ctx)
	if err != nil {
		return err
	}
	sumLen, indexedDocs, _ := ps.CorpusStats(ctx)
	meta, hnswOK, _ := index.LoadHNSWMeta(ctx, ps)

	if *asJSON {
		out := pebbleInfoJSON(d, st, sumLen, indexedDocs, meta, hnswOK)
		blob, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(blob))
		return nil
	}

	fmt.Printf("PebbleStore: %s\n\n", d)
	fmt.Printf("  documents:    %d\n", st.Documents)
	// pebble-info already needs
	// to read the store; reading them costs O(1) and gives operators 'is this
	// store healthy' without round-tripping through pebble-serve /stats.
	if indexedDocs > 0 {
		fmt.Printf("  indexed_docs: %d\n", indexedDocs)
		fmt.Printf("  sum_doc_len:  %d\n", sumLen)
		fmt.Printf("  avg_doc_len:  %.2f\n", float64(sumLen)/float64(indexedDocs))
	}
	// when an HNSW meta blob is persisted, surface dim+nodes here
	// too. Same cheap 20-byte read pebble-serve does at startup.
	// Operators running pebble-info to inspect an offline store get the dense
	// shape without opening pebble-serve.
	// bm25/bm25-mlt
	// always. dense/hybrid need the graph (meta present) AND, for non-URL
	// modes, a runtime embedder — offline we can only check the persisted
	// shape, so we report 'available with embedder' rather than asserting.
	if hnswOK {
		fmt.Printf("  vector_nodes: %d\n", meta.NodeCount)
		fmt.Printf("  vector_dim:   %d\n", meta.Dim)
	}
	fmt.Println()
	fmt.Println("--- retrievers ---")
	fmt.Println("  bm25, bm25-mlt: always (BM25 index)")
	if hnswOK {
		fmt.Println("  dense:          available — /find_similar?url= works without embedder;")
		fmt.Println("                  /search /answer /research need an embedder configured at server start")
		fmt.Println("  hybrid:         available (same requirements as dense)")
	} else {
		fmt.Println("  dense, hybrid:  unavailable — no HNSW graph persisted in this store")
		fmt.Println("                  (build embeddings during crawl with cfg.Embeddings.Model set)")
	}
	fmt.Println()
	fmt.Println("--- pebble.Metrics ---")
	fmt.Println(ps.Metrics().String())
	return nil
}

// verify the running counters ('m'+indexed_docs,
// 'm'+sum_doc_len) against an authoritative scan of the 'l' family.
// Drift would mean a crash or bug left the counter inconsistent —
// flagged here so it can be re-derived from the scan.
func runVerifyPebble(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (defaults to <cfg.DataDir>/pebble)")
	// machine-readable output for CI integration.
	asJSON := fs.Bool("json", false, "emit JSON report instead of human text (suitable for jq / CI)")
	// -server URL routes the check through a running pebble-serve's
	// /verify endpoint instead of opening the store directly. Useful when a
	// crawl or pebble-serve is holding the writer lock.
	serverURL := fs.String("server", "", "pebble-serve URL — when set, GETs /verify instead of opening the store")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *serverURL != "" {
		return runVerifyViaServer(ctx, *serverURL, *asJSON)
	}
	d := *dir
	if d == "" {
		d = filepath.Join(cfg.DataDir, "pebble")
	}
	ps, err := openPebbleOrFriendlyErr(d)
	if err != nil {
		return err
	}
	defer ps.Close()

	counterSum, counterCount, err := ps.CorpusStats(ctx)
	if err != nil {
		return fmt.Errorf("read counters: %w", err)
	}
	scanSum, scanCount, err := ps.SumDocLengths(ctx)
	if err != nil {
		return fmt.Errorf("scan 'l' family: %w", err)
	}

	driftCount := counterCount - scanCount
	driftSum := counterSum - scanSum
	ok := driftCount == 0 && driftSum == 0

	if *asJSON {
		out := map[string]any{
			"path":                 d,
			"ok":                   ok,
			"indexed_docs_counter": counterCount,
			"indexed_docs_scan":    scanCount,
			"indexed_docs_drift":   driftCount,
			"sum_doc_len_counter":  counterSum,
			"sum_doc_len_scan":     scanSum,
			"sum_doc_len_drift":    driftSum,
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(out)
		if !ok {
			return fmt.Errorf("counter drift detected")
		}
		return nil
	}

	fmt.Printf("PebbleStore: %s\n\n", d)
	fmt.Printf("  indexed_docs (counter): %d\n", counterCount)
	fmt.Printf("  indexed_docs (scan):    %d\n", scanCount)
	fmt.Printf("  sum_doc_len  (counter): %d\n", counterSum)
	fmt.Printf("  sum_doc_len  (scan):    %d\n", scanSum)
	if ok {
		fmt.Println("\nOK: counters match the 'l' family scan.")
		return nil
	}
	fmt.Printf("\nDRIFT: indexed_docs Δ=%+d, sum_doc_len Δ=%+d\n", driftCount, driftSum)
	return fmt.Errorf("counter drift detected")
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeProblem(w http.ResponseWriter, status int, detail string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error":  http.StatusText(status),
		"status": status,
		"detail": detail,
	})
}
