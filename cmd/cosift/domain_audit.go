package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"sort"
	"strings"

	"github.com/pilot-protocol/cosift/internal/authority"
	"github.com/pilot-protocol/cosift/internal/store"
)

// runDomainAudit opens a PebbleStore offline, iterates the 'h' family
// once, joins each host against the authority Scorer, and emits a
// sorted-by-doc-count JSONL inventory.
//
// Output schema per line:
//
//	{
//	  "host":      "foo.example.com",
//	  "count":     1234,
//	  "etld1":     "example.com",
//	  "tld":       "com",
//	  "authority": 0.42,
//	  "recommend": "block" | "down-weight" | "keep" | "trusted"
//	}
//
// The operator pipes the output through jq to extract proposed
// exclude_domains entries (recommend == "block"), or uses it as the
// input to a manual review tool. This is the inventory-producing
// step; actual blocklist application is a separate flow.
//
// Recommendations are deterministic:
//
//	authority >= 0.75               → trusted
//	authority >= 0.40               → keep
//	authority >= 0.20               → down-weight
//	authority <  0.20               → block
//
// The scoring runs in two passes — first to compute subdomain density
// per eTLD+1, then to apply the full Scorer with that data populated.
// At 9M hosts on the GH200 this is ~30-60s of disk I/O, single scan.
func runDomainAudit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("domain-audit", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (required; same dir as pebble-serve -dir)")
	out := fs.String("out", "-", "output path; '-' = stdout")
	trancoCSV := fs.String("tranco", "", "optional Tranco top-1M CSV path (rank,host per line)")
	majesticCSV := fs.String("majestic", "", "optional Majestic Million CSV path")
	minCount := fs.Int("min-count", 1, "skip hosts with fewer than this many docs")
	topN := fs.Int("top", 0, "only emit the top N rows by doc count (0 = all)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return fmt.Errorf("-dir required")
	}

	ps, err := store.OpenPebble(*dir)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer ps.Close()

	scorer := authority.New()
	if *trancoCSV != "" {
		f, err := os.Open(*trancoCSV)
		if err != nil {
			return fmt.Errorf("tranco: %w", err)
		}
		n, err := scorer.LoadTranco(f)
		_ = f.Close()
		if err != nil {
			return fmt.Errorf("tranco parse: %w", err)
		}
		fmt.Fprintf(os.Stderr, "domain-audit: loaded %d Tranco entries\n", n)
	}
	if *majesticCSV != "" {
		f, err := os.Open(*majesticCSV)
		if err != nil {
			return fmt.Errorf("majestic: %w", err)
		}
		n, err := scorer.LoadMajestic(f)
		_ = f.Close()
		if err != nil {
			return fmt.Errorf("majestic parse: %w", err)
		}
		fmt.Fprintf(os.Stderr, "domain-audit: loaded %d Majestic entries\n", n)
	}

	// Pass 1: bulk-collect (host, count) pairs + eTLD+1 subdomain density.
	type row struct {
		Host, Etld1, TLD string
		Count            int
	}
	var rows []row
	subdomain := map[string]int{}
	totalHosts := 0
	fmt.Fprintln(os.Stderr, "domain-audit: pass 1 scanning 'h' family...")
	err = ps.IterateDomains(ctx, func(host string, count int) bool {
		if count < *minCount {
			return true
		}
		totalHosts++
		e := etld1OfHost(host)
		t := tldOfHost(host)
		rows = append(rows, row{Host: host, Etld1: e, TLD: t, Count: count})
		subdomain[e]++
		return true
	})
	if err != nil {
		return fmt.Errorf("iterate: %w", err)
	}
	fmt.Fprintf(os.Stderr, "domain-audit: pass 1 done — %d distinct hosts\n", totalHosts)
	scorer.SetSubdomainCounts(subdomain)

	sort.Slice(rows, func(i, j int) bool { return rows[i].Count > rows[j].Count })
	if *topN > 0 && len(rows) > *topN {
		rows = rows[:*topN]
	}

	// Pass 2: score + emit JSONL.
	var w io.Writer = os.Stdout
	if *out != "-" {
		f, err := os.Create(*out)
		if err != nil {
			return fmt.Errorf("create output: %w", err)
		}
		defer f.Close()
		w = f
	}
	enc := json.NewEncoder(w)
	type outRow struct {
		Host       string  `json:"host"`
		Count      int     `json:"count"`
		Etld1      string  `json:"etld1"`
		TLD        string  `json:"tld"`
		Authority  float64 `json:"authority"`
		Recommend  string  `json:"recommend"`
		SubdomainN int     `json:"subdomain_n"`
	}
	blockCount, downCount, keepCount, trustedCount := 0, 0, 0, 0
	for _, r := range rows {
		score := scorer.Score(r.Host)
		recommend := classify(score)
		switch recommend {
		case "block":
			blockCount++
		case "down-weight":
			downCount++
		case "keep":
			keepCount++
		case "trusted":
			trustedCount++
		}
		if err := enc.Encode(outRow{
			Host: r.Host, Count: r.Count, Etld1: r.Etld1, TLD: r.TLD,
			Authority: score, Recommend: recommend,
			SubdomainN: subdomain[r.Etld1],
		}); err != nil {
			return err
		}
	}
	fmt.Fprintf(os.Stderr, "domain-audit: emitted %d rows  (block=%d down-weight=%d keep=%d trusted=%d)\n",
		len(rows), blockCount, downCount, keepCount, trustedCount)
	return nil
}

// classify maps an authority score to the recommended action.
// Thresholds chosen against the live GH200 distribution: the
// suspect-TLD penalty (-0.3) and subdomain-density penalty (-0.3)
// each pull score from 0.5 to 0.2; either signal alone leaves a host
// at "down-weight," both push to "block." A .cfd subdomain of a
// 200k-host farm scores 0.0 → block. A plain .cfd domain with no
// farm signal scores 0.2 → block (cheap TLDs are not the long tail
// we want to preserve). An unknown but legitimate site scores 0.5 →
// keep.
func classify(score float64) string {
	switch {
	case score >= 0.75:
		return "trusted"
	case score >= 0.40:
		return "keep"
	case score >= 0.25:
		return "down-weight"
	default:
		return "block"
	}
}

// etld1OfHost delegates to the publicsuffix-backed helper in the
// authority package. Inlined separately, the audit binary would
// produce different bucket keys than the live Scorer (the bug we
// shipped publicsuffix to close).
func etld1OfHost(host string) string { return authority.ETLD1(host) }

func tldOfHost(host string) string {
	i := strings.LastIndex(host, ".")
	if i < 0 || i == len(host)-1 {
		return ""
	}
	return strings.ToLower(host[i+1:])
}

// hostFromAuditURL extracts a host from a URL string. Reserved for
// follow-up integrations that need to map sample URLs back to hosts;
// kept here so the audit binary stays self-contained.
var _ = func(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return ""
	}
	return strings.ToLower(u.Host)
}
