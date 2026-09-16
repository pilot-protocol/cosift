package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/pilot-protocol/cosift/internal/store"
)

// nonEnglishTLDs is the fixed country-TLD list used to size non-English content.
var nonEnglishTLDs = []string{
	"jp", "cn", "kr", "ru", "pl", "hu", "nl", "nu", "de", "fr", "es", "it", "br", "tw", "vn",
	"tr", "id", "th", "ua", "cz", "se", "no", "dk", "fi", "gr", "ro", "bg", "rs", "hr", "sk",
	"si", "ir", "sa", "ae", "eg", "il", "in", "pk", "bd", "ar", "mx", "cl", "co", "pe",
}

var trackerParams = map[string]bool{
	"ref": true, "ref_src": true, "fbclid": true, "gclid": true,
	"mc_cid": true, "mc_eid": true, "_ga": true,
}

type censusBucket struct {
	Key    string `json:"key"`
	Docs   int64  `json:"docs"`
	Tokens int64  `json:"tokens"`
}

type censusNonEnglish struct {
	Docs     int64          `json:"docs"`
	Tokens   int64          `json:"tokens"`
	DocShare float64        `json:"doc_share"`
	TLDs     []censusBucket `json:"tlds"`
}

type censusLang struct {
	English    int64          `json:"english"`
	NonEnglish int64          `json:"non_english"`
	Unknown    int64          `json:"unknown"`
	Distinct   int            `json:"distinct"`
	Top        []censusBucket `json:"top"`
}

type censusDupGroup struct {
	Count int      `json:"count"`
	URLs  []string `json:"urls"`
}

type censusDups struct {
	Groups       int64            `json:"groups"`
	DocsInGroups int64            `json:"docs_in_groups"`
	Removable    int64            `json:"removable"`
	Unparseable  int64            `json:"unparseable"`
	TopHosts     []censusBucket   `json:"top_hosts"`
	Samples      []censusDupGroup `json:"samples"`
}

type censusReport struct {
	Dir          string           `json:"dir"`
	Docs         int64            `json:"docs"`
	Tokens       int64            `json:"tokens"`
	TextBytes    int64            `json:"text_bytes,omitempty"`
	ElapsedSec   float64          `json:"elapsed_sec"`
	DocsPerSec   float64          `json:"docs_per_sec"`
	DistinctTLDs int              `json:"distinct_tlds"`
	TopTLDs      []censusBucket   `json:"top_tlds"`
	NonEnglish   censusNonEnglish `json:"non_english"`
	Lang         *censusLang      `json:"lang,omitempty"`
	Dups         *censusDups      `json:"dups,omitempty"`
}

// runCensus is a read-only sweep of the 'i' family (URL + title) that buckets
// the corpus by TLD, optionally by stored Lang (full 'd' decode per doc) and
// by normalized-URL duplicates (64-bit hash → count map, ~450 MB at 17M docs).
//
//	cosift census -dir /data/pebble -out census.json
//	cosift census -dir /data/pebble -dups -lang -out census.json
func runCensus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("census", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (required; same dir as pebble-serve -dir)")
	readonly := fs.Bool("readonly", true, "open the store read-only (no lock) — runs alongside a live pebble-serve")
	limit := fs.Int64("limit", 0, "stop after scanning this many docs (0 = all)")
	top := fs.Int("top", 50, "how many top TLDs / languages / hosts to report")
	out := fs.String("out", "", "write the JSON report to this file")
	dups := fs.Bool("dups", false, "enable the duplicate-URL census (second pass over the 'i' family)")
	lang := fs.Bool("lang", false, "enable the language census (one full document read per doc)")
	progress := fs.Int64("progress", 1_000_000, "log progress every N docs")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return fmt.Errorf("-dir required")
	}

	var ps *store.PebbleStore
	var err error
	if *readonly {
		ps, err = store.OpenPebbleReadOnly(*dir)
	} else {
		ps, err = store.OpenPebble(*dir)
	}
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer ps.Close()

	_, expected, _ := ps.CorpusStats(ctx)
	fmt.Fprintf(os.Stderr, "census: scanning %d docs (lang=%v dups=%v limit=%d)\n", expected, *lang, *dups, *limit)
	start := time.Now()

	rep := &censusReport{Dir: *dir}
	tldDocs := map[string]int64{}
	tldTokens := map[string]int64{}
	langDocs := map[string]int64{}
	var dupCounts map[uint64]uint32
	var unparseable int64
	if *dups {
		hint := expected
		if *limit > 0 && *limit < hint {
			hint = *limit
		}
		dupCounts = make(map[uint64]uint32, hint)
	}

	logProgress := func(phase string) {
		el := time.Since(start).Seconds()
		fmt.Fprintf(os.Stderr, "census: %s %d docs, %.0fs elapsed, %.0f docs/s\n", phase, rep.Docs, el, float64(rep.Docs)/el)
	}

	err = ps.IterDocMeta(ctx, func(docID int64, rawURL, _ string) error {
		rep.Docs++
		if *progress > 0 && rep.Docs%*progress == 0 {
			logProgress("scanned")
		}
		host := hostFromURL(rawURL)
		tld := tldOfHost(host)
		if tld == "" {
			tld = "(none)"
		}
		n, _, _ := ps.GetDocLen(ctx, docID)
		rep.Tokens += n
		tldDocs[tld]++
		tldTokens[tld] += n
		if *lang {
			d, derr := ps.GetDocByID(ctx, docID)
			if derr == nil {
				langDocs[normalizeLang(d.Lang)]++
				rep.TextBytes += int64(len(d.Text))
			} else {
				langDocs[""]++
			}
		}
		if *dups {
			if h, _, ok := urlDupKey(rawURL); ok {
				dupCounts[h]++
			} else {
				unparseable++
			}
		}
		if *limit > 0 && rep.Docs >= *limit {
			return errStopSweep
		}
		return nil
	})
	if err != nil && err != errStopSweep {
		return fmt.Errorf("sweep: %w", err)
	}

	rep.DistinctTLDs = len(tldDocs)
	rep.TopTLDs = topBuckets(tldDocs, tldTokens, *top)
	rep.NonEnglish = nonEnglishFootprint(tldDocs, tldTokens, rep.Docs)
	if *lang {
		rep.Lang = langCensus(langDocs, *top)
	}
	if *dups {
		rep.Dups, err = censusDupPass(ctx, ps, dupCounts, *limit, *top)
		if err != nil {
			return fmt.Errorf("dup pass: %w", err)
		}
		rep.Dups.Unparseable = unparseable
	}

	rep.ElapsedSec = time.Since(start).Seconds()
	if rep.ElapsedSec > 0 {
		rep.DocsPerSec = float64(rep.Docs) / rep.ElapsedSec
	}
	printCensus(os.Stdout, rep)
	if *out != "" {
		data, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(*out, data, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", *out, err)
		}
		fmt.Fprintf(os.Stderr, "census: wrote %s\n", *out)
	}
	return nil
}

// censusDupPass re-scans the 'i' family and attributes every member after the
// first of each duplicate group to its host; collects 20 sample groups.
func censusDupPass(ctx context.Context, ps *store.PebbleStore, counts map[uint64]uint32, limit int64, top int) (*censusDups, error) {
	d := &censusDups{}
	for _, c := range counts {
		if c > 1 {
			d.Groups++
			d.DocsInGroups += int64(c)
		}
	}
	d.Removable = d.DocsInGroups - d.Groups
	if d.Groups == 0 {
		return d, nil
	}
	seen := make(map[uint64]uint32, d.Groups)
	hostRemovable := map[string]int64{}
	samples := map[uint64]*censusDupGroup{}
	var order []uint64
	var scanned int64
	err := ps.IterDocMeta(ctx, func(_ int64, rawURL, _ string) error {
		scanned++
		h, host, ok := urlDupKey(rawURL)
		if ok && counts[h] > 1 {
			seen[h]++
			if seen[h] > 1 {
				hostRemovable[host]++
			}
			g := samples[h]
			if g == nil && len(order) < 20 {
				g = &censusDupGroup{Count: int(counts[h])}
				samples[h] = g
				order = append(order, h)
			}
			if g != nil && len(g.URLs) < 3 {
				g.URLs = append(g.URLs, rawURL)
			}
		}
		if limit > 0 && scanned >= limit {
			return errStopSweep
		}
		return nil
	})
	if err != nil && err != errStopSweep {
		return nil, err
	}
	d.TopHosts = topBuckets(hostRemovable, nil, top)
	for _, h := range order {
		d.Samples = append(d.Samples, *samples[h])
	}
	return d, nil
}

// urlDupKey returns the FNV-1a hash of the normalized URL plus the bare host.
func urlDupKey(raw string) (uint64, string, bool) {
	key, host, ok := normalizeURLKey(raw)
	if !ok {
		return 0, "", false
	}
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64(), host, true
}

// normalizeURLKey folds http/https, www., default ports, fragments, trailing
// slashes and tracker query params so near-identical URLs share one key.
func normalizeURLKey(raw string) (key, host string, ok bool) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Hostname() == "" {
		return "", "", false
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme == "http" {
		scheme = "https"
	}
	host = strings.TrimPrefix(strings.ToLower(u.Hostname()), "www.")
	hostPort := host
	if p := u.Port(); p != "" && p != "80" && p != "443" {
		hostPort = host + ":" + p
	}
	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	if len(path) > 1 {
		path = strings.TrimRight(path, "/")
		if path == "" {
			path = "/"
		}
	}
	q := u.Query()
	for k := range q {
		if trackerParams[k] || strings.HasPrefix(k, "utm_") {
			delete(q, k)
		}
	}
	for _, vs := range q {
		sort.Strings(vs)
	}
	key = scheme + "://" + hostPort + path
	if enc := q.Encode(); enc != "" {
		key += "?" + enc
	}
	return key, host, true
}

// normalizeLang reduces "en-US"/"EN_gb" to the primary subtag; "" = unknown.
func normalizeLang(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if i := strings.IndexAny(s, "-_"); i >= 0 {
		s = s[:i]
	}
	return s
}

func nonEnglishFootprint(tldDocs, tldTokens map[string]int64, total int64) censusNonEnglish {
	var ne censusNonEnglish
	for _, t := range nonEnglishTLDs {
		b := censusBucket{Key: t, Docs: tldDocs[t], Tokens: tldTokens[t]}
		ne.TLDs = append(ne.TLDs, b)
		ne.Docs += b.Docs
		ne.Tokens += b.Tokens
	}
	if total > 0 {
		ne.DocShare = float64(ne.Docs) / float64(total)
	}
	return ne
}

func langCensus(langDocs map[string]int64, top int) *censusLang {
	l := &censusLang{Top: topBuckets(langDocs, nil, top)}
	for code, n := range langDocs {
		switch code {
		case "":
			l.Unknown += n
		case "en":
			l.English += n
			l.Distinct++
		default:
			l.NonEnglish += n
			l.Distinct++
		}
	}
	return l
}

func topBuckets(docs, tokens map[string]int64, top int) []censusBucket {
	rows := make([]censusBucket, 0, len(docs))
	for k, v := range docs {
		rows = append(rows, censusBucket{Key: k, Docs: v, Tokens: tokens[k]})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Docs != rows[j].Docs {
			return rows[i].Docs > rows[j].Docs
		}
		return rows[i].Key < rows[j].Key
	})
	if top > 0 && len(rows) > top {
		rows = rows[:top]
	}
	return rows
}

func printBuckets(w *os.File, label string, rows []censusBucket, withTokens bool) {
	if len(rows) == 0 {
		return
	}
	fmt.Fprintf(w, "\n%s:\n", label)
	for _, r := range rows {
		if withTokens {
			fmt.Fprintf(w, "  %10d  %14d  %s\n", r.Docs, r.Tokens, r.Key)
		} else {
			fmt.Fprintf(w, "  %10d  %s\n", r.Docs, r.Key)
		}
	}
}

func printCensus(w *os.File, rep *censusReport) {
	fmt.Fprintf(w, "census: %s\n", rep.Dir)
	fmt.Fprintf(w, "  docs=%d tokens=%d", rep.Docs, rep.Tokens)
	if rep.Lang != nil {
		fmt.Fprintf(w, " text_bytes=%d", rep.TextBytes)
	}
	fmt.Fprintf(w, " elapsed=%.1fs docs/s=%.0f distinct_tlds=%d\n", rep.ElapsedSec, rep.DocsPerSec, rep.DistinctTLDs)
	printBuckets(w, "top TLDs (docs, tokens)", rep.TopTLDs, true)
	fmt.Fprintf(w, "\nnon-English footprint: docs=%d (%.2f%%) tokens=%d\n",
		rep.NonEnglish.Docs, rep.NonEnglish.DocShare*100, rep.NonEnglish.Tokens)
	for _, b := range rep.NonEnglish.TLDs {
		if b.Docs > 0 {
			fmt.Fprintf(w, "  %10d  %14d  .%s\n", b.Docs, b.Tokens, b.Key)
		}
	}
	if l := rep.Lang; l != nil {
		fmt.Fprintf(w, "\nlanguage: english=%d non_english=%d unknown=%d distinct=%d\n",
			l.English, l.NonEnglish, l.Unknown, l.Distinct)
		printBuckets(w, "top languages", l.Top, false)
	}
	if d := rep.Dups; d != nil {
		fmt.Fprintf(w, "\nduplicate URLs: groups=%d docs_in_groups=%d removable=%d unparseable=%d\n",
			d.Groups, d.DocsInGroups, d.Removable, d.Unparseable)
		printBuckets(w, "top hosts by removable docs", d.TopHosts, false)
		if len(d.Samples) > 0 {
			fmt.Fprintln(w, "\nsample duplicate groups:")
			for _, g := range d.Samples {
				fmt.Fprintf(w, "  x%d\n", g.Count)
				for _, u := range g.URLs {
					fmt.Fprintln(w, "    "+u)
				}
			}
		}
	}
}
