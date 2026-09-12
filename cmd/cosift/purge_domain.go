package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/pilot-protocol/cosift/internal/store"
)

// runPurgeDomain sweeps an offline PebbleStore and soft-deletes every document
// whose host matches one of the given domain/TLD suffixes (dot-boundary), e.g.
// -suffix cfd,sbs removes every *.cfd and *.sbs page regardless of content.
//
// Companion to the crawler's exclude_domains blacklist: the blacklist stops
// NEW pages from those TLDs being crawled, this clears the backlog already
// indexed. Unlike purge-adult (which only removes pages that ALSO trip the
// adult classifier), this is a pure host-suffix sweep.
//
// DRY RUN BY DEFAULT. -apply soft-deletes (store.SoftDeleteDocument), leaving
// inverted-index postings as harmless orphans (retrieval skips any docID whose
// meta is gone), so it's a few point-deletes per doc rather than a full index
// rewrite — tractable across a multi-million-doc corpus. After purging a large
// fraction, run a compaction to reclaim disk and correct IDF.
//
// Suffixes come from -suffix (CSV), -blocklist (file, one suffix per line,
// # comments), or both; -keep exempts hosts that would otherwise match.
//
//	cosift purge-domain -dir /data/pebble -suffix cfd,sbs            # dry run + report
//	cosift purge-domain -dir /data/pebble -suffix cfd,sbs -apply     # delete
//	cosift purge-domain -dir /data/pebble -suffix cfd,sbs -readonly  # dry run alongside a live serve
//	cosift purge-domain -dir /data/pebble -blocklist spam.txt -keep bbc.co.uk -apply
func runPurgeDomain(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("purge-domain", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (required; same dir as pebble-serve -dir)")
	suffixCSV := fs.String("suffix", "", "CSV of host/TLD suffixes to purge, dot-boundary match (e.g. cfd,sbs)")
	blocklist := fs.String("blocklist", "", "file of host/TLD suffixes to purge, one per line (# comments); merged with -suffix")
	keepCSV := fs.String("keep", "", "CSV of host suffixes to skip even when matched by -suffix/-blocklist (e.g. bbc.co.uk)")
	apply := fs.Bool("apply", false, "actually soft-delete matches (default: dry run, report only)")
	limit := fs.Int("limit", 0, "stop after deleting this many docs (0 = no limit)")
	topHosts := fs.Int("top-hosts", 25, "how many top matched hosts/TLDs to print in the report")
	readonly := fs.Bool("readonly", false, "open the store read-only (no lock) — runs alongside a live pebble-serve; forces dry run")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return fmt.Errorf("-dir required")
	}
	suffixes := splitDomainsCSV(*suffixCSV)
	fromFile := 0
	if *blocklist != "" {
		fileSuffixes, err := loadSuffixFile(*blocklist)
		if err != nil {
			return fmt.Errorf("-blocklist: %w", err)
		}
		fromFile = len(fileSuffixes)
		suffixes = append(suffixes, fileSuffixes...)
	}
	keep := splitDomainsCSV(*keepCSV)
	if len(suffixes) == 0 {
		return fmt.Errorf("-suffix or -blocklist required (e.g. -suffix cfd,sbs)")
	}
	if *readonly && *apply {
		return fmt.Errorf("-readonly cannot be combined with -apply (read-only opens take no write lock)")
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

	_, before, _ := ps.CorpusStats(ctx)
	mode := "DRY RUN (no deletes)"
	if *apply {
		mode = "APPLY (soft-deleting matches)"
	}
	fmt.Fprintf(os.Stderr, "purge-domain: %s — scanning %d docs for %d suffixes (%d from -blocklist), keep %v\n", mode, before, len(suffixes), fromFile, keep)
	match := newSuffixMatcher(suffixes)
	keepMatch := newSuffixMatcher(keep)

	var scanned, matched, kept, deleted int64
	tldHist := map[string]int64{}
	hostHist := map[string]int64{}
	var samples []string

	err = ps.IterDocMeta(ctx, func(docID int64, url, title string) error {
		scanned++
		if scanned%500_000 == 0 {
			fmt.Fprintf(os.Stderr, "purge-domain: scanned %d, matched %d, deleted %d\n", scanned, matched, deleted)
		}
		host := hostFromURL(url)
		if !match.matches(host) {
			return nil
		}
		if keepMatch.matches(host) {
			kept++
			return nil
		}
		matched++
		hostHist[host]++
		tldHist["."+tldOfHost(host)]++
		if len(samples) < 20 {
			samples = append(samples, url)
		}
		if *apply {
			ok, derr := ps.SoftDeleteDocument(ctx, docID, url)
			if derr != nil {
				return fmt.Errorf("delete doc %d: %w", docID, derr)
			}
			if ok {
				deleted++
			}
			if *limit > 0 && deleted >= int64(*limit) {
				return errStopSweep
			}
		}
		return nil
	})
	if err != nil && err != errStopSweep {
		return fmt.Errorf("sweep: %w", err)
	}

	_, after, _ := ps.CorpusStats(ctx)
	fmt.Fprintf(os.Stderr, "\npurge-domain: done — scanned=%d matched=%d kept=%d deleted=%d\n", scanned, matched, kept, deleted)
	fmt.Fprintf(os.Stderr, "purge-domain: corpus indexed_docs %d → %d\n", before, after)
	printHist(os.Stderr, "top matched TLDs", tldHist, *topHosts)
	printHist(os.Stderr, "top matched hosts", hostHist, *topHosts)
	if len(samples) > 0 {
		fmt.Fprintln(os.Stderr, "\nsample matches:")
		for _, s := range samples {
			fmt.Fprintln(os.Stderr, "  "+s)
		}
	}
	if !*apply && matched > 0 {
		fmt.Fprintf(os.Stderr, "\npurge-domain: DRY RUN — re-run with -apply to soft-delete the %d matched docs.\n", matched)
	}
	return nil
}

func loadSuffixFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = line[:i]
		}
		if t := strings.ToLower(strings.TrimSpace(line)); t != "" {
			out = append(out, t)
		}
	}
	return out, sc.Err()
}

const suffixBucketThreshold = 500

// suffixMatcher is matchesAnyDomain with the suffixes bucketed by last label
// once the list is large; a dot-boundary match forces host and suffix to
// share their last label, so scanning one bucket is exact.
type suffixMatcher struct {
	linear  []string
	buckets map[string][]string
}

func newSuffixMatcher(suffixes []string) *suffixMatcher {
	m := &suffixMatcher{linear: suffixes}
	if len(suffixes) <= suffixBucketThreshold {
		return m
	}
	m.buckets = make(map[string][]string)
	for _, s := range suffixes {
		k := suffixBucket(s)
		m.buckets[k] = append(m.buckets[k], s)
	}
	return m
}

func (m *suffixMatcher) matches(host string) bool {
	if m.buckets == nil {
		return matchesAnyDomain(host, m.linear)
	}
	return matchesAnyDomain(host, m.buckets[suffixBucket(host)])
}

func suffixBucket(s string) string {
	if !strings.Contains(s, ".") {
		return strings.ToLower(s)
	}
	return tldOfHost(s)
}
