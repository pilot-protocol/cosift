package main

import (
	"context"
	"flag"
	"fmt"
	"os"

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
//	cosift purge-domain -dir /data/pebble -suffix cfd,sbs            # dry run + report
//	cosift purge-domain -dir /data/pebble -suffix cfd,sbs -apply     # delete
//	cosift purge-domain -dir /data/pebble -suffix cfd,sbs -readonly  # dry run alongside a live serve
func runPurgeDomain(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("purge-domain", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (required; same dir as pebble-serve -dir)")
	suffixCSV := fs.String("suffix", "", "CSV of host/TLD suffixes to purge, dot-boundary match (e.g. cfd,sbs)")
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
	if len(suffixes) == 0 {
		return fmt.Errorf("-suffix required (e.g. -suffix cfd,sbs)")
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
	fmt.Fprintf(os.Stderr, "purge-domain: %s — scanning %d docs for suffixes %v\n", mode, before, suffixes)

	var scanned, matched, deleted int64
	tldHist := map[string]int64{}
	hostHist := map[string]int64{}
	var samples []string

	err = ps.IterDocMeta(ctx, func(docID int64, url, title string) error {
		scanned++
		if scanned%500_000 == 0 {
			fmt.Fprintf(os.Stderr, "purge-domain: scanned %d, matched %d, deleted %d\n", scanned, matched, deleted)
		}
		host := hostFromURL(url)
		if !matchesAnyDomain(host, suffixes) {
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
	fmt.Fprintf(os.Stderr, "\npurge-domain: done — scanned=%d matched=%d deleted=%d\n", scanned, matched, deleted)
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
