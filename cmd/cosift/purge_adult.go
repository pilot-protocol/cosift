package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/pilot-protocol/cosift/internal/adultfilter"
	"github.com/pilot-protocol/cosift/internal/store"
)

// runPurgeAdult sweeps an offline PebbleStore, classifies every document with
// the adultfilter (host + lexical signals over URL + title, optionally body),
// and soft-deletes the adult ones so they vanish from retrieval.
//
// DRY RUN BY DEFAULT. Without -apply it only counts and reports — including a
// histogram of the TLDs and hosts that dominate the matches, so an operator
// can see exactly what the filter would remove (and spot any TLD worth adding
// to the classifier's blocklist) before committing. Pass -apply to delete.
//
// Soft delete (store.SoftDeleteDocument) leaves the inverted-index postings as
// orphans — harmless, since retrieval skips any docID whose meta is gone — so
// the sweep is a handful of point-deletes per doc rather than a full index
// rewrite. That is what makes it tractable across a multi-million-doc corpus.
//
//	cosift purge-adult -dir /data/pebble                # dry run + report
//	cosift purge-adult -dir /data/pebble -apply         # delete (URL+title)
//	cosift purge-adult -dir /data/pebble -deep -apply   # also scan body text
func runPurgeAdult(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("purge-adult", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (required; same dir as pebble-serve -dir)")
	apply := fs.Bool("apply", false, "actually soft-delete matches (default: dry run, report only)")
	deep := fs.Bool("deep", false, "fetch full body text per doc for lexical scan (slower, higher recall)")
	limit := fs.Int("limit", 0, "stop after deleting this many docs (0 = no limit)")
	topHosts := fs.Int("top-hosts", 25, "how many top offending hosts/TLDs to print in the report")
	readonly := fs.Bool("readonly", false, "open the store read-only (no lock) — runs alongside a live pebble-serve; forces dry run")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return fmt.Errorf("-dir required")
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
	fmt.Fprintf(os.Stderr, "purge-adult: %s — scanning %d docs (deep=%v)\n", mode, before, *deep)

	var scanned, matched, deleted int64
	tldHist := map[string]int64{}
	hostHist := map[string]int64{}
	var samples []string

	err = ps.IterDocMeta(ctx, func(docID int64, url, title string) error {
		scanned++
		if scanned%500_000 == 0 {
			fmt.Fprintf(os.Stderr, "purge-adult: scanned %d, matched %d, deleted %d\n", scanned, matched, deleted)
		}

		body := ""
		if *deep {
			if d, e := ps.GetDocByID(ctx, docID); e == nil && d != nil {
				body = d.Text
			}
		}
		adult, score, reason := adultfilter.Classify(title, body, url)
		if !adult {
			return nil
		}
		matched++
		host := hostFromURL(url)
		hostHist[host]++
		tldHist["."+tldOfHost(host)]++
		if len(samples) < 20 {
			samples = append(samples, fmt.Sprintf("[score=%d %s] %s", score, reason, url))
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
	fmt.Fprintf(os.Stderr, "\npurge-adult: done — scanned=%d matched=%d deleted=%d\n", scanned, matched, deleted)
	fmt.Fprintf(os.Stderr, "purge-adult: corpus indexed_docs %d → %d\n", before, after)

	printHist(os.Stderr, "top offending TLDs", tldHist, *topHosts)
	printHist(os.Stderr, "top offending hosts", hostHist, *topHosts)
	if len(samples) > 0 {
		fmt.Fprintln(os.Stderr, "\nsample matches:")
		for _, s := range samples {
			fmt.Fprintln(os.Stderr, "  "+s)
		}
	}
	if !*apply && matched > 0 {
		fmt.Fprintf(os.Stderr, "\npurge-adult: DRY RUN — re-run with -apply to soft-delete the %d matched docs.\n", matched)
	}
	return nil
}

// errStopSweep is the sentinel returned from the IterDocMeta callback to stop
// early once -limit deletes have been made (not a real error).
var errStopSweep = fmt.Errorf("purge-adult: stop sweep")

func printHist(w *os.File, label string, h map[string]int64, top int) {
	if len(h) == 0 {
		return
	}
	type kv struct {
		k string
		v int64
	}
	rows := make([]kv, 0, len(h))
	for k, v := range h {
		rows = append(rows, kv{k, v})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].v > rows[j].v })
	if top > 0 && len(rows) > top {
		rows = rows[:top]
	}
	fmt.Fprintf(w, "\n%s:\n", label)
	for _, r := range rows {
		fmt.Fprintf(w, "  %8d  %s\n", r.v, r.k)
	}
}
