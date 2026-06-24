package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/pilot-protocol/cosift/internal/store"
)

// runBackfillHostPostings rebuilds the 'P' host-partition family from existing
// 'h' + 'p' data so site= queries can scan a single host's postings
// (O(site_docs)) via SearchInHost instead of the global lists (O(corpus)).
//
// It re-uses the data already on disk — no re-tokenize, no embeddings — so it's
// a one-time key reshuffle, typically ~1-2h on a multi-million-doc corpus. The
// store opens read-write because it writes the new family, but it never touches
// 'p'/'d'/'v', so it is safe to run alongside a live pebble-serve. After it
// completes, set COSIFT_HOST_PARTITION_READ=1 on the server to route site=
// queries to the partition (and COSIFT_HOST_PARTITION=1 to keep it fresh on
// future crawls).
//
//	cosift backfill-host-postings -dir /home/ubuntu/cosift-data/pebble
func runBackfillHostPostings(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("backfill-host-postings", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (required; same dir as pebble-serve -dir)")
	host := fs.String("host", "", "backfill only this host (fast 'g'-index path); empty = full corpus single-pass")
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

	start := time.Now()
	if *host != "" {
		fmt.Fprintf(os.Stderr, "backfill-host-postings: targeted backfill for host %q (fast 'g'-index path)…\n", *host)
	} else {
		fmt.Fprintf(os.Stderr, "backfill-host-postings: full-corpus single-pass 'p'→'P'…\n")
	}
	last := start
	written, err := ps.BackfillHostPostings(ctx, *host, func(n int64) {
		now := time.Now()
		if now.Sub(last) >= 5*time.Second {
			rate := float64(n) / time.Since(start).Seconds()
			fmt.Fprintf(os.Stderr, "  %d host postings written (%.0f/s, %s elapsed)\n",
				n, rate, time.Since(start).Round(time.Second))
			last = now
		}
	})
	if err != nil {
		return fmt.Errorf("backfill: %w (wrote %d before failing)", err, written)
	}
	fmt.Fprintf(os.Stderr, "backfill-host-postings: done — %d host postings in %s\n",
		written, time.Since(start).Round(time.Second))
	fmt.Fprintf(os.Stderr, "Next: set COSIFT_HOST_PARTITION_READ=1 (read) + COSIFT_HOST_PARTITION=1 (keep fresh) and restart pebble-serve.\n")
	return nil
}
