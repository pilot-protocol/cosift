package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/pilot-protocol/cosift/internal/store"
)

// runBackfillContentSHA indexes every document by content hash and marks the
// index complete. It needs the store lock, so stop the service first.
//
//	cosift backfill-content-sha -dir /data/pebble
func runBackfillContentSHA(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("backfill-content-sha", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (required; same dir as pebble-serve -dir)")
	rescan := fs.Bool("rescan", false, "walk the whole corpus again even if the index is already marked complete")
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
	last := start
	res, err := ps.BackfillContentSHA(ctx, *rescan, func(r store.ContentSHABackfill) {
		if now := time.Now(); now.Sub(last) >= 5*time.Second {
			fmt.Fprintln(os.Stderr, "  "+backfillProgressLine(r, now.Sub(start)))
			last = now
		}
	})
	if err != nil {
		return fmt.Errorf("backfill: %w (scanned %d, rerun to resume)", err, res.Scanned)
	}
	if res.AlreadyComplete {
		fmt.Fprintln(os.Stderr, "backfill-content-sha: index already complete; use -rescan to walk the corpus again")
		return nil
	}
	fmt.Fprintf(os.Stderr, "backfill-content-sha: done — scanned %d documents, indexed %d, in %s; index marked complete\n",
		res.Scanned, res.Indexed, time.Since(start).Round(time.Second))
	return nil
}

func backfillProgressLine(r store.ContentSHABackfill, elapsed time.Duration) string {
	pct, eta := 100.0, "0s"
	if r.MaxID > 0 {
		pct = 100 * float64(r.LastID) / float64(r.MaxID)
	}
	if done := r.LastID - r.ResumedFrom; done > 0 && elapsed > 0 && r.LastID < r.MaxID {
		rate := float64(done) / elapsed.Seconds()
		eta = (time.Duration(float64(r.MaxID-r.LastID)/rate) * time.Second).Round(time.Second).String()
	}
	return fmt.Sprintf("%.1f%% (doc id %d of %d, %d scanned, %d indexed, %s elapsed, ETA %s)",
		pct, r.LastID, r.MaxID, r.Scanned, r.Indexed, elapsed.Round(time.Second), eta)
}
