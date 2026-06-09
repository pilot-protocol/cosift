package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/pilot-protocol/cosift/internal/store"
)

// runPebbleCompact opens a Pebble store and forces compaction over a
// key range to collapse tombstones. Used after a /admin/frontier-clear
// against a frontier with millions of entries — the resulting tombstones
// would otherwise make RecoverInFlight's startup scan take 10+ minutes
// every restart (observed on GH200 after the 8M-URL spam-discovery
// crawl was wiped).
//
// Requires cosift-serve to be stopped (Pebble's exclusive write lock).
// Operator workflow on the GH200 case:
//
//	sudo systemctl stop cosift-serve
//	cosift pebble-compact -dir /home/ubuntu/cosift-data/pebble -range f
//	sudo systemctl start cosift-serve
//
// Pebble does the work in parallel; an 8M-tombstone collapse on the
// GH200 NVMe runs in ~30 seconds.
func runPebbleCompact(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("pebble-compact", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (required)")
	rng := fs.String("range", "all", "key range to compact: all | f (frontier) | d (docs) | l (doc_len) | v (vectors)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return fmt.Errorf("-dir required")
	}

	var lo, hi []byte
	switch *rng {
	case "all":
		lo = []byte{0x00}
		hi = []byte{0xff}
	case "f":
		lo = []byte{'f'}
		hi = []byte{'f' + 1}
	case "d":
		lo = []byte{'d'}
		hi = []byte{'d' + 1}
	case "l":
		lo = []byte{'l'}
		hi = []byte{'l' + 1}
	case "v":
		lo = []byte{'v'}
		hi = []byte{'v' + 1}
	default:
		return fmt.Errorf("unknown range %q (try: all | f | d | l | v)", *rng)
	}

	ps, err := store.OpenPebble(*dir)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer ps.Close()

	db := ps.DB()
	if db == nil {
		return fmt.Errorf("store has no Pebble handle")
	}
	t0 := time.Now()
	fmt.Fprintf(fs.Output(), "compacting range %q [%x, %x)...\n", *rng, lo, hi)
	if err := db.Compact(lo, hi, true); err != nil {
		return fmt.Errorf("compact: %w", err)
	}
	fmt.Fprintf(fs.Output(), "compact done in %s\n", time.Since(t0))
	return nil
}

// Reference to *pebble import so the compile-time symbol stays in
// case future range alternatives need the pebble.Options type.
var _ pebble.Options
