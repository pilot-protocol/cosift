package main

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"time"

	"github.com/calinteodor/cosift/internal/config"
	"github.com/calinteodor/cosift/internal/index"
)

// runHNSWRebuild reconstructs the HNSW graph in a pebble dir from valid
// (vec != nil) nodes only. Removes zombies, recovers full M-neighbor
// connectivity, persists the fresh graph back into the same dir under a
// cleared 'v' family. Invalidates 'q' (PQ) — operators must re-train PQ
// after rebuild via /admin/pq-train.
//
// Use this against a Pebble dir whose serve has been stopped (Pebble locks
// the dir exclusively). Pair with /admin/checkpoint to take a consistent
// snapshot first, run the rebuild against that copy, then swap. Iter 428.
func runHNSWRebuild(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("hnsw-rebuild", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (defaults to <cfg.DataDir>/pebble)")
	dryRun := fs.Bool("dry-run", false, "load and report counts without writing changes back")
	force := fs.Bool("force", false, "rebuild even when zombie count is 0 — useful for restoring graph topology that has degraded as the corpus grew via AddPassage (iter 439)")
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

	fmt.Printf("hnsw-rebuild: opening %s\n", d)
	t0 := time.Now()
	g, ok, err := index.LoadHNSW(ctx, ps)
	if err != nil {
		return fmt.Errorf("load HNSW: %w", err)
	}
	if !ok {
		return fmt.Errorf("no HNSW graph found in %s", d)
	}
	loadDur := time.Since(t0)

	// SampleVectors(n=Len, seed=any) returns the full set of valid (vec != nil)
	// nodes — internally filters zombies via the same defensive guard search
	// uses. Cheapest way to count without exposing internals.
	allVecs := g.SampleVectors(g.Len(), 1)
	valid := len(allVecs)
	zombies := g.Len() - valid
	fmt.Printf("hnsw-rebuild: loaded in %s. nodes=%d valid=%d zombies=%d (%.1f%%)\n",
		loadDur.Round(time.Millisecond), g.Len(), valid, zombies,
		100*float64(zombies)/float64(g.Len()))

	if zombies == 0 && !*force {
		fmt.Println("hnsw-rebuild: no zombies — nothing to do (pass -force to rebuild anyway for topology refresh)")
		return nil
	}

	rebuildT0 := time.Now()
	fmt.Printf("hnsw-rebuild: rebuilding graph from %d valid nodes...\n", valid)
	fresh := g.Rebuild()
	rebuildDur := time.Since(rebuildT0)
	fmt.Printf("hnsw-rebuild: built fresh graph in %s (Len=%d)\n",
		rebuildDur.Round(time.Second), fresh.Len())

	if *dryRun {
		fmt.Println("hnsw-rebuild: dry-run — skipping persist")
		return nil
	}

	clearT0 := time.Now()
	if err := ps.ClearVectorFamily(ctx); err != nil {
		return fmt.Errorf("clear vector family: %w", err)
	}
	if err := ps.ClearPQFamily(ctx); err != nil {
		return fmt.Errorf("clear PQ family: %w", err)
	}
	fmt.Printf("hnsw-rebuild: cleared old v + q families in %s\n", time.Since(clearT0).Round(time.Millisecond))

	persistT0 := time.Now()
	if err := fresh.Persist(ctx, ps); err != nil {
		return fmt.Errorf("persist: %w", err)
	}
	fmt.Printf("hnsw-rebuild: persisted in %s\n", time.Since(persistT0).Round(time.Second))

	fmt.Printf("hnsw-rebuild: DONE. nodes %d → %d (PQ codes cleared; run /admin/pq-train to restore PQ)\n",
		g.Len(), fresh.Len())
	return nil
}
