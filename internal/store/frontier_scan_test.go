package store

import (
	"context"
	"fmt"
	"testing"
)

func TestScanFrontierReadsRowsByStatusWithoutClaiming(t *testing.T) {
	ctx := context.Background()
	p, err := OpenPebble(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for i := 0; i < 7; i++ {
		if err := p.PushFrontier(ctx, fmt.Sprintf("https://h%d.example.org/p", i), 0, 1); err != nil {
			t.Fatal(err)
		}
	}
	for _, i := range []int{1, 4, 5} {
		if err := p.FailFrontier(ctx, fmt.Sprintf("https://h%d.example.org/p", i), fmt.Sprintf("http 40%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.CompleteFrontier(ctx, "https://h2.example.org/p"); err != nil {
		t.Fatal(err)
	}
	var errored []string
	after, rounds := "", 0
	for {
		rows, next, end, err := p.ScanFrontier(ctx, FrontierStatusError, after, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rows {
			errored = append(errored, r.URL+"="+r.LastError)
		}
		after, rounds = next, rounds+1
		if end {
			break
		}
		if rounds > 10 {
			t.Fatal("the scan does not end")
		}
	}
	if fmt.Sprint(errored) != "[https://h1.example.org/p=http 401 https://h4.example.org/p=http 404 https://h5.example.org/p=http 405]" || rounds != 4 {
		t.Fatalf("%v in %d rounds", errored, rounds)
	}
	rows, _, _, _ := p.ScanFrontier(ctx, FrontierStatusQueued, "", 100)
	if len(rows) != 3 {
		t.Fatalf("queued rows: %d", len(rows))
	}
	stats, _ := p.GetFrontierStats(ctx)
	if stats.Queued != 3 || stats.InFlight != 0 || stats.Done != 1 || stats.Errored != 3 {
		t.Fatalf("a scan must not claim: %+v", stats)
	}
	if rows, next, end, _ := p.ScanFrontier(ctx, FrontierStatusError, "zzz", 5); len(rows) != 0 || next != "zzz" || !end {
		t.Fatalf("past the end: %v %q %v", rows, next, end)
	}
}
