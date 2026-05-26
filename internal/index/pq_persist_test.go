package index

import (
	"context"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/calinteodor/cosift/internal/store"
)

func TestPQCodebookEncodeDecodeRoundTrip(t *testing.T) {
	dim := 32
	M := 4
	K := 16
	rng := rand.New(rand.NewSource(7))
	train := make([][]float32, 500)
	for i := range train {
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		train[i] = v
	}
	cb, err := TrainPQCodebook(train, dim, M, K, 25, rng)
	if err != nil {
		t.Fatalf("train: %v", err)
	}
	blob := EncodePQCodebook(cb)
	cb2, err := DecodePQCodebook(blob)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if cb2.Dim != cb.Dim || cb2.M != cb.M || cb2.K != cb.K || cb2.SubDim != cb.SubDim {
		t.Errorf("header mismatch: %+v vs %+v", cb, cb2)
	}
	for sub := 0; sub < M; sub++ {
		for c := 0; c < K; c++ {
			for d := 0; d < cb.SubDim; d++ {
				if cb.Centroids[sub][c][d] != cb2.Centroids[sub][c][d] {
					t.Fatalf("centroid mismatch at [%d][%d][%d]: %v vs %v", sub, c, d, cb.Centroids[sub][c][d], cb2.Centroids[sub][c][d])
				}
			}
		}
	}
}

func TestPQCodebookDecodeRejectsBadInput(t *testing.T) {
	cases := []struct {
		name string
		buf  []byte
	}{
		{"empty", nil},
		{"short", []byte{1, 2, 3}},
		{"bad magic", append([]byte("NOPE"), make([]byte, 16)...)},
		{"truncated body", func() []byte {
			b := EncodePQCodebook(&PQCodebook{Dim: 4, M: 2, K: 2, SubDim: 2, Centroids: [][][]float32{
				{{1, 2}, {3, 4}},
				{{5, 6}, {7, 8}},
			}})
			return b[:len(b)-4]
		}()},
	}
	for _, c := range cases {
		if _, err := DecodePQCodebook(c.buf); err == nil {
			t.Errorf("%s: expected error, got nil", c.name)
		}
	}
}

func TestPQCodeRoundTrip(t *testing.T) {
	code := []uint16{0, 1, 255, 256, 1023, 32768, 65535}
	blob := EncodePQCode(code)
	got, err := DecodePQCode(blob)
	if err != nil {
		t.Fatalf("decode code: %v", err)
	}
	if len(got) != len(code) {
		t.Fatalf("len: want %d, got %d", len(code), len(got))
	}
	for i := range code {
		if got[i] != code[i] {
			t.Errorf("code[%d]: want %d, got %d", i, code[i], got[i])
		}
	}
	if _, err := DecodePQCode([]byte{1, 2, 3}); err == nil {
		t.Error("odd-length should error")
	}
}

func TestPQCodebookPersistAndLoad(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	defer ps.Close()
	ctx := context.Background()
	// Absent until persisted.
	if _, ok, err := LoadPQCodebook(ctx, ps); err != nil || ok {
		t.Fatalf("expected (nil, false, nil), got (ok=%v, err=%v)", ok, err)
	}
	rng := rand.New(rand.NewSource(1))
	train := make([][]float32, 200)
	for i := range train {
		v := make([]float32, 16)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		train[i] = v
	}
	cb, err := TrainPQCodebook(train, 16, 4, 8, 10, rng)
	if err != nil {
		t.Fatalf("train: %v", err)
	}
	if err := cb.Persist(ctx, ps); err != nil {
		t.Fatalf("persist: %v", err)
	}
	cb2, ok, err := LoadPQCodebook(ctx, ps)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if cb2.M != cb.M || cb2.K != cb.K || cb2.Dim != cb.Dim {
		t.Errorf("header mismatch after load: %+v vs %+v", cb, cb2)
	}
	// Per-node code round-trip via the batch API.
	codes := []store.PQCodeEntry{}
	for i := uint64(0); i < 5; i++ {
		c, _ := cb.Encode(train[i])
		codes = append(codes, store.PQCodeEntry{ID: i, Blob: EncodePQCode(c)})
	}
	if err := ps.PutPQCodesBatch(ctx, codes); err != nil {
		t.Fatalf("PutPQCodesBatch: %v", err)
	}
	read := map[uint64][]uint16{}
	err = ps.IteratePQCodes(ctx, func(nodeID uint64, blob []byte) bool {
		code, err := DecodePQCode(blob)
		if err != nil {
			t.Errorf("decode code for %d: %v", nodeID, err)
			return false
		}
		read[nodeID] = code
		return true
	})
	if err != nil {
		t.Fatalf("IteratePQCodes: %v", err)
	}
	if len(read) != 5 {
		t.Fatalf("read %d codes, want 5", len(read))
	}
}
