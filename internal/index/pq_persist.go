// PQ persistence — codebook + per-vector code blobs. Iter 414.
//
// Format (codebook):
//
//	magic   [4]byte = "PQB1"
//	dim     uint32
//	M       uint32
//	K       uint32
//	subDim  uint32
//	centroids  M*K*subDim float32 (LittleEndian)
//
// Format (code blob, one per node):
//
//	[M]uint16  little-endian (2*M bytes total)
//
// Codebook lives at 'q' + 0x00 + "codebook"; codes live at
// 'q' + 0x01 + uint64-be(nodeID). See store/pebble.go iter-414 comment.
package index

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/calinteodor/cosift/internal/store"
)

const pqCodebookMagic = "PQB1"

// EncodePQCodebook serializes a PQCodebook to the iter-414 format.
func EncodePQCodebook(cb *PQCodebook) []byte {
	header := 4 + 4*4
	body := cb.M * cb.K * cb.SubDim * 4
	buf := make([]byte, header+body)
	copy(buf[0:4], pqCodebookMagic)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(cb.Dim))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(cb.M))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(cb.K))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(cb.SubDim))
	off := header
	for sub := 0; sub < cb.M; sub++ {
		for c := 0; c < cb.K; c++ {
			for d := 0; d < cb.SubDim; d++ {
				binary.LittleEndian.PutUint32(buf[off:off+4], math.Float32bits(cb.Centroids[sub][c][d]))
				off += 4
			}
		}
	}
	return buf
}

// DecodePQCodebook restores a PQCodebook from a blob written by
// EncodePQCodebook. Iter 414.
func DecodePQCodebook(buf []byte) (*PQCodebook, error) {
	if len(buf) < 20 || string(buf[0:4]) != pqCodebookMagic {
		return nil, errors.New("pq codebook: bad magic / too short")
	}
	dim := int(binary.LittleEndian.Uint32(buf[4:8]))
	M := int(binary.LittleEndian.Uint32(buf[8:12]))
	K := int(binary.LittleEndian.Uint32(buf[12:16]))
	subDim := int(binary.LittleEndian.Uint32(buf[16:20]))
	if dim == 0 || M == 0 || K == 0 || subDim == 0 {
		return nil, errors.New("pq codebook: zero dims in header")
	}
	if dim != M*subDim {
		return nil, fmt.Errorf("pq codebook: header inconsistent (dim=%d M*subDim=%d)", dim, M*subDim)
	}
	expectedBytes := 20 + M*K*subDim*4
	if len(buf) != expectedBytes {
		return nil, fmt.Errorf("pq codebook: got %d bytes, want %d", len(buf), expectedBytes)
	}
	cb := &PQCodebook{Dim: dim, M: M, K: K, SubDim: subDim,
		Centroids: make([][][]float32, M)}
	off := 20
	for sub := 0; sub < M; sub++ {
		cb.Centroids[sub] = make([][]float32, K)
		for c := 0; c < K; c++ {
			cb.Centroids[sub][c] = make([]float32, subDim)
			for d := 0; d < subDim; d++ {
				cb.Centroids[sub][c][d] = math.Float32frombits(binary.LittleEndian.Uint32(buf[off : off+4]))
				off += 4
			}
		}
	}
	return cb, nil
}

// EncodePQCode packs an []uint16 of length M into 2*M bytes LE.
func EncodePQCode(code []uint16) []byte {
	out := make([]byte, 2*len(code))
	for i, c := range code {
		binary.LittleEndian.PutUint16(out[2*i:2*i+2], c)
	}
	return out
}

// DecodePQCode unpacks the inverse.
func DecodePQCode(buf []byte) ([]uint16, error) {
	if len(buf)%2 != 0 {
		return nil, fmt.Errorf("pq code: odd length %d", len(buf))
	}
	out := make([]uint16, len(buf)/2)
	for i := range out {
		out[i] = binary.LittleEndian.Uint16(buf[2*i : 2*i+2])
	}
	return out, nil
}

// PersistPQCodebook writes the codebook to Pebble. Iter 414.
func (cb *PQCodebook) Persist(ctx context.Context, ps *store.PebbleStore) error {
	return ps.PutPQCodebook(ctx, EncodePQCodebook(cb))
}

// LoadPQCodebook reads a persisted codebook, or (nil, false, nil) if absent.
func LoadPQCodebook(ctx context.Context, ps *store.PebbleStore) (*PQCodebook, bool, error) {
	blob, ok, err := ps.GetPQCodebook(ctx)
	if err != nil || !ok {
		return nil, ok, err
	}
	cb, err := DecodePQCodebook(blob)
	if err != nil {
		return nil, false, err
	}
	return cb, true, nil
}
