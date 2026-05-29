// HNSW persistence to PebbleStore blobs. Fourth piece of the
// path-2 rework. Without persistence, an HNSW index has to be rebuilt from
// raw embeddings on every process restart; for a 1M-passage corpus that's a
// minutes-to-hours cost on a single VM. With persistence the index loads in
// roughly O(N) sequential disk reads.
//
// Format is hand-rolled binary (NOT gob — per-key gob streams add a
// type-info header per node and don't compose well with Pebble's per-key
// values). Each node is one Pebble blob under the 'v' family;
// graph metadata (entry point, max level, dim, node count) is a single
// blob under 'v' + 0x00 + "meta".
//
// Node blob layout (all integers little-endian):
//
//	urlLen   uint16
//	url      [urlLen]byte
//	titleLen uint16
//	title    [titleLen]byte
//	offset   int32
//	length   int32
//	level    int32
//	dim      int32
//	vec      [dim]float32      (4 bytes each)
//	for each layer 0..level:
//	  nbCount uint16
//	  nbs     [nbCount]int32
//
// Meta blob layout:
//
//	magic       [4]byte = "HSW1"
//	dim         int32
//	maxLevel    int32
//	entryPoint  int32
//	nodeCount   int32

package index

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"

	"github.com/pilot-protocol/cosift/internal/store"
)

const hnswMetaMagic = "HSW1"

// Persist serializes every node + meta into the PebbleStore. Safe to call
// during ongoing search (acquires RLock); does NOT acquire the write lock,
// so concurrent Add() during Persist will partially leak into the saved
// snapshot — callers expecting a clean snapshot should quiesce writes first.
func (h *HNSW) Persist(ctx context.Context, ps *store.PebbleStore) error {
	return h.PersistFrom(ctx, ps, 0)
}

// PersistFrom writes meta + nodes[fromIdx:] in a single Pebble batch. The
// crawl-time checkpoint goroutine uses this with fromIdx = last-persisted
// count, so each checkpoint touches only the newly-added nodes. Meta is
// always re-written so a reader can size the slice correctly.
//
// Caveat: existing nodes whose neighbor lists got new back-pointers since
// the last persist are NOT rewritten — those edges are lost until the next
// full-from-zero persist. Acceptable for crawl-time approximations; final
// shutdown persist always does fromIdx=0.
func (h *HNSW) PersistFrom(ctx context.Context, ps *store.PebbleStore, fromIdx int) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if fromIdx >= len(h.nodes) {
		// Even with no node writes, refresh meta so changes to maxLevel /
		// entryPoint land on disk.
		meta := encodeHNSWMeta(h.dim, h.maxLevel, h.entryPoint, len(h.nodes))
		return ps.PutVectorMeta(ctx, meta)
	}
	// The earlier order (meta then
	// nodes) was unsafe — if the node batch failed, meta would point past
	// actual data on disk and LoadHNSW would allocate slots for nodes that
	// never landed, causing 'neighbors[-1]' panics during search.
	// New order: meta ALWAYS lags or equals nodes-on-disk. Worst case after
	// partial write: meta says N nodes, disk has N+M; the M extras are
	// orphan but harmless (LoadHNSW caps at meta.nodeCount).
	entries := make([]store.VectorNodeEntry, 0, len(h.nodes)-fromIdx)
	for i := fromIdx; i < len(h.nodes); i++ {
		entries = append(entries, store.VectorNodeEntry{
			ID:   uint64(i),
			Blob: encodeHNSWNode(&h.nodes[i]),
		})
	}
	if err := ps.PutVectorNodesBatch(ctx, entries); err != nil {
		return fmt.Errorf("put vector nodes batch: %w", err)
	}
	meta := encodeHNSWMeta(h.dim, h.maxLevel, h.entryPoint, len(h.nodes))
	if err := ps.PutVectorMeta(ctx, meta); err != nil {
		return fmt.Errorf("put vector meta: %w", err)
	}
	return nil
}

// LoadHNSW returns an HNSW reconstructed from a PebbleStore. Returns
// (nil, false, nil) when no persisted index exists — callers should
// build a fresh one in that case. Errors are reserved for actual decode
// failures or storage I/O problems.

// HNSWMeta is the small index-shape summary callers want without paying
// the cost of loading the full graph: vector dimension and node count.
type HNSWMeta struct {
	Dim       int
	NodeCount int
}

// LoadHNSWMeta reads just the persisted meta blob (20 bytes) and returns
// (dim, nodeCount). Cheap — does not iterate the 'v' family node entries.
// Returns ok=false when no HNSW meta has been persisted yet.
func LoadHNSWMeta(ctx context.Context, ps *store.PebbleStore) (HNSWMeta, bool, error) {
	blob, ok, err := ps.GetVectorMeta(ctx)
	if err != nil || !ok {
		return HNSWMeta{}, ok, err
	}
	m, err := decodeHNSWMeta(blob)
	if err != nil {
		return HNSWMeta{}, false, err
	}
	return HNSWMeta{Dim: m.dim, NodeCount: m.nodeCount}, true, nil
}

// LoadHNSW reconstructs an HNSW graph from a PebbleStore. Returns
// (nil, false, nil) when no persisted index exists — callers should
// build a fresh one in that case. Errors are reserved for actual decode
// failures or storage I/O problems.
func LoadHNSW(ctx context.Context, ps *store.PebbleStore) (*HNSW, bool, error) {
	metaBlob, ok, err := ps.GetVectorMeta(ctx)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	meta, err := decodeHNSWMeta(metaBlob)
	if err != nil {
		return nil, false, fmt.Errorf("decode meta: %w", err)
	}

	h := NewHNSW(meta.dim)
	h.entryPoint = meta.entryPoint
	h.maxLevel = meta.maxLevel
	h.nodes = make([]hnswNode, meta.nodeCount)

	// Corrupt blobs (bit rot, partial writes from prior crashes) leave the
	// slot at its zero value — same shape as a zombie node, which the Search
	// path already filters via len(vec) == 0. Log the first few so they're
	// visible without flooding the journal, plus a final tally.
	var skipped int
	const logFirst = 5
	err = ps.IterateVectorNodes(ctx, func(nodeID uint64, blob []byte) bool {
		if int(nodeID) >= len(h.nodes) {
			return true // out-of-bounds — skip silently
		}
		n, e := decodeHNSWNode(blob, meta.dim)
		if e != nil {
			if skipped < logFirst {
				log.Printf("LoadHNSW: skipping corrupt node %d: %v", nodeID, e)
			}
			skipped++
			return true
		}
		h.nodes[nodeID] = *n
		return true
	})
	if err != nil {
		return nil, false, err
	}
	if skipped > 0 {
		log.Printf("LoadHNSW: %d corrupt node blob(s) skipped (left as zombies)", skipped)
	}
	return h, true, nil
}

// hnswMetaDecoded is the in-memory shape of the meta blob.
type hnswMetaDecoded struct {
	dim, maxLevel, entryPoint, nodeCount int
}

func encodeHNSWMeta(dim, maxLevel, entryPoint, nodeCount int) []byte {
	buf := make([]byte, 4+4*4)
	copy(buf[0:4], hnswMetaMagic)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(dim))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(maxLevel))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(entryPoint))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(nodeCount))
	return buf
}

func decodeHNSWMeta(buf []byte) (hnswMetaDecoded, error) {
	if len(buf) != 20 {
		return hnswMetaDecoded{}, fmt.Errorf("meta blob: got %d bytes, want 20", len(buf))
	}
	if string(buf[0:4]) != hnswMetaMagic {
		return hnswMetaDecoded{}, fmt.Errorf("meta magic: got %q, want %q", buf[0:4], hnswMetaMagic)
	}
	return hnswMetaDecoded{
		dim:        int(int32(binary.LittleEndian.Uint32(buf[4:8]))),
		maxLevel:   int(int32(binary.LittleEndian.Uint32(buf[8:12]))),
		entryPoint: int(int32(binary.LittleEndian.Uint32(buf[12:16]))),
		nodeCount:  int(int32(binary.LittleEndian.Uint32(buf[16:20]))),
	}, nil
}

func encodeHNSWNode(n *hnswNode) []byte {
	// Pre-size: header (2+url) + (2+title) + 4+4+4+4 + dim*4 + per-layer
	// (2 + nb*4). Slight over-allocation is fine.
	dim := len(n.vec)
	nbBytes := 0
	for _, layer := range n.neighbors {
		nbBytes += 2 + 4*len(layer)
	}
	buf := make([]byte, 0, 2+len(n.url)+2+len(n.title)+4+4+4+4+dim*4+nbBytes)

	tmp := make([]byte, 4)
	put16 := func(v uint16) {
		binary.LittleEndian.PutUint16(tmp[:2], v)
		buf = append(buf, tmp[:2]...)
	}
	put32 := func(v uint32) {
		binary.LittleEndian.PutUint32(tmp[:4], v)
		buf = append(buf, tmp[:4]...)
	}

	put16(uint16(len(n.url)))
	buf = append(buf, n.url...)
	put16(uint16(len(n.title)))
	buf = append(buf, n.title...)
	put32(uint32(int32(n.offset)))
	put32(uint32(int32(n.length)))
	put32(uint32(int32(n.level)))
	put32(uint32(int32(dim)))

	// Vec: dim float32 little-endian.
	vecBytes := make([]byte, dim*4)
	for i, f := range n.vec {
		binary.LittleEndian.PutUint32(vecBytes[i*4:i*4+4], math.Float32bits(f))
	}
	buf = append(buf, vecBytes...)

	// Per-layer neighbor lists.
	for _, layer := range n.neighbors {
		put16(uint16(len(layer)))
		nbBuf := make([]byte, 4*len(layer))
		for i, nb := range layer {
			binary.LittleEndian.PutUint32(nbBuf[i*4:i*4+4], uint32(int32(nb)))
		}
		buf = append(buf, nbBuf...)
	}
	return buf
}

func decodeHNSWNode(buf []byte, expectedDim int) (*hnswNode, error) {
	off := 0
	read16 := func() (uint16, error) {
		if off+2 > len(buf) {
			return 0, fmt.Errorf("node blob: truncated at offset %d (want 2 bytes)", off)
		}
		v := binary.LittleEndian.Uint16(buf[off : off+2])
		off += 2
		return v, nil
	}
	read32 := func() (uint32, error) {
		if off+4 > len(buf) {
			return 0, fmt.Errorf("node blob: truncated at offset %d (want 4 bytes)", off)
		}
		v := binary.LittleEndian.Uint32(buf[off : off+4])
		off += 4
		return v, nil
	}
	readBytes := func(n int) ([]byte, error) {
		if off+n > len(buf) {
			return nil, fmt.Errorf("node blob: truncated at offset %d (want %d bytes)", off, n)
		}
		v := buf[off : off+n]
		off += n
		return v, nil
	}

	urlLen, err := read16()
	if err != nil {
		return nil, err
	}
	urlB, err := readBytes(int(urlLen))
	if err != nil {
		return nil, err
	}
	titleLen, err := read16()
	if err != nil {
		return nil, err
	}
	titleB, err := readBytes(int(titleLen))
	if err != nil {
		return nil, err
	}
	offset, err := read32()
	if err != nil {
		return nil, err
	}
	length, err := read32()
	if err != nil {
		return nil, err
	}
	level, err := read32()
	if err != nil {
		return nil, err
	}
	dim, err := read32()
	if err != nil {
		return nil, err
	}
	if expectedDim > 0 && int(dim) != expectedDim {
		return nil, fmt.Errorf("node blob dim mismatch: got %d, expected %d", dim, expectedDim)
	}
	vec := make([]float32, dim)
	for i := range vec {
		raw, err := read32()
		if err != nil {
			return nil, err
		}
		vec[i] = math.Float32frombits(raw)
	}
	neighbors := make([][]int, int(int32(level))+1)
	for l := 0; l <= int(int32(level)); l++ {
		nbCount, err := read16()
		if err != nil {
			return nil, err
		}
		layer := make([]int, nbCount)
		for i := range layer {
			v, err := read32()
			if err != nil {
				return nil, err
			}
			layer[i] = int(int32(v))
		}
		neighbors[l] = layer
	}

	return &hnswNode{
		vecDoc: vecDoc{
			url:    string(urlB),
			title:  string(titleB),
			offset: int(int32(offset)),
			length: int(int32(length)),
			vec:    vec,
		},
		level:     int(int32(level)),
		neighbors: neighbors,
	}, nil
}
