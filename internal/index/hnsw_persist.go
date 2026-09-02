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
//	magic       [4]byte = "HSW1" | "HSW2"
//	dim         int32
//	maxLevel    int32
//	entryPoint  int32
//	nodeCount   int32
//	slot        uint8            (HSW2 only; HSW1 implies slot 0x01)
//
// HSW1 is still emitted while the graph lives in slot 0x01 so a store that
// has never swapped stays readable by older binaries.

package index

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"sort"
	"time"

	"github.com/pilot-protocol/cosift/internal/store"
)

const (
	hnswMetaMagic   = "HSW1"
	hnswMetaMagicV2 = "HSW2"
)

// persistWindowBytes bounds encoded blobs held in memory at once: a full
// persist that materializes every blob first costs ~vec-bytes of extra heap
// (~240 GB at 80M nodes) and OOMs before writing anything. Var for tests.
var persistWindowBytes = 1 << 30

// persistFlushed is a test hook observing each flushed window (nil in prod).
var persistFlushed func(nodes, bytes int)

// PersistProgress is reported by full persists after every flushed window.
type PersistProgress struct {
	Written, Total int
	Bytes          int64
}

// Persist writes every node + meta into the graph's current slot. Prefer
// PersistSwap for full rewrites of a graph that already lives on disk.
func (h *HNSW) Persist(ctx context.Context, ps *store.PebbleStore) error {
	return h.PersistFrom(ctx, ps, 0)
}

// PersistFrom writes nodes[fromIdx:] plus every node dirtied since the last
// persist (invalidated, or handed new back-links), then meta. The crawl-time
// checkpoint uses this with fromIdx = last-persisted count. Blocks while
// another persist or compact runs.
func (h *HNSW) PersistFrom(ctx context.Context, ps *store.PebbleStore, fromIdx int) error {
	h.persistMu.Lock()
	defer h.persistMu.Unlock()
	h.mu.RLock()
	slot := h.slot
	h.mu.RUnlock()
	return h.persistNodes(ctx, ps, fromIdx, slot, nil)
}

// TryPersistFrom is PersistFrom that returns (false, nil) instead of waiting
// when a persist or compact is already running.
func (h *HNSW) TryPersistFrom(ctx context.Context, ps *store.PebbleStore, fromIdx int) (bool, error) {
	if !h.persistMu.TryLock() {
		return false, nil
	}
	defer h.persistMu.Unlock()
	h.mu.RLock()
	slot := h.slot
	h.mu.RUnlock()
	return true, h.persistNodes(ctx, ps, fromIdx, slot, nil)
}

// PersistSwap writes the whole graph into the inactive slot, then points meta
// at it. The previous slot stays loadable until the meta write; the caller
// clears it afterwards (store.OtherVectorSlot of the new Slot()).
func (h *HNSW) PersistSwap(ctx context.Context, ps *store.PebbleStore, progress func(PersistProgress)) error {
	h.persistMu.Lock()
	defer h.persistMu.Unlock()
	h.mu.RLock()
	old := h.slot
	h.mu.RUnlock()
	target := store.OtherVectorSlot(old)
	if err := ps.ClearVectorSlot(ctx, target); err != nil {
		return fmt.Errorf("clear target slot: %w", err)
	}
	if err := h.persistNodes(ctx, ps, 0, target, progress); err != nil {
		return err
	}
	h.mu.Lock()
	h.slot = target
	h.mu.Unlock()
	return nil
}

// persistNodes is the shared write loop. Node blobs are encoded per window
// under the read lock and written outside it, so writers stall for one
// window at most. Meta is written last, from the same lock hold that
// observed the final node count, so it never points past written nodes.
// Caller holds persistMu.
func (h *HNSW) persistNodes(ctx context.Context, ps *store.PebbleStore, fromIdx int, slot byte, progress func(PersistProgress)) error {
	h.mu.Lock()
	dirty := h.dirty
	h.dirty = make(map[int32]struct{})
	h.mu.Unlock()
	dirtyIDs := make([]int32, 0, len(dirty))
	for id := range dirty {
		if int(id) < fromIdx {
			dirtyIDs = append(dirtyIDs, id)
		}
	}
	sort.Slice(dirtyIDs, func(a, b int) bool { return dirtyIDs[a] < dirtyIDs[b] })
	restoreDirty := func() {
		h.mu.Lock()
		for id := range dirty {
			h.dirty[id] = struct{}{}
		}
		h.mu.Unlock()
	}

	start := time.Now()
	window := make([]store.VectorNodeEntry, 0, 4096)
	windowBytes, written, flushes := 0, 0, 0
	var bytesWritten int64
	total := 0
	flush := func() error {
		if len(window) == 0 {
			return nil
		}
		if err := ps.PutVectorNodesBatch(ctx, slot, window); err != nil {
			return fmt.Errorf("put vector nodes batch: %w", err)
		}
		written += len(window)
		bytesWritten += int64(windowBytes)
		flushes++
		if persistFlushed != nil {
			persistFlushed(len(window), windowBytes)
		}
		if flushes > 1 || written < total {
			elapsed := max(time.Since(start).Seconds(), 0.001)
			rate := float64(written) / elapsed
			eta := time.Duration(float64(total-written) / rate * float64(time.Second)).Round(time.Second)
			log.Printf("hnsw persist: %d/%d nodes (%.1f GiB, %.0f nodes/s, eta %s)",
				written, total, float64(bytesWritten)/(1<<30), rate, eta)
		}
		if progress != nil {
			progress(PersistProgress{Written: written, Total: total, Bytes: bytesWritten})
		}
		clear(window)
		window = window[:0]
		windowBytes = 0
		return nil
	}

	i, di := fromIdx, 0
	var meta []byte
	for {
		h.mu.RLock()
		n := len(h.nodes)
		total = max(n-fromIdx, 0) + len(dirtyIDs)
		for windowBytes < persistWindowBytes {
			var id int
			switch {
			case di < len(dirtyIDs):
				id = int(dirtyIDs[di])
				di++
			case i < n:
				id = i
				i++
			default:
				id = -1
			}
			if id < 0 {
				break
			}
			if id >= n {
				continue
			}
			blob := encodeHNSWNode(&h.nodes[id])
			window = append(window, store.VectorNodeEntry{ID: uint64(id), Blob: blob})
			windowBytes += len(blob) + 16
		}
		done := di >= len(dirtyIDs) && i >= n
		if done {
			meta = encodeHNSWMeta(h.dim, h.maxLevel, h.entryPoint, n, slot)
		}
		h.mu.RUnlock()
		if err := flush(); err != nil {
			restoreDirty()
			return err
		}
		if done {
			break
		}
	}
	if err := ps.PutVectorMeta(ctx, meta); err != nil {
		restoreDirty()
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
// loadCheckEvery is the node cadence for the progress + ctx-cancel checks in
// LoadHNSWProgress; a var (not const) so tests can lower it below the corpus.
var loadCheckEvery uint64 = 100_000

func LoadHNSW(ctx context.Context, ps *store.PebbleStore) (*HNSW, bool, error) {
	return LoadHNSWProgress(ctx, ps, nil)
}

// LoadHNSWProgress is LoadHNSW with an optional progress callback invoked
// periodically with (loaded, total) node counts as the graph decodes. It also
// checks ctx on the same cadence and aborts early (returning ctx.Err()) so a
// shutdown mid-load doesn't block for the full O(N) decode. progress may be nil.
func LoadHNSWProgress(ctx context.Context, ps *store.PebbleStore, progress func(loaded, total uint64)) (*HNSW, bool, error) {
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
	h.slot = meta.slot
	h.nodes = make([]hnswNode, meta.nodeCount)

	// Corrupt blobs (bit rot, partial writes from prior crashes) leave the
	// slot at its zero value — same shape as a zombie node, which the Search
	// path already filters via len(vec) == 0. Log the first few so they're
	// visible without flooding the journal, plus a final tally.
	total := uint64(meta.nodeCount)
	var loaded, processed uint64
	var skipped int
	const logFirst = 5
	err = ps.IterateVectorNodes(ctx, meta.slot, func(nodeID uint64, blob []byte) bool {
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
		if len(n.vec) > 0 && n.url != "" {
			h.byURL[n.url] = append(h.byURL[n.url], int32(nodeID))
			h.valid++
		}
		loaded++
		processed++
		if processed%loadCheckEvery == 0 {
			if progress != nil {
				progress(loaded, total)
			}
			if ctx.Err() != nil {
				return false // abort promptly on shutdown
			}
		}
		return true
	})
	if err != nil {
		return nil, false, err
	}
	if ctx.Err() != nil {
		return nil, false, ctx.Err()
	}
	if progress != nil {
		progress(loaded, total)
	}
	if skipped > 0 {
		log.Printf("LoadHNSW: %d corrupt node blob(s) skipped (left as zombies)", skipped)
	}
	return h, true, nil
}

// hnswMetaDecoded is the in-memory shape of the meta blob.
type hnswMetaDecoded struct {
	dim, maxLevel, entryPoint, nodeCount int
	slot                                 byte
}

func encodeHNSWMeta(dim, maxLevel, entryPoint, nodeCount int, slot byte) []byte {
	buf := make([]byte, 4+4*4, 4+4*4+1)
	copy(buf[0:4], hnswMetaMagic)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(dim))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(maxLevel))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(entryPoint))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(nodeCount))
	if slot != store.VectorSlotA {
		copy(buf[0:4], hnswMetaMagicV2)
		buf = append(buf, slot)
	}
	return buf
}

func decodeHNSWMeta(buf []byte) (hnswMetaDecoded, error) {
	var m hnswMetaDecoded
	switch {
	case len(buf) == 20 && string(buf[0:4]) == hnswMetaMagic:
		m.slot = store.VectorSlotA
	case len(buf) == 21 && string(buf[0:4]) == hnswMetaMagicV2:
		m.slot = buf[20]
		if m.slot != store.VectorSlotA && m.slot != store.VectorSlotB {
			return hnswMetaDecoded{}, fmt.Errorf("meta slot: got %#x", m.slot)
		}
	case len(buf) != 20 && len(buf) != 21:
		return hnswMetaDecoded{}, fmt.Errorf("meta blob: got %d bytes, want 20 or 21", len(buf))
	default:
		return hnswMetaDecoded{}, fmt.Errorf("meta magic: got %q, want %q or %q", buf[0:4], hnswMetaMagic, hnswMetaMagicV2)
	}
	m.dim = int(int32(binary.LittleEndian.Uint32(buf[4:8])))
	m.maxLevel = int(int32(binary.LittleEndian.Uint32(buf[8:12])))
	m.entryPoint = int(int32(binary.LittleEndian.Uint32(buf[12:16])))
	m.nodeCount = int(int32(binary.LittleEndian.Uint32(buf[16:20])))
	return m, nil
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

	// Per-layer neighbor lists. Always emit exactly level+1 layer headers
	// so the decoder, which reads `level+1` layers, sees a complete blob
	// even when the in-memory node has nil or short neighbors (zero-value
	// slots from LoadHNSW's pre-allocation, for instance). Missing layers
	// are written as nbCount=0; the decoder rebuilds them as empty slices.
	expectedLayers := n.level + 1
	for l := 0; l < expectedLayers; l++ {
		var layer []int
		if l < len(n.neighbors) {
			layer = n.neighbors[l]
		}
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
	// dim==0 is the deliberate zombie sentinel that encodeHNSWNode writes
	// for nodes whose vec was nil'd by MarkURLPassagesInvalid. The empty-vec
	// node still carries url/title/level/neighbors so the graph adjacency
	// stays consistent; Search filters these via len(vec)==0. Any other
	// non-matching dim is real corruption.
	if dim != 0 && int(dim) != expectedDim {
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
			// Legacy 20-byte zombie blob: dim=0 + no neighbor section. Old
			// encoder wrote nothing past the dim field when n.neighbors was
			// nil. The new encoder always emits level+1 headers, so this
			// branch only triggers on pre-fix on-disk blobs. Treat as a
			// zero-state zombie with no graph adjacency — Search filters
			// via len(vec)==0 and an empty neighbor list is fine for
			// addEdges to skip cleanly. The next clean shutdown will
			// rewrite the slot with the new encoder and heal it.
			if dim == 0 && l == 0 {
				neighbors = nil
				break
			}
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
