// Package index — HNSW vector index, pure Go, no external deps.
//
// Hierarchical Navigable Small World graph (Malkov & Yashunin 2018) for
// approximate kNN at logarithmic query cost. Sized for the iter-199 rework:
// the existing VectorIndex (brute-force cosine over the full passage set) is
// adequate to ~200k passages, then per-query cost grows linearly. HNSW is
// O(log N) per query with recall ≥95% at standard parameters, so a single
// process can serve ~10–100M passages on commodity hardware before memory
// becomes the bottleneck.
//
// Conforms to the same Add/Search shape as VectorIndex so the server can
// switch implementations via config without rewiring the retrieval pipeline.
//
// Storage is in-memory only at this layer; persistence (memory-mapped
// segment files) is a follow-up. For now, the index rebuilds on startup
// from the persisted passage embeddings — same constraint as the
// brute-force VectorIndex.
package index

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
)

// HNSW default parameters. Tunable per build; suitable for general-purpose
// dense retrieval at the 1M–100M passage scale.
const (
	// HNSWM is the target neighbor count per node per layer. 16 is the
	// Malkov & Yashunin reference value; tradeoff between recall (more
	// neighbors = better) and memory + build time (more neighbors = costlier).
	HNSWM = 16
	// HNSWMmax0 is the neighbor cap at layer 0. The bottom layer carries
	// every node and typically benefits from a higher cap (2*M is the
	// convention).
	HNSWMmax0 = 32
	// HNSWefConstruction is the candidate-list size during build. Bigger
	// values produce a better graph at higher index-build cost.
	HNSWefConstruction = 200
	// HNSWefSearch is the candidate-list size during query. Bigger values
	// raise recall at proportional query cost. Operators can tune at runtime
	// if needed; the default targets ≥95% recall at ~1ms/query for 1M-passage
	// indexes.
	HNSWefSearch = 50
)

// HNSW is an approximate kNN index conforming to the same Search/Add
// interface as VectorIndex (brute-force) so callers can swap freely.
type HNSW struct {
	mu  sync.RWMutex
	dim int
	rng *rand.Rand

	M              int
	Mmax0          int
	efConstruction int
	efSearch       int
	levelMult      float64 // 1 / ln(M), used for stochastic level assignment

	nodes      []hnswNode
	entryPoint int // index into nodes; -1 when empty
	maxLevel   int // current top layer

	// Iter 416: optional PQ acceleration. When codebook != nil, Search uses
	// asymmetric distance against per-node codes instead of the d-dim dot
	// product. Set via UsePQ() at startup. AddPassage continues to write
	// raw vectors; new nodes need a subsequent pq-train to get codes.
	codebook *PQCodebook
	codes    [][]uint16 // parallel to nodes; nil entries fall back to raw vec
}

type hnswNode struct {
	vecDoc           // embed: url, title, offset, length, vec (unit-normalized)
	level     int    // top layer this node participates in
	neighbors [][]int // per-layer adjacency lists (layer 0 .. level)
}

// NewHNSW constructs an empty HNSW index for vectors of the given dim.
// Uses default parameters; tunable via the public fields after construction.
func NewHNSW(dim int) *HNSW {
	return &HNSW{
		dim:            dim,
		rng:            rand.New(rand.NewSource(1)), // deterministic by default
		M:              HNSWM,
		Mmax0:          HNSWMmax0,
		efConstruction: HNSWefConstruction,
		efSearch:       HNSWefSearch,
		levelMult:      1.0 / math.Log(float64(HNSWM)),
		entryPoint:     -1,
	}
}

// Len reports the number of inserted vectors.
func (h *HNSW) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.nodes)
}

// UsePQ enables iter-416 asymmetric-distance search. codes is parallel to
// h.nodes; a nil/empty entry at index i means that node is searched via
// its raw vec (graceful coexistence during gradual rollouts). Call once
// at startup after LoadHNSW. Iter 416.
func (h *HNSW) UsePQ(cb *PQCodebook, codes [][]uint16) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.codebook = cb
	h.codes = codes
}

// HasPQ reports whether a codebook is wired. Iter 416.
func (h *HNSW) HasPQ() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.codebook != nil
}

// PQStatus returns an observability snapshot: codebook shape + number of
// nodes that currently have a code (== len(code) == M). Iter 423.
type PQStatus struct {
	Enabled       bool
	Dim, M, K     int
	NodesWithCode int
	NodesTotal    int
}

func (h *HNSW) PQStatus() PQStatus {
	h.mu.RLock()
	defer h.mu.RUnlock()
	st := PQStatus{NodesTotal: len(h.nodes)}
	if h.codebook == nil {
		return st
	}
	st.Enabled = true
	st.Dim = h.codebook.Dim
	st.M = h.codebook.M
	st.K = h.codebook.K
	for _, c := range h.codes {
		if len(c) == h.codebook.M {
			st.NodesWithCode++
		}
	}
	return st
}

// distanceToNode returns a comparable distance from query q to node[idx].
// Lower = closer in BOTH branches. Iter 416.
//
//   - PQ branch: float32(PQDistance(table, code, M, K)) — squared-L2 distance
//     over reconstructed (uncompressed) approximation. For unit-normalized
//     vectors this is monotonic with cosine distance, so HNSW pruning
//     thresholds stay coherent.
//   - Raw branch: -dot(q, h.nodes[idx].vec). Identical to the iter-203
//     baseline.
//
// pqTable is the M*K-element lookup precomputed once per search via
// codebook.QueryTable.
func (h *HNSW) distanceToNode(q []float32, pqTable []float32, idx int) float64 {
	// Iter 417 fix: PQ branch requires a non-nil pqTable. AddPassage's
	// internal greedyDescend/searchLayer calls pass nil because graph
	// construction always uses raw vecs — without this guard, the PQ
	// branch fires with a nil table and PQDistance panics indexing it.
	if pqTable != nil && h.codebook != nil && idx < len(h.codes) && len(h.codes[idx]) == h.codebook.M {
		return float64(PQDistance(pqTable, h.codes[idx], h.codebook.M, h.codebook.K))
	}
	if len(h.nodes[idx].vec) == 0 {
		return math.MaxFloat64 // skip zombie nodes (iter 411 defensive)
	}
	return -float64(dot(q, h.nodes[idx].vec))
}

// SampleVectors returns up to n vectors drawn uniformly at random from the
// graph (without replacement). Used by iter-415 PQ codebook training to
// build a representative training set without loading the entire corpus
// into RAM. Returns fewer than n when the graph has fewer nodes.
// Iter 415.
func (h *HNSW) SampleVectors(n int, rngSeed int64) [][]float32 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.nodes) == 0 {
		return nil
	}
	// Iter 415: filter out zero-value / partial-persisted nodes whose vec
	// is empty — same defensive guard searchLayer uses (iter 411).
	idxs := make([]int, 0, len(h.nodes))
	for i := range h.nodes {
		if len(h.nodes[i].vec) > 0 {
			idxs = append(idxs, i)
		}
	}
	if len(idxs) == 0 {
		return nil
	}
	if n >= len(idxs) {
		out := make([][]float32, 0, len(idxs))
		for _, i := range idxs {
			cp := make([]float32, len(h.nodes[i].vec))
			copy(cp, h.nodes[i].vec)
			out = append(out, cp)
		}
		return out
	}
	rng := rand.New(rand.NewSource(rngSeed))
	rng.Shuffle(len(idxs), func(i, j int) { idxs[i], idxs[j] = idxs[j], idxs[i] })
	idxs = idxs[:n]
	out := make([][]float32, 0, n)
	for _, i := range idxs {
		cp := make([]float32, len(h.nodes[i].vec))
		copy(cp, h.nodes[i].vec)
		out = append(out, cp)
	}
	return out
}

// EncodeAll iterates every node in the graph and encodes its vector
// against the supplied codebook. Returns parallel slices (nodeIDs, codes)
// suitable for batched persist via PebbleStore.PutPQCodesBatch. Iter 415.
func (h *HNSW) EncodeAll(cb *PQCodebook) ([]uint64, [][]uint16, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ids := make([]uint64, 0, len(h.nodes))
	codes := make([][]uint16, 0, len(h.nodes))
	for i := range h.nodes {
		if len(h.nodes[i].vec) == 0 {
			continue
		}
		code, err := cb.Encode(h.nodes[i].vec)
		if err != nil {
			return nil, nil, fmt.Errorf("encode node %d: %w", i, err)
		}
		ids = append(ids, uint64(i))
		codes = append(codes, code)
	}
	return ids, codes, nil
}

// LookupVectorByURL returns the persisted unit-normalized vector for the
// first passage whose url matches. Used by /find_similar?retriever=dense to
// skip the embed RPC — the source doc's vector is already in the graph.
// Linear scan; for 1M passages this is ~few ms, dominated by cache misses.
// Returns (nil, false) when the URL has no indexed passage. Iter 371.
func (h *HNSW) LookupVectorByURL(url string) ([]float32, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for i := range h.nodes {
		if h.nodes[i].url == url {
			// Copy: caller shouldn't be able to mutate graph internals.
			cp := make([]float32, len(h.nodes[i].vec))
			copy(cp, h.nodes[i].vec)
			return cp, true
		}
	}
	return nil, false
}

// Add inserts a doc-level embedding without span info. Mirrors
// VectorIndex.Add — equivalent to AddPassage(url, title, 0, 0, vec).
func (h *HNSW) Add(url, title string, vec []float32) {
	h.AddPassage(url, title, 0, 0, vec)
}

// codeFor encodes one vector against the loaded codebook. Caller is the
// AddPassage hot path. Returns nil if codebook isn't set or encode fails.
// Caller holds h.mu (write or no lock during init; encode is read-only).
// Iter 417.
func (h *HNSW) codeFor(vec []float32) []uint16 {
	if h.codebook == nil {
		return nil
	}
	code, err := h.codebook.Encode(vec)
	if err != nil {
		return nil
	}
	return code
}

// AddPassage inserts a passage with explicit byte-span info. The vector is
// L2-normalized in place before storage. Bidirectional links are created
// from the new node to its nearest neighbors at every layer up to its
// stochastic level.
func (h *HNSW) AddPassage(url, title string, offset, length int, vec []float32) {
	if len(vec) != h.dim {
		return
	}
	cp := make([]float32, len(vec))
	copy(cp, vec)
	normalizeInPlace(cp)

	h.mu.Lock()
	defer h.mu.Unlock()

	level := h.randLevel()
	newIdx := len(h.nodes)
	h.nodes = append(h.nodes, hnswNode{
		vecDoc:    vecDoc{url: url, title: title, offset: offset, length: length, vec: cp},
		level:     level,
		neighbors: make([][]int, level+1),
	})
	// Iter 417: when a codebook is loaded, encode the new vec inline and
	// keep h.codes parallel to h.nodes. The crawl-time PQ checkpoint
	// (iter 417 in pebble_serve) writes [lastN, len) of these to Pebble.
	if h.codebook != nil {
		code := h.codeFor(cp)
		if cap(h.codes) <= newIdx {
			grown := make([][]uint16, newIdx+1, (newIdx+1)*2)
			copy(grown, h.codes)
			h.codes = grown
		} else if len(h.codes) <= newIdx {
			h.codes = h.codes[:newIdx+1]
		}
		h.codes[newIdx] = code
	}

	// First node: becomes the entry point trivially.
	if newIdx == 0 {
		h.entryPoint = 0
		h.maxLevel = level
		return
	}

	// 1. Greedy-descend from the current entry point down to level+1.
	curEntry := h.entryPoint
	for lvl := h.maxLevel; lvl > level; lvl-- {
		curEntry = h.greedyDescend(cp, nil, curEntry, lvl)
	}

	// 2. For each layer ≤ level, find efConstruction candidates and link
	//    the new node to its top-M nearest among them. Then make the link
	//    bidirectional, pruning the neighbor's list if it overflows.
	for lvl := minInt(level, h.maxLevel); lvl >= 0; lvl-- {
		cands := h.searchLayer(cp, nil, []int{curEntry}, h.efConstruction, lvl)
		mCap := h.M
		if lvl == 0 {
			mCap = h.Mmax0
		}
		// Pick the top-M nearest from cands.
		topM := selectTopM(cands, mCap)
		// Wire neighbors both ways.
		h.nodes[newIdx].neighbors[lvl] = make([]int, 0, len(topM))
		for _, c := range topM {
			h.nodes[newIdx].neighbors[lvl] = append(h.nodes[newIdx].neighbors[lvl], c.idx)
			// Add back-link, pruning if overfull.
			h.addBackLink(c.idx, newIdx, lvl)
		}
		// Set next layer's entry to the best candidate at this layer.
		if len(topM) > 0 {
			curEntry = topM[0].idx
		}
	}

	// 3. Update entry point if the new node sits above maxLevel.
	if level > h.maxLevel {
		h.entryPoint = newIdx
		h.maxLevel = level
	}
}

// Search returns the top-k by cosine similarity, deduplicated by URL.
// Same semantics as VectorIndex.Search.
func (h *HNSW) Search(_ context.Context, query []float32, k int) []VectorHit {
	if len(query) != h.dim {
		return nil
	}
	q := make([]float32, len(query))
	copy(q, query)
	normalizeInPlace(q)

	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.nodes) == 0 {
		return nil
	}
	// Iter 416: precompute the PQ asymmetric-distance lookup table once per
	// search if codebook is loaded. nil otherwise → raw dot-product path.
	var pqTable []float32
	if h.codebook != nil {
		var err error
		pqTable, err = h.codebook.QueryTable(q)
		if err != nil {
			pqTable = nil
		}
	}

	// 1. Greedy-descend through upper layers to find the layer-0 entry.
	ep := h.entryPoint
	for lvl := h.maxLevel; lvl > 0; lvl-- {
		ep = h.greedyDescend(q, pqTable, ep, lvl)
	}

	// 2. ef-search at layer 0.
	ef := h.efSearch
	if ef < k {
		ef = k
	}
	cands := h.searchLayer(q, pqTable, []int{ep}, ef, 0)

	// 3. Doc-level max-passage aggregation (mirrors VectorIndex.Search).
	type best struct {
		nodeIdx int
		score   float32
	}
	bestByURL := make(map[string]best, len(cands))
	for _, c := range cands {
		url := h.nodes[c.idx].url
		// Iter 416: convert c.dist back to a cosine-shaped score for output.
		// Raw branch stored c.dist = -dot (cos = -dist).
		// PQ branch stored c.dist = L2² over unit-norm vecs ≈ 2(1-cos), so
		// cos ≈ 1 - dist/2.
		var score float32
		if h.codebook != nil {
			score = float32(1 - c.dist/2)
		} else {
			score = float32(-c.dist)
		}
		cur, ok := bestByURL[url]
		if !ok || score > cur.score {
			bestByURL[url] = best{nodeIdx: c.idx, score: score}
		}
	}
	out := make([]best, 0, len(bestByURL))
	for _, e := range bestByURL {
		out = append(out, e)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].score > out[b].score })
	if k > len(out) {
		k = len(out)
	}
	hits := make([]VectorHit, 0, k)
	for i := 0; i < k; i++ {
		n := h.nodes[out[i].nodeIdx]
		hits = append(hits, VectorHit{
			URL:    n.url,
			Title:  n.title,
			Score:  float64(out[i].score),
			Offset: n.offset,
			Length: n.length,
		})
	}
	return hits
}

// greedyDescend walks the graph at a single layer toward the query, always
// stepping to the neighbor closer to the query than the current node.
// Returns the index of the local minimum found at this layer.
// Iter 416: pqTable threaded through to enable PQ-distance traversal.
func (h *HNSW) greedyDescend(q []float32, pqTable []float32, start int, lvl int) int {
	cur := start
	curDist := float32(h.distanceToNode(q, pqTable, cur))
	for {
		moved := false
		// Iter 411: same defensive guard as searchLayer — skip zero-value
		// nodes (corrupt-load gap) instead of panicking.
		if len(h.nodes[cur].neighbors) == 0 {
			return cur
		}
		for _, nb := range h.nodes[cur].neighbors[minInt(lvl, len(h.nodes[cur].neighbors)-1)] {
			d := float32(h.distanceToNode(q, pqTable, nb))
			if d < curDist {
				curDist = d
				cur = nb
				moved = true
			}
		}
		if !moved {
			return cur
		}
	}
}

// candEntry tracks a candidate in the search frontier (heap).
type candEntry struct {
	idx  int
	dist float32 // 1 - cos(q, v); smaller = closer
}

// searchLayer is the core HNSW search routine. From the given entry points,
// expands the nearest-first frontier until the top ef candidates are stable.
// Returns the ef best candidates at this layer, sorted by ascending dist.
// Iter 416: pqTable enables PQ-distance traversal when set (nil = raw).
func (h *HNSW) searchLayer(q []float32, pqTable []float32, entryPoints []int, ef int, lvl int) []candEntry {
	visited := make(map[int]struct{}, ef*2)
	// Candidates: min-heap by dist (front-of-queue is the nearest to expand).
	cands := &candMinHeap{}
	heap.Init(cands)
	// Results: max-heap by dist (front is the farthest, evicted first when full).
	results := &candMaxHeap{}
	heap.Init(results)

	for _, ep := range entryPoints {
		d := float32(h.distanceToNode(q, pqTable, ep))
		heap.Push(cands, candEntry{idx: ep, dist: d})
		heap.Push(results, candEntry{idx: ep, dist: d})
		visited[ep] = struct{}{}
	}

	for cands.Len() > 0 {
		nearest := heap.Pop(cands).(candEntry)
		// If the closest unexplored candidate is farther than the worst kept
		// result, expansion can't improve the result set — stop.
		if results.Len() >= ef && nearest.dist > (*results)[0].dist {
			break
		}
		// Expand neighbors at this layer.
		// Iter 411: defensive — a node with len(neighbors)==0 (zero-value
		// from a failed-incremental-persist gap pre-iter-411 fix) would
		// produce nbIdx=-1 and panic. Skip such nodes; their absence from
		// the graph is harmless beyond reduced recall.
		if len(h.nodes[nearest.idx].neighbors) == 0 {
			continue
		}
		nbIdx := minInt(lvl, len(h.nodes[nearest.idx].neighbors)-1)
		for _, nb := range h.nodes[nearest.idx].neighbors[nbIdx] {
			if _, ok := visited[nb]; ok {
				continue
			}
			visited[nb] = struct{}{}
			d := float32(h.distanceToNode(q, pqTable, nb))
			if results.Len() < ef {
				heap.Push(cands, candEntry{idx: nb, dist: d})
				heap.Push(results, candEntry{idx: nb, dist: d})
			} else if d < (*results)[0].dist {
				heap.Push(cands, candEntry{idx: nb, dist: d})
				heap.Pop(results) // drop the current worst
				heap.Push(results, candEntry{idx: nb, dist: d})
			}
		}
	}

	// Drain results — convert max-heap to ascending-by-dist slice.
	out := make([]candEntry, results.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(results).(candEntry)
	}
	return out
}

// addBackLink wires a back-edge from neighbor to newIdx at the given layer,
// pruning neighbor's list if it overflows the per-layer cap.
func (h *HNSW) addBackLink(neighbor, newIdx, lvl int) {
	if lvl >= len(h.nodes[neighbor].neighbors) {
		return // neighbor doesn't participate at this layer
	}
	mCap := h.M
	if lvl == 0 {
		mCap = h.Mmax0
	}
	h.nodes[neighbor].neighbors[lvl] = append(h.nodes[neighbor].neighbors[lvl], newIdx)
	if len(h.nodes[neighbor].neighbors[lvl]) <= mCap {
		return
	}
	// Overfull — re-rank neighbors by distance to this node, keep top mCap.
	nbVec := h.nodes[neighbor].vec
	cands := make([]candEntry, 0, len(h.nodes[neighbor].neighbors[lvl]))
	for _, nb := range h.nodes[neighbor].neighbors[lvl] {
		cands = append(cands, candEntry{idx: nb, dist: -dot(nbVec, h.nodes[nb].vec)})
	}
	sort.Slice(cands, func(a, b int) bool { return cands[a].dist < cands[b].dist })
	cands = cands[:mCap]
	out := make([]int, len(cands))
	for i, c := range cands {
		out[i] = c.idx
	}
	h.nodes[neighbor].neighbors[lvl] = out
}

// randLevel draws a level from the geometric distribution used by HNSW.
// Level 0 is the densest (every node); higher levels are exponentially sparser.
func (h *HNSW) randLevel() int {
	return int(math.Floor(-math.Log(h.rng.Float64()) * h.levelMult))
}

// selectTopM returns the M candidates with smallest dist, from a sorted-by-dist
// candidate list (ascending). Defensive against shorter input.
func selectTopM(cands []candEntry, m int) []candEntry {
	if len(cands) <= m {
		return cands
	}
	return cands[:m]
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// candMinHeap is a min-heap of candEntries by dist (smaller = front).
type candMinHeap []candEntry

func (h candMinHeap) Len() int            { return len(h) }
func (h candMinHeap) Less(i, j int) bool  { return h[i].dist < h[j].dist }
func (h candMinHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *candMinHeap) Push(x interface{}) { *h = append(*h, x.(candEntry)) }
func (h *candMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// candMaxHeap is a max-heap of candEntries by dist (larger = front, evict-first).
type candMaxHeap []candEntry

func (h candMaxHeap) Len() int            { return len(h) }
func (h candMaxHeap) Less(i, j int) bool  { return h[i].dist > h[j].dist }
func (h candMaxHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *candMaxHeap) Push(x interface{}) { *h = append(*h, x.(candEntry)) }
func (h *candMaxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
