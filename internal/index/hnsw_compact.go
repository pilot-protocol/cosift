package index

import "log"

// compactProgressEvery paces the in-compact progress logs; the whole pass
// runs under the write lock, so these lines are the only liveness signal.
// A var (not const) so tests can lower it below the fixture size.
var compactProgressEvery = 10_000_000

// Rebuild constructs a fresh HNSW with the same parameters as h and inserts
// every valid (vec != nil) node from h via AddPassage. Unlike Compact, which
// merely removes zombies and rewires existing edges, Rebuild reconstructs
// the graph topology — each surviving node gets a full M-neighbor selection
// drawn from other surviving nodes, recovering the recall lost to zombie
// fragmentation.
//
// O(N log N) construction. For 63K valid nodes with M=16 this takes
// roughly 30 seconds single-threaded. Callers should run it offline (e.g.,
// from a CLI subcommand or admin endpoint) and persist the result.
//
// PQ codes are NOT carried over — a fresh codebook should be retrained
// against the rebuilt graph because node indices change.
func (h *HNSW) Rebuild() *HNSW {
	h.mu.RLock()
	defer h.mu.RUnlock()

	fresh := NewHNSW(h.dim)
	fresh.M = h.M
	fresh.Mmax0 = h.Mmax0
	fresh.efConstruction = h.efConstruction
	fresh.efSearch = h.efSearch
	fresh.levelMult = h.levelMult

	for i := range h.nodes {
		if len(h.nodes[i].vec) == 0 {
			continue
		}
		// Make a local copy so AddPassage's in-place normalize doesn't
		// touch the source. Source vecs are already unit-norm (AddPassage
		// normalized on the way in), but normalizeInPlace is idempotent
		// only modulo floating-point drift — fine either way.
		cp := make([]float32, len(h.nodes[i].vec))
		copy(cp, h.nodes[i].vec)
		fresh.AddPassage(h.nodes[i].url, h.nodes[i].title, h.nodes[i].offset, h.nodes[i].length, cp)
	}
	return fresh
}

// Compact removes zombie nodes (vec == nil) in place. Rebuilds h.nodes and
// h.codes, then walks every surviving neighbor list and rewrites indices to
// match the new positions, dropping refs to removed nodes. After compaction
// the graph contains only valid nodes — graph navigation can no longer
// stall in zombie-heavy regions.
//
// Returns the number of zombies removed. Cost: O(N + total_edges). The
// graph topology among surviving nodes is preserved exactly.
//
// production HNSW had 768K zombie slots (~92% of total) from
// pre partial persists; bench-pq showed Recall@10 dropping to 74%
// even without PQ. Compaction restores the recall the underlying corpus
// can support.
func (h *HNSW) Compact() (removed int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.nodes) == 0 {
		return 0
	}

	// 1. Build oldIdx → newIdx remap, gather kept node + code blobs.
	remap := make([]int, len(h.nodes))
	newNodes := make([]hnswNode, 0, len(h.nodes))
	hasCodes := h.codes != nil
	var newCodes [][]uint16
	if hasCodes {
		newCodes = make([][]uint16, 0, len(h.codes))
	}
	for i := range h.nodes {
		if i > 0 && i%compactProgressEvery == 0 {
			log.Printf("hnsw compact: scanning %d/%d nodes", i, len(h.nodes))
		}
		if len(h.nodes[i].vec) == 0 {
			remap[i] = -1
			continue
		}
		remap[i] = len(newNodes)
		newNodes = append(newNodes, h.nodes[i])
		if hasCodes {
			if i < len(h.codes) {
				newCodes = append(newCodes, h.codes[i])
			} else {
				newCodes = append(newCodes, nil)
			}
		}
	}
	removed = len(h.nodes) - len(newNodes)

	// 2. Remap neighbor lists. Iterating in increasing order so writes only
	//    touch slots we've already read from the source array.
	for i := range newNodes {
		if i > 0 && i%compactProgressEvery == 0 {
			log.Printf("hnsw compact: rewiring neighbors %d/%d nodes", i, len(newNodes))
		}
		for lvl := range newNodes[i].neighbors {
			old := newNodes[i].neighbors[lvl]
			out := make([]int, 0, len(old))
			for _, n := range old {
				if n >= 0 && n < len(remap) {
					if r := remap[n]; r >= 0 {
						out = append(out, r)
					}
				}
			}
			newNodes[i].neighbors[lvl] = out
		}
	}

	// 3. Pick new entry point as the highest-level surviving node.
	h.nodes = newNodes
	h.codes = newCodes
	if len(h.nodes) == 0 {
		h.entryPoint = -1
		h.maxLevel = 0
		return removed
	}
	h.entryPoint = 0
	h.maxLevel = h.nodes[0].level
	for i := 1; i < len(h.nodes); i++ {
		if h.nodes[i].level > h.maxLevel {
			h.entryPoint = i
			h.maxLevel = h.nodes[i].level
		}
	}
	return removed
}
