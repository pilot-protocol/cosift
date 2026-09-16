package index

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/pilot-protocol/cosift/internal/store"
)

// checkURLIndex asserts the index invariant: a node is live iff its id is in
// byURL[url], and valid counts exactly the live nodes.
func checkURLIndex(t *testing.T, h *HNSW, where string) {
	t.Helper()
	h.mu.RLock()
	defer h.mu.RUnlock()
	live := 0
	indexed := map[int32]string{}
	for url, ids := range h.byURL {
		for _, id := range ids {
			if _, dup := indexed[id]; dup {
				t.Fatalf("%s: node %d indexed twice", where, id)
			}
			indexed[id] = url
		}
	}
	for i := range h.nodes {
		url, ok := indexed[int32(i)]
		if len(h.nodes[i].vec) > 0 {
			live++
			if !ok || url != h.nodes[i].url {
				t.Fatalf("%s: live node %d (%s) missing from index (got %q)", where, i, h.nodes[i].url, url)
			}
		} else if ok {
			t.Fatalf("%s: zombie node %d still indexed under %s", where, i, url)
		}
	}
	if len(indexed) != live || h.valid != live {
		t.Fatalf("%s: indexed=%d valid=%d live=%d", where, len(indexed), h.valid, live)
	}
	if h.entryPoint >= 0 && len(h.nodes[h.entryPoint].vec) == 0 {
		t.Fatalf("%s: entry point %d is a zombie", where, h.entryPoint)
	}
	if h.entryPoint < 0 && live > 0 {
		t.Fatalf("%s: no entry point with %d live nodes", where, live)
	}
}

func TestHNSWURLIndexInvariant(t *testing.T) {
	h := buildTestHNSW(300, 8, 3, 5)
	checkURLIndex(t, h, "build")

	batch := make([]PassageInput, 0, 12)
	for i := 0; i < 12; i++ {
		v := make([]float32, 8)
		v[i%8] = 1
		batch = append(batch, PassageInput{URL: fmt.Sprintf("https://x/%d", i), Title: "regen", Offset: i, Length: 1, Vec: v})
	}
	h.AddPassageBatch(batch)
	checkURLIndex(t, h, "batch")
	if len(h.byURL["https://x/1"]) != 2 {
		t.Fatalf("second generation not indexed: %v", h.byURL["https://x/1"])
	}

	for i := 0; i < 40; i++ {
		h.MarkURLPassagesInvalid(fmt.Sprintf("https://x/%d", i))
	}
	checkURLIndex(t, h, "reclaim")
	h.ReconcileURLs(func(u string) bool { return u != "https://x/100" && u != "https://x/200" })
	checkURLIndex(t, h, "reconcile")

	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	ctx := context.Background()
	if err := h.Persist(ctx, ps); err != nil {
		t.Fatal(err)
	}
	loaded, ok, err := LoadHNSW(ctx, ps)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	checkURLIndex(t, loaded, "load")
	if loaded.valid != h.valid || len(loaded.byURL) != len(h.byURL) {
		t.Fatalf("load counters: valid %d/%d urls %d/%d", loaded.valid, h.valid, len(loaded.byURL), len(h.byURL))
	}

	h.Compact()
	checkURLIndex(t, h, "compact")
	fresh := h.Rebuild()
	checkURLIndex(t, fresh, "rebuild")
	if fresh.Len() != h.Len() {
		t.Fatalf("rebuild len %d != compact len %d", fresh.Len(), h.Len())
	}
}

// Reclaim must touch only the URL's own nodes: the dirty set (every node
// written to) equals exactly the invalidated ids.
func TestHNSWReclaimTouchesOnlyURLNodes(t *testing.T) {
	h := buildTestHNSW(5000, 8, 3, 5)
	for i := 0; i < 3; i++ {
		v := make([]float32, 8)
		v[i] = 1
		h.AddPassage("https://multi", "m", i*10, 10, v)
	}
	want := map[int32]struct{}{}
	for _, id := range h.byURL["https://multi"] {
		want[id] = struct{}{}
	}
	h.dirty = make(map[int32]struct{})

	if n := h.MarkURLPassagesInvalid("https://multi"); n != 3 {
		t.Fatalf("reclaimed %d, want 3", n)
	}
	if len(h.dirty) != 3 {
		t.Fatalf("reclaim touched %d nodes, want 3", len(h.dirty))
	}
	for id := range want {
		if _, ok := h.dirty[id]; !ok {
			t.Fatalf("node %d not marked dirty", id)
		}
	}
	if h.Reclaimed() != 3 || h.MarkURLPassagesInvalid("https://multi") != 0 {
		t.Fatalf("reclaim counters: total=%d", h.Reclaimed())
	}
	if _, ok := h.byURL["https://multi"]; ok {
		t.Fatal("index entry survived reclaim")
	}
	checkURLIndex(t, h, "after")
}

// Re-crawl: reclaim then add; search sees only the new generation and
// LookupVectorByURL never returns a zombie's empty vector.
func TestHNSWDuplicateGenerationReclaim(t *testing.T) {
	h := NewHNSW(4)
	h.AddPassage("https://doc", "v1", 0, 50, []float32{1, 0, 0, 0})
	h.AddPassage("https://doc", "v1", 50, 50, []float32{0, 1, 0, 0})
	h.Add("https://other", "o", []float32{0, 0, 0, 1})

	if n := h.MarkURLPassagesInvalid("https://doc"); n != 2 {
		t.Fatalf("reclaim %d, want 2", n)
	}
	if _, ok := h.LookupVectorByURL("https://doc"); ok {
		t.Fatal("LookupVectorByURL returned a zombie")
	}
	h.AddPassage("https://doc", "v2", 0, 80, []float32{0, 0, 1, 0})
	v, ok := h.LookupVectorByURL("https://doc")
	if !ok || v[2] < 0.99 {
		t.Fatalf("lookup after re-add: ok=%v v=%v", ok, v)
	}
	hits := h.Search(context.Background(), []float32{1, 0, 0, 0}, 5)
	for _, hit := range hits {
		if hit.URL == "https://doc" && hit.Title != "v2" {
			t.Fatalf("stale generation surfaced: %+v", hit)
		}
	}
	st := h.PQStatus()
	if st.NodesTotal != 4 || st.NodesValid != 2 {
		t.Fatalf("status %+v", st)
	}
	checkURLIndex(t, h, "regen")
}

func TestHNSWEntryPointRelocation(t *testing.T) {
	h := buildTestHNSW(400, 8, 3, 5)
	ep := h.entryPoint
	epURL := h.nodes[ep].url
	h.MarkURLPassagesInvalid(epURL)
	if h.entryPoint == ep || h.zombieIdx(h.entryPoint) {
		t.Fatalf("entry point not relocated: %d (was %d)", h.entryPoint, ep)
	}
	if h.nodes[h.entryPoint].level != h.maxLevel {
		t.Fatalf("maxLevel %d != entry level %d", h.maxLevel, h.nodes[h.entryPoint].level)
	}
	q := make([]float32, 8)
	q[0] = 1
	if hits := h.Search(context.Background(), q, 10); len(hits) != 10 {
		t.Fatalf("search after relocation returned %d hits", len(hits))
	}

	inv, _ := h.ReconcileURLs(func(string) bool { return false })
	if inv != 399 || h.entryPoint != -1 {
		t.Fatalf("reconcile all: inv=%d ep=%d", inv, h.entryPoint)
	}
	if hits := h.Search(context.Background(), q, 10); len(hits) != 0 {
		t.Fatalf("all-zombie graph returned %d hits", len(hits))
	}
	h.AddPassage("https://new", "n", 0, 1, q)
	if h.entryPoint != 400 || h.maxLevel != h.nodes[400].level {
		t.Fatalf("first live node did not become entry point: ep=%d", h.entryPoint)
	}
	if hits := h.Search(context.Background(), q, 10); len(hits) != 1 || hits[0].URL != "https://new" {
		t.Fatalf("search after revival: %+v", hits)
	}
	checkURLIndex(t, h, "revived")
}

// Regression for the 2026-08 production failure: an invalidated cluster
// around the entry point must not starve dense search. Zombies are
// traversed but never admitted to the ef window.
func TestHNSWSearchThroughZombieCluster(t *testing.T) {
	const n, dim, k = 2000, 16, 10
	recall := func(h *HNSW) float64 {
		rng := rand.New(rand.NewSource(77))
		total := 0.0
		const nq = 20
		for qi := 0; qi < nq; qi++ {
			q := make([]float32, dim)
			for j := range q {
				q[j] = float32(rng.NormFloat64())
			}
			gt := map[string]bool{}
			for _, g := range h.BruteForceTopK(q, k) {
				gt[g.URL] = true
			}
			hits := 0
			for _, a := range h.Search(context.Background(), q, k) {
				if gt[a.URL] {
					hits++
				}
			}
			total += float64(hits) / float64(len(gt))
		}
		return total / nq
	}
	cluster := func(h *HNSW, hops int) map[int]struct{} {
		set := map[int]struct{}{h.entryPoint: {}}
		frontier := []int{h.entryPoint}
		for hop := 0; hop < hops; hop++ {
			var next []int
			for _, i := range frontier {
				for _, nb := range h.nodes[i].neighbors[0] {
					if _, ok := set[nb]; !ok {
						set[nb] = struct{}{}
						next = append(next, nb)
					}
				}
			}
			frontier = next
		}
		return set
	}

	// Variant A: the reclaim path (index-maintained, entry point relocates).
	h := buildTestHNSW(n, dim, 3, 5)
	clean := recall(h)
	zombies := cluster(h, 2)
	for i := range zombies {
		h.MarkURLPassagesInvalid(h.nodes[i].url)
	}
	t.Logf("variant A: zombified %d nodes around the entry point", len(zombies))
	checkURLIndex(t, h, "cluster")
	if r := recall(h); r < 0.9 || r < clean-0.05 {
		t.Fatalf("variant A recall %.3f (clean %.3f)", r, clean)
	}

	// Variant B: raw zombification with the entry point left in place — the
	// on-disk shape after an offline purge on an old binary.
	h = buildTestHNSW(n, dim, 3, 5)
	zombies = cluster(h, 2)
	for i := range zombies {
		h.nodes[i].vec = nil
	}
	if !h.zombieIdx(h.entryPoint) {
		t.Fatal("variant B: entry point should be a zombie")
	}
	hits := h.Search(context.Background(), make([]float32, dim), k)
	for _, hit := range hits {
		if _, z := zombies[int(h.byURL[hit.URL][0])]; z {
			t.Fatalf("zombie surfaced: %+v", hit)
		}
	}
	if r := recall(h); r < 0.9 || r < clean-0.05 {
		t.Fatalf("variant B recall %.3f (clean %.3f)", r, clean)
	}

	// Variant C: zombies as bridges — every layer-0 neighbour of the true
	// top-k is invalidated, so the targets are reachable only through
	// zombies. Without transit the ef window fills with finite results and
	// the traversal never expands past the ring.
	h = buildTestHNSW(n, dim, 3, 5)
	rng := rand.New(rand.NewSource(77))
	ring := map[int]struct{}{}
	for qi := 0; qi < 20; qi++ {
		q := make([]float32, dim)
		for j := range q {
			q[j] = float32(rng.NormFloat64())
		}
		targets := map[int]struct{}{}
		for _, g := range h.BruteForceTopK(q, k) {
			targets[int(h.byURL[g.URL][0])] = struct{}{}
		}
		for i := range targets {
			for _, nb := range h.nodes[i].neighbors[0] {
				if _, isTarget := targets[nb]; !isTarget {
					ring[nb] = struct{}{}
				}
			}
		}
	}
	for i := range ring {
		if _, ok := h.byURL[h.nodes[i].url]; ok {
			h.MarkURLPassagesInvalid(h.nodes[i].url)
		}
	}
	t.Logf("variant C: zombified %d ring nodes", len(ring))
	if r := recall(h); r < 0.85 {
		t.Fatalf("variant C recall %.3f (clean %.3f)", r, clean)
	}
}
