package index

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"

	"github.com/pilot-protocol/cosift/internal/store"
)

// TestHNSWPersistRoundTrip — build, persist, load into a fresh HNSW,
// verify Search returns the same top-k hits as the original. Locks in
// the binary format contract.
func TestHNSWPersistRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	defer ps.Close()
	ctx := context.Background()

	const (
		n   = 500
		dim = 32
	)
	original := NewHNSW(dim)
	original.rng = rand.New(rand.NewSource(13))
	rng := rand.New(rand.NewSource(7))

	for i := 0; i < n; i++ {
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		original.AddPassage(fmt.Sprintf("https://x/%d", i), fmt.Sprintf("doc %d", i), i*100, 50, v)
	}

	// Capture top-k hits from the original on a fixed set of queries.
	queries := make([][]float32, 10)
	for i := range queries {
		q := make([]float32, dim)
		for j := range q {
			q[j] = float32(rng.NormFloat64())
		}
		queries[i] = q
	}
	originalHits := make([][]string, len(queries))
	for i, q := range queries {
		hits := original.Search(ctx, q, 10)
		urls := make([]string, len(hits))
		for j, h := range hits {
			urls[j] = h.URL
		}
		originalHits[i] = urls
	}

	// Persist + reload.
	if err := original.Persist(ctx, ps); err != nil {
		t.Fatalf("persist: %v", err)
	}
	loaded, ok, err := LoadHNSW(ctx, ps)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !ok {
		t.Fatal("loaded ok=false; expected persisted index")
	}
	if loaded.Len() != n {
		t.Errorf("loaded.Len(): want %d, got %d", n, loaded.Len())
	}
	if loaded.maxLevel != original.maxLevel {
		t.Errorf("maxLevel mismatch: want %d, got %d", original.maxLevel, loaded.maxLevel)
	}
	if loaded.entryPoint != original.entryPoint {
		t.Errorf("entryPoint mismatch: want %d, got %d", original.entryPoint, loaded.entryPoint)
	}

	// Re-run the same queries through the loaded index. With deterministic
	// graph + identical vectors, top-k order must match exactly — no
	// approximate-recall slack here.
	for i, q := range queries {
		hits := loaded.Search(ctx, q, 10)
		urls := make([]string, len(hits))
		for j, h := range hits {
			urls[j] = h.URL
		}
		if !sameURLOrder(urls, originalHits[i]) {
			t.Errorf("query %d: loaded hits don't match original\n  original: %v\n  loaded:   %v", i, originalHits[i], urls)
		}
	}
}

// TestLoadHNSWAbsent — fresh Pebble store: LoadHNSW returns (nil, false, nil).
func TestLoadHNSWAbsent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	defer ps.Close()
	h, ok, err := LoadHNSW(context.Background(), ps)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if ok {
		t.Errorf("expected ok=false on empty store")
	}
	if h != nil {
		t.Errorf("expected nil HNSW")
	}
}

// TestLoadHNSWZombieRoundTrip locks in the encoder/decoder symmetry that
// the GH200 OOM-cycle investigation surfaced: MarkURLPassagesInvalid sets
// vec=nil; the next Persist encodes those nodes with dim=0; LoadHNSW must
// accept dim=0 as the deliberate zombie sentinel and preserve everything
// else (url, title, offset, length, level, neighbors) so the graph
// adjacency stays consistent across restarts.
func TestLoadHNSWZombieRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	defer ps.Close()
	ctx := context.Background()

	const (
		n        = 10
		dim      = 8
		zombieID = 4
	)
	h := NewHNSW(dim)
	h.rng = rand.New(rand.NewSource(1))
	for i := 0; i < n; i++ {
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32(i+1) * 0.1
		}
		h.AddPassage(fmt.Sprintf("https://x/%d", i), fmt.Sprintf("title %d", i), i*100, 50, v)
	}
	// Zombie one node via the production code path — mirrors what
	// MarkURLPassagesInvalid does when a URL is re-crawled.
	h.MarkURLPassagesInvalid(fmt.Sprintf("https://x/%d", zombieID))
	if err := h.Persist(ctx, ps); err != nil {
		t.Fatalf("persist: %v", err)
	}

	loaded, ok, err := LoadHNSW(ctx, ps)
	if err != nil {
		t.Fatalf("LoadHNSW: %v (zombie must NOT be treated as corruption)", err)
	}
	if !ok {
		t.Fatal("LoadHNSW ok=false; expected true")
	}
	if loaded.Len() != n {
		t.Errorf("loaded.Len(): want %d, got %d", n, loaded.Len())
	}
	// Zombie slot: empty vec, but url/title/offset/length/level/neighbors
	// must survive so search-layer graph navigation works against it.
	z := loaded.nodes[zombieID]
	if len(z.vec) != 0 {
		t.Errorf("zombie vec: want len 0, got %d", len(z.vec))
	}
	wantURL := fmt.Sprintf("https://x/%d", zombieID)
	if z.url != wantURL {
		t.Errorf("zombie url: want %q, got %q", wantURL, z.url)
	}
	if z.title == "" {
		t.Errorf("zombie title: want preserved, got empty")
	}
	if z.offset != zombieID*100 {
		t.Errorf("zombie offset: want %d, got %d", zombieID*100, z.offset)
	}
	if z.length != 50 {
		t.Errorf("zombie length: want 50, got %d", z.length)
	}
	if len(z.neighbors) == 0 {
		t.Errorf("zombie neighbors: want non-empty layer list (preserves graph adjacency), got empty")
	}
	// Every NON-zombie slot still has its vec.
	for i := 0; i < n; i++ {
		if i == zombieID {
			continue
		}
		if got := len(loaded.nodes[i].vec); got != dim {
			t.Errorf("slot %d vec: want len %d, got %d", i, dim, got)
		}
	}
}

// TestLoadHNSWSkipsCorruptNode — actual storage-level corruption (a truncated
// node blob, not a zombie) must not ground the load. Slot becomes empty,
// rest of the index is intact.
func TestLoadHNSWSkipsCorruptNode(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	defer ps.Close()
	ctx := context.Background()

	const (
		n         = 10
		dim       = 8
		corruptID = 4
	)
	h := NewHNSW(dim)
	h.rng = rand.New(rand.NewSource(1))
	for i := 0; i < n; i++ {
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32(i+1) * 0.1
		}
		h.AddPassage(fmt.Sprintf("https://x/%d", i), "t", i*100, 50, v)
	}
	if err := h.Persist(ctx, ps); err != nil {
		t.Fatalf("persist: %v", err)
	}

	// Overwrite node `corruptID`'s blob with a blob whose declared dim is
	// neither 0 (the zombie sentinel) nor the meta dim. This is the real-
	// corruption shape the decoder should still reject.
	bad := make([]byte, 0, 32)
	bad = append(bad, 0, 0)        // urlLen
	bad = append(bad, 0, 0)        // titleLen
	bad = append(bad, 0, 0, 0, 0)  // offset
	bad = append(bad, 0, 0, 0, 0)  // length
	bad = append(bad, 0, 0, 0, 0)  // level
	bad = append(bad, 99, 0, 0, 0) // dim = 99 — mismatch with meta dim=8
	if err := ps.PutVectorNodesBatch(ctx, []store.VectorNodeEntry{
		{ID: corruptID, Blob: bad},
	}); err != nil {
		t.Fatalf("PutVectorNodesBatch: %v", err)
	}

	loaded, ok, err := LoadHNSW(ctx, ps)
	if err != nil {
		t.Fatalf("LoadHNSW: %v (expected nil — corrupt node should be skipped, not grounded)", err)
	}
	if !ok {
		t.Fatal("LoadHNSW ok=false; expected true")
	}
	if loaded.Len() != n {
		t.Errorf("loaded.Len(): want %d (slot count is meta.nodeCount), got %d", n, loaded.Len())
	}
	if got := len(loaded.nodes[corruptID].vec); got != 0 {
		t.Errorf("corrupt slot vec: want len 0 (skipped → empty), got %d", got)
	}
	for i := 0; i < n; i++ {
		if i == corruptID {
			continue
		}
		if got := len(loaded.nodes[i].vec); got != dim {
			t.Errorf("slot %d vec: want len %d, got %d", i, dim, got)
		}
	}
}

// TestHNSWNodeEncodingExact — a single-node round-trip with controlled
// values asserts every field survives encode→decode bit-for-bit.
func TestHNSWNodeEncodingExact(t *testing.T) {
	n := &hnswNode{
		vecDoc: vecDoc{
			url:    "https://example.com/path/to/doc",
			title:  "A Title With Spaces",
			offset: 12345,
			length: 6789,
			vec:    []float32{0.1, -0.5, 3.14, 42.0, 0.0, 1.0},
		},
		level: 2,
		neighbors: [][]int{
			{1, 2, 3, 4, 5}, // layer 0
			{1, 3, 5},       // layer 1
			{2, 4},          // layer 2
		},
	}
	blob := encodeHNSWNode(n)
	decoded, err := decodeHNSWNode(blob, 6)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.url != n.url || decoded.title != n.title {
		t.Errorf("url/title mismatch: %+v", decoded.vecDoc)
	}
	if decoded.offset != n.offset || decoded.length != n.length || decoded.level != n.level {
		t.Errorf("scalar mismatch: %+v", decoded)
	}
	if len(decoded.vec) != len(n.vec) {
		t.Fatalf("vec len: got %d, want %d", len(decoded.vec), len(n.vec))
	}
	for i := range n.vec {
		if decoded.vec[i] != n.vec[i] {
			t.Errorf("vec[%d]: got %f, want %f", i, decoded.vec[i], n.vec[i])
		}
	}
	if len(decoded.neighbors) != len(n.neighbors) {
		t.Fatalf("neighbor layers: got %d, want %d", len(decoded.neighbors), len(n.neighbors))
	}
	for l := range n.neighbors {
		if !sameIntSlice(decoded.neighbors[l], n.neighbors[l]) {
			t.Errorf("layer %d: got %v, want %v", l, decoded.neighbors[l], n.neighbors[l])
		}
	}
}

// TestDecodeHNSWMetaMagicMismatch — load against a corrupt blob fails cleanly.
func TestDecodeHNSWMetaMagicMismatch(t *testing.T) {
	buf := []byte("NOPE\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")
	if _, err := decodeHNSWMeta(buf); err == nil {
		t.Errorf("expected magic-mismatch error")
	}
}

func sameURLOrder(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestLoadHNSWMeta locks down the cheap meta-only read must return
// the same {Dim, NodeCount} that a freshly persisted HNSW graph reports.
// Lives in the persistence test file because it shares the Pebble-fixture
// scaffold.
func TestLoadHNSWMeta(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	defer ps.Close()
	ctx := context.Background()

	// Before any persist: no meta blob → ok=false, no error.
	if _, ok, err := LoadHNSWMeta(ctx, ps); err != nil || ok {
		t.Fatalf("LoadHNSWMeta on empty store: want (ok=false, err=nil), got (ok=%v, err=%v)", ok, err)
	}

	const dim = 16
	h := NewHNSW(dim)
	h.rng = rand.New(rand.NewSource(42))
	rng := rand.New(rand.NewSource(99))
	const n = 25
	for i := 0; i < n; i++ {
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		h.AddPassage(fmt.Sprintf("https://x/%d", i), fmt.Sprintf("doc %d", i), 0, 0, v)
	}
	if err := h.Persist(ctx, ps); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	meta, ok, err := LoadHNSWMeta(ctx, ps)
	if err != nil {
		t.Fatalf("LoadHNSWMeta after persist: %v", err)
	}
	if !ok {
		t.Fatalf("LoadHNSWMeta after persist: want ok=true")
	}
	if meta.Dim != dim {
		t.Errorf("meta.Dim: want %d, got %d", dim, meta.Dim)
	}
	if meta.NodeCount != n {
		t.Errorf("meta.NodeCount: want %d, got %d", n, meta.NodeCount)
	}
}

func sameIntSlice(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	ac := make([]int, len(a))
	bc := make([]int, len(b))
	copy(ac, a)
	copy(bc, b)
	sort.Ints(ac)
	sort.Ints(bc)
	for i := range ac {
		if ac[i] != bc[i] {
			return false
		}
	}
	return true
}
