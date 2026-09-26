package articles

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"

	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/store"
	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

func TestKeyEncodingRoundTrip(t *testing.T) {
	id := ulid(7)
	cases := []struct {
		key  []byte
		want parsedKey
	}{
		{recordKey(id), parsedKey{sub: subRecord, suffix: id}},
		{versionKey(id, 3), parsedKey{sub: subVersion, suffix: id, version: 3}},
		{slugKey("rust-async-runtimes"), parsedKey{sub: subSlug, suffix: "rust-async-runtimes"}},
		{topicKey("8624225ee5e2b9f9"), parsedKey{sub: subTopic, suffix: "8624225ee5e2b9f9"}},
		{embeddingKey(id), parsedKey{sub: subEmbedding, suffix: id}},
		{fingerprintKey(id), parsedKey{sub: subFingerprint, suffix: id}},
		{countersKey(id), parsedKey{sub: subCounters, suffix: id}},
		{metaKey("golive"), parsedKey{sub: subMeta, suffix: "golive"}},
	}
	subs := map[byte]bool{}
	for _, c := range cases {
		if c.key[0] != 'R' {
			t.Errorf("key %q outside the R family", c.key)
		}
		got, ok := parseKey(c.key)
		if !ok || got != c.want {
			t.Errorf("parseKey(%q) = %+v, %v; want %+v", c.key, got, ok, c.want)
		}
		subs[c.key[1]] = true
	}
	if len(subs) != 8 {
		t.Errorf("%d sub-families, want 8", len(subs))
	}
	lower, upper := versionPrefix(id)
	for _, v := range []int{1, 2, 1 << 40} {
		k := versionKey(id, v)
		if bytes.Compare(k, lower) < 0 || bytes.Compare(k, upper) >= 0 {
			t.Errorf("version %d key outside its prefix", v)
		}
	}
	if bytes.Compare(versionKey(id, 2), versionKey(id, 10)) >= 0 {
		t.Error("version keys do not sort numerically")
	}
	for _, bad := range [][]byte{[]byte("Ra"), []byte("Ra" + id[:25]), append(key(subVersion, id), 1), []byte("Rtzz"), []byte("Rx" + id), []byte("da" + id)} {
		if _, ok := parseKey(bad); ok {
			t.Errorf("parseKey(%q) accepted", bad)
		}
	}
}

func TestVectorEncodingRoundTrip(t *testing.T) {
	rows := []row{{kind: kindTitle, vec: axis(1)}, {kind: kindLead, vec: axis(2)}, {kind: kindAlias, alias: 0, vec: axis(3)}, {kind: kindAlias, alias: 1, vec: mix(4, 5, 0.6)}}
	model, dim, got, err := decodeEmbeddings(encodeEmbeddings("m", testDim, rows))
	if err != nil || model != "m" || dim != testDim || len(got) != 4 {
		t.Fatalf("decode = %q %d %d %v", model, dim, len(got), err)
	}
	for i := range rows {
		if got[i].kind != rows[i].kind || got[i].alias != rows[i].alias || !slices.Equal(got[i].vec, rows[i].vec) {
			t.Errorf("row %d = %+v, want %+v", i, got[i], rows[i])
		}
	}
	var raw embeddingsJSON
	_ = json.Unmarshal(encodeEmbeddings("m", testDim, rows), &raw)
	if raw.Vectors[0].I != nil || raw.Vectors[2].I == nil || *raw.Vectors[2].I != 0 {
		t.Errorf("alias index encoding: %+v", raw.Vectors)
	}
	m, d, v, err := decodeFingerprint(encodeFingerprint("m", testDim, axis(9)))
	if err != nil || m != "m" || d != testDim || !slices.Equal(v, axis(9)) {
		t.Errorf("fingerprint round trip: %q %d %v %v", m, d, v, err)
	}
}

func TestCreateAndRead(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	out := h.put(synthProd, id, with(articleBody("Rust async runtimes"), "aliases", []string{"Async executors in Rust"}, "topic_ids", []string{topic(1)}), http.StatusCreated)
	a := article(out)
	if out["result"] != "created" || a["slug"] != "rust-async-runtimes" || a["version"] != 1.0 || a["prelive"] != false || a["status"] != "published" {
		t.Fatalf("create = %v", out)
	}
	if len(a["content_sha256"].(string)) != 64 {
		t.Errorf("content_sha256 = %v", a["content_sha256"])
	}
	pub := h.call(wiki, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK)
	for _, k := range []string{"aliases", "topic_ids", "cluster_ids", "sensitivity", "moderation", "models", "build", "prelive"} {
		if _, ok := pub[k]; ok {
			t.Errorf("public projection carries %s", k)
		}
	}
	if pub["promoted"] != true || pub["ai_generated"] != true || pub["title"] != "Rust async runtimes" {
		t.Errorf("public = %v", pub)
	}
	bySlug := h.call(wiki, http.MethodGet, "/v1/articles/by-slug/rust-async-runtimes", nil, http.StatusOK)
	if bySlug["id"] != id {
		t.Errorf("by-slug = %v", bySlug)
	}
	fullRec := h.call(dashProd, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK)
	for _, k := range []string{"aliases", "topic_ids", "models", "build", "prelive", "readers_7d", "readers_by_day", "has_fingerprint", "base_slug"} {
		if _, ok := fullRec[k]; !ok {
			t.Errorf("full projection lacks %s", k)
		}
	}
	list := h.call(wiki, http.MethodGet, "/v1/articles", nil, http.StatusOK)
	if items := list["items"].([]any); len(items) != 1 || items[0].(map[string]any)["id"] != id {
		t.Errorf("list = %v", list)
	}
	h.restart()
	if again := h.call(wiki, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK); again["slug"] != "rust-async-runtimes" {
		t.Errorf("after restart = %v", again)
	}
	if _, ok := h.raw(topicKey(topic(1))); !ok {
		t.Error("topic claim not stored")
	}
}

// Articles never reach postings, HNSW, docMeta or the corpus counters.
func TestArticlesLeaveCorpusUntouched(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if _, err := h.ps.UpsertDocument(ctx, &store.Document{URL: "https://example.org/doc", Title: "Doc", Text: "some text"}); err != nil {
		t.Fatal(err)
	}
	if err := h.ps.IndexDocument(ctx, 0, "Doc", "some text", func(s string) []string { return []string{s} }, 1); err != nil {
		t.Fatal(err)
	}
	outside := func() [32]byte {
		d := sha256.New()
		it, _ := h.ps.DB().NewIter(&pebble.IterOptions{})
		defer it.Close()
		for ok := it.First(); ok; ok = it.Next() {
			if it.Key()[0] != 'R' {
				d.Write(it.Key())
				d.Write([]byte{0})
				d.Write(it.Value())
			}
		}
		return [32]byte(d.Sum(nil))
	}
	before := outside()
	sumBefore, countBefore, _ := h.ps.CorpusStats(ctx)

	id := ulid(1)
	h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "lead", "A new lead. It changes the content."), http.StatusOK)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(2), stubBodyFor("Go memory model", topic(2)), http.StatusCreated)
	h.moderate(dashProd, id, "tombstone", "privacy", http.StatusOK)

	sumAfter, countAfter, _ := h.ps.CorpusStats(ctx)
	if outside() != before {
		t.Error("keys or values outside the R family changed")
	}
	if sumBefore != sumAfter || countBefore != countAfter {
		t.Errorf("corpus counters changed: %d/%d → %d/%d", sumBefore, countBefore, sumAfter, countAfter)
	}
	h.restart()
	if outside() != before {
		t.Error("keys or values outside the R family changed across a restart")
	}
}

func TestOpenRefusesCachedEmbedder(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = Open(Options{DB: db, Policy: v1.NewFakePolicy(), Embedder: embed.NewCachedEmbedder(newFakeEmbedder(), t.TempDir())})
	if err == nil {
		t.Fatal("Open accepted a CachedEmbedder")
	}
}

// The store embeds every text through the embedder it was given, uncached. The
// cache_dir proof through the serve wiring is the joint test in cmd/cosift.
func TestEmbedsEveryTextUncached(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.emb.set("Rust async runtimes", axis(1))
	h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "lead", "A second lead. It changes."), http.StatusOK)
	for range 2 {
		h.match(mcpProd, map[string]any{"q": "rust async runtimes"}, http.StatusOK)
		h.match(synthProd, map[string]any{"q": "rust async runtimes", "purpose": "build"}, http.StatusOK)
	}
	if n := h.emb.count("rust async runtimes"); n != 4 {
		t.Errorf("a repeated match text reached the embedder %d times, want 4", n)
	}
	if n := h.emb.count("Rust async runtimes"); n != 2 {
		t.Errorf("the title reached the embedder %d times over two content writes, want 2", n)
	}
	stub := ulid(2)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+stub, stubBodyFor("Go memory model", topic(2)), http.StatusCreated)
	h.moderate(dashProd, stub, "tombstone", "legal", http.StatusOK)
	if _, ok := h.raw(fingerprintKey(stub)); !ok || h.emb.count("Go memory model") != 1 {
		t.Error("the stub tombstone did not embed its title for the fingerprint")
	}
}

// Every 'R' write path syncs: each case's change is the last write before a crash.
func TestWritesSurviveCrash(t *testing.T) {
	cases := []struct {
		name  string
		setup func(h *harness)
		op    func(h *harness)
		check func(ix *index) bool
	}{
		{"create", nil, func(h *harness) { h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated) },
			func(ix *index) bool { return ix.recs[ulid(1)] != nil && len(ix.rows[ulid(1)]) == 2 }},
		{"stub", nil, func(h *harness) {
			h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(1), stubBodyFor("Go memory model", topic(1)), http.StatusCreated)
		}, func(ix *index) bool { return ix.recs[ulid(1)] != nil && ix.claims[topic(1)] == ulid(1) }},
		{"stub delete", func(h *harness) {
			h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(1), stubBodyFor("Go memory model", topic(1)), http.StatusCreated)
		}, func(h *harness) { h.call(resolverProd, http.MethodDelete, "/v1/articles/"+ulid(1), nil, http.StatusOK) },
			func(ix *index) bool { return ix.recs[ulid(1)] == nil && ix.claims[topic(1)] == "" }},
		{"hold", func(h *harness) { h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated) },
			func(h *harness) { h.moderate(dashProd, ulid(1), "hold", "", http.StatusOK) },
			func(ix *index) bool { return ix.recs[ulid(1)] != nil && ix.recs[ulid(1)].Status == StatusHeld }},
		{"tombstone", func(h *harness) { h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated) },
			func(h *harness) { h.moderate(dashProd, ulid(1), "tombstone", "privacy", http.StatusOK) },
			func(ix *index) bool {
				return ix.recs[ulid(1)] != nil && ix.recs[ulid(1)].Residue && ix.hasFingerprint(ulid(1))
			}},
		{"erase fingerprint", func(h *harness) {
			h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated)
			h.moderate(dashProd, ulid(1), "tombstone", "privacy", http.StatusOK)
		}, func(h *harness) { h.moderate(dashProd, ulid(1), "erase_fingerprint", "privacy", http.StatusOK) },
			func(ix *index) bool { return ix.recs[ulid(1)] != nil && !ix.hasFingerprint(ulid(1)) }},
		{"purge", nil, func(h *harness) {
			h.call(goliveAdmin, http.MethodPost, "/v1/articles/purge-prelive", map[string]any{"apply": true, "expect_records": 0}, http.StatusOK)
		}, func(ix *index) bool { return ix.golive != nil }},
		{"counters", func(h *harness) {
			h.emb.set("Rust async runtimes", axis(1))
			h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated)
			h.match(mcpProd, map[string]any{"q": "Rust async runtimes", "reader": strings.Repeat("a", 32)}, http.StatusOK)
		}, func(h *harness) {
			h.s.writeMu.Lock()
			defer h.s.writeMu.Unlock()
			if err := h.s.flushLocked(); err != nil {
				h.t.Fatal(err)
			}
		}, func(ix *index) bool { return ix.recs[ulid(1)] != nil }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fs := vfs.NewStrictMem()
			if err := fs.MkdirAll("db", 0o755); err != nil {
				t.Fatal(err)
			}
			if root, err := fs.OpenDir(""); err != nil || root.Sync() != nil {
				t.Fatal("sync root", err)
			}
			emb := newFakeEmbedder()
			open := func() (*pebble.DB, *Store) {
				db, err := pebble.Open("db", &pebble.Options{FS: fs})
				if err != nil {
					t.Fatal(err)
				}
				s, err := Open(Options{DB: db, Embedder: emb, Policy: v1.NewFakePolicy(synthProd, resolverProd, mcpProd, dashProd, goliveAdmin),
					Corpus: fakeCorpus{urls: map[string]bool{"https://example.org/doc": true}}, ThresholdsPath: t.TempDir() + "/none.json", Logf: func(string, ...any) {}})
				if err != nil {
					t.Fatal(err)
				}
				if err := s.Rebuild(context.Background()); err != nil {
					t.Fatal(err)
				}
				return db, s
			}
			db, s := open()
			h := &harness{t: t, s: s, emb: emb, mux: http.NewServeMux()}
			for _, rt := range s.Routes() {
				h.mux.Handle(rt.Pattern(), rt.Handler)
			}
			if c.setup != nil {
				c.setup(h)
			}
			c.op(h)
			fs.SetIgnoreSyncs(true)
			close(s.stop)
			s.jobs.Wait()
			_ = db.Close()
			fs.ResetToSyncedState()
			fs.SetIgnoreSyncs(false)

			db, s = open()
			defer db.Close()
			defer s.Close()
			ok := true
			s.view(func(ix *index) { ok = c.check(ix) })
			if !ok {
				t.Error("change lost in the crash")
			}
			if c.name == "counters" {
				v, closer, err := db.Get(countersKey(ulid(1)))
				if err != nil {
					t.Fatal("counters lost in the crash")
				}
				closer.Close()
				_ = v
			}
		})
	}
}
