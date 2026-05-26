// Pebble-backed document store. Iter 200 — first piece of the path-2 storage
// rework that lets cosift scale past SQLite's million-row ceiling.
//
// Why Pebble: cockroachdb/pebble is a mature pure-Go LSM-tree from
// CockroachDB. Block-level compression, mmap reads, batch writes, native
// prefix scans. At million-doc scale it sustains roughly 100k writes/sec on
// commodity hardware vs SQLite WAL's ~10k cap; reads scale similarly via
// block cache. No cgo, embedded, single-process — same operational shape
// as SQLite but with a higher-throughput hot path.
//
// This package does NOT replace the SQLite Store. Both coexist for now;
// operators choose via config. Migration tooling (copy SQLite → Pebble)
// lands in a follow-up iter; for now PebbleStore is the destination format
// for new deployments and exists in parallel.
//
// Schema (all keys prefixed with a one-byte family tag):
//
//	'd' + uint64(docID, big-endian) → gob(Document)        // primary by ID
//	'u' + url                       → uint64(docID)        // URL → ID index
//	'h' + host + 0x00 + uint64(id)  → empty                // host scan index
//	'm' + "next_doc_id"             → uint64(next ID)      // counter
//
// Big-endian IDs give natural ascending order for prefix scans. Family-tag
// prefix bytes keep families disjoint so a scan over 'd' won't touch 'u' rows.
package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
)

// PebbleStore is a Pebble-backed implementation of the hot Store API surface.
// Currently supports Document CRUD; postings + frontier come in follow-up iters.
type PebbleStore struct {
	db        *pebble.DB
	nextID    atomic.Int64
	mu        sync.Mutex            // serializes the rare URL→ID race during Upsert
	writeOpts *pebble.WriteOptions  // iter 219: Sync (default) or NoSync (crawl-workload opt-in)
}

const (
	famDoc     byte = 'd'
	famURL     byte = 'u'
	famHost    byte = 'h'
	famMeta    byte = 'm'
	famTerm    byte = 't' // 't' + term-string → (termID, doc_freq) packed
	famPosting byte = 'p' // 'p' + termID + docID → tf (varint)
	famDocLen  byte = 'l' // 'l' + docID → doc_len (varint)
	famVector  byte = 'v' // 'v' + 0x01 + uint64-be(nodeID) → hnsw node blob
	//                       'v' + 0x00 + "meta"           → hnsw graph meta
	famDocMeta byte = 'i' // 'i' + uint64-be(docID) → uvarint(urlLen)+url+uvarint(titleLen)+title
	//                      iter 207: cheap URL+title side-blob (~50 bytes vs ~1KB+ gob)
	//                      so BM25 hit-resolution skips the full Document decode.
	famDocTerms byte = 'g' // 'g' + uint64-be(docID) → uvarint(N) + N×uvarint(termID)
	//                       iter 208: reverse index from docID to its term IDs, so
	//                       re-indexing the same doc can delete orphaned postings
	//                       for terms that vanished from the new content.
	famFrontier byte = 'f' // 'f' + 'u' + url → packed frontier entry
	//                       iter 209: per-URL frontier row. Status byte +
	//                       depth + priority + enqueued_at + attempts + host
	//                       + last_error all in one value. Secondary indexes
	//                       (host-fair claim) land in iter 210.
)

// OpenPebble opens (or creates) a Pebble store at path with memory-bounded
// defaults. Iter 218 — caps the block cache + memtables so cosift fits on
// commodity VMs (16 GB RAM was OOM-killed under sustained crawl load
// because Pebble's defaults are sized for CockroachDB-style servers).
//
// Tunable via env vars:
//   COSIFT_PEBBLE_CACHE_MB     — block cache size in MB (default 128)
//   COSIFT_PEBBLE_MEMTABLE_MB  — single memtable size in MB (default 32)
//   COSIFT_PEBBLE_MEMTABLES    — max memtables in memory (default 2)
//
// Total Pebble memory ceiling ≈ cache + memtables × memtable_size, so the
// defaults pin Pebble at roughly 128 + 2×32 = 192 MB. Real working set
// climbs higher (compaction, write batches, block readers) but the
// OOM-prone block cache growth is bounded.
func OpenPebble(path string) (*PebbleStore, error) {
	cacheMB := envInt("COSIFT_PEBBLE_CACHE_MB", 128)
	memtableMB := envInt("COSIFT_PEBBLE_MEMTABLE_MB", 32)
	memtables := envInt("COSIFT_PEBBLE_MEMTABLES", 2)

	cache := pebble.NewCache(int64(cacheMB) << 20)
	defer cache.Unref()
	opts := &pebble.Options{
		Cache:                       cache,
		MemTableSize:                uint64(memtableMB) << 20,
		MemTableStopWritesThreshold: memtables + 2,
	}
	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, fmt.Errorf("pebble.Open(%s): %w", path, err)
	}
	// Iter 219: write-sync mode. Sync (default) fsyncs each commit — safe
	// against VM crash but expensive under sustained write load (the iter-
	// 218 OOM root cause: batches stacked faster than fsync could drain).
	// NoSync skips the fsync; durability stays vs PROCESS crash (WAL is
	// still written), drops vs OS crash (OS buffer cache could lose seconds
	// of writes). Acceptable for crawl workloads since the frontier
	// resumes cleanly on next start.
	wopts := pebble.Sync
	if os.Getenv("COSIFT_PEBBLE_SYNC") == "false" {
		wopts = pebble.NoSync
	}
	p := &PebbleStore{db: db, writeOpts: wopts}
	val, closer, err := db.Get(metaKey("next_doc_id"))
	switch {
	case errors.Is(err, pebble.ErrNotFound):
		p.nextID.Store(0)
	case err != nil:
		_ = db.Close()
		return nil, fmt.Errorf("read next_doc_id: %w", err)
	default:
		if len(val) == 8 {
			p.nextID.Store(int64(binary.BigEndian.Uint64(val)))
		}
		_ = closer.Close()
	}
	return p, nil
}

// Close flushes and closes the underlying Pebble DB.
func (p *PebbleStore) Close() error { return p.db.Close() }

// envInt reads an env var as int with a default. Empty / malformed → default.
func envInt(name string, defaultV int) int {
	v := os.Getenv(name)
	if v == "" {
		return defaultV
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return defaultV
	}
	return n
}

// Metrics returns Pebble's built-in metrics — LSM-level breakdown,
// on-disk size, WAL state, compaction stats. Iter 217 — surfaced via
// `cosift pebble-info` for operator sizing + diagnosis.
//
// The returned *pebble.Metrics has a String() that formats a multi-line
// human-readable table; callers usually want that directly.
func (p *PebbleStore) Metrics() *pebble.Metrics { return p.db.Metrics() }

// UpsertDocument writes (or overwrites) a document. Returns the assigned ID.
// New URLs get a fresh monotonic ID; existing URLs reuse their ID and the
// document row is rewritten with the new payload — same semantics as the
// SQLite Store.
func (p *PebbleStore) UpsertDocument(ctx context.Context, d *Document) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if d.URL == "" {
		return 0, errors.New("PebbleStore.UpsertDocument: empty URL")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Resolve existing ID by URL, or allocate a fresh one.
	var id int64
	if existingID, ok, err := p.lookupIDByURL(d.URL); err != nil {
		return 0, err
	} else if ok {
		id = existingID
	} else {
		id = p.nextID.Add(1)
	}
	d.ID = id

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(d); err != nil {
		return 0, fmt.Errorf("encode doc: %w", err)
	}

	idBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(idBuf, uint64(id))

	batch := p.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(docKey(id), buf.Bytes(), nil); err != nil {
		return 0, err
	}
	if err := batch.Set(urlKey(d.URL), idBuf, nil); err != nil {
		return 0, err
	}
	// Iter 207: cheap (url, title) side-blob so hit-meta lookups skip the
	// gob decode of the full Document.
	if err := batch.Set(docMetaKey(id), packDocMeta(d.URL, d.Title), nil); err != nil {
		return 0, err
	}
	if d.Domain != "" {
		if err := batch.Set(hostKey(d.Domain, id), nil, nil); err != nil {
			return 0, err
		}
	}
	if err := batch.Set(metaKey("next_doc_id"), idBuf, nil); err != nil {
		return 0, err
	}
	if err := batch.Commit(p.writeOpts); err != nil {
		return 0, fmt.Errorf("commit batch: %w", err)
	}
	return id, nil
}

// GetDocMeta returns (URL, title) for docID via the cheap 'i' side blob.
// ok=false when nothing was indexed for the ID. Iter 207 — avoids the full
// Document gob decode that GetDocByID does.
func (p *PebbleStore) GetDocMeta(ctx context.Context, docID int64) (url, title string, ok bool, err error) {
	if err := ctx.Err(); err != nil {
		return "", "", false, err
	}
	val, closer, err := p.db.Get(docMetaKey(docID))
	if errors.Is(err, pebble.ErrNotFound) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, err
	}
	defer closer.Close()
	return unpackDocMeta(val)
}

func docMetaKey(docID int64) []byte {
	k := make([]byte, 1+8)
	k[0] = famDocMeta
	binary.BigEndian.PutUint64(k[1:], uint64(docID))
	return k
}

func docTermsKey(docID int64) []byte {
	k := make([]byte, 1+8)
	k[0] = famDocTerms
	binary.BigEndian.PutUint64(k[1:], uint64(docID))
	return k
}

// frontierKey is 'f' + 'u' + url (primary, dedup-by-URL).
func frontierKey(url string) []byte {
	k := make([]byte, 2+len(url))
	k[0] = famFrontier
	k[1] = 'u'
	copy(k[2:], url)
	return k
}

// Iter 210: two secondary indexes keyed by host so ClaimFrontier can pick
// host-fair without an O(N) scan over every queued URL.
//
//	'f' + 'q' + host + 0x00 + url → empty   (present iff status='queued')
//	'f' + 'i' + host + 0x00 + url → empty   (present iff status='in_flight')
//
// The 0x00 separator keeps the host field prefix-disambiguated so a URL
// can't slide into a different host's row even if it byte-prefixes a
// host name.
func frontierStatusIndexKey(sub byte, host, url string) []byte {
	k := make([]byte, 2+len(host)+1+len(url))
	k[0] = famFrontier
	k[1] = sub
	copy(k[2:], host)
	k[2+len(host)] = 0x00
	copy(k[2+len(host)+1:], url)
	return k
}

// frontierStatusIndexHost extracts the host portion of a secondary-index
// key. Returns "" if the key shape is wrong.
func frontierStatusIndexHost(key []byte) string {
	if len(key) < 3 || key[0] != famFrontier {
		return ""
	}
	rest := key[2:]
	for i, b := range rest {
		if b == 0x00 {
			return string(rest[:i])
		}
	}
	return ""
}

// FrontierStatus is the lifecycle position of a frontier URL.
type FrontierStatus byte

const (
	FrontierStatusQueued   FrontierStatus = 'q'
	FrontierStatusInFlight FrontierStatus = 'i'
	FrontierStatusDone     FrontierStatus = 'd'
	FrontierStatusError    FrontierStatus = 'e'
)

// frontierEntry is the value side of the 'f' + 'u' + url key. Packed
// little-endian: status (1) + depth (varint) + priority (float64-le) +
// enqueued_at (varint) + attempts (varint) + host (varint-len + bytes) +
// last_error (varint-len + bytes).
type frontierEntry struct {
	Status      FrontierStatus
	Depth       int64
	Priority    float64
	EnqueuedAt  int64
	Attempts    int64
	Host        string
	LastError   string
}

func packFrontierEntry(e frontierEntry) []byte {
	tmp := make([]byte, binary.MaxVarintLen64)
	out := make([]byte, 0, 1+8+len(e.Host)+len(e.LastError)+30)
	out = append(out, byte(e.Status))
	n := binary.PutVarint(tmp, e.Depth)
	out = append(out, tmp[:n]...)
	priBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(priBuf, math.Float64bits(e.Priority))
	out = append(out, priBuf...)
	n = binary.PutVarint(tmp, e.EnqueuedAt)
	out = append(out, tmp[:n]...)
	n = binary.PutVarint(tmp, e.Attempts)
	out = append(out, tmp[:n]...)
	n = binary.PutUvarint(tmp, uint64(len(e.Host)))
	out = append(out, tmp[:n]...)
	out = append(out, e.Host...)
	n = binary.PutUvarint(tmp, uint64(len(e.LastError)))
	out = append(out, tmp[:n]...)
	out = append(out, e.LastError...)
	return out
}

func unpackFrontierEntry(buf []byte) (frontierEntry, error) {
	var e frontierEntry
	if len(buf) < 1 {
		return e, fmt.Errorf("frontierEntry: empty buf")
	}
	e.Status = FrontierStatus(buf[0])
	buf = buf[1:]
	depth, n := binary.Varint(buf)
	if n <= 0 {
		return e, fmt.Errorf("frontierEntry: bad depth")
	}
	e.Depth = depth
	buf = buf[n:]
	if len(buf) < 8 {
		return e, fmt.Errorf("frontierEntry: short priority")
	}
	e.Priority = math.Float64frombits(binary.LittleEndian.Uint64(buf[:8]))
	buf = buf[8:]
	enq, n := binary.Varint(buf)
	if n <= 0 {
		return e, fmt.Errorf("frontierEntry: bad enqueued_at")
	}
	e.EnqueuedAt = enq
	buf = buf[n:]
	at, n := binary.Varint(buf)
	if n <= 0 {
		return e, fmt.Errorf("frontierEntry: bad attempts")
	}
	e.Attempts = at
	buf = buf[n:]
	hostLen, n := binary.Uvarint(buf)
	if n <= 0 || int(hostLen) > len(buf)-n {
		return e, fmt.Errorf("frontierEntry: bad host length")
	}
	buf = buf[n:]
	e.Host = string(buf[:hostLen])
	buf = buf[hostLen:]
	errLen, n := binary.Uvarint(buf)
	if n <= 0 || int(errLen) > len(buf)-n {
		return e, fmt.Errorf("frontierEntry: bad last_error length")
	}
	buf = buf[n:]
	e.LastError = string(buf[:errLen])
	return e, nil
}

func packDocTerms(termIDs []int64) []byte {
	tmp := make([]byte, binary.MaxVarintLen64)
	out := make([]byte, 0, 2+len(termIDs)*2)
	n := binary.PutUvarint(tmp, uint64(len(termIDs)))
	out = append(out, tmp[:n]...)
	for _, id := range termIDs {
		n = binary.PutUvarint(tmp, uint64(id))
		out = append(out, tmp[:n]...)
	}
	return out
}

func unpackDocTerms(buf []byte) ([]int64, error) {
	count, n := binary.Uvarint(buf)
	if n <= 0 {
		return nil, fmt.Errorf("docTerms: malformed count")
	}
	buf = buf[n:]
	out := make([]int64, 0, count)
	for i := uint64(0); i < count; i++ {
		id, n := binary.Uvarint(buf)
		if n <= 0 {
			return nil, fmt.Errorf("docTerms: malformed termID at index %d", i)
		}
		buf = buf[n:]
		out = append(out, int64(id))
	}
	return out, nil
}

func packDocMeta(url, title string) []byte {
	buf := make([]byte, 0, 4+len(url)+2+len(title))
	tmp := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(tmp, uint64(len(url)))
	buf = append(buf, tmp[:n]...)
	buf = append(buf, url...)
	n = binary.PutUvarint(tmp, uint64(len(title)))
	buf = append(buf, tmp[:n]...)
	buf = append(buf, title...)
	return buf
}

func unpackDocMeta(buf []byte) (url, title string, ok bool, err error) {
	urlLen, n := binary.Uvarint(buf)
	if n <= 0 || int(urlLen) > len(buf)-n {
		return "", "", false, fmt.Errorf("docMeta: malformed url length")
	}
	buf = buf[n:]
	url = string(buf[:urlLen])
	buf = buf[urlLen:]
	titleLen, n := binary.Uvarint(buf)
	if n <= 0 || int(titleLen) > len(buf)-n {
		return "", "", false, fmt.Errorf("docMeta: malformed title length")
	}
	buf = buf[n:]
	title = string(buf[:titleLen])
	return url, title, true, nil
}

// GetDocByURL retrieves a single document, or ErrNotFound if missing.
// Matches the SQLite Store's same-named method.
func (p *PebbleStore) GetDocByURL(ctx context.Context, url string) (*Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id, ok, err := p.lookupIDByURL(url)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotFound
	}
	return p.GetDocByID(ctx, id)
}

// GetDocByID retrieves a document by its primary key. ErrNotFound when absent.
func (p *PebbleStore) GetDocByID(ctx context.Context, id int64) (*Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	val, closer, err := p.db.Get(docKey(id))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	var d Document
	if err := gob.NewDecoder(bytes.NewReader(val)).Decode(&d); err != nil {
		return nil, fmt.Errorf("decode doc %d: %w", id, err)
	}
	return &d, nil
}

// Stats returns a minimal counter view for parity with the SQLite Stats call.
// Postings + terms counts are zero until those families land in a follow-up iter.
func (p *PebbleStore) Stats(ctx context.Context) (Stats, error) {
	if err := ctx.Err(); err != nil {
		return Stats{}, err
	}
	// Count docs via prefix scan. Cheap on Pebble — iterator skips block
	// boundaries, no full scan needed beyond the 'd' family.
	prefix := []byte{famDoc}
	upper := []byte{famDoc + 1}
	it, err := p.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
	if err != nil {
		return Stats{}, err
	}
	defer it.Close()
	var n int64
	for valid := it.First(); valid; valid = it.Next() {
		n++
	}
	return Stats{Documents: n}, nil
}

// lookupIDByURL returns the stored doc ID for a URL, ok=false when missing.
func (p *PebbleStore) lookupIDByURL(url string) (int64, bool, error) {
	val, closer, err := p.db.Get(urlKey(url))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	defer closer.Close()
	if len(val) != 8 {
		return 0, false, fmt.Errorf("malformed URL→ID value: len=%d", len(val))
	}
	return int64(binary.BigEndian.Uint64(val)), true, nil
}

// Key encoders. Family-tag prefix isolates each logical "table" from others
// so prefix scans don't accidentally cross families.

func docKey(id int64) []byte {
	k := make([]byte, 1+8)
	k[0] = famDoc
	binary.BigEndian.PutUint64(k[1:], uint64(id))
	return k
}

func urlKey(url string) []byte {
	k := make([]byte, 1+len(url))
	k[0] = famURL
	copy(k[1:], url)
	return k
}

func hostKey(host string, id int64) []byte {
	k := make([]byte, 1+len(host)+1+8)
	k[0] = famHost
	copy(k[1:], host)
	k[1+len(host)] = 0x00
	binary.BigEndian.PutUint64(k[1+len(host)+1:], uint64(id))
	return k
}

func metaKey(name string) []byte {
	k := make([]byte, 1+len(name))
	k[0] = famMeta
	copy(k[1:], name)
	return k
}

func termKey(term string) []byte {
	k := make([]byte, 1+len(term))
	k[0] = famTerm
	copy(k[1:], term)
	return k
}

func postingKey(termID, docID int64) []byte {
	k := make([]byte, 1+8+8)
	k[0] = famPosting
	binary.BigEndian.PutUint64(k[1:], uint64(termID))
	binary.BigEndian.PutUint64(k[9:], uint64(docID))
	return k
}

func postingPrefix(termID int64) []byte {
	k := make([]byte, 1+8)
	k[0] = famPosting
	binary.BigEndian.PutUint64(k[1:], uint64(termID))
	return k
}

// postingPrefixUpper returns the exclusive upper bound for a termID prefix scan.
func postingPrefixUpper(termID int64) []byte {
	k := make([]byte, 1+8)
	k[0] = famPosting
	binary.BigEndian.PutUint64(k[1:], uint64(termID+1))
	return k
}

func docLenKey(docID int64) []byte {
	k := make([]byte, 1+8)
	k[0] = famDocLen
	binary.BigEndian.PutUint64(k[1:], uint64(docID))
	return k
}

// Two sub-prefixes under the 'v' family: 0x00 for meta, 0x01 for nodes.
// Sorting on Pebble's byte ordering puts meta before nodes, so a startup
// iterator can read meta first and use it to size the node slice.
func vectorMetaKey() []byte { return []byte{famVector, 0x00, 'm', 'e', 't', 'a'} }

func vectorNodeKey(nodeID uint64) []byte {
	k := make([]byte, 1+1+8)
	k[0] = famVector
	k[1] = 0x01
	binary.BigEndian.PutUint64(k[2:], nodeID)
	return k
}

// PutVectorMeta / GetVectorMeta — opaque blob holder under the 'v' family
// for index-level metadata (entry point, max level, dim, node count). Format
// is owned by the index package; store stays format-agnostic.
func (p *PebbleStore) PutVectorMeta(ctx context.Context, blob []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.db.Set(vectorMetaKey(), blob, p.writeOpts)
}

func (p *PebbleStore) GetVectorMeta(ctx context.Context) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	val, closer, err := p.db.Get(vectorMetaKey())
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer closer.Close()
	out := make([]byte, len(val))
	copy(out, val)
	return out, true, nil
}

// PutVectorNode writes one HNSW-node blob under its ID. Caller-owned format.
func (p *PebbleStore) PutVectorNode(ctx context.Context, nodeID uint64, blob []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.db.Set(vectorNodeKey(nodeID), blob, p.writeOpts)
}

// IterateVectorNodes scans every persisted HNSW node in ascending ID order,
// invoking fn(nodeID, blob) for each. Returning false from fn stops the scan.
func (p *PebbleStore) IterateVectorNodes(ctx context.Context, fn func(nodeID uint64, blob []byte) bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	lo := []byte{famVector, 0x01}
	hi := []byte{famVector, 0x02}
	it, err := p.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return err
	}
	defer it.Close()
	for valid := it.First(); valid; valid = it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := it.Key()
		if len(key) != 10 {
			continue
		}
		val, err := it.ValueAndErr()
		if err != nil {
			return err
		}
		blob := make([]byte, len(val))
		copy(blob, val)
		id := binary.BigEndian.Uint64(key[2:])
		if !fn(id, blob) {
			return nil
		}
	}
	return nil
}

// TermInfo bundles per-term metadata: stable integer ID and document
// frequency (how many docs contain the term).
type TermInfo struct {
	ID     int64
	DocFreq int64
}

// IndexDocument tokenizes (title, text) and writes a complete set of
// postings for docID under the Pebble schema. Replaces any existing
// postings for the same docID (re-indexing is idempotent — the caller
// is expected to call this exactly once per doc state).
//
// Tokenization happens here, not in the caller, because the Pebble store
// owns the postings layout. Title tokens contribute the iter-197 title
// boost (3x TF). doc_len records raw token count for BM25 length norm.
func (p *PebbleStore) IndexDocument(ctx context.Context, docID int64, title, text string, tokenize func(string) []string, titleBoost int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if titleBoost <= 0 {
		titleBoost = 1
	}
	titleTokens := tokenize(title)
	bodyTokens := tokenize(text)
	if len(titleTokens)+len(bodyTokens) == 0 {
		return nil
	}
	tf := make(map[string]int, len(titleTokens)+len(bodyTokens))
	for _, t := range titleTokens {
		tf[t] += titleBoost
	}
	for _, t := range bodyTokens {
		tf[t]++
	}

	docLen := len(titleTokens) + len(bodyTokens)
	lenBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(lenBuf, uint64(docLen))

	p.mu.Lock()
	defer p.mu.Unlock()

	// Iter 207: maintain running (sum_doc_len, indexed_doc_count) counters
	// under 'm' so corpusStats is O(1) per query instead of O(N) per query.
	// On re-index we subtract the OLD doc_len; on first-index we just add.
	oldLen, hadOld, err := p.readDocLenLocked(docID)
	if err != nil {
		return err
	}
	sumLen, _ := p.readMetaInt64Locked("sum_doc_len")
	indexedCount, _ := p.readMetaInt64Locked("indexed_docs")
	if hadOld {
		sumLen -= oldLen
	} else {
		indexedCount++
	}
	sumLen += int64(docLen)

	batch := p.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(docLenKey(docID), lenBuf, nil); err != nil {
		return err
	}
	// Counters are appended to the same batch so all-or-nothing semantics
	// hold — a torn write can't leave the counters out of sync with 'l'.
	sumBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(sumBuf, uint64(sumLen))
	if err := batch.Set(metaKey("sum_doc_len"), sumBuf, nil); err != nil {
		return err
	}
	countBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(countBuf, uint64(indexedCount))
	if err := batch.Set(metaKey("indexed_docs"), countBuf, nil); err != nil {
		return err
	}
	// Iter 208: load the previous term-ID set for this docID. On re-index,
	// terms that no longer appear get their postings deleted so phantom
	// matches don't leak. New term-ID set is written under 'g' at the end
	// of the batch.
	oldTermIDs, err := p.readDocTermsLocked(docID)
	if err != nil {
		return err
	}
	oldSet := make(map[int64]struct{}, len(oldTermIDs))
	for _, id := range oldTermIDs {
		oldSet[id] = struct{}{}
	}
	newSet := make(map[int64]struct{}, len(tf))

	for term, freq := range tf {
		info, ok, err := p.getTermInfoLocked(term)
		if err != nil {
			return err
		}
		if !ok {
			info.ID = p.nextTermID()
			info.DocFreq = 1
		} else if _, alreadyIn := oldSet[info.ID]; !alreadyIn {
			// Term existed in the corpus but THIS doc didn't have it before.
			// Iter 208: replaces the iter-201 postingExistsLocked check; we
			// now know from oldSet whether the doc had this term.
			info.DocFreq++
		}
		newSet[info.ID] = struct{}{}
		if err := batch.Set(termKey(term), packTermInfo(info), nil); err != nil {
			return err
		}
		// Iter 207: pack (tf, docLen) into the posting value so the search
		// path doesn't need a separate GetDocLen per posting.
		pvBuf := make([]byte, 16)
		binary.BigEndian.PutUint64(pvBuf[0:8], uint64(freq))
		binary.BigEndian.PutUint64(pvBuf[8:16], uint64(docLen))
		if err := batch.Set(postingKey(info.ID, docID), pvBuf, nil); err != nil {
			return err
		}
	}

	// Iter 208: delete postings for terms in oldSet \ newSet (vanished from
	// the new content). We don't decrement the term's doc_freq here — that
	// would require term-string lookup by termID (no reverse index today).
	// doc_freq becomes a slight over-count for terms that lost docs; IDF
	// shifts by less than the rounding noise.
	for oldID := range oldSet {
		if _, stillPresent := newSet[oldID]; stillPresent {
			continue
		}
		if err := batch.Delete(postingKey(oldID, docID), nil); err != nil {
			return err
		}
	}

	// Write the new term-ID set for this doc.
	newIDs := make([]int64, 0, len(newSet))
	for id := range newSet {
		newIDs = append(newIDs, id)
	}
	if err := batch.Set(docTermsKey(docID), packDocTerms(newIDs), nil); err != nil {
		return err
	}
	return batch.Commit(p.writeOpts)
}

// PushFrontier inserts a URL into the queue. INSERT-OR-IGNORE semantics:
// if the URL already exists in any state, this is a no-op. Iter 209.
// Iter 210: also writes the 'f'+'q' secondary index for host-fair claim.
func (p *PebbleStore) PushFrontier(ctx context.Context, url string, depth int, priority float64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, closer, err := p.db.Get(frontierKey(url)); err == nil {
		_ = closer.Close()
		return nil // already exists; dedup
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	host := extractHost(url)
	entry := frontierEntry{
		Status:     FrontierStatusQueued,
		Depth:      int64(depth),
		Priority:   priority,
		EnqueuedAt: time.Now().Unix(),
		Host:       host,
	}
	batch := p.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(frontierKey(url), packFrontierEntry(entry), nil); err != nil {
		return err
	}
	if err := batch.Set(frontierStatusIndexKey('q', host, url), nil, nil); err != nil {
		return err
	}
	return batch.Commit(p.writeOpts)
}

// ClaimFrontier picks one queued URL, atomically marks it in_flight, and
// returns the FrontierItem. ok=false when the queue is empty. Iter 210:
// host-fair via two secondary-index scans — O(distinct in-flight hosts +
// distinct queued URLs walked until a free host found). At a healthy
// crawl where most hosts are NOT in-flight, this is effectively O(1).
//
// Tradeoff: priority ordering is no longer enforced across hosts. The
// iter-209 implementation traversed every queued URL to honor strict
// (priority DESC, enqueued ASC) order; iter 210 trades that for the
// host-fair scheduling that the iter-190 SQLite-side Claim provides.
// Within a host's queued URLs Pebble returns them in URL-byte order,
// which approximates enqueue order for outbound-link discovery.
func (p *PebbleStore) ClaimFrontier(ctx context.Context) (FrontierItem, bool, error) {
	if err := ctx.Err(); err != nil {
		return FrontierItem{}, false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	// Step 1: build the set of hosts currently in-flight. Iter 221: wrap in
	// closure so iIt.Close() runs even if iteration panics (was explicit
	// Close after the loop; a panic inside leaked the iterator).
	inflightHosts := make(map[string]struct{}, 32)
	if err := func() error {
		iIt, err := p.db.NewIter(&pebble.IterOptions{
			LowerBound: []byte{famFrontier, 'i'},
			UpperBound: []byte{famFrontier, 'i' + 1},
		})
		if err != nil {
			return err
		}
		defer iIt.Close()
		for valid := iIt.First(); valid; valid = iIt.Next() {
			h := frontierStatusIndexHost(iIt.Key())
			if h != "" {
				inflightHosts[h] = struct{}{}
			}
		}
		return nil
	}(); err != nil {
		return FrontierItem{}, false, err
	}

	// Step 2: walk queued URLs in key order (host-then-URL). Pick the first
	// whose host is NOT in inflightHosts. Fall back to first overall if all
	// hosts are busy.
	qIt, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{famFrontier, 'q'},
		UpperBound: []byte{famFrontier, 'q' + 1},
	})
	if err != nil {
		return FrontierItem{}, false, err
	}
	defer qIt.Close()

	var pickedHost, pickedURL string
	var fallbackHost, fallbackURL string
	var fallbackFound bool
	for valid := qIt.First(); valid; valid = qIt.Next() {
		host := frontierStatusIndexHost(qIt.Key())
		if host == "" {
			continue
		}
		// The URL portion follows host + 0x00.
		key := qIt.Key()
		urlOffset := 2 + len(host) + 1
		if urlOffset > len(key) {
			continue
		}
		url := string(key[urlOffset:])
		if !fallbackFound {
			fallbackHost = host
			fallbackURL = url
			fallbackFound = true
		}
		if _, busy := inflightHosts[host]; !busy {
			pickedHost = host
			pickedURL = url
			break
		}
	}
	if pickedURL == "" {
		if !fallbackFound {
			return FrontierItem{}, false, nil
		}
		pickedHost = fallbackHost
		pickedURL = fallbackURL
	}

	// Step 3: atomic transition. Read primary, flip status, swap indexes.
	val, closer, err := p.db.Get(frontierKey(pickedURL))
	if err != nil {
		return FrontierItem{}, false, err
	}
	entry, err := unpackFrontierEntry(val)
	_ = closer.Close()
	if err != nil {
		return FrontierItem{}, false, err
	}
	entry.Status = FrontierStatusInFlight

	batch := p.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(frontierKey(pickedURL), packFrontierEntry(entry), nil); err != nil {
		return FrontierItem{}, false, err
	}
	if err := batch.Delete(frontierStatusIndexKey('q', pickedHost, pickedURL), nil); err != nil {
		return FrontierItem{}, false, err
	}
	if err := batch.Set(frontierStatusIndexKey('i', pickedHost, pickedURL), nil, nil); err != nil {
		return FrontierItem{}, false, err
	}
	if err := batch.Commit(p.writeOpts); err != nil {
		return FrontierItem{}, false, err
	}
	return FrontierItem{URL: pickedURL, Depth: int(entry.Depth), Priority: entry.Priority}, true, nil
}

// CompleteFrontier marks a URL as successfully processed.
func (p *PebbleStore) CompleteFrontier(ctx context.Context, url string) error {
	return p.transitionFrontier(ctx, url, FrontierStatusDone, "")
}

// FailFrontier marks a URL as errored. The error string is stored on the
// row (capped to keep frontier rows small).
func (p *PebbleStore) FailFrontier(ctx context.Context, url, errMsg string) error {
	const maxErrLen = 500
	if len(errMsg) > maxErrLen {
		errMsg = errMsg[:maxErrLen-3] + "..."
	}
	return p.transitionFrontier(ctx, url, FrontierStatusError, errMsg)
}

func (p *PebbleStore) transitionFrontier(ctx context.Context, url string, newStatus FrontierStatus, errMsg string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	val, closer, err := p.db.Get(frontierKey(url))
	if errors.Is(err, pebble.ErrNotFound) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	entry, err := unpackFrontierEntry(val)
	_ = closer.Close()
	if err != nil {
		return err
	}
	oldStatus := entry.Status
	entry.Status = newStatus
	if newStatus == FrontierStatusError {
		entry.Attempts++
		entry.LastError = errMsg
	}
	// Iter 210: keep secondary indexes consistent with the primary status.
	// Done/Error rows are not in any 'f'+q/'f'+i index — only queued and
	// in_flight have a secondary entry. So a transition out of in_flight
	// deletes 'f'+'i'; into in_flight adds 'f'+'i'; into queued (recovery)
	// re-adds 'f'+'q'; out of queued deletes 'f'+'q'.
	batch := p.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(frontierKey(url), packFrontierEntry(entry), nil); err != nil {
		return err
	}
	switch oldStatus {
	case FrontierStatusQueued:
		if err := batch.Delete(frontierStatusIndexKey('q', entry.Host, url), nil); err != nil {
			return err
		}
	case FrontierStatusInFlight:
		if err := batch.Delete(frontierStatusIndexKey('i', entry.Host, url), nil); err != nil {
			return err
		}
	}
	switch newStatus {
	case FrontierStatusQueued:
		if err := batch.Set(frontierStatusIndexKey('q', entry.Host, url), nil, nil); err != nil {
			return err
		}
	case FrontierStatusInFlight:
		if err := batch.Set(frontierStatusIndexKey('i', entry.Host, url), nil, nil); err != nil {
			return err
		}
	}
	return batch.Commit(p.writeOpts)
}

// GetFrontierStats walks the frontier and tallies by status. O(N); iter
// 210 maintains per-status counters in 'm' for O(1) access at scale.
// Returns the canonical store.FrontierStats shape (Errored, not Error)
// for parity with the SQLite Store's same-named method.
func (p *PebbleStore) GetFrontierStats(ctx context.Context) (FrontierStats, error) {
	if err := ctx.Err(); err != nil {
		return FrontierStats{}, err
	}
	lo := []byte{famFrontier, 'u'}
	hi := []byte{famFrontier, 'u' + 1}
	it, err := p.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return FrontierStats{}, err
	}
	defer it.Close()
	var s FrontierStats
	for valid := it.First(); valid; valid = it.Next() {
		val, err := it.ValueAndErr()
		if err != nil {
			return s, err
		}
		if len(val) == 0 {
			continue
		}
		switch FrontierStatus(val[0]) {
		case FrontierStatusQueued:
			s.Queued++
		case FrontierStatusInFlight:
			s.InFlight++
		case FrontierStatusDone:
			s.Done++
		case FrontierStatusError:
			s.Errored++
		}
	}
	return s, nil
}

// CountQueuedPerHost returns a host → queued-URL-count map for the given
// hosts. Iter 211 — Pebble parity for iter-195 SQLite primitive. Used by
// crawler.enqueueLinks to enforce the per-host enqueue cap; one
// prefix-count per host against the 'f'+'q' secondary index.
//
// Hosts with zero queued URLs simply don't appear in the result map.
func (p *PebbleStore) CountQueuedPerHost(ctx context.Context, hosts []string) (map[string]int, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make(map[string]int, len(hosts))
	for _, host := range hosts {
		if host == "" {
			continue
		}
		// Prefix bound: 'f' + 'q' + host + 0x00 .. 'f' + 'q' + host + 0x01
		lo := make([]byte, 2+len(host)+1)
		lo[0] = famFrontier
		lo[1] = 'q'
		copy(lo[2:], host)
		lo[2+len(host)] = 0x00
		hi := append([]byte{}, lo...)
		hi[2+len(host)] = 0x01
		// Iter 221: per-iteration closure so defer it.Close() runs at each
		// host's end, not at the enclosing function's return. Without the
		// closure a `defer` here would stack iterators until function exit
		// (Go defers fire on FUNCTION return, not on loop iteration).
		n, err := func() (int, error) {
			it, err := p.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
			if err != nil {
				return 0, err
			}
			defer it.Close()
			var count int
			for valid := it.First(); valid; valid = it.Next() {
				count++
			}
			return count, nil
		}()
		if err != nil {
			return nil, err
		}
		if n > 0 {
			out[host] = n
		}
	}
	return out, nil
}

// RecrawlURL transitions an existing URL back to queued status, regardless
// of its current state (done / error / even queued). Used by admin
// /recrawl + cosift crawl -refresh. Iter 211 — Pebble parity for the
// SQLite Store's same-named method.
//
// Returns ErrNotFound if the URL was never enqueued.
func (p *PebbleStore) RecrawlURL(ctx context.Context, url string) error {
	return p.transitionFrontier(ctx, url, FrontierStatusQueued, "")
}

// RecoverInFlight moves all in_flight rows back to queued. Called at
// crawler startup to recover work that was claimed but not completed
// before a previous crash.
func (p *PebbleStore) RecoverInFlight(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	lo := []byte{famFrontier, 'u'}
	hi := []byte{famFrontier, 'u' + 1}
	it, err := p.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return err
	}
	defer it.Close()
	batch := p.db.NewBatch()
	defer batch.Close()
	for valid := it.First(); valid; valid = it.Next() {
		val, err := it.ValueAndErr()
		if err != nil {
			return err
		}
		entry, err := unpackFrontierEntry(val)
		if err != nil {
			continue
		}
		if entry.Status != FrontierStatusInFlight {
			continue
		}
		// Extract URL from primary key ('f' + 'u' + url) — defensive copy
		// because Pebble reuses the iterator's underlying buffer.
		urlBytes := append([]byte{}, it.Key()[2:]...)
		url := string(urlBytes)

		entry.Status = FrontierStatusQueued
		key := append([]byte{}, it.Key()...)
		if err := batch.Set(key, packFrontierEntry(entry), nil); err != nil {
			return err
		}
		// Iter 210: rebuild secondary indexes for the transition.
		if err := batch.Delete(frontierStatusIndexKey('i', entry.Host, url), nil); err != nil {
			return err
		}
		if err := batch.Set(frontierStatusIndexKey('q', entry.Host, url), nil, nil); err != nil {
			return err
		}
	}
	return batch.Commit(p.writeOpts)
}

// readDocTermsLocked reads the 'g' family entry for docID under p.mu.
// Returns an empty slice with no error when no prior entry exists.
func (p *PebbleStore) readDocTermsLocked(docID int64) ([]int64, error) {
	val, closer, err := p.db.Get(docTermsKey(docID))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return unpackDocTerms(val)
}

// GetTermInfo looks up a term's (ID, DocFreq) by term string. ok=false
// when the term has never been indexed.
func (p *PebbleStore) GetTermInfo(ctx context.Context, term string) (TermInfo, bool, error) {
	if err := ctx.Err(); err != nil {
		return TermInfo{}, false, err
	}
	return p.getTermInfoLocked(term)
}

func (p *PebbleStore) getTermInfoLocked(term string) (TermInfo, bool, error) {
	val, closer, err := p.db.Get(termKey(term))
	if errors.Is(err, pebble.ErrNotFound) {
		return TermInfo{}, false, nil
	}
	if err != nil {
		return TermInfo{}, false, err
	}
	defer closer.Close()
	return unpackTermInfo(val)
}

func (p *PebbleStore) postingExistsLocked(termID, docID int64) (bool, error) {
	_, closer, err := p.db.Get(postingKey(termID, docID))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_ = closer.Close()
	return true, nil
}

// PostingEntry is one (docID, tf, docLen) tuple returned by IteratePostings.
// Iter 207: docLen moved INSIDE the posting value (was a separate GetDocLen
// per posting). At N=10k this saves ~25k Pebble Gets per query, the
// dominant remaining cost after the iter-207 GetDocMeta + corpusStats fixes.
type PostingEntry struct {
	DocID  int64
	TF     int64
	DocLen int64
}

// IteratePostings prefix-scans the 'p' family for the given termID and
// invokes fn(docID, tf) for each. Returning false from fn stops the scan.
// Used by BM25.Search to walk a term's posting list without loading it all.
func (p *PebbleStore) IteratePostings(ctx context.Context, termID int64, fn func(PostingEntry) bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	it, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: postingPrefix(termID),
		UpperBound: postingPrefixUpper(termID),
	})
	if err != nil {
		return err
	}
	defer it.Close()
	for valid := it.First(); valid; valid = it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := it.Key()
		if len(key) != 17 {
			continue
		}
		val, err := it.ValueAndErr()
		if err != nil {
			return err
		}
		// Iter 207 format: 16 bytes (tf, docLen). Iter 201 legacy 8-byte tf
		// is no longer valid; fresh stores after iter-207 commit only.
		if len(val) != 16 {
			continue
		}
		entry := PostingEntry{
			DocID:  int64(binary.BigEndian.Uint64(key[9:])),
			TF:     int64(binary.BigEndian.Uint64(val[0:8])),
			DocLen: int64(binary.BigEndian.Uint64(val[8:16])),
		}
		if !fn(entry) {
			return nil
		}
	}
	return nil
}

// readDocLenLocked is the unlocked variant used inside IndexDocument's
// existing p.mu critical section.
func (p *PebbleStore) readDocLenLocked(docID int64) (int64, bool, error) {
	val, closer, err := p.db.Get(docLenKey(docID))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	defer closer.Close()
	if len(val) != 8 {
		return 0, false, fmt.Errorf("malformed doc_len: len=%d", len(val))
	}
	return int64(binary.BigEndian.Uint64(val)), true, nil
}

// readMetaInt64Locked fetches an int64 counter under the 'm' family.
// Missing key returns 0 with no error — counters bootstrap to zero.
func (p *PebbleStore) readMetaInt64Locked(name string) (int64, bool) {
	val, closer, err := p.db.Get(metaKey(name))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false
	}
	if err != nil {
		return 0, false
	}
	defer closer.Close()
	if len(val) != 8 {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(val)), true
}

// CorpusStats returns (sum_doc_len, indexed_docs) in O(1) via the running
// counters maintained by IndexDocument. Iter 207 — replaces the per-query
// O(N) scan over the 'l' family that the iter-206 bench surfaced as
// PebbleBM25.Search's dominant cost.
func (p *PebbleStore) CorpusStats(ctx context.Context) (sumLen int64, count int64, err error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	sumLen, _ = p.readMetaInt64Locked("sum_doc_len")
	count, _ = p.readMetaInt64Locked("indexed_docs")
	return sumLen, count, nil
}

// DomainCount is the (host, doc count) tuple returned by TopDomains.
type DomainCount struct {
	Host  string `json:"host"`
	Count int    `json:"count"`
}

// TopDomains prefix-scans the 'h' family (which holds host -> docID
// mappings, one entry per indexed doc) and returns the top-N hosts by
// count, sorted desc. Linear in the number of indexed docs but very fast
// on Pebble (key-only scan, no value decode). Iter 405.
func (p *PebbleStore) TopDomains(ctx context.Context, topN int) ([]DomainCount, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if topN <= 0 {
		topN = 20
	}
	it, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{famHost},
		UpperBound: []byte{famHost + 1},
	})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	counts := make(map[string]int, 256)
	for valid := it.First(); valid; valid = it.Next() {
		k := it.Key()
		// Key layout: 'h' + host + '\0' + 8-byte ID. Find the null.
		if len(k) < 1+1+8 {
			continue
		}
		hostBytes := k[1:]
		// Trim trailing 8-byte id + 1-byte separator.
		if len(hostBytes) <= 9 {
			continue
		}
		host := string(hostBytes[:len(hostBytes)-9])
		counts[host]++
	}
	out := make([]DomainCount, 0, len(counts))
	for h, c := range counts {
		out = append(out, DomainCount{Host: h, Count: c})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Host < out[j].Host
	})
	if len(out) > topN {
		out = out[:topN]
	}
	return out, nil
}

// SumDocLengths scans the 'l' family and returns (total doc_len, count).
// Used by BM25 to compute avg_doc_len for length normalization. O(N) over
// indexed docs — fine at the v0 scale; a running-counter optimization in
// the 'm' family lands when this becomes a hot spot.
func (p *PebbleStore) SumDocLengths(ctx context.Context) (int64, int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	it, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{famDocLen},
		UpperBound: []byte{famDocLen + 1},
	})
	if err != nil {
		return 0, 0, err
	}
	defer it.Close()
	var total, count int64
	for valid := it.First(); valid; valid = it.Next() {
		val, err := it.ValueAndErr()
		if err != nil {
			return 0, 0, err
		}
		if len(val) != 8 {
			continue
		}
		total += int64(binary.BigEndian.Uint64(val))
		count++
	}
	return total, count, nil
}

// GetDocLen returns the indexed doc_len (raw token count) for docID, or
// (0, false) if no postings have been written for this doc.
func (p *PebbleStore) GetDocLen(ctx context.Context, docID int64) (int64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	val, closer, err := p.db.Get(docLenKey(docID))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	defer closer.Close()
	if len(val) != 8 {
		return 0, false, fmt.Errorf("malformed doc_len: len=%d", len(val))
	}
	return int64(binary.BigEndian.Uint64(val)), true, nil
}

// nextTermID atomically allocates the next term ID. Called under p.mu.
func (p *PebbleStore) nextTermID() int64 {
	val, closer, err := p.db.Get(metaKey("next_term_id"))
	var current int64
	if !errors.Is(err, pebble.ErrNotFound) && err == nil {
		if len(val) == 8 {
			current = int64(binary.BigEndian.Uint64(val))
		}
		_ = closer.Close()
	}
	next := current + 1
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(next))
	// Persist immediately; this is called inside an unflushed batch but the
	// counter race with concurrent IndexDocument is held off by p.mu above.
	_ = p.db.Set(metaKey("next_term_id"), buf, nil)
	return next
}

func packTermInfo(t TermInfo) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[0:8], uint64(t.ID))
	binary.BigEndian.PutUint64(b[8:16], uint64(t.DocFreq))
	return b
}

func unpackTermInfo(b []byte) (TermInfo, bool, error) {
	if len(b) != 16 {
		return TermInfo{}, false, fmt.Errorf("malformed term value: len=%d", len(b))
	}
	return TermInfo{
		ID:      int64(binary.BigEndian.Uint64(b[0:8])),
		DocFreq: int64(binary.BigEndian.Uint64(b[8:16])),
	}, true, nil
}
