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
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

// PebbleStore is a Pebble-backed implementation of the hot Store API surface.
// Currently supports Document CRUD; postings + frontier come in follow-up iters.
type PebbleStore struct {
	db     *pebble.DB
	nextID atomic.Int64
	mu     sync.Mutex // serializes the rare URL→ID race during Upsert
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
)

// OpenPebble opens (or creates) a Pebble store at path. On open it
// reconciles the persistent next_doc_id counter so concurrent Upserts pick
// up unique IDs from the right baseline.
func OpenPebble(path string) (*PebbleStore, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("pebble.Open(%s): %w", path, err)
	}
	p := &PebbleStore{db: db}
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
	if err := batch.Commit(pebble.Sync); err != nil {
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
	return p.db.Set(vectorMetaKey(), blob, pebble.Sync)
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
	return p.db.Set(vectorNodeKey(nodeID), blob, pebble.Sync)
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
	for term, freq := range tf {
		info, ok, err := p.getTermInfoLocked(term)
		if err != nil {
			return err
		}
		if !ok {
			info.ID = p.nextTermID()
			info.DocFreq = 1
		} else {
			// Re-indexing the same doc: don't bump doc_freq if this docID
			// already has a posting for the term. Simple approach for now:
			// look up the existing posting; if present, leave doc_freq alone.
			if exists, err := p.postingExistsLocked(info.ID, docID); err != nil {
				return err
			} else if !exists {
				info.DocFreq++
			}
		}
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
	return batch.Commit(pebble.Sync)
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
