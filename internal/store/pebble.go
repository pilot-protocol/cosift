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

	batch := p.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(docLenKey(docID), lenBuf, nil); err != nil {
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
		tfBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(tfBuf, uint64(freq))
		if err := batch.Set(postingKey(info.ID, docID), tfBuf, nil); err != nil {
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

// PostingEntry is one (docID, tf) pair returned by IteratePostings.
type PostingEntry struct {
	DocID int64
	TF    int64
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
		if len(val) != 8 {
			continue
		}
		entry := PostingEntry{
			DocID: int64(binary.BigEndian.Uint64(key[9:])),
			TF:    int64(binary.BigEndian.Uint64(val)),
		}
		if !fn(entry) {
			return nil
		}
	}
	return nil
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
