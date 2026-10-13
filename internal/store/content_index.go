package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
)

// The content-SHA index maps Document.ContentSHA to the documents that carried
// it when written. Entries are never removed: a reader accepts one only if
// that document still exists and still has the hash (see HasLiveContent), so a
// delete, purge or changed recrawl cannot leave an entry that vouches for gone
// content.
const (
	metaContentSHAComplete = "content_sha_complete"
	metaContentSHACursor   = "content_sha_cursor"
)

// contentSHABatch is how many documents one backfill commit covers.
var contentSHABatch = 2000

func contentSHAKey(sha []byte, docID int64) []byte {
	k := make([]byte, 1+sha256.Size+8)
	k[0] = famContentSHA
	copy(k[1:], sha)
	binary.BigEndian.PutUint64(k[1+sha256.Size:], uint64(docID))
	return k
}

func setContentSHA(batch *pebble.Batch, sha []byte, docID int64) error {
	if len(sha) != sha256.Size {
		return nil
	}
	return batch.Set(contentSHAKey(sha, docID), nil, nil)
}

// HasLiveContent reports whether a document other than excludeDocID currently
// stores text with this SHA-256. Pass 0 to exclude nothing.
func (p *PebbleStore) HasLiveContent(ctx context.Context, sha []byte, excludeDocID int64) (bool, error) {
	if len(sha) != sha256.Size {
		return false, nil
	}
	prefix := contentSHAKey(sha, 0)[:1+sha256.Size]
	it, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(append([]byte{}, prefix...), bytes.Repeat([]byte{0xFF}, 9)...),
	})
	if err != nil {
		return false, err
	}
	defer it.Close()
	for valid := it.First(); valid; valid = it.Next() {
		key := it.Key()
		if len(key) != 1+sha256.Size+8 {
			continue
		}
		id := int64(binary.BigEndian.Uint64(key[1+sha256.Size:]))
		if id == excludeDocID {
			continue
		}
		doc, err := p.GetDocByID(ctx, id)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return false, err
		}
		if bytes.Equal(doc.ContentSHA, sha) {
			return true, nil
		}
	}
	return false, it.Error()
}

// ContentIndexComplete reports whether a backfill has indexed every document
// that predates the index. Until then HasLiveContent can miss older content.
func (p *PebbleStore) ContentIndexComplete(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	val, closer, err := p.db.Get(metaKey(metaContentSHAComplete))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer closer.Close()
	return len(val) == 1 && val[0] == 1, nil
}

// ContentSHABackfill reports a backfill pass. Scanned and Indexed count this
// pass only; ResumedFrom is the last document ID an earlier pass finished.
type ContentSHABackfill struct {
	Scanned         int64
	Indexed         int64
	LastID          int64
	MaxID           int64
	ResumedFrom     int64
	AlreadyComplete bool
}

// BackfillContentSHA indexes every stored document, resuming after the last
// committed batch of an interrupted pass, and marks the index complete when it
// finishes. A finished index is left alone unless rescan is set. progress, if
// non-nil, runs after each committed batch.
func (p *PebbleStore) BackfillContentSHA(ctx context.Context, rescan bool, progress func(ContentSHABackfill)) (ContentSHABackfill, error) {
	res := ContentSHABackfill{MaxID: p.nextID.Load()}
	if rescan {
		batch := p.db.NewBatch()
		defer batch.Close()
		_ = batch.Delete(metaKey(metaContentSHAComplete), nil)
		_ = batch.Delete(metaKey(metaContentSHACursor), nil)
		if err := batch.Commit(p.writeOpts); err != nil {
			return res, err
		}
	} else if done, err := p.ContentIndexComplete(ctx); err != nil {
		return res, err
	} else if done {
		res.AlreadyComplete = true
		return res, nil
	}
	if val, closer, err := p.db.Get(metaKey(metaContentSHACursor)); err == nil {
		if len(val) == 8 {
			res.ResumedFrom = int64(binary.BigEndian.Uint64(val))
		}
		closer.Close()
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return res, err
	}
	res.LastID = res.ResumedFrom

	it, err := p.db.NewIter(&pebble.IterOptions{LowerBound: docKey(res.ResumedFrom + 1), UpperBound: []byte{famDoc + 1}})
	if err != nil {
		return res, err
	}
	defer it.Close()

	batch := p.db.NewBatch()
	defer func() { batch.Close() }()
	pending := 0
	commit := func(final bool) error {
		if final {
			_ = batch.Set(metaKey(metaContentSHAComplete), []byte{1}, nil)
			_ = batch.Delete(metaKey(metaContentSHACursor), nil)
		} else {
			cursor := make([]byte, 8)
			binary.BigEndian.PutUint64(cursor, uint64(res.LastID))
			_ = batch.Set(metaKey(metaContentSHACursor), cursor, nil)
		}
		if err := batch.Commit(p.writeOpts); err != nil {
			return err
		}
		batch.Close()
		batch = p.db.NewBatch()
		pending = 0
		if progress != nil {
			progress(res)
		}
		return nil
	}
	for valid := it.First(); valid; valid = it.Next() {
		if err := ctx.Err(); err != nil {
			if pending > 0 {
				if cerr := commit(false); cerr != nil {
					return res, cerr
				}
			}
			return res, err
		}
		key := it.Key()
		if len(key) != 1+8 {
			continue
		}
		id := int64(binary.BigEndian.Uint64(key[1:]))
		val, err := it.ValueAndErr()
		if err != nil {
			return res, err
		}
		var d Document
		if err := gob.NewDecoder(bytes.NewReader(val)).Decode(&d); err != nil {
			return res, fmt.Errorf("decode doc %d: %w", id, err)
		}
		res.Scanned++
		res.LastID = id
		if len(d.ContentSHA) == sha256.Size {
			if err := setContentSHA(batch, d.ContentSHA, id); err != nil {
				return res, err
			}
			res.Indexed++
		}
		pending++
		if pending >= contentSHABatch {
			if err := commit(false); err != nil {
				return res, err
			}
		}
	}
	if err := it.Error(); err != nil {
		return res, err
	}
	return res, commit(true)
}
