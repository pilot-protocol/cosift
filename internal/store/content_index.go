package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/cockroachdb/pebble"
)

// The content-SHA index maps Document.ContentSHA to the documents that carried
// it when written. Entries are never removed: a reader accepts one only if
// that document still exists and still has the hash (see HasLiveContent), so a
// delete, purge or changed recrawl cannot leave an entry that vouches for gone
// content.
const metaContentSHAComplete = "content_sha_complete"

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
