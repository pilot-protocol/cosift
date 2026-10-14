package work

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/cockroachdb/pebble"
)

// The 'W' family: W a <url> attestation record, W t <url sha> pending text, W s <seq> settled
// event, W k <url sha> park, W m <name> counters and cursors.
var (
	prefixRecord = []byte("Wa")
	prefixText   = []byte("Wt")
	prefixEvent  = []byte("Ws")
	prefixPark   = []byte("Wk")
	keyPoolCur   = []byte("Wmpool_cursor")
	keySeq       = []byte("Wmsettled_seq")
	keyModerated = []byte("Wmmoderations")
)

func urlSHA(url string) string {
	sum := sha256.Sum256([]byte(url))
	return hex.EncodeToString(sum[:])
}

func keyOf(prefix []byte, rest string) []byte { return append(append([]byte{}, prefix...), rest...) }

func keyEvent(seq uint64) []byte {
	k := append([]byte{}, prefixEvent...)
	return binary.BigEndian.AppendUint64(k, seq)
}

// upperBound is the exclusive end of the keys that start with prefix.
func upperBound(prefix []byte) []byte {
	end := append([]byte{}, prefix...)
	end[len(end)-1]++
	return end
}

func (s *Service) get(key []byte) ([]byte, error) {
	v, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return append([]byte{}, v...), nil
}

func (s *Service) getRecord(url string) (*Record, error) {
	v, err := s.get(keyOf(prefixRecord, url))
	if err != nil || v == nil {
		return nil, err
	}
	var r Record
	if err := json.Unmarshal(v, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (s *Service) getText(sha string) (*Text, error) {
	v, err := s.get(keyOf(prefixText, sha))
	if err != nil || v == nil {
		return nil, err
	}
	var t Text
	if err := json.Unmarshal(v, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// forEachRecord visits every attestation record; fn returns false to stop.
func (s *Service) forEachRecord(fn func(url string, r *Record) bool) error {
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefixRecord, UpperBound: upperBound(prefixRecord)})
	if err != nil {
		return err
	}
	defer it.Close()
	for ok := it.First(); ok; ok = it.Next() {
		var r Record
		if err := json.Unmarshal(it.Value(), &r); err != nil {
			continue
		}
		if !fn(string(it.Key()[len(prefixRecord):]), &r) {
			break
		}
	}
	return it.Error()
}

func (s *Service) isParked(sha string) bool {
	v, err := s.get(keyOf(prefixPark, sha))
	if err != nil || v == nil {
		return false
	}
	var p struct {
		Until time.Time `json:"until"`
	}
	return json.Unmarshal(v, &p) == nil && s.now().Before(p.Until)
}

// txn stages one transition; Commit makes it durable and only then moves the in-memory counters.
type txn struct {
	s        *Service
	b        *pebble.Batch
	seq      uint64
	textAdd  []string
	textDrop []string
	events   []Event
}

func (s *Service) begin() *txn { return &txn{s: s, b: s.db.NewBatch(), seq: s.lastSeq} }

func (t *txn) putRecord(url string, r *Record) error {
	r.UpdatedAt = t.s.now().UTC()
	v, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return t.b.Set(keyOf(prefixRecord, url), v, nil)
}

func (t *txn) delRecord(url string) error { return t.b.Delete(keyOf(prefixRecord, url), nil) }

func (t *txn) putText(sha string, x *Text) error {
	v, err := json.Marshal(x)
	if err != nil {
		return err
	}
	t.textAdd = append(t.textAdd, sha)
	return t.b.Set(keyOf(prefixText, sha), v, nil)
}

func (t *txn) delText(sha string) error {
	t.textDrop = append(t.textDrop, sha)
	return t.b.Delete(keyOf(prefixText, sha), nil)
}

func (t *txn) park(sha string, until time.Time) error {
	v, _ := json.Marshal(struct {
		Until time.Time `json:"until"`
	}{until.UTC()})
	return t.b.Set(keyOf(prefixPark, sha), v, nil)
}

// event appends a settled event for the workers; an empty list appends nothing.
func (t *txn) event(urlSha, verdict string, workers []string, novel bool) error {
	if len(workers) == 0 {
		return nil
	}
	t.seq++
	ev := Event{Seq: t.seq, URLSha: urlSha, Verdict: verdict, Workers: workers, ContentNovel: novel, At: t.s.now().UTC()}
	v, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	t.events = append(t.events, ev)
	return errors.Join(t.b.Set(keyEvent(ev.Seq), v, nil), t.b.Set(keySeq, binary.BigEndian.AppendUint64(nil, ev.Seq), nil))
}

func (t *txn) commit() error {
	defer t.b.Close()
	if err := t.b.Commit(pebble.Sync); err != nil {
		return err
	}
	t.s.lastSeq = t.seq
	for _, sha := range t.textAdd {
		t.s.texts[sha] = struct{}{}
	}
	for _, sha := range t.textDrop {
		delete(t.s.texts, sha)
	}
	for _, ev := range t.events {
		t.s.metrics.settled.add(ev.Verdict)
	}
	return nil
}

func (t *txn) discard() { t.b.Close() }
