package work

import (
	"encoding/json"
	"time"

	"github.com/cockroachdb/pebble"
)

var pebbleSync = pebble.Sync

// eventsAfter reads settled events with a sequence above after, oldest first.
func (s *Service) eventsAfter(after uint64, limit int) []Event {
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: keyEvent(after + 1), UpperBound: upperBound(prefixEvent)})
	if err != nil {
		return nil
	}
	defer it.Close()
	var out []Event
	for ok := it.First(); ok && len(out) < limit; ok = it.Next() {
		var ev Event
		if json.Unmarshal(it.Value(), &ev) == nil {
			out = append(out, ev)
		}
	}
	return out
}

// parksPast stages the deletion of every park that has ended.
func (s *Service) parksPast(now time.Time, b *pebble.Batch) {
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefixPark, UpperBound: upperBound(prefixPark)})
	if err != nil {
		return
	}
	defer it.Close()
	for ok := it.First(); ok; ok = it.Next() {
		var p struct {
			Until time.Time `json:"until"`
		}
		if json.Unmarshal(it.Value(), &p) != nil || !now.Before(p.Until) {
			_ = b.Delete(append([]byte{}, it.Key()...), nil)
		}
	}
}

// SettledResult is the broker's feed of finished outcomes.
type SettledResult struct {
	Events            []Event
	Next, LastSeq     uint64
	ModerationCeiling bool
}

// Settled returns up to limit events after the sequence the broker last applied. A cursor beyond the
// last sequence issued means the engine was restored from an older snapshot: the answer is empty.
func (s *Service) Settled(after uint64, limit int) (SettledResult, error) {
	_, reached := s.ceiling(s.now())
	s.mu.Lock()
	last := s.lastSeq
	s.mu.Unlock()
	res := SettledResult{Events: []Event{}, Next: after, LastSeq: last, ModerationCeiling: reached}
	if after > last {
		return res, nil
	}
	if evs := s.eventsAfter(after, limit); len(evs) > 0 {
		res.Events, res.Next = evs, evs[len(evs)-1].Seq
	}
	return res, nil
}
