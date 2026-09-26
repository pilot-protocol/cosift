package articles

import (
	"encoding/json"
	"maps"
	"sort"
	"sync"
	"time"
)

const counterDays = 14

type countersJSON struct {
	V    int            `json:"v"`
	Days map[string]int `json:"days"`
}

// counters holds the distinct-reader counts per article and UTC day. The
// reader sets of the current day live only here and are never written.
type counters struct {
	mu    sync.Mutex
	day   string
	base  map[string]map[string]int
	sets  map[string]map[string]struct{}
	dirty map[string]bool
}

func newCounters() *counters {
	return &counters{base: map[string]map[string]int{}, sets: map[string]map[string]struct{}{}, dirty: map[string]bool{}}
}

func utcDay(t time.Time) string { return t.UTC().Format("2006-01-02") }

func (c *counters) load(days map[string]map[string]int, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.day = utcDay(now)
	c.base = map[string]map[string]int{}
	for id, d := range days {
		c.base[id] = maps.Clone(d)
	}
	c.sets = map[string]map[string]struct{}{}
	c.dirty = map[string]bool{}
}

// rollLocked folds the finished day's sets into the persisted counts.
func (c *counters) rollLocked(now time.Time) {
	today := utcDay(now)
	if today == c.day {
		return
	}
	for id, set := range c.sets {
		if c.base[id] == nil {
			c.base[id] = map[string]int{}
		}
		c.base[id][c.day] += len(set)
		c.dirty[id] = true
	}
	c.sets = map[string]map[string]struct{}{}
	c.day = today
	cutoff := utcDay(now.AddDate(0, 0, -(counterDays - 1)))
	for id, d := range c.base {
		for day := range d {
			if day < cutoff {
				delete(d, day)
				c.dirty[id] = true
			}
		}
	}
}

func (c *counters) add(id, reader string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rollLocked(now)
	set := c.sets[id]
	if set == nil {
		set = map[string]struct{}{}
		c.sets[id] = set
	}
	if _, seen := set[reader]; !seen {
		set[reader] = struct{}{}
		c.dirty[id] = true
	}
}

// days returns id's counts by day, the current day's set included.
func (c *counters) days(id string, now time.Time) map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rollLocked(now)
	return c.daysLocked(id)
}

func (c *counters) daysLocked(id string) map[string]int {
	out := maps.Clone(c.base[id])
	if n := len(c.sets[id]); n > 0 {
		if out == nil {
			out = map[string]int{}
		}
		out[c.day] += n
	}
	return out
}

// readers7d sums the last 7 UTC days, today included.
func (c *counters) readers7d(id string, now time.Time) int {
	d := c.days(id, now)
	cutoff := utcDay(now.AddDate(0, 0, -6))
	n := 0
	for day, v := range d {
		if day >= cutoff {
			n += v
		}
	}
	return n
}

func (c *counters) drop(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.base, id)
	delete(c.sets, id)
	delete(c.dirty, id)
}

// flushLocked writes the changed counters in one batch; callers hold writeMu.
func (s *Store) flushLocked() error {
	c := s.counters
	c.mu.Lock()
	c.rollLocked(s.now())
	ids := make([]string, 0, len(c.dirty))
	for id := range c.dirty {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make(map[string]map[string]int, len(ids))
	for _, id := range ids {
		out[id] = c.daysLocked(id)
		delete(c.dirty, id)
	}
	c.mu.Unlock()
	if len(ids) == 0 {
		return nil
	}
	var live []string
	s.view(func(ix *index) {
		for _, id := range ids {
			if r := ix.recs[id]; r != nil && !r.Residue {
				live = append(live, id)
			} else {
				c.drop(id)
			}
		}
	})
	b := s.db.NewBatch()
	for _, id := range live {
		v, _ := json.Marshal(countersJSON{V: 1, Days: nonNilMap(out[id])})
		_ = b.Set(countersKey(id), v, nil)
	}
	if err := s.commit(b); err != nil {
		c.mu.Lock()
		for _, id := range live {
			c.dirty[id] = true
		}
		c.mu.Unlock()
		return err
	}
	return nil
}

func nonNilMap(m map[string]int) map[string]int {
	if m == nil {
		return map[string]int{}
	}
	return m
}
