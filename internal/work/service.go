package work

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pilot-protocol/cosift/internal/config"
)

// Options configure a Service.
type Options struct {
	DB     *pebble.DB
	Config config.WorkSettings
	// Frontier turns on the third feeder, the queued frontier rows.
	Frontier bool
	Deps     Deps
	Now      func() time.Time
	Logf     func(format string, args ...any)
}

// Service holds the work state. Every mutation of the 'W' family and of the pool serializes on mu;
// fetches, model calls and indexing run outside it.
type Service struct {
	cfg      config.WorkSettings
	frontier bool
	deps     Deps
	now      func() time.Time
	logf     func(string, ...any)
	db       *pebble.DB

	mu       sync.Mutex
	pool     map[string]*entry
	queues   map[Class]*classQueue
	leases   map[string]*lease
	byWorker map[string]int
	consumed map[string]*consumedLease
	hostNext map[string]time.Time
	parked   map[string]time.Time
	texts    map[string]struct{}
	inflight map[string]struct{}
	lastSeq  uint64

	errCursor      string
	frontierCursor string

	modMu   sync.Mutex
	metrics metrics
	wake    chan struct{}
}

// New opens the work state over db, which must be the engine's Pebble database.
func New(o Options) (*Service, error) {
	if o.DB == nil || o.Deps.Canonical == nil || o.Deps.Allowed == nil || o.Deps.Fetch == nil || o.Deps.Moderate == nil || o.Deps.Index == nil {
		return nil, errors.New("work: incomplete options")
	}
	s := &Service{
		cfg: o.Config, frontier: o.Frontier, deps: o.Deps, now: o.Now, logf: o.Logf, db: o.DB,
		pool: map[string]*entry{}, queues: map[Class]*classQueue{}, leases: map[string]*lease{}, byWorker: map[string]int{},
		consumed: map[string]*consumedLease{}, hostNext: map[string]time.Time{}, parked: map[string]time.Time{},
		texts: map[string]struct{}{}, inflight: map[string]struct{}{}, wake: make(chan struct{}, 1),
	}
	if s.now == nil {
		s.now = time.Now
	}
	if s.logf == nil {
		s.logf = func(string, ...any) {}
	}
	for _, c := range classOrder {
		s.queues[c] = &classQueue{byHost: map[string][]*entry{}}
	}
	if v, err := s.get(keySeq); err != nil {
		return nil, err
	} else if len(v) == 8 {
		s.lastSeq = binary.BigEndian.Uint64(v)
	}
	if v, err := s.get(keyPoolCur); err != nil {
		return nil, err
	} else {
		s.errCursor = string(v)
	}
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefixText, UpperBound: upperBound(prefixText)})
	if err != nil {
		return nil, err
	}
	for ok := it.First(); ok; ok = it.Next() {
		s.texts[string(it.Key()[len(prefixText):])] = struct{}{}
	}
	if err := it.Close(); err != nil {
		return nil, err
	}
	return s, nil
}

// ExtractVersion is the version a lease carries and a submission must echo.
func (s *Service) extractVersion() int { return extractVersion }

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("work: no randomness: %v", err))
	}
	return hex.EncodeToString(b[:])
}

type counter struct {
	mu sync.Mutex
	m  map[string]int64
}

func (c *counter) add(label string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = map[string]int64{}
	}
	c.m[label]++
}

func (c *counter) snapshot() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int64, len(c.m))
	for k, v := range c.m {
		out[k] = v
	}
	return out
}

type metrics struct{ leases, submissions, settled, moderations, refetch counter }
