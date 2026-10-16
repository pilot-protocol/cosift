package community

import (
	"container/list"
	"context"
	"database/sql"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

const (
	termsVersion  = 1
	joinCacheKeys = 10000
	joinCacheAge  = 60 * time.Second
	joinReadWait  = 10 * time.Second
)

type workMember struct{ cid string }

type workRefusal struct {
	status int
	code   string
}

// joinCache keeps one account's join state for joinCacheAge; a full cache drops its oldest entry, never refuses a new one.
type joinCache struct {
	mu    sync.Mutex
	max   int
	keys  map[string]*list.Element
	order *list.List
}

type joinEntry struct {
	uid   string
	state sharedaccount.JoinState
	until time.Time
}

func newJoinCache(max int) *joinCache {
	return &joinCache{max: max, keys: map[string]*list.Element{}, order: list.New()}
}

func (c *joinCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.keys)
}

func (c *joinCache) get(uid string, now time.Time) (sharedaccount.JoinState, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.keys[uid]
	if !ok || !e.Value.(*joinEntry).until.After(now) {
		return sharedaccount.JoinState{}, false
	}
	return e.Value.(*joinEntry).state, true
}

func (c *joinCache) put(uid string, state sharedaccount.JoinState, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.keys[uid]; ok {
		c.order.Remove(e)
		delete(c.keys, uid)
	}
	for len(c.keys) >= c.max {
		oldest := c.order.Front()
		delete(c.keys, oldest.Value.(*joinEntry).uid)
		c.order.Remove(oldest)
	}
	c.keys[uid] = c.order.PushBack(&joinEntry{uid: uid, state: state, until: now.Add(joinCacheAge)})
}

// joinGate decides whether the account may fetch work: it must have joined, not be banned, and have accepted the current terms.
func (s *Server) joinGate(ctx context.Context, userID string) (workMember, *workRefusal) {
	var uid string
	err := s.db.QueryRowContext(ctx, `SELECT uid FROM shared_identities WHERE user_id=?`, userID).Scan(&uid)
	if errors.Is(err, sql.ErrNoRows) {
		return workMember{}, &workRefusal{http.StatusForbidden, "not_joined"}
	}
	if err != nil {
		return workMember{}, &workRefusal{http.StatusServiceUnavailable, "contributors_unavailable"}
	}
	now := s.now()
	state, cached := s.work.joins.get(uid, now)
	if !cached {
		readCtx, cancel := context.WithTimeout(ctx, joinReadWait)
		state, err = s.work.source.JoinState(readCtx, uid)
		cancel()
		if err != nil {
			return workMember{}, &workRefusal{http.StatusServiceUnavailable, "contributors_unavailable"}
		}
		s.work.joins.put(uid, state, now)
	}
	switch {
	case state.CID == "":
		return workMember{}, &workRefusal{http.StatusForbidden, "not_joined"}
	case state.Banned:
		return workMember{}, &workRefusal{http.StatusForbidden, "banned"}
	case state.TermsVersion < termsVersion:
		return workMember{}, &workRefusal{http.StatusForbidden, "terms_outdated"}
	}
	return workMember{cid: state.CID}, nil
}
