package community

import (
	"context"
	"sync"
)

const (
	dispatchBatch = 20
	turnsRemember = 10000
	turnsScan     = 1000
)

// turnOrder remembers when each account last had a job taken, so the dispatch loop serves accounts in rotation.
type turnOrder struct {
	mu   sync.Mutex
	seq  uint64
	last map[string]uint64
}

func (t *turnOrder) pick(candidates []job) job {
	t.mu.Lock()
	defer t.mu.Unlock()
	best := 0
	for i, c := range candidates {
		if t.last[c.userID] < t.last[candidates[best].userID] {
			best = i
		}
	}
	if t.last == nil || len(t.last) >= turnsRemember {
		t.last = map[string]uint64{}
	}
	t.seq++
	t.last[candidates[best].userID] = t.seq
	return candidates[best]
}

// nextJob takes the oldest due job of the account served longest ago; one account's backlog delays another's job by one job at most.
func (s *Server) nextJob(ctx context.Context) (job, bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,url,attempts,COALESCE(user_id,'') FROM (
SELECT id,url,attempts,user_id,next_attempt,created_at,ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY next_attempt,created_at,id) AS rn
FROM submissions WHERE status IN ('pending','config_error') AND next_attempt<=?) WHERE rn=1 ORDER BY next_attempt,created_at,id LIMIT ?`, s.now().Unix(), turnsScan)
	if err != nil {
		return job{}, false, err
	}
	var first []job
	for rows.Next() {
		var j job
		if err := rows.Scan(&j.id, &j.url, &j.attempts, &j.userID); err != nil {
			rows.Close()
			return job{}, false, err
		}
		first = append(first, j)
	}
	err = rows.Err()
	rows.Close()
	if err != nil || len(first) == 0 {
		return job{}, false, err
	}
	return s.turns.pick(first), true, nil
}
