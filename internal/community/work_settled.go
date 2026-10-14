package community

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

const (
	settledRoute        = "/v1/work/settled"
	settledPage         = 200
	settledPagesPerTick = 5
	settledEvery        = 10 * time.Second
	settledCursorKey    = "work_settled_seq"
	fetchReward         = 2
)

var (
	urlShaPattern   = regexp.MustCompile(`^[0-9a-f]{64}$`)
	workerPattern   = regexp.MustCompile(`^[0-9a-f]{32}$`)
	settledVerdicts = map[string]bool{"verified": true, "mismatch": true, "rejected": true, "expired": true}
)

type settledEvent struct {
	Seq          int64    `json:"seq"`
	URLSha       string   `json:"url_sha"`
	Verdict      string   `json:"verdict"`
	Workers      []string `json:"workers"`
	ContentNovel bool     `json:"content_novel"`
}

type settledFeed struct {
	Events  []settledEvent `json:"events"`
	Next    int64          `json:"next"`
	LastSeq int64          `json:"last_seq"`
	Ceiling bool           `json:"moderation_ceiling"`
}

// pollSettled applies the engine's settled outcomes after the stored cursor; the engine is read before any write, so no transaction spans the call.
func (s *Server) pollSettled(ctx context.Context) error {
	cursor, err := s.settledCursor(ctx)
	if err != nil {
		return err
	}
	for page := 0; page < settledPagesPerTick; page++ {
		feed, err := s.readSettled(ctx, cursor)
		if err != nil {
			return err
		}
		if feed.Ceiling {
			s.noteCeiling(ceilingWork)
		}
		if cursor > feed.LastSeq {
			if err := s.saveSettledCursor(ctx, 0); err != nil {
				return err
			}
			cursor = 0
			continue
		}
		for _, ev := range feed.Events {
			if err := s.applySettled(ctx, ev); err != nil {
				return err
			}
		}
		if feed.Next > cursor {
			cursor = feed.Next
			if err := s.saveSettledCursor(ctx, cursor); err != nil {
				return err
			}
		}
		if len(feed.Events) < settledPage {
			return nil
		}
	}
	return nil
}

func (s *Server) settledCursor(ctx context.Context) (int64, error) {
	var v string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM settings WHERE key=?`, settledCursorKey).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(v, 10, 64)
}

func (s *Server) saveSettledCursor(ctx context.Context, seq int64) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, settledCursorKey, strconv.FormatInt(seq, 10))
	return err
}

func (s *Server) readSettled(ctx context.Context, after int64) (settledFeed, error) {
	path := fmt.Sprintf("%s?after=%d&limit=%d", settledRoute, after, settledPage)
	res, err := s.engine.do(ctx, http.MethodGet, path, nil, workRouteTimeout)
	if err != nil {
		return settledFeed{}, fmt.Errorf("GET %s failed", settledRoute)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		log.Printf("community: GET %s returned HTTP %d", settledRoute, res.StatusCode)
		return settledFeed{}, fmt.Errorf("GET %s returned HTTP %d", settledRoute, res.StatusCode)
	}
	var feed settledFeed
	if json.NewDecoder(io.LimitReader(res.Body, 4<<20)).Decode(&feed) != nil {
		return settledFeed{}, fmt.Errorf("GET %s returned an unreadable body", settledRoute)
	}
	return feed, nil
}

func (s *Server) applySettled(ctx context.Context, ev settledEvent) error {
	if !urlShaPattern.MatchString(ev.URLSha) || !settledVerdicts[ev.Verdict] {
		return nil
	}
	seen := map[string]bool{}
	for _, worker := range ev.Workers {
		if !workerPattern.MatchString(worker) || seen[worker] {
			continue
		}
		seen[worker] = true
		var userID, cid string
		err := s.db.QueryRowContext(ctx, `SELECT user_id,cid FROM work_workers WHERE worker=?`, worker).Scan(&userID, &cid)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		switch ev.Verdict {
		case "verified":
			s.bumpFetch(cid, fetchVerified, 1)
			if !ev.ContentNovel {
				continue
			}
			credited, err := s.creditFetch(ctx, ev.URLSha, userID)
			if err != nil {
				return err
			}
			if credited {
				s.bumpFetch(cid, fetchCredits, fetchReward)
			}
		case "mismatch":
			s.bumpFetch(cid, fetchMismatch, 1)
		case "rejected":
			s.bumpFetch(cid, fetchRejected, 1)
		}
	}
	return nil
}

// creditFetch pays one page to one account in one statement: the ledger id makes a replay a no-op and the count predicate holds the day's cap.
func (s *Server) creditFetch(ctx context.Context, urlSha, userID string) (bool, error) {
	now := s.now()
	res, err := s.db.ExecContext(ctx, `INSERT INTO credit_ledger(id,user_id,delta,reason,created_at)
SELECT ?,?,?,'fetch_verified',? WHERE (SELECT count(*) FROM credit_ledger WHERE user_id=? AND reason='fetch_verified' AND created_at>=?)<? ON CONFLICT(id) DO NOTHING`,
		"fetch:"+urlSha+":"+userID, userID, fetchReward, now.Unix(), userID, utcDayStart(now).Unix(), s.fetchCap())
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}
