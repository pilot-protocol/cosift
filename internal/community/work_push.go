package community

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

const (
	pushRoute  = "/v1/work/push"
	pushBatch  = 100
	pushScan   = 2000
	pushMemory = 24 * time.Hour
)

// pushBlocked offers the pages Teo's check could not fetch for a blocking reason to the work pool, at most pushBatch not offered in the last day.
func (s *Server) pushBlocked(ctx context.Context) error {
	now := s.now()
	if _, err := s.db.ExecContext(ctx, `DELETE FROM work_pushed WHERE pushed_at<=?`, now.Add(-pushMemory).Unix()); err != nil {
		return err
	}
	urls, err := s.pushCandidates(ctx)
	if err != nil || len(urls) == 0 {
		return err
	}
	body, _ := json.Marshal(map[string]any{"urls": urls})
	res, err := s.engine.post(ctx, pushRoute, body, workRouteTimeout)
	if err != nil {
		return fmt.Errorf("POST %s failed", pushRoute)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		log.Printf("community: POST %s returned HTTP %d", pushRoute, res.StatusCode)
		return fmt.Errorf("POST %s returned HTTP %d", pushRoute, res.StatusCode)
	}
	var answer struct {
		Rejected []struct {
			Index int    `json:"index"`
			Rule  string `json:"rule"`
		} `json:"rejected"`
	}
	if json.NewDecoder(io.LimitReader(res.Body, workReplyBytes)).Decode(&answer) != nil {
		return fmt.Errorf("POST %s returned an unreadable body", pushRoute)
	}
	full := map[int]bool{}
	for _, rej := range answer.Rejected {
		full[rej.Index] = full[rej.Index] || rej.Rule == "pool_full"
	}
	for i, u := range urls {
		if full[i] {
			continue
		}
		if _, err := s.db.ExecContext(ctx, `INSERT INTO work_pushed(url_sha,pushed_at) VALUES(?,?) ON CONFLICT(url_sha) DO UPDATE SET pushed_at=excluded.pushed_at`, urlDigest(u), now.Unix()); err != nil {
			return err
		}
	}
	return nil
}

func urlDigest(u string) string {
	sum := sha256.Sum256([]byte(u))
	return hex.EncodeToString(sum[:])
}

func (s *Server) pushCandidates(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT url FROM submissions WHERE fail_class=? AND status IN ('pending','unverified') GROUP BY url ORDER BY min(created_at),url LIMIT ?`, failFetch, pushScan)
	if err != nil {
		return nil, err
	}
	var all []string
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			rows.Close()
			return nil, err
		}
		all = append(all, u)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	var urls []string
	for _, u := range all {
		if len(urls) == pushBatch {
			break
		}
		var found int
		switch err := s.db.QueryRowContext(ctx, `SELECT 1 FROM work_pushed WHERE url_sha=?`, urlDigest(u)).Scan(&found); {
		case err == nil:
		case errors.Is(err, sql.ErrNoRows):
			urls = append(urls, u)
		default:
			return nil, err
		}
	}
	return urls, nil
}
