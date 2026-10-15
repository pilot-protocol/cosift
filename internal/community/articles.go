package community

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"time"
)

var ulidPattern = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)

const articleTimeout = 15 * time.Second

type ArticleRecord struct {
	Origin       string   `json:"origin"`
	Contributors []string `json:"contributors"`
	Version      int64    `json:"version"`
}

// ErrArticleGone means the engine no longer returns the article (tombstoned, held, rejected or absent).
var ErrArticleGone = errors.New("article is not available")

// ArticleReader reads an article from the engine; any error other than ErrArticleGone is worth retrying.
type ArticleReader interface {
	ArticleByID(ctx context.Context, id string) (ArticleRecord, error)
}

type engineArticles struct{ client *engineClient }

func (e engineArticles) ArticleByID(ctx context.Context, id string) (ArticleRecord, error) {
	if !ulidPattern.MatchString(id) {
		return ArticleRecord{}, fmt.Errorf("invalid article id")
	}
	res, err := e.client.do(ctx, http.MethodGet, "/v1/articles/"+id, nil, articleTimeout)
	if err != nil {
		return ArticleRecord{}, fmt.Errorf("engine unreachable")
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound, http.StatusGone:
		return ArticleRecord{}, ErrArticleGone
	default:
		return ArticleRecord{}, fmt.Errorf("engine returned HTTP %d", res.StatusCode)
	}
	var rec ArticleRecord
	body, err := io.ReadAll(io.LimitReader(res.Body, 2<<20))
	if err != nil || json.Unmarshal(body, &rec) != nil {
		return ArticleRecord{}, fmt.Errorf("engine returned an unreadable article")
	}
	return rec, nil
}
