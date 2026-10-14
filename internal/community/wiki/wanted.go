package wiki

import (
	"context"
	"net/http"
	"sync"
	"time"
)

const (
	wantedLimit = 200
	boardBound  = hubBound
)

var wantedTmpl = parse("wanted.html")

// board is the rendered wanted page and when it was fetched; one engine fetch serves every concurrent miss.
type board struct {
	mu      sync.Mutex
	body    []byte
	fetched time.Time
}

type wantedItem struct{ Title, Vertical, Slug string }

type wantedView struct {
	Head               head
	Stubs, Thin        []wantedItem
	MoreStubs, MoreLow bool
	Limit              int
}

// wantedItems keeps the items that pass the board's own test; the rest are dropped, not shown.
func wantedItems(list engineList, status, tier string) []wantedItem {
	var out []wantedItem
	for _, it := range list.Items {
		title := cleanLine(it.Title)
		if it.Status != status || it.QualityTier != tier || !idPattern.MatchString(it.ID) || !pageSlug(it.Slug) || title == "" {
			continue
		}
		v := wantedItem{Title: title, Vertical: verticals[it.Vertical]}
		if status == "published" {
			v.Slug = it.Slug
		}
		out = append(out, v)
	}
	return out
}

// renderWanted fetches both lists; a stub shows its generic title and vertical only, never an id or a slug.
func (w *Wiki) renderWanted() ([]byte, string) {
	ctx := context.Background()
	stubs, code := w.engine.board(ctx, "status=pending")
	if code != "" {
		return nil, code
	}
	thin, code := w.engine.board(ctx, "status=published&tier=thin")
	if code != "" {
		return nil, code
	}
	h := w.head("Wanted articles")
	h.Canonical = w.canonical("/wiki/wanted")
	h.Description = "Subjects Cosift has no article on yet, and articles that can be improved."
	return execute(wantedTmpl, wantedView{Head: h, Stubs: wantedItems(stubs, "pending", ""), Thin: wantedItems(thin, "published", "thin"),
		MoreStubs: stubs.NextCursor != nil, MoreLow: thin.NextCursor != nil, Limit: wantedLimit}), ""
}

func (w *Wiki) serveWanted(rw http.ResponseWriter, r *http.Request) {
	body, fetched, ok := w.wantedBoard(rw, r)
	if ok {
		w.write(rw, http.StatusOK, htmlType, body, maxAge(boardBound, w.now().Sub(fetched)))
	}
}

// wantedBoard returns the cached board, or fetches it for the first miss while the others wait; on false it has answered.
func (w *Wiki) wantedBoard(rw http.ResponseWriter, r *http.Request) ([]byte, time.Time, bool) {
	w.board.mu.Lock()
	defer w.board.mu.Unlock()
	if w.board.body == nil || w.now().Sub(w.board.fetched) >= boardBound {
		if !w.charge(rw, r, false) {
			return nil, time.Time{}, false
		}
		body, code := w.renderWanted()
		if code != "" {
			w.warn(code)
			w.unavailableNow(rw)
			return nil, time.Time{}, false
		}
		w.board.body, w.board.fetched = body, w.now()
	}
	return w.board.body, w.board.fetched, true
}
