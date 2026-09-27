// Package wiki serves the community app's public article pages from the
// engine's /v1 article routes over loopback.
package wiki

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const DefaultEngineURL = "http://127.0.0.1:7779"

const (
	refreshEvery = 240 * time.Second
	retryEvery   = 15 * time.Second
	maxWalkPages = 2000
)

var (
	keyPattern    = regexp.MustCompile(`^csk_[0-9a-f]{16}_[A-Z2-7]{52}$`)
	mailtoPattern = regexp.MustCompile(`^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+$`)
	gscPattern    = regexp.MustCompile(`^[A-Za-z0-9_-]{10,100}$`)
	pagePattern   = regexp.MustCompile(`^[1-9][0-9]{0,5}$`)
	sitemapName   = regexp.MustCompile(`^wiki-([1-9][0-9]{0,5})\.xml$`)
)

type Config struct {
	// Public is COSIFT_WIKI_PUBLIC=1; off, nothing is mounted or started.
	Public          bool
	EngineURL       string
	EngineKey       string
	ReportMailto    string
	PublicURL       string
	GAMeasurementID string
	GSCVerification string
	ClientIP        func(*http.Request) string
	// Now is for tests; nil is time.Now.
	Now  func() time.Time
	Logf func(format string, args ...any)
}

// CheckGSC validates COSIFT_GSC_VERIFICATION; empty is allowed.
func CheckGSC(token string) error {
	if token != "" && !gscPattern.MatchString(token) {
		return errors.New("COSIFT_GSC_VERIFICATION must match [A-Za-z0-9_-]{10,100}")
	}
	return nil
}

func engineBase(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return "", errors.New("bad url")
	}
	ip, err := netip.ParseAddr(u.Hostname())
	if err != nil || ip.Zone() != "" || !(ip.Is4() && ip.IsLoopback() || ip == netip.IPv6Loopback()) {
		return "", errors.New("not a loopback IP literal")
	}
	if p := u.Port(); p != "" {
		if n, err := strconv.Atoi(p); err != nil || n < 1 || n > 65535 {
			return "", errors.New("bad port")
		}
	}
	return "http://" + u.Host, nil
}

// Wiki is the page layer: routes, the engine client, the snapshot of the
// published list and the page cache.
type Wiki struct {
	on     bool
	cfg    Config
	engine *engine
	now    func() time.Time
	logf   func(format string, args ...any)

	hubPage     int
	sitemapPage int
	poll        time.Duration

	mu        sync.Mutex
	snap      *snapshot
	removed   map[string]time.Time
	walking   bool
	pages     map[string]*entry
	negatives int
	rendered  map[string][]byte
	inflight  map[string]*flight
	budget    tokens
	swept     time.Time

	ips       ipLimiter
	refreshMu sync.Mutex

	logMu  sync.Mutex
	logged map[string]time.Time

	notFound, gone, unavailable, tooMany []byte
}

// New builds the page layer; with Public off it is dark and validates nothing.
func New(cfg Config) (*Wiki, error) {
	if !cfg.Public {
		return &Wiki{}, nil
	}
	if cfg.EngineURL == "" {
		cfg.EngineURL = DefaultEngineURL
	}
	base, err := engineBase(cfg.EngineURL)
	if err != nil {
		return nil, errors.New("COSIFT_WIKI_ENGINE_URL must be an http URL on a loopback IP literal, with no path")
	}
	if !keyPattern.MatchString(cfg.EngineKey) {
		return nil, errors.New("COSIFT_WIKI_ENGINE_KEY must be set to a csk_ service key when COSIFT_WIKI_PUBLIC=1")
	}
	if len(cfg.ReportMailto) > 254 || !mailtoPattern.MatchString(cfg.ReportMailto) {
		return nil, errors.New("COSIFT_REPORT_MAILTO must be set to a bare email address when COSIFT_WIKI_PUBLIC=1")
	}
	if err := CheckGSC(cfg.GSCVerification); err != nil {
		return nil, err
	}
	if cfg.PublicURL == "" || cfg.ClientIP == nil {
		return nil, errors.New("wiki: public URL and client IP are required")
	}
	cfg.PublicURL = strings.TrimRight(cfg.PublicURL, "/")
	w := &Wiki{on: true, cfg: cfg, engine: newEngine(base, cfg.EngineKey), now: cfg.Now, logf: cfg.Logf,
		hubPage: 200, sitemapPage: 50000, poll: time.Second,
		removed: map[string]time.Time{}, pages: map[string]*entry{}, rendered: map[string][]byte{},
		inflight: map[string]*flight{}, logged: map[string]time.Time{}}
	if w.now == nil {
		w.now = time.Now
	}
	if w.logf == nil {
		w.logf = log.Printf
	}
	w.notFound = w.renderNotice("Article not found", "There is no article at this address.", "")
	w.gone = w.renderNotice("This article was removed", "It is no longer available on Cosift.", "")
	w.unavailable = w.renderNotice("Articles are temporarily unavailable", "Please try again in a few minutes.", "")
	w.tooMany = w.renderNotice("Too many requests", "Please wait a minute and try again.", "")
	w.logf("wiki: public pages on (engine %s)", base)
	return w, nil
}

// Mount registers the page routes; a dark Wiki registers none.
func (w *Wiki) Mount(mux *http.ServeMux) {
	if !w.on {
		return
	}
	mux.HandleFunc("GET /wiki", w.guard(w.serveAZ))
	mux.HandleFunc("GET /wiki/{rest...}", w.guard(w.serveWiki))
	mux.HandleFunc("GET /sitemap.xml", w.guard(w.serveSitemapIndex))
	mux.HandleFunc("GET /sitemaps/{name}", w.guard(w.serveSitemap))
	mux.HandleFunc("GET /robots.txt", w.guard(w.serveRobots))
	mux.HandleFunc("GET /wiki.css", w.asset("wiki.css", "text/css; charset=utf-8"))
	mux.HandleFunc("GET /wiki.js", w.asset("wiki.js", "text/javascript; charset=utf-8"))
}

func (w *Wiki) asset(name, kind string) http.HandlerFunc {
	body, err := assetFS.ReadFile("assets/" + name)
	if err != nil {
		panic(err)
	}
	return func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", kind)
		_, _ = rw.Write(body)
	}
}

// guard refuses any escaped raw path, so a page has one URL.
func (w *Wiki) guard(h http.HandlerFunc) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.EscapedPath(), "%") {
			w.write(rw, http.StatusNotFound, htmlType, w.notFound, -1)
			return
		}
		h(rw, r)
	}
}

const (
	htmlType = "text/html; charset=utf-8"
	xmlType  = "application/xml; charset=utf-8"
)

// write sends a page; maxAge < 0 is no-store.
func (w *Wiki) write(rw http.ResponseWriter, status int, kind string, body []byte, maxAge int) {
	h := rw.Header()
	if maxAge < 0 {
		h.Set("Cache-Control", "no-store")
	} else {
		h.Set("Cache-Control", fmt.Sprintf("public, max-age=%d, s-maxage=%d", maxAge, maxAge))
	}
	h.Set("Content-Type", kind)
	rw.WriteHeader(status)
	_, _ = rw.Write(body)
}

func (w *Wiki) unavailableNow(rw http.ResponseWriter) {
	w.write(rw, http.StatusServiceUnavailable, htmlType, w.unavailable, -1)
}

// charge applies the per-IP limit to a request that missed the cache.
func (w *Wiki) charge(rw http.ResponseWriter, r *http.Request) bool {
	ok, retry := w.ips.allow(clientKey(w.cfg.ClientIP(r)), w.now())
	if ok {
		return true
	}
	rw.Header().Set("Retry-After", strconv.Itoa(max(1, int((retry+time.Second-1)/time.Second))))
	w.write(rw, http.StatusTooManyRequests, htmlType, w.tooMany, -1)
	return false
}

func (w *Wiki) serveAZ(rw http.ResponseWriter, r *http.Request) { w.serveHub(rw, r, "", 1) }

func (w *Wiki) serveWiki(rw http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.PathValue("rest"), "/")
	page := func(s string) int {
		if !pagePattern.MatchString(s) {
			return 0
		}
		n, _ := strconv.Atoi(s)
		return n
	}
	switch {
	case len(parts) == 1 && pageSlug(parts[0]):
		w.serveArticle(rw, r, parts[0])
	case len(parts) == 2 && parts[0] == "index" && page(parts[1]) >= 2:
		w.serveHub(rw, r, "", page(parts[1]))
	case len(parts) == 2 && parts[0] == "v" && verticals[parts[1]] != "":
		w.serveHub(rw, r, parts[1], 1)
	case len(parts) == 3 && parts[0] == "v" && verticals[parts[1]] != "" && page(parts[2]) >= 2:
		w.serveHub(rw, r, parts[1], page(parts[2]))
	default:
		w.write(rw, http.StatusNotFound, htmlType, w.notFound, -1)
	}
}

// fromSnapshot serves a page sliced from the snapshot, never from one older than bound.
func (w *Wiki) fromSnapshot(rw http.ResponseWriter, r *http.Request, kind string, bound time.Duration, render func(*snapshot) []byte) {
	now := w.now()
	key := r.URL.Path
	w.mu.Lock()
	snap, body := w.snap, w.rendered[key]
	w.mu.Unlock()
	if snap == nil || now.Sub(snap.at) >= bound {
		w.unavailableNow(rw)
		return
	}
	if body == nil {
		if !w.charge(rw, r) {
			return
		}
		if body = render(snap); body == nil {
			w.write(rw, http.StatusNotFound, htmlType, w.notFound, -1)
			return
		}
		w.mu.Lock()
		if w.snap == snap {
			w.rendered[key] = body
		}
		w.mu.Unlock()
	}
	w.write(rw, http.StatusOK, kind, body, maxAge(bound, now.Sub(snap.at)))
}

func (w *Wiki) serveHub(rw http.ResponseWriter, r *http.Request, vertical string, n int) {
	w.fromSnapshot(rw, r, htmlType, hubBound, func(s *snapshot) []byte {
		if vertical == "" {
			return w.renderHub("Articles A–Z", "/wiki", s.promoted, n)
		}
		return w.renderHub(verticals[vertical], "/wiki/v/"+vertical, s.vertical(vertical), n)
	})
}

func (w *Wiki) serveSitemapIndex(rw http.ResponseWriter, r *http.Request) {
	w.fromSnapshot(rw, r, xmlType, sitemapBound, w.renderSitemapIndex)
}

func (w *Wiki) serveSitemap(rw http.ResponseWriter, r *http.Request) {
	m := sitemapName.FindStringSubmatch(r.PathValue("name"))
	if m == nil {
		w.write(rw, http.StatusNotFound, htmlType, w.notFound, -1)
		return
	}
	n, _ := strconv.Atoi(m[1])
	w.fromSnapshot(rw, r, xmlType, sitemapBound, func(s *snapshot) []byte { return w.renderSitemap(s, n) })
}

func (w *Wiki) serveRobots(rw http.ResponseWriter, r *http.Request) {
	w.write(rw, http.StatusOK, "text/plain; charset=utf-8", w.renderRobots(), 3600)
}

func (w *Wiki) serveEntry(rw http.ResponseWriter, e *entry, now time.Time) {
	if e.location != "" {
		rw.Header().Set("Location", e.location)
	}
	w.write(rw, e.status, htmlType, e.body, maxAge(e.bound, now.Sub(e.fetched)))
}

func (w *Wiki) serveArticle(rw http.ResponseWriter, r *http.Request, slug string) {
	now := w.now()
	w.mu.Lock()
	e := w.pages[slug]
	w.mu.Unlock()
	if e != nil {
		if _, live := e.remaining(now); live {
			w.serveEntry(rw, e, now)
			return
		}
	}
	if !w.charge(rw, r) {
		return
	}
	e, status := w.load(slug, now)
	switch {
	case e != nil:
		w.serveEntry(rw, e, w.now())
	case status == http.StatusNotFound:
		w.write(rw, http.StatusNotFound, htmlType, w.notFound, -1)
	default:
		w.unavailableNow(rw)
	}
}

// load fetches slug from the engine, sharing one call among concurrent
// misses. A nil entry comes with 404 (unknown-slug budget spent) or 503.
func (w *Wiki) load(slug string, now time.Time) (*entry, int) {
	w.mu.Lock()
	if f := w.inflight[slug]; f != nil {
		w.mu.Unlock()
		<-f.done
		if f.e == nil {
			return nil, http.StatusServiceUnavailable
		}
		return f.e, 0
	}
	if w.snap == nil || !w.snap.known[slug] {
		if !w.budget.take(now) {
			ready := w.snap != nil
			w.mu.Unlock()
			if ready {
				return nil, http.StatusNotFound
			}
			return nil, http.StatusServiceUnavailable
		}
	}
	f := &flight{done: make(chan struct{})}
	w.inflight[slug] = f
	w.mu.Unlock()

	res := w.engine.bySlug(context.Background(), slug)
	fetched := w.now()
	var e *entry
	switch res.kind {
	case outArticle:
		e = &entry{kind: outArticle, status: http.StatusOK, body: w.renderArticle(res.rec), bound: articleBound}
	case outStub:
		e = &entry{status: http.StatusOK, body: w.renderStub(res.rec), bound: articleBound}
	case outRedirect:
		loc := w.canonical("/wiki/" + res.target)
		e = &entry{status: http.StatusMovedPermanently, location: loc, bound: articleBound,
			body: w.renderNotice("This article has moved", "It now lives at a new address.", "/wiki/"+res.target)}
	case outGone:
		e = &entry{status: http.StatusGone, body: w.gone, bound: articleBound}
	case outNotFound:
		e = &entry{status: http.StatusNotFound, body: w.notFound, bound: notFoundBound}
	default:
		w.warn(res.code)
	}
	if e != nil {
		e.fetched = fetched
	}
	w.mu.Lock()
	delete(w.inflight, slug)
	if e != nil {
		w.store(slug, e, fetched)
	}
	w.mu.Unlock()
	f.e = e
	close(f.done)
	if e == nil {
		return nil, http.StatusServiceUnavailable
	}
	return e, 0
}

// store caches e under slug; an engine 404 or 410 also drops the slug from
// the snapshot at once. Called with w.mu held.
func (w *Wiki) store(slug string, e *entry, now time.Time) {
	if e.status == http.StatusNotFound || e.status == http.StatusGone {
		if w.walking {
			w.removed[slug] = now
		}
		if w.snap != nil && w.snap.known[slug] {
			w.setSnapshot(w.snap.without(slug))
		}
	}
	w.drop(slug)
	if len(w.pages) >= pageCap || now.Sub(w.swept) >= notFoundBound {
		w.sweep(now)
	}
	if len(w.pages) >= pageCap || (e.status == http.StatusNotFound && w.negatives >= negativeCap) {
		return
	}
	w.pages[slug] = e
	if e.status == http.StatusNotFound {
		w.negatives++
	}
}

func (w *Wiki) drop(slug string) {
	if old := w.pages[slug]; old != nil {
		if old.status == http.StatusNotFound {
			w.negatives--
		}
		delete(w.pages, slug)
	}
}

func (w *Wiki) sweep(now time.Time) {
	for slug, e := range w.pages {
		if _, live := e.remaining(now); !live {
			w.drop(slug)
		}
	}
	w.swept = now
}

// setSnapshot installs s and drops every page rendered from the old one.
// Called with w.mu held.
func (w *Wiki) setSnapshot(s *snapshot) {
	w.snap = s
	w.rendered = map[string][]byte{}
}

func (w *Wiki) warn(code string) {
	now := w.now()
	w.logMu.Lock()
	last, seen := w.logged[code]
	quiet := seen && now.Sub(last) < time.Minute
	if !quiet {
		w.logged[code] = now
	}
	w.logMu.Unlock()
	if !quiet {
		w.logf("wiki: WARN engine answer unusable (%s); page served as 503", code)
	}
}

// Refresh walks the whole published list and swaps the snapshot in only
// after a complete, successful walk.
func (w *Wiki) Refresh(ctx context.Context) error {
	if !w.on {
		return nil
	}
	w.refreshMu.Lock()
	defer w.refreshMu.Unlock()
	w.mu.Lock()
	start := w.now()
	w.walking = true
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.walking = false
		clear(w.removed)
		w.mu.Unlock()
	}()
	var items []listing
	seen := map[string]bool{}
	cursor := ""
	for page := 0; ; page++ {
		if page >= maxWalkPages {
			return w.refreshFailed("engine_walk")
		}
		list, code := w.engine.list(ctx, cursor)
		if code != "" {
			return w.refreshFailed(code)
		}
		for _, it := range list.Items {
			l, ok := toListing(it)
			if ok && !seen[l.Slug] {
				seen[l.Slug] = true
				items = append(items, l)
			}
		}
		if list.NextCursor == nil {
			break
		}
		if *list.NextCursor == cursor {
			return w.refreshFailed("engine_walk")
		}
		cursor = *list.NextCursor
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	kept := items[:0]
	for _, it := range items {
		if at, ok := w.removed[it.Slug]; !ok || at.Before(start) {
			kept = append(kept, it)
		}
	}
	s := buildSnapshot(start, kept)
	for slug, e := range w.pages {
		if e.kind == outArticle && !s.known[slug] {
			w.drop(slug)
		}
	}
	w.setSnapshot(s)
	return nil
}

func (w *Wiki) refreshFailed(code string) error {
	w.logf("wiki: WARN article list refresh failed (%s); keeping the previous set", code)
	return fmt.Errorf("wiki: refresh failed (%s)", code)
}

// toListing keeps a published item with valid fields.
func toListing(it engineItem) (listing, bool) {
	t, err := time.Parse(time.RFC3339, it.UpdatedAt)
	title := cleanLine(it.Title)
	if it.Status != "published" || !idPattern.MatchString(it.ID) || !pageSlug(it.Slug) || verticals[it.Vertical] == "" || title == "" || err != nil {
		return listing{}, false
	}
	return listing{ID: it.ID, Slug: it.Slug, Title: title, Vertical: it.Vertical, Updated: t,
		UpdatedRaw: t.UTC().Format(time.RFC3339), Promoted: it.Promoted}, true
}

// Run refreshes the snapshot every refreshEvery, and every retryEvery after a
// failed walk. A dark Wiki returns at once.
func (w *Wiki) Run(ctx context.Context) {
	if !w.on {
		return
	}
	t := time.NewTicker(w.poll)
	defer t.Stop()
	var last time.Time
	failed := false
	for {
		every := refreshEvery
		if failed {
			every = retryEvery
		}
		if now := w.now(); last.IsZero() || now.Sub(last) >= every {
			last = now
			failed = w.Refresh(ctx) != nil
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
