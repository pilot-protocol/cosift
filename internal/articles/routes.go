package articles

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

const (
	limitMatch  = 8 << 10
	limitPut    = 512 << 10
	limitStub   = 16 << 10
	limitStatus = 4 << 10
	limitPurge  = 1 << 10
)

// Routes is the article route table the /v1 listener mounts.
func (s *Store) Routes() []v1.Route {
	read, readAll := v1.ScopeArticlesRead, v1.ScopeArticlesReadAll
	return []v1.Route{
		{Method: http.MethodPost, Path: "/v1/articles/match", Scopes: []v1.Scope{read}, BodyLimit: limitMatch, Handler: s.gated(s.handleMatch)},
		{Method: http.MethodGet, Path: "/v1/articles", Scopes: []v1.Scope{read, readAll}, Handler: s.gated(s.handleList)},
		{Method: http.MethodGet, Path: "/v1/articles/{id}", Scopes: []v1.Scope{read, readAll}, Handler: s.gated(s.handleGet)},
		{Method: http.MethodGet, Path: "/v1/articles/by-slug/{slug}", Scopes: []v1.Scope{read, readAll}, Handler: s.gated(s.handleBySlug)},
		{Method: http.MethodGet, Path: "/v1/articles/stats", Scopes: []v1.Scope{readAll}, Handler: s.gated(s.handleStats)},
		{Method: http.MethodPut, Path: "/v1/articles/{id}", Scopes: []v1.Scope{v1.ScopeArticlesWrite, v1.ScopeArticlesStub}, BodyLimit: limitPut, Handler: s.gated(s.handlePut)},
		{Method: http.MethodDelete, Path: "/v1/articles/{id}", Scopes: []v1.Scope{v1.ScopeArticlesStub}, Handler: s.gated(s.handleDeleteStub)},
		{Method: http.MethodPost, Path: "/v1/articles/{id}/status", Scopes: []v1.Scope{v1.ScopeArticlesModerate}, BodyLimit: limitStatus, Handler: s.gated(s.handleStatus)},
		{Method: http.MethodPost, Path: "/v1/articles/purge-prelive", Scopes: []v1.Scope{v1.ScopeArticlesAdmin}, BodyLimit: limitPurge, Handler: s.gated(s.handlePurge)},
		{Method: http.MethodGet, Path: "/v1/article-versions/{id}", Scopes: []v1.Scope{readAll}, Handler: s.gated(s.handleVersions)},
		{Method: http.MethodGet, Path: "/v1/article-versions/{id}/{version}", Scopes: []v1.Scope{readAll}, Handler: s.gated(s.handleVersion)},
	}
}

type handler func(w http.ResponseWriter, r *http.Request, p v1.Principal)

func (s *Store) gated(h handler) http.Handler {
	return s.rd.Gate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := v1.PrincipalFrom(r.Context())
		if !ok {
			v1.WriteError(w, v1.Unauthenticated())
			return
		}
		h(w, r, p)
	}))
}

func fail(e v1.Error) *v1.Error { return &e }

// allowed authorises the operation about to run: the caller holds one of scopes.
func allowed(w http.ResponseWriter, r *http.Request, scopes ...v1.Scope) bool {
	_, ok := v1.RequireAny(w, r, scopes...)
	return ok
}

// restorePending reports a store restored from before the recorded go-live.
func (s *Store) restorePending() bool {
	_, set := s.policy.GoliveAt()
	return set && !s.golive()
}

func (s *Store) refuseRestorePending(w http.ResponseWriter) bool {
	if s.restorePending() {
		v1.WriteError(w, errRestorePending())
		return true
	}
	return false
}

// chargeWrite refuses frozen writes, then draws one unit of the write budget.
func (s *Store) chargeWrite(w http.ResponseWriter, p v1.Principal) bool {
	if s.policy.WritesFrozen() {
		v1.WriteError(w, v1.WritesFrozen())
		return false
	}
	if retry, ok := s.policy.ChargeWrite(p.ID); !ok {
		v1.WriteError(w, v1.WriteBudget(retry))
		return false
	}
	return true
}

// activeLocked re-checks the caller against the active config inside the
// write lock: a request authorised before a reload or before go-live does
// not commit after it.
func (s *Store) activeLocked(p v1.Principal, need v1.Scope, write bool) (v1.Principal, *v1.Error) {
	if s.closed {
		return p, fail(v1.IndexUnavailable())
	}
	a, ok := s.policy.Principal(p.ID)
	if !ok {
		return p, fail(v1.MissingScope(need))
	}
	if a.Env == v1.EnvStaging && s.idx.golive != nil && need != v1.ScopeArticlesAdmin {
		return a, fail(v1.EnvGolive())
	}
	if !a.Has(need) {
		return a, fail(v1.MissingScope(need))
	}
	if write && s.policy.WritesFrozen() {
		return a, fail(v1.WritesFrozen())
	}
	if need == v1.ScopeArticlesWrite || need == v1.ScopeArticlesStub {
		if _, set := s.policy.GoliveAt(); set && s.idx.golive == nil {
			return a, fail(errRestorePending())
		}
	}
	return a, nil
}

// decodeStrict decodes an already-read JSON object, refusing unknown fields.
func decodeStrict(raw []byte, dst any) *v1.Error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		if quoted, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
			if name, err := strconv.Unquote(quoted); err == nil {
				return fail(v1.UnknownField(name))
			}
		}
		return fail(v1.InvalidBody())
	}
	return nil
}

type countingReader struct {
	io.ReadCloser
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	c.n += int64(n)
	return n, err
}
