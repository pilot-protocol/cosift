package svcauth

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net/http"
	"path"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

const unmatched = "unmatched"

type reqInfo struct {
	route     string
	principal string
	reason    string
	client    string
	quiet     bool
}

type infoKey struct{}

func infoFrom(r *http.Request) *reqInfo {
	if ri, ok := r.Context().Value(infoKey{}).(*reqInfo); ok {
		return ri
	}
	return &reqInfo{}
}

// buildMux registers routes behind the per-route chain, with the JSON 404 as
// the default. It returns an error where ServeMux would panic.
func (s *Service) buildMux(routes []v1.Route) (mux *http.ServeMux, err error) {
	defer func() {
		if p := recover(); p != nil {
			mux, err = nil, fmt.Errorf("svcauth: route table: %v", p)
		}
	}()
	mux = http.NewServeMux()
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v1.WriteError(w, v1.NotFound())
	}))
	for _, rt := range routes {
		if rt.Handler == nil {
			return nil, fmt.Errorf("svcauth: route %s has no handler", rt.Pattern())
		}
		mux.Handle(rt.Pattern(), s.chain(rt))
	}
	return mux, nil
}

// chain is authenticate → principal limit → readiness → any-of scope →
// body cap → handler, which authorises the exact operation after decoding.
func (s *Service) chain(rt v1.Route) http.Handler {
	pattern := rt.Pattern()
	var h http.Handler = rt.Handler
	h = bodyCap(rt.BodyLimit, h)
	h = scopeCheck(rt.Scopes, h)
	if !rt.Ungated {
		h = s.ready.Gate(h)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ri := infoFrom(r)
		ri.route = pattern
		st := s.Policy.state()
		if st == nil {
			v1.WriteError(w, v1.Unauthenticated())
			return
		}
		out := s.authenticate(r, st)
		if out.unavailable {
			ri.reason, ri.client = "auth_unavailable", clientKey(r, s.opts.ClientIPHeader)
			v1.WriteError(w, v1.AuthUnavailable())
			return
		}
		if out.ps == nil {
			s.refuse(w, r, ri, st, out.reason)
			return
		}
		ri.principal = out.ps.cfg.ID
		if retry, ok := s.Policy.allowRequest(out.ps); !ok {
			s.Metrics.limited(ri.principal, "principal")
			v1.WriteError(w, v1.RateLimited(retry))
			return
		}
		h.ServeHTTP(w, v1.RequestWithPrincipal(r, out.ps.p))
	})
}

func (s *Service) refuse(w http.ResponseWriter, r *http.Request, ri *reqInfo, st *state, reason string) {
	ri.reason, ri.client = reason, clientKey(r, s.opts.ClientIPHeader)
	s.Metrics.authFailure(reason)
	if retry, ok := s.failed.fail(ri.client, st.cfg.FailedAuth); !ok {
		ri.quiet = true
		s.Metrics.limited("-", "failed_auth")
		v1.WriteError(w, v1.AuthThrottled(retry))
		return
	}
	v1.WriteError(w, v1.Unauthenticated())
}

func scopeCheck(scopes []v1.Scope, next http.Handler) http.Handler {
	if len(scopes) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := v1.PrincipalFrom(r.Context())
		if !ok || !p.HasAny(scopes...) {
			v1.WriteError(w, v1.MissingScope(scopes[0]))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func bodyCap(limit int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, limit)
		next.ServeHTTP(w, r)
	})
}

// cleanPath reports whether p is served at all: ServeMux would redirect an
// unclean path, and ids and slugs never need escaping.
func cleanPath(r *http.Request) bool {
	p := r.URL.Path
	raw, _, _ := strings.Cut(r.RequestURI, "?")
	return strings.HasPrefix(p, "/") && path.Clean(p) == p && r.URL.EscapedPath() == p &&
		!strings.ContainsRune(raw, '%') && !strings.ContainsRune(p, '%')
}

// ServeHTTP is the outer wrapper every /v1 request passes through.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.inflight.enter()
	defer s.inflight.leave()
	start := s.now()
	ri := &reqInfo{route: unmatched}
	rw := &respWriter{ResponseWriter: w}
	defer func() {
		ms := s.now().Sub(start).Milliseconds()
		principal := ri.principal
		if principal == "" {
			principal = "-"
		}
		s.Metrics.request(principal, ri.route, rw.code())
		if ri.quiet {
			return
		}
		route := ri.route
		if route != unmatched {
			route = fmt.Sprintf("%q", route)
		}
		line := fmt.Sprintf("v1 req=%s principal=%s route=%s status=%d ms=%d", requestID(start), principal, route, rw.code(), ms)
		if ri.reason != "" {
			line += " reason=" + ri.reason + " client=" + ri.client
		}
		s.log.Print(line)
	}()
	defer func() {
		if p := recover(); p != nil {
			if p == http.ErrAbortHandler {
				panic(p)
			}
			s.log.Printf("v1 panic route=%s\n%s", ri.route, debug.Stack())
			if !rw.wrote {
				v1.WriteError(rw, v1.InternalError())
			} else {
				rw.status = http.StatusInternalServerError
			}
		}
	}()
	if !cleanPath(r) {
		v1.WriteError(rw, v1.NotFound())
		return
	}
	s.mounted().ServeHTTP(rw, r.WithContext(context.WithValue(r.Context(), infoKey{}, ri)))
}

// respWriter records the status and enforces the headers every /v1 response
// carries, whatever the handler set.
type respWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *respWriter) WriteHeader(code int) {
	if w.wrote {
		return
	}
	w.wrote, w.status = true, code
	h := w.Header()
	for k := range h {
		if strings.HasPrefix(k, "Access-Control-") || k == "Set-Cookie" || k == "Location" {
			delete(h, k)
		}
	}
	h.Set("Content-Type", "application/json")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	w.ResponseWriter.WriteHeader(code)
}

func (w *respWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

func (w *respWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *respWriter) code() int {
	if !w.wrote {
		return http.StatusOK
	}
	return w.status
}

type tracker struct {
	mu   sync.Mutex
	n    int
	idle chan struct{}
}

func (t *tracker) enter() {
	t.mu.Lock()
	t.n++
	t.mu.Unlock()
}

func (t *tracker) leave() {
	t.mu.Lock()
	t.n--
	if t.n == 0 && t.idle != nil {
		close(t.idle)
		t.idle = nil
	}
	t.mu.Unlock()
}

// wait returns once no request is in a handler, or when ctx is done.
func (t *tracker) wait(ctx context.Context) error {
	t.mu.Lock()
	if t.n == 0 {
		t.mu.Unlock()
		return nil
	}
	if t.idle == nil {
		t.idle = make(chan struct{})
	}
	ch := t.idle
	t.mu.Unlock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// requestID is a ULID: 48 bits of milliseconds, 80 random bits.
func requestID(t time.Time) string {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], uint64(t.UnixMilli())<<16)
	_, _ = rand.Read(b[6:])
	hi, lo := binary.BigEndian.Uint64(b[:8]), binary.BigEndian.Uint64(b[8:])
	var out [26]byte
	for i := 25; i >= 0; i-- {
		out[i] = crockford[lo&31]
		lo = lo>>5 | hi<<59
		hi >>= 5
	}
	return string(out[:])
}
