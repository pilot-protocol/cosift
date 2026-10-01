package v1

import (
	"net/http"
	"sync/atomic"
)

// Route is one /v1 endpoint as the listener mounts it.
type Route struct {
	Method string
	Path   string
	// Scopes that may reach Handler: the principal needs one of them, and the
	// handler authorises the exact operation after decoding the body.
	Scopes []Scope
	// BodyLimit caps the request body in bytes; 0 means the route takes no body.
	BodyLimit int64
	// Ungated routes are served before the article index is ready.
	Ungated bool
	Handler http.Handler
}

// Pattern is the ServeMux pattern, which is also r.Pattern inside the handler.
func (rt Route) Pattern() string { return rt.Method + " " + rt.Path }

// Readiness records whether the article index has finished its startup
// rebuild. The zero value is not ready.
type Readiness struct {
	ready atomic.Bool
}

func (rd *Readiness) SetReady() { rd.ready.Store(true) }

func (rd *Readiness) Ready() bool { return rd.ready.Load() }

// Gate answers 503 index_unavailable until SetReady has been called.
func (rd *Readiness) Gate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !rd.Ready() {
			WriteError(w, IndexUnavailable())
			return
		}
		next.ServeHTTP(w, r)
	})
}
