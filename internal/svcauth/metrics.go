package svcauth

import (
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Metrics holds the cosift_v1_* series. Every label value comes from a
// bounded set: principal ids from config, route patterns, fixed reasons.
type Metrics struct {
	mu            sync.Mutex
	requests      map[[3]string]uint64
	authFailures  map[string]uint64
	rateLimited   map[[2]string]uint64
	writes        map[string]uint64
	certRefresh   map[string]uint64
	configReloads map[string]uint64
	serverErrors  atomic.Uint64
	certAge       func() (float64, bool)
}

func NewMetrics() *Metrics {
	return &Metrics{
		requests:      map[[3]string]uint64{},
		authFailures:  map[string]uint64{},
		rateLimited:   map[[2]string]uint64{},
		writes:        map[string]uint64{},
		certRefresh:   map[string]uint64{},
		configReloads: map[string]uint64{},
	}
}

func (m *Metrics) request(principal, route string, code int) {
	m.mu.Lock()
	m.requests[[3]string{principal, route, strconv.Itoa(code)}]++
	m.mu.Unlock()
}

func (m *Metrics) authFailure(reason string) {
	m.mu.Lock()
	m.authFailures[reason]++
	m.mu.Unlock()
}

func (m *Metrics) limited(principal, kind string) {
	m.mu.Lock()
	m.rateLimited[[2]string{principal, kind}]++
	m.mu.Unlock()
}

func (m *Metrics) write(principal string) {
	m.mu.Lock()
	m.writes[principal]++
	m.mu.Unlock()
}

func (m *Metrics) certResult(ok bool) {
	m.mu.Lock()
	m.certRefresh[result(ok)]++
	m.mu.Unlock()
}

func (m *Metrics) reload(ok bool) {
	m.mu.Lock()
	m.configReloads[result(ok)]++
	m.mu.Unlock()
}

func result(ok bool) string {
	if ok {
		return "ok"
	}
	return "error"
}

func (m *Metrics) setCertAge(f func() (float64, bool)) {
	m.mu.Lock()
	m.certAge = f
	m.mu.Unlock()
}

// serverErrorWriter is the listener's ErrorLog output: it counts and drops.
type serverErrorWriter struct{ m *Metrics }

func (w serverErrorWriter) Write(p []byte) (int, error) {
	w.m.serverErrors.Add(1)
	return len(p), nil
}

// WritePrometheus writes the series in the Prometheus text format.
func (m *Metrics) WritePrometheus(w io.Writer) {
	m.mu.Lock()
	certAge := m.certAge
	m.mu.Unlock()
	age, haveAge := 0.0, false
	if certAge != nil {
		age, haveAge = certAge()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	family(w, "cosift_v1_requests_total", "counter", "Responses of the /v1 listener.")
	for _, k := range sortedKeys(m.requests, func(k [3]string) string { return strings.Join(k[:], "\x00") }) {
		fmt.Fprintf(w, "cosift_v1_requests_total{principal=%s,route=%s,code=%s} %d\n", q(k[0]), q(k[1]), q(k[2]), m.requests[k])
	}
	family(w, "cosift_v1_auth_failures_total", "counter", "Failed /v1 authentications by reason.")
	for _, k := range sortedKeys(m.authFailures, func(k string) string { return k }) {
		fmt.Fprintf(w, "cosift_v1_auth_failures_total{reason=%s} %d\n", q(k), m.authFailures[k])
	}
	family(w, "cosift_v1_rate_limited_total", "counter", "429 responses of the /v1 listener.")
	for _, k := range sortedKeys(m.rateLimited, func(k [2]string) string { return k[0] + "\x00" + k[1] }) {
		fmt.Fprintf(w, "cosift_v1_rate_limited_total{principal=%s,kind=%s} %d\n", q(k[0]), q(k[1]), m.rateLimited[k])
	}
	family(w, "cosift_v1_writes_total", "counter", "Article and stub writes charged to the write budget.")
	for _, k := range sortedKeys(m.writes, func(k string) string { return k }) {
		fmt.Fprintf(w, "cosift_v1_writes_total{principal=%s} %d\n", q(k), m.writes[k])
	}
	family(w, "cosift_v1_cert_refresh_total", "counter", "Google certificate refreshes.")
	for _, r := range []string{"ok", "error"} {
		fmt.Fprintf(w, "cosift_v1_cert_refresh_total{result=%s} %d\n", q(r), m.certRefresh[r])
	}
	if haveAge {
		family(w, "cosift_v1_cert_age_seconds", "gauge", "Age of the certificate copy in service.")
		fmt.Fprintf(w, "cosift_v1_cert_age_seconds %.0f\n", age)
	}
	family(w, "cosift_v1_config_reloads_total", "counter", "service-auth.json reloads.")
	for _, r := range []string{"ok", "error"} {
		fmt.Fprintf(w, "cosift_v1_config_reloads_total{result=%s} %d\n", q(r), m.configReloads[r])
	}
	family(w, "cosift_v1_server_errors_total", "counter", "Internal errors of the /v1 http.Server.")
	fmt.Fprintf(w, "cosift_v1_server_errors_total %d\n", m.serverErrors.Load())
}

func family(w io.Writer, name, typ, help string) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
}

func q(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s) + `"`
}

func sortedKeys[K comparable, V any](m map[K]V, key func(K) string) []K {
	ks := make([]K, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	slices.SortFunc(ks, func(a, b K) int { return strings.Compare(key(a), key(b)) })
	return ks
}
