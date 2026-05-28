package server

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Prometheus-text metrics. No client lib dep — the format is just text.
//
// What we expose:
//   - cosift_requests_total{path="..."}                                  counter
//   - cosift_rate_limit_denied_total                                     counter
//   - cosift_request_duration_seconds_bucket{path,le}                    histogram
//   - cosift_request_duration_seconds_sum{path}, _count{path}            histogram
//
// Standard Prometheus histogram buckets that Grafana auto-recognizes. Lets a
// dashboard compute p50/p95/p99 from `histogram_quantile()` without us having
// to estimate percentiles in-process (which means we can stay lock-free past
// the map lookup and let Prometheus do the aggregation).
var defaultBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0}

type histogram struct {
	buckets []int64 // cumulative-count form (each entry is "count where d <= buckets[i]")
	count   int64
	sumUS   int64 // sum in microseconds — int64 keeps the math integer until print
}

func newHistogram() *histogram { return &histogram{buckets: make([]int64, len(defaultBuckets))} }

func (h *histogram) Observe(d time.Duration) {
	s := d.Seconds()
	atomic.AddInt64(&h.count, 1)
	atomic.AddInt64(&h.sumUS, d.Microseconds())
	for i, b := range defaultBuckets {
		if s <= b {
			atomic.AddInt64(&h.buckets[i], 1)
		}
	}
}

// Metrics is the server's in-process counter/histogram store.
type Metrics struct {
	mu              sync.Mutex
	requests        map[string]int64
	latency         map[string]*histogram
	rateLimitDenied int64
	// Paraphrase cache observability. Per-layer counters so the
	// dashboard can chart hit-rate trends — if L2 hit rate drops, the index
	// or query mix shifted; if L1 stays high, the workload has repeated queries.
	paraphraseHitsL1 int64
	paraphraseHitsL2 int64
	paraphraseMisses int64
	// HyDE cache observability. Same shape as paraphrase counters.
	hydeHitsL1 int64
	hydeHitsL2 int64
	hydeMisses int64
	// corpus_size gauge. Wired by the server in WithCorpusSize().
	// Optional — nil = gauge omitted. Always read at scrape time so it
	// reflects the actual current state, not a stale snapshot.
	corpusSize func() (docs, passages, paraphrases int64)
}

// WithCorpusSize registers a function that returns current document/passage/
// paraphrase counts. Called at every /metrics scrape; should be cheap.
func (m *Metrics) WithCorpusSize(fn func() (docs, passages, paraphrases int64)) {
	m.corpusSize = fn
}

// paraphraseRows passes int64 through unchanged; helper kept so the
// metrics.go signature stays readable even as we add gauge fields.
func paraphraseRows(n int64) int64 { return n }

func NewMetrics() *Metrics {
	return &Metrics{
		requests: make(map[string]int64),
		latency:  make(map[string]*histogram),
	}
}

// RecordRequest is called from log middleware on every served request.
func (m *Metrics) RecordRequest(path string, d time.Duration) {
	m.mu.Lock()
	m.requests[path]++
	h, ok := m.latency[path]
	if !ok {
		h = newHistogram()
		m.latency[path] = h
	}
	m.mu.Unlock()
	h.Observe(d)
}

// RecordRateLimit is called when the per-IP limiter rejects a request.
func (m *Metrics) RecordRateLimit() { atomic.AddInt64(&m.rateLimitDenied, 1) }

// Paraphrase cache observability — three layers, distinct counters.
func (m *Metrics) RecordParaphraseL1Hit() { atomic.AddInt64(&m.paraphraseHitsL1, 1) }
func (m *Metrics) RecordParaphraseL2Hit() { atomic.AddInt64(&m.paraphraseHitsL2, 1) }
func (m *Metrics) RecordParaphraseMiss()  { atomic.AddInt64(&m.paraphraseMisses, 1) }

// HyDE cache observability —; mirrors paraphrase counters.
func (m *Metrics) RecordHyDEL1Hit() { atomic.AddInt64(&m.hydeHitsL1, 1) }
func (m *Metrics) RecordHyDEL2Hit() { atomic.AddInt64(&m.hydeHitsL2, 1) }
func (m *Metrics) RecordHyDEMiss()  { atomic.AddInt64(&m.hydeMisses, 1) }

// WritePrometheus emits the standard text format. Sorted output so a diff
// between scrapes is deterministic.
func (m *Metrics) WritePrometheus(w io.Writer) {
	m.mu.Lock()
	defer m.mu.Unlock()

	paths := make([]string, 0, len(m.requests))
	for p := range m.requests {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	fmt.Fprintln(w, "# HELP cosift_requests_total Total HTTP requests handled, by path.")
	fmt.Fprintln(w, "# TYPE cosift_requests_total counter")
	for _, p := range paths {
		fmt.Fprintf(w, "cosift_requests_total{path=%q} %d\n", p, m.requests[p])
	}

	fmt.Fprintln(w, "# HELP cosift_rate_limit_denied_total Requests rejected by the per-IP rate limiter.")
	fmt.Fprintln(w, "# TYPE cosift_rate_limit_denied_total counter")
	fmt.Fprintf(w, "cosift_rate_limit_denied_total %d\n", atomic.LoadInt64(&m.rateLimitDenied))

	fmt.Fprintln(w, "# HELP cosift_paraphrase_cache_total Paraphrase cache lookups by result layer.")
	fmt.Fprintln(w, "# TYPE cosift_paraphrase_cache_total counter")
	fmt.Fprintf(w, "cosift_paraphrase_cache_total{result=\"l1_hit\"} %d\n", atomic.LoadInt64(&m.paraphraseHitsL1))
	fmt.Fprintf(w, "cosift_paraphrase_cache_total{result=\"l2_hit\"} %d\n", atomic.LoadInt64(&m.paraphraseHitsL2))
	fmt.Fprintf(w, "cosift_paraphrase_cache_total{result=\"miss\"} %d\n", atomic.LoadInt64(&m.paraphraseMisses))

	fmt.Fprintln(w, "# HELP cosift_hyde_cache_total HyDE passage cache lookups by result layer.")
	fmt.Fprintln(w, "# TYPE cosift_hyde_cache_total counter")
	fmt.Fprintf(w, "cosift_hyde_cache_total{result=\"l1_hit\"} %d\n", atomic.LoadInt64(&m.hydeHitsL1))
	fmt.Fprintf(w, "cosift_hyde_cache_total{result=\"l2_hit\"} %d\n", atomic.LoadInt64(&m.hydeHitsL2))
	fmt.Fprintf(w, "cosift_hyde_cache_total{result=\"miss\"} %d\n", atomic.LoadInt64(&m.hydeMisses))

	if m.corpusSize != nil {
		docs, passages, paraphrases := m.corpusSize()
		fmt.Fprintln(w, "# HELP cosift_corpus_size Current document/passage/paraphrase counts in the store.")
		fmt.Fprintln(w, "# TYPE cosift_corpus_size gauge")
		fmt.Fprintf(w, "cosift_corpus_size{type=\"documents\"} %d\n", docs)
		fmt.Fprintf(w, "cosift_corpus_size{type=\"passages\"} %d\n", passages)
		fmt.Fprintf(w, "cosift_corpus_size{type=\"paraphrases\"} %d\n", paraphraseRows(paraphrases))
	}

	fmt.Fprintln(w, "# HELP cosift_request_duration_seconds Wall-clock request duration in seconds.")
	fmt.Fprintln(w, "# TYPE cosift_request_duration_seconds histogram")
	histPaths := make([]string, 0, len(m.latency))
	for p := range m.latency {
		histPaths = append(histPaths, p)
	}
	sort.Strings(histPaths)
	for _, p := range histPaths {
		h := m.latency[p]
		for i, b := range defaultBuckets {
			fmt.Fprintf(w, "cosift_request_duration_seconds_bucket{path=%q,le=\"%g\"} %d\n",
				p, b, atomic.LoadInt64(&h.buckets[i]))
		}
		count := atomic.LoadInt64(&h.count)
		// +Inf bucket carries the total count.
		fmt.Fprintf(w, "cosift_request_duration_seconds_bucket{path=%q,le=\"+Inf\"} %d\n", p, count)
		fmt.Fprintf(w, "cosift_request_duration_seconds_sum{path=%q} %f\n",
			p, float64(atomic.LoadInt64(&h.sumUS))/1e6)
		fmt.Fprintf(w, "cosift_request_duration_seconds_count{path=%q} %d\n", p, count)
	}
}
