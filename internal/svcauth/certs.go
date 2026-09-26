package svcauth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pilot-protocol/cosift/internal/netguard"
)

const (
	CertsURL = "https://www.googleapis.com/oauth2/v3/certs"

	certFetchTimeout = 5 * time.Second
	certMaxHeader    = 64 << 10
	certMaxBody      = 256 << 10
	certEarlyEvery   = 5 * time.Minute
	certStaleFor     = 24 * time.Hour
	certBackoffMin   = 10 * time.Second
	certBackoffMax   = 5 * time.Minute
)

var (
	ErrNoCerts = errors.New("svcauth: no usable certificates")
	errCertURL = errors.New("svcauth: certificate URL refused")
)

// CertSource performs one fetch of the certificates URL.
type CertSource func(ctx context.Context) (*http.Response, error)

// HTTPCertSource fetches CertsURL with client, which must not follow redirects.
func HTTPCertSource(client *http.Client) CertSource {
	return func(ctx context.Context) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, CertsURL, nil)
		if err != nil {
			return nil, err
		}
		return client.Do(req)
	}
}

// NewCertHTTPClient is the client HTTPCertSource uses in production.
func NewCertHTTPClient() *http.Client {
	tr := netguard.Protect(&http.Transport{
		MaxResponseHeaderBytes: certMaxHeader,
		TLSHandshakeTimeout:    certFetchTimeout,
		ResponseHeaderTimeout:  certFetchTimeout,
		ForceAttemptHTTP2:      true,
	}, true)
	return &http.Client{
		Transport:     tr,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Timeout:       certFetchTimeout,
	}
}

type certCopy struct {
	body    []byte
	kids    map[string]bool
	fetched time.Time
	expires time.Time
}

// CertCache is the idtoken library's certificate transport. It answers from
// the last good copy with max-age=0, so the library keeps nothing, and only
// its own background loop ever fetches.
type CertCache struct {
	source  CertSource
	now     func() time.Time
	metrics *Metrics
	logf    func(string, ...any)
	kick    chan struct{}

	mu        sync.Mutex
	cur       *certCopy
	lastEarly time.Time
	lastGood  time.Time
	started   time.Time
}

func NewCertCache(source CertSource, m *Metrics, now func() time.Time, logf func(string, ...any)) *CertCache {
	if now == nil {
		now = time.Now
	}
	c := &CertCache{source: source, now: now, metrics: m, logf: logf, kick: make(chan struct{}, 1)}
	m.setCertAge(c.age)
	return c
}

func (c *CertCache) current() *certCopy {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cur != nil && now.After(c.cur.expires.Add(certStaleFor)) {
		c.cur = nil
	}
	return c.cur
}

func (c *CertCache) Usable() bool { return c.current() != nil }

func (c *CertCache) HasKid(kid string) bool {
	cp := c.current()
	return cp != nil && cp.kids[kid]
}

// RequestEarly asks the loop for a refresh, at most once per certEarlyEvery.
func (c *CertCache) RequestEarly() {
	now := c.now()
	c.mu.Lock()
	if !c.lastEarly.IsZero() && now.Sub(c.lastEarly) < certEarlyEvery {
		c.mu.Unlock()
		return
	}
	c.lastEarly = now
	c.mu.Unlock()
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

func (c *CertCache) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		_ = req.Body.Close()
	}
	if req.Method != http.MethodGet || req.URL.String() != CertsURL {
		return nil, errCertURL
	}
	cp := c.current()
	if cp == nil {
		return nil, ErrNoCerts
	}
	return &http.Response{
		Status:        "200 OK",
		StatusCode:    http.StatusOK,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        http.Header{"Cache-Control": {"max-age=0"}, "Content-Type": {"application/json"}},
		Body:          io.NopCloser(bytes.NewReader(cp.body)),
		ContentLength: int64(len(cp.body)),
		Request:       req,
	}, nil
}

// Run fetches at once, then before each copy expires, and early on request;
// failures back off from 10 s to 5 min. It returns when ctx is done.
func (c *CertCache) Run(ctx context.Context) {
	c.mu.Lock()
	c.started = c.now()
	c.mu.Unlock()
	var backoff time.Duration
	for {
		var wait time.Duration
		if c.refresh(ctx) {
			backoff = 0
			wait = c.scheduled()
		} else {
			backoff = nextBackoff(backoff)
			wait = backoff
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		case <-c.kick:
			t.Stop()
		}
	}
}

func nextBackoff(d time.Duration) time.Duration {
	return min(max(2*d, certBackoffMin), certBackoffMax)
}

func (c *CertCache) scheduled() time.Duration {
	c.mu.Lock()
	cp := c.cur
	c.mu.Unlock()
	if cp == nil {
		return certBackoffMin
	}
	at := cp.fetched.Add(cp.expires.Sub(cp.fetched) * 9 / 10)
	return max(at.Sub(c.now()), certBackoffMin)
}

// refresh performs one detached fetch and installs the copy if it is good.
func (c *CertCache) refresh(ctx context.Context) bool {
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), certFetchTimeout)
	defer cancel()
	cp, class := c.fetch(fctx)
	if cp == nil {
		c.metrics.certResult(false)
		if c.logf != nil {
			c.logf("svcauth: certificate refresh failed (%s)", class)
		}
		return false
	}
	c.mu.Lock()
	c.cur, c.lastGood = cp, cp.fetched
	c.mu.Unlock()
	c.metrics.certResult(true)
	return true
}

func (c *CertCache) fetch(ctx context.Context) (*certCopy, string) {
	resp, err := c.source(ctx)
	if err != nil {
		return nil, "request"
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, "status " + strconv.Itoa(resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, certMaxBody+1))
	if err != nil {
		return nil, "body read"
	}
	if len(body) > certMaxBody {
		return nil, "body too large"
	}
	kids, ok := parseKeySet(body)
	if !ok {
		return nil, "no key set"
	}
	now := c.now()
	return &certCopy{body: body, kids: kids, fetched: now, expires: now.Add(freshFor(resp.Header))}, ""
}

// parseKeySet requires a JSON object with a non-empty keys array.
func parseKeySet(body []byte) (map[string]bool, bool) {
	var doc map[string]json.RawMessage
	if json.Unmarshal(body, &doc) != nil {
		return nil, false
	}
	var keys []struct {
		Kid string `json:"kid"`
	}
	if raw, ok := doc["keys"]; !ok || json.Unmarshal(raw, &keys) != nil || len(keys) == 0 {
		return nil, false
	}
	kids := make(map[string]bool, len(keys))
	for _, k := range keys {
		kids[k.Kid] = true
	}
	return kids, true
}

// freshFor is max-age minus Age; zero when either is missing or malformed.
func freshFor(h http.Header) time.Duration {
	maxAge := -1
	for _, d := range strings.Split(h.Get("Cache-Control"), ",") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(d), "max-age="); ok {
			if n, err := strconv.Atoi(v); err == nil && n >= 0 {
				maxAge = n
			}
		}
	}
	if maxAge < 0 {
		return 0
	}
	age := 0
	if a := h.Get("Age"); a != "" {
		n, err := strconv.Atoi(a)
		if err != nil || n < 0 {
			return 0
		}
		age = n
	}
	return time.Duration(max(maxAge-age, 0)) * time.Second
}

// age runs from the last good fetch, or from the loop's start before the
// first one, so it keeps growing through an outage; absent before Run.
func (c *CertCache) age() (float64, bool) {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case !c.lastGood.IsZero():
		return now.Sub(c.lastGood).Seconds(), true
	case !c.started.IsZero():
		return now.Sub(c.started).Seconds(), true
	}
	return 0, false
}
