package community

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	moderateRoute    = "/v1/community/moderate"
	enqueueRoute     = "/v1/community/enqueue"
	healthRoute      = "/healthz"
	defaultEngineURL = "http://127.0.0.1:7779"

	enqueueTimeout  = 150 * time.Second
	moderateTimeout = 60 * time.Second
)

var brokerKeyPattern = regexp.MustCompile(`^csk_[0-9a-f]{16}_[A-Z2-7]{52}$`)

var errNoBrokerKey = errors.New("no broker key configured")

// engineClient is the app's only way to the engine's /v1 listener: one loopback origin, one bearer key, no redirects.
type engineClient struct {
	base string
	key  string
	http *http.Client
}

func newEngineClient(rawURL, key string) (*engineClient, error) {
	if rawURL == "" {
		rawURL = defaultEngineURL
	}
	if key != "" && !brokerKeyPattern.MatchString(key) {
		return nil, fmt.Errorf("COSIFT_COMMUNITY_BROKER_KEY must be an engine csk_ key")
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme != "http" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || !loopbackHost(u.Hostname()) {
		return nil, fmt.Errorf("COSIFT_COMMUNITY_ENGINE_URL must be a loopback http origin")
	}
	return &engineClient{base: strings.TrimRight(rawURL, "/"), key: key, http: &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

func loopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (c *engineClient) configured() bool { return c.key != "" }

func (c *engineClient) do(ctx context.Context, method, path string, body []byte, timeout time.Duration) (*http.Response, error) {
	if c.key == "" {
		return nil, errNoBrokerKey
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+c.key)
	client := *c.http
	client.Timeout = timeout
	return client.Do(req)
}

func (c *engineClient) post(ctx context.Context, path string, body []byte, timeout time.Duration) (*http.Response, error) {
	return c.do(ctx, http.MethodPost, path, body, timeout)
}

// reachable is false only when the listener cannot be connected to; any HTTP reply counts as reachable.
func (c *engineClient) reachable(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	res, err := c.do(ctx, http.MethodGet, healthRoute, nil, 5*time.Second)
	if errors.Is(err, errNoBrokerKey) {
		return true
	}
	if err != nil {
		return !backendUnreachable(err)
	}
	res.Body.Close()
	return true
}
