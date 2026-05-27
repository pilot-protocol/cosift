package crawler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// remoteFetcherTransport wraps an HTTP RoundTripper so every outbound GET
// becomes a POST to an external "fetch service" (a Cloudflare Worker, in
// the canonical deployment — see scripts/cf-fetch-worker.js). The crawler
// code is unaware of this — it still calls c.http.Do(req) as before.
//
// Wire shape:
//   request:  POST <RemoteFetcherURL>
//             Authorization: Bearer <RemoteFetcherToken>
//             Content-Type:  application/json
//             body:          {"url": "...", "user_agent": "...", "headers": {...}}
//
//   response: 200 OK on success; raw upstream body in the response body.
//             Special headers:
//               X-Final-URL     — fully-resolved URL after redirects
//               X-Origin-Status — upstream HTTP status (e.g. 404, 200, 500)
//                                 cosift consults this for status; the
//                                 outer 200 is just "worker reachable"
//
// Non-GET requests (auth probes, robots.txt fetches, etc.) bypass the
// worker and go direct via the inner transport. Iter 474.
type remoteFetcherTransport struct {
	inner http.RoundTripper
	url   string
	token string
	c     *http.Client
}

func newRemoteFetcherTransport(workerURL, token string, inner http.RoundTripper) *remoteFetcherTransport {
	return &remoteFetcherTransport{
		inner: inner,
		url:   workerURL,
		token: token,
		c:     &http.Client{Transport: inner, Timeout: 60 * time.Second},
	}
}

type remoteFetcherReq struct {
	URL       string            `json:"url"`
	UserAgent string            `json:"user_agent,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
}

func (t *remoteFetcherTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet {
		return t.inner.RoundTrip(req)
	}

	// Build worker request.
	headers := map[string]string{}
	for k, vs := range req.Header {
		if len(vs) > 0 && k != "Authorization" && k != "Cookie" {
			headers[k] = vs[0]
		}
	}
	payload := remoteFetcherReq{
		URL:       req.URL.String(),
		UserAgent: req.UserAgent(),
		Headers:   headers,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("remote-fetcher: marshal: %w", err)
	}
	workerReq, err := http.NewRequestWithContext(req.Context(), http.MethodPost, t.url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("remote-fetcher: build: %w", err)
	}
	workerReq.Header.Set("Content-Type", "application/json")
	if t.token != "" {
		workerReq.Header.Set("Authorization", "Bearer "+t.token)
	}

	resp, err := t.c.Do(workerReq)
	if err != nil {
		return nil, fmt.Errorf("remote-fetcher: do: %w", err)
	}
	// Worker errors out → propagate as a fetch failure so the crawler
	// records this URL as errored (rather than indexing the JSON error blob).
	if resp.StatusCode >= 500 {
		buf, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		return nil, fmt.Errorf("remote-fetcher: upstream %d: %s", resp.StatusCode, string(buf))
	}

	// Translate the worker's response into a response that LOOKS like a
	// direct fetch of the target URL. The crawler downstream cares about
	// status code, body, content-type, and the resolved URL.
	originStatus, _ := strconv.Atoi(resp.Header.Get("X-Origin-Status"))
	if originStatus == 0 {
		originStatus = resp.StatusCode
	}
	finalURL := resp.Header.Get("X-Final-URL")
	if finalURL == "" {
		finalURL = req.URL.String()
	}

	// Build a synthetic *http.Response so downstream code sees the
	// upstream status, not the worker's 200.
	out := &http.Response{
		Status:        fmt.Sprintf("%d %s", originStatus, http.StatusText(originStatus)),
		StatusCode:    originStatus,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        http.Header{},
		Body:          resp.Body,
		ContentLength: resp.ContentLength,
		Request:       req,
	}
	// Copy headers EXCEPT the X-* worker-meta + Authorization noise.
	for k, vs := range resp.Header {
		if k == "X-Origin-Status" || k == "X-Final-Url" || k == "X-Origin-Ip" {
			continue
		}
		for _, v := range vs {
			out.Header.Add(k, v)
		}
	}
	// Override the request URL on the response so r.Request.URL.String()
	// reflects redirects (crawler uses res.finalURL).
	if finalURL != req.URL.String() {
		if u, err := req.URL.Parse(finalURL); err == nil {
			out.Request = req.Clone(req.Context())
			out.Request.URL = u
		}
	}
	return out, nil
}
