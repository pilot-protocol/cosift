package sharedaccount

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"google.golang.org/api/idtoken"
)

type remote struct {
	url      string
	client   *http.Client
	identity oauth2.TokenSource
}

func newRemote(ctx context.Context, raw, audience string, mcp bool) (*remote, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"))) {
		return nil, fmt.Errorf("shared service URL must use HTTPS (HTTP only on loopback)")
	}
	if !mcp && u.Path != "" && u.Path != "/" {
		return nil, fmt.Errorf("auth URL must be an origin")
	}
	r := &remote{url: strings.TrimRight(raw, "/"), client: &http.Client{Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	if audience != "" {
		r.identity, err = idtoken.NewTokenSource(ctx, audience)
		if err != nil {
			return nil, fmt.Errorf("initialize Cloud Run identity token source: %w", err)
		}
	}
	return r, nil
}
func (r *remote) call(ctx context.Context, path, token string, body any, out any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return ErrInvalid
	}
	req, err := http.NewRequestWithContext(ctx, "POST", r.url+path, bytes.NewReader(encoded))
	if err != nil {
		return ErrUnavailable
	}
	req.Header.Set("Content-Type", "application/json")
	if strings.HasPrefix(path, "/auth/") {
		if ip, ok := ctx.Value(clientIPKey{}).(string); ok {
			req.Header.Set("X-Forwarded-For", ip)
		}
	}
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("MCP-Protocol-Version", "2025-03-26")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if r.identity != nil {
		credential, err := r.identity.Token()
		if err != nil {
			return ErrUnavailable
		}
		req.Header.Set("X-Serverless-Authorization", "Bearer "+credential.AccessToken)
	}
	res, err := r.client.Do(req)
	if err != nil {
		return ErrUnavailable
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case 401:
		return ErrUnauthorized
	case 403:
		return ErrBanned
	case 429:
		return ErrLimited
	}
	if res.StatusCode != 200 {
		return ErrUnavailable
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 || json.Unmarshal(data, out) != nil {
		return ErrUnavailable
	}
	return nil
}
func (c *Client) Start(ctx context.Context, email string) (Challenge, error) {
	var result Challenge
	err := c.auth.call(ctx, "/auth/start", "", map[string]string{"email": email}, &result)
	if err == nil && (result.RequestID == "" || !result.ExpiresAt.After(time.Now())) {
		err = ErrUnavailable
	}
	return result, err
}
func (c *Client) Finish(ctx context.Context, requestID, code string) (Issued, error) {
	var result Issued
	err := c.auth.call(ctx, "/auth/verify", "", map[string]string{"request_id": requestID, "code": code}, &result)
	if err == nil {
		if _, e := Parse(result.Token); e != nil || !UIDPattern.MatchString(result.UID) {
			err = ErrUnavailable
		}
	}
	return result, err
}
func (c *Client) Revoke(ctx context.Context, token string) error {
	var result map[string]any
	return c.auth.call(ctx, "/auth/revoke", token, map[string]any{}, &result)
}
func (c *Client) Call(ctx context.Context, token, tool string, args map[string]any) (map[string]any, error) {
	switch tool {
	case "cosift_topics", "cosift_lookup", "cosift_request":
	default:
		return nil, ErrInvalid
	}
	var envelope struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      int             `json:"id"`
		Error   json.RawMessage `json:"error"`
		Result  struct {
			IsError bool `json:"isError"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
	}
	err := c.mcp.call(ctx, "", token, map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": map[string]any{"name": tool, "arguments": args}}, &envelope)
	if err != nil {
		return nil, err
	}
	if envelope.JSONRPC != "2.0" || envelope.ID != 1 || (len(envelope.Error) > 0 && string(envelope.Error) != "null") || envelope.Result.IsError || len(envelope.Result.Content) != 1 || envelope.Result.Content[0].Type != "text" {
		return nil, ErrUnavailable
	}
	var payload map[string]any
	if json.Unmarshal([]byte(envelope.Result.Content[0].Text), &payload) != nil || payload == nil {
		return nil, ErrUnavailable
	}
	if payload["status"] == "unavailable" || payload["unavailable"] == true {
		if payload["reason"] == "quota_exceeded" {
			return nil, ErrLimited
		}
		return nil, ErrUnavailable
	}
	if payload["status"] == "invalid" {
		return nil, ErrInvalid
	}
	return payload, nil
}
