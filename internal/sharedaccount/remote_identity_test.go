package sharedaccount

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRemoteDistinguishesCosiftRejectionFromCloudRunFailure(t *testing.T) {
	for _, tt := range []struct {
		name, path, body string
		status           int
		want             error
	}{
		{"auth revoked", "/auth/revoke", `{"error":"Unauthorized","status":401,"detail":"invalid or revoked token"}`, 401, ErrUnauthorized},
		{"auth bad code", "/auth/verify", `{"error":"Unauthorized","status":401,"detail":"invalid or expired code"}`, 401, ErrUnauthorized},
		{"auth banned", "/auth/revoke", `{"error":"Forbidden","status":403,"detail":"account suspended"}`, 403, ErrBanned},
		{"MCP revoked", "", `{"error":"invalid_token"}`, 401, ErrUnauthorized},
		{"MCP banned", "", `{"error":"account_banned"}`, 403, ErrBanned},
		{"Google auth identity expired", "/auth/verify", `<html><title>Unauthorized</title></html>`, 401, ErrUnavailable},
		{"Google auth invoker denied", "/auth/start", `<html><title>Forbidden</title></html>`, 403, ErrUnavailable},
		{"Google MCP identity expired", "", `<html><title>Unauthorized</title></html>`, 401, ErrUnavailable},
		{"Google MCP invoker denied", "", `<html><title>Forbidden</title></html>`, 403, ErrUnavailable},
		{"Google JSON denial", "", `{"error":{"code":403,"status":"PERMISSION_DENIED"}}`, 403, ErrUnavailable},
		{"unknown auth denial", "/auth/start", `{"error":"Forbidden","status":403,"detail":"gateway identity denied"}`, 403, ErrUnavailable},
		{"unknown MCP denial", "", `{"error":"permission_denied"}`, 403, ErrUnavailable},
		{"mismatched auth status", "/auth/revoke", `{"error":"Forbidden","status":401,"detail":"account suspended"}`, 403, ErrUnavailable},
		{"wrong service envelope", "/auth/revoke", `{"error":"account_banned"}`, 403, ErrUnavailable},
		{"wrong MCP status", "", `{"error":"account_banned"}`, 401, ErrUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()
			remote, err := newRemote(context.Background(), server.URL, "", false)
			if err != nil {
				t.Fatal(err)
			}
			var out map[string]any
			if err := remote.call(context.Background(), tt.path, "ck_1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", nil, &out); !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
}
