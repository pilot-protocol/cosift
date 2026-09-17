package community

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

// These fixtures exercise the gateway's HTTP and storage boundaries separately
// from the provider's Firestore and MCP transport contract tests.
type scriptedShared struct {
	fakeShared
	startFn  func(context.Context, string) (sharedaccount.Challenge, error)
	finishFn func(context.Context, string, string) (sharedaccount.Issued, error)
	verifyFn func(context.Context, string) (sharedaccount.Identity, error)
	revokeFn func(context.Context, string) error
	callFn   func(context.Context, string, string, map[string]any) (map[string]any, error)
}

func (f *scriptedShared) Start(ctx context.Context, email string) (sharedaccount.Challenge, error) {
	if f.startFn != nil {
		return f.startFn(ctx, email)
	}
	return f.fakeShared.Start(ctx, email)
}
func (f *scriptedShared) Finish(ctx context.Context, id, code string) (sharedaccount.Issued, error) {
	if f.finishFn != nil {
		return f.finishFn(ctx, id, code)
	}
	return f.fakeShared.Finish(ctx, id, code)
}
func (f *scriptedShared) Verify(ctx context.Context, token string) (sharedaccount.Identity, error) {
	if f.verifyFn != nil {
		return f.verifyFn(ctx, token)
	}
	return f.fakeShared.Verify(ctx, token)
}
func (f *scriptedShared) Revoke(ctx context.Context, token string) error {
	if f.revokeFn != nil {
		return f.revokeFn(ctx, token)
	}
	return f.fakeShared.Revoke(ctx, token)
}
func (f *scriptedShared) Call(ctx context.Context, token, tool string, args map[string]any) (map[string]any, error) {
	if f.callFn != nil {
		return f.callFn(ctx, token, tool, args)
	}
	return f.fakeShared.Call(ctx, token, tool, args)
}

func sharedJSON(s *Server, path, body, token, peer string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Cosift-Client", "community")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if peer != "" {
		r.RemoteAddr = peer
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}

func TestSharedLoginDisabledAndAdvertised(t *testing.T) {
	s := testServer(t, nil)
	w := request(t, s, "GET", "/api/auth/config", nil, nil)
	expect(t, w, 200)
	if strings.TrimSpace(w.Body.String()) != `{"shared":false}` {
		t.Fatal(w.Body.String())
	}
	for _, path := range []string{"/api/auth/start", "/api/auth/verify"} {
		expect(t, sharedJSON(s, path, `{}`, "", ""), 404)
	}
	// A shared bearer cannot accidentally authorize an independent local account.
	expect(t, bearerRequest(s, "/api/me", sharedTestToken), 401)
	cookie := account(t, s, "local-only@example.com")
	expect(t, request(t, s, "POST", "/api/shared", map[string]string{"tool": "cosift_topics", "action": "list"}, cookie), 404)
	s.cfg.Shared = &fakeShared{}
	w = request(t, s, "GET", "/api/auth/config", nil, nil)
	expect(t, w, 200)
	if strings.TrimSpace(w.Body.String()) != `{"shared":true}` {
		t.Fatal(w.Body.String())
	}
}

func TestSharedStartValidatesBeforeSendingEmail(t *testing.T) {
	cases := []string{
		`{`, `{"email":"shared@example.com","unexpected":true}`,
		`{"email":"shared@example.com"} {}`, `{"email":null}`, `{"email":""}`,
		`{"email":"invalid"}`, `{"email":"Someone <shared@example.com>"}`,
		fmt.Sprintf(`{"email":%q}`, strings.Repeat("a", 243)+"@example.com"),
	}
	for _, body := range cases {
		t.Run(body, func(t *testing.T) {
			s := testServer(t, nil)
			s.cfg.Shared = &scriptedShared{startFn: func(context.Context, string) (sharedaccount.Challenge, error) {
				t.Error("invalid email reached upstream")
				return sharedaccount.Challenge{}, nil
			}}
			expect(t, sharedJSON(s, "/api/auth/start", body, "", ""), 400)
		})
	}
	t.Run("normalizes and returns only challenge", func(t *testing.T) {
		s := testServer(t, nil)
		challenge := sharedaccount.Challenge{RequestID: "01ARZ3NDEKTSV4RRFFQ69G5FAV", ExpiresAt: time.Now().UTC().Add(time.Minute).Truncate(time.Second)}
		s.cfg.Shared = &scriptedShared{startFn: func(_ context.Context, email string) (sharedaccount.Challenge, error) {
			if email != "shared@example.com" {
				t.Errorf("upstream email %q", email)
			}
			return challenge, nil
		}}
		w := sharedJSON(s, "/api/auth/start", `{"email":"  SHARED@Example.com  "}`, "", "")
		expect(t, w, 200)
		var got sharedaccount.Challenge
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || got != challenge {
			t.Fatalf("challenge %+v, %v", got, err)
		}
		if len(w.Result().Cookies()) != 0 {
			t.Fatal("email challenge granted a session")
		}
	})
}

func TestSharedLoginUpstreamErrorsStayPrivate(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
	}{
		{"invalid", sharedaccount.ErrInvalid, 400},
		{"unauthorized", sharedaccount.ErrUnauthorized, 401},
		{"banned", sharedaccount.ErrBanned, 403},
		{"limited", sharedaccount.ErrLimited, 429},
		{"unavailable", sharedaccount.ErrUnavailable, 503},
		{"unexpected", errors.New("private-service-account-and-database-details"), 503},
	} {
		for _, path := range []string{"/api/auth/start", "/api/auth/verify"} {
			t.Run(tc.name+path, func(t *testing.T) {
				s := testServer(t, nil)
				f := &scriptedShared{fakeShared: fakeShared{err: fmt.Errorf("private-upstream-detail: %w", tc.err)}}
				f.verifyFn = func(context.Context, string) (sharedaccount.Identity, error) {
					t.Error("failed code exchange proceeded to token verification")
					return sharedaccount.Identity{}, nil
				}
				s.cfg.Shared = f
				body := `{"email":"shared@example.com"}`
				if path == "/api/auth/verify" {
					body = `{"request_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","code":"123456"}`
				}
				w := sharedJSON(s, path, body, "", "")
				expect(t, w, tc.status)
				if strings.Contains(w.Body.String(), "private-") || len(w.Result().Cookies()) != 0 || f.revoked {
					t.Fatalf("failed exchange leaked details or changed credentials: %s", w.Body.String())
				}
			})
		}
	}
}

func TestSharedLoginRateLimitsArePerClientAndEndpoint(t *testing.T) {
	s := testServer(t, nil)
	starts, finishes := 0, 0
	s.cfg.Shared = &scriptedShared{
		startFn: func(context.Context, string) (sharedaccount.Challenge, error) {
			starts++
			return sharedaccount.Challenge{}, nil
		},
		finishFn: func(context.Context, string, string) (sharedaccount.Issued, error) {
			finishes++
			return sharedaccount.Issued{}, sharedaccount.ErrUnauthorized
		},
	}
	start := `{"email":"shared@example.com"}`
	finish := `{"request_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","code":"123456"}`
	for i := 0; i < 10; i++ {
		expect(t, sharedJSON(s, "/api/auth/start", start, "", "192.0.2.1:1234"), 200)
	}
	expect(t, sharedJSON(s, "/api/auth/start", start, "", "192.0.2.1:5678"), 429)
	expect(t, sharedJSON(s, "/api/auth/start", start, "", "192.0.2.2:1234"), 200)
	for i := 0; i < 20; i++ {
		expect(t, sharedJSON(s, "/api/auth/verify", finish, "", "192.0.2.1:1234"), 401)
	}
	expect(t, sharedJSON(s, "/api/auth/verify", finish, "", "192.0.2.1:5678"), 429)
	expect(t, sharedJSON(s, "/api/auth/verify", finish, "", "192.0.2.2:1234"), 401)
	if starts != 11 || finishes != 21 {
		t.Fatalf("rejected requests reached upstream: starts=%d finishes=%d", starts, finishes)
	}
}

func TestSharedFinishRejectsMalformedCodesBeforeExchange(t *testing.T) {
	for _, body := range []string{
		`{`, `{"request_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","code":"123456","token":"supplied"}`,
		`{"request_id":"short","code":"123456"}`,
		`{"request_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","code":"12345"}`,
		`{"request_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","code":"1234567"}`,
		`{"request_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","code":"12345a"}`,
		`{"request_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","code":" 12345"}`,
	} {
		t.Run(body, func(t *testing.T) {
			s := testServer(t, nil)
			s.cfg.Shared = &scriptedShared{finishFn: func(context.Context, string, string) (sharedaccount.Issued, error) {
				t.Error("malformed OTP reached upstream")
				return sharedaccount.Issued{}, nil
			}}
			expect(t, sharedJSON(s, "/api/auth/verify", body, "", ""), 400)
		})
	}
}

func TestSharedFinishRevokesIssuedTokenWhenAccountCannotBeEstablished(t *testing.T) {
	for _, tc := range []struct {
		name      string
		identity  sharedaccount.Identity
		verifyErr error
		closeDB   bool
		status    int
	}{
		{"revoked before use", sharedaccount.Identity{}, sharedaccount.ErrUnauthorized, false, 401},
		{"banned account", sharedaccount.Identity{}, sharedaccount.ErrBanned, false, 403},
		{"provider failure", sharedaccount.Identity{}, sharedaccount.ErrUnavailable, false, 503},
		{"UID mismatch", sharedaccount.Identity{UID: "fedcba9876543210", Email: "shared@example.com"}, nil, false, 401},
		{"missing verified email", sharedaccount.Identity{UID: "0123456789abcdef"}, nil, false, 503},
		{"database unavailable", sharedaccount.Identity{UID: "0123456789abcdef", Email: "shared@example.com"}, nil, true, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testServer(t, nil)
			if tc.closeDB {
				if err := s.db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			revokes := 0
			s.cfg.Shared = &scriptedShared{
				verifyFn: func(_ context.Context, token string) (sharedaccount.Identity, error) {
					if token != sharedTestToken {
						t.Errorf("verified wrong issued token %q", token)
					}
					return tc.identity, tc.verifyErr
				},
				revokeFn: func(ctx context.Context, token string) error {
					revokes++
					deadline, ok := ctx.Deadline()
					if token != sharedTestToken || ctx.Err() != nil || !ok || time.Until(deadline) > 5*time.Second {
						t.Error("cleanup token or bounded context incorrect")
					}
					return errors.New("revocation also unavailable")
				},
			}
			w := sharedJSON(s, "/api/auth/verify", `{"request_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","code":"123456"}`, "", "")
			expect(t, w, tc.status)
			if revokes != 1 || len(w.Result().Cookies()) != 0 || strings.Contains(w.Body.String(), sharedTestToken) {
				t.Fatal("failed login did not safely discard issued credential")
			}
			if !tc.closeDB {
				var count int
				if err := s.db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count); err != nil || count != 0 {
					t.Fatalf("failed login created a user: count=%d err=%v", count, err)
				}
			}
		})
	}
}

func TestSharedFinishUsesSecureCookieAndIndependentCleanupContext(t *testing.T) {
	t.Run("HTTPS login", func(t *testing.T) {
		s := testServer(t, nil)
		s.cfg.PublicURL = "https://community.example.com"
		s.cfg.Shared = &scriptedShared{finishFn: func(_ context.Context, id, code string) (sharedaccount.Issued, error) {
			if id != "01ARZ3NDEKTSV4RRFFQ69G5FAV" || code != "012345" {
				t.Error("OTP or request ID changed")
			}
			return sharedaccount.Issued{Token: sharedTestToken, UID: "0123456789abcdef"}, nil
		}}
		w := sharedJSON(s, "/api/auth/verify", `{"request_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","code":"012345"}`, "", "")
		expect(t, w, 200)
		cookies := w.Result().Cookies()
		if len(cookies) != 1 {
			t.Fatal("missing session cookie")
		}
		c := cookies[0]
		if c.Value != sharedTestToken || !c.Secure || !c.HttpOnly || c.Path != "/" || c.SameSite != http.SameSiteLaxMode || c.MaxAge != int(sessionAge.Seconds()) || !c.Expires.After(time.Now()) {
			t.Fatalf("unsafe session cookie: %+v", c)
		}
		var u User
		if err := json.Unmarshal(w.Body.Bytes(), &u); err != nil || u.Email != "shared@example.com" || u.ID == "" || u.Onboarded {
			t.Fatalf("unexpected initial user %+v, %v", u, err)
		}
		if strings.Contains(w.Body.String(), sharedTestToken) {
			t.Fatal("token exposed to JavaScript")
		}
		expect(t, request(t, s, "GET", "/api/me", nil, c), 200)
	})
	t.Run("disconnected browser still revokes orphaned token", func(t *testing.T) {
		s := testServer(t, nil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		revoked := false
		s.cfg.Shared = &scriptedShared{
			finishFn: func(context.Context, string, string) (sharedaccount.Issued, error) {
				cancel()
				return sharedaccount.Issued{Token: sharedTestToken, UID: "0123456789abcdef"}, nil
			},
			verifyFn: func(ctx context.Context, _ string) (sharedaccount.Identity, error) {
				return sharedaccount.Identity{}, ctx.Err()
			},
			revokeFn: func(ctx context.Context, token string) error {
				revoked = ctx.Err() == nil && token == sharedTestToken
				return nil
			},
		}
		r := httptest.NewRequest("POST", "/api/auth/verify", strings.NewReader(`{"request_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","code":"123456"}`)).WithContext(ctx)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Cosift-Client", "community")
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		expect(t, w, 503)
		if !revoked || len(w.Result().Cookies()) != 0 {
			t.Fatal("browser cancellation prevented credential cleanup")
		}
	})
}

func TestSharedAuthCredentialPrecedenceAndNoGuestFallback(t *testing.T) {
	for _, tc := range []struct {
		name, header, cookie string
		providerErr          error
		status, verifies     int
		cleared              bool
	}{
		{"missing credentials", "", "", nil, 200, 0, false},
		{"empty cookie", "", "empty", nil, 401, 0, false},
		{"malformed cookie", "", "not-a-token", nil, 401, 0, false},
		{"malformed bearer overrides cookie", "Basic irrelevant", sharedTestToken, nil, 401, 0, false},
		{"empty bearer overrides cookie", "Bearer", sharedTestToken, nil, 401, 0, false},
		{"revoked cookie", "", sharedTestToken, sharedaccount.ErrUnauthorized, 401, 1, true},
		{"provider unavailable", "", sharedTestToken, sharedaccount.ErrUnavailable, 503, 1, false},
		{"banned bearer", "Bearer " + sharedTestToken, sharedOtherToken, sharedaccount.ErrBanned, 403, 1, false},
		{"revoked bearer preserves unrelated cookie", "Bearer " + sharedTestToken, sharedOtherToken, sharedaccount.ErrUnauthorized, 401, 1, false},
		{"case insensitive bearer", "bEaReR " + sharedTestToken, "", nil, 200, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engineCalls, verifies := 0, 0
			s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				engineCalls++
				_, _ = w.Write([]byte(`{"hits":[]}`))
			}))
			s.cfg.Shared = &scriptedShared{verifyFn: func(ctx context.Context, token string) (sharedaccount.Identity, error) {
				verifies++
				deadline, ok := ctx.Deadline()
				if token != sharedTestToken || !ok || time.Until(deadline) > 10*time.Second {
					t.Error("wrong credential or unbounded authentication call")
				}
				return sharedaccount.Identity{UID: "0123456789abcdef", Email: "shared@example.com"}, tc.providerErr
			}}
			r := httptest.NewRequest("GET", "/api/search?q=rust", nil)
			if tc.header != "" {
				r.Header.Set("Authorization", tc.header)
			}
			if tc.cookie != "" {
				value := tc.cookie
				if value == "empty" {
					value = ""
				}
				r.AddCookie(&http.Cookie{Name: cookieName, Value: value})
			}
			w := httptest.NewRecorder()
			s.ServeHTTP(w, r)
			expect(t, w, tc.status)
			wantEngine := 0
			if tc.status == 200 {
				wantEngine = 1
			}
			if engineCalls != wantEngine || verifies != tc.verifies {
				t.Fatalf("calls engine=%d verify=%d", engineCalls, verifies)
			}
			cookies := w.Result().Cookies()
			if tc.cleared {
				if len(cookies) != 1 || cookies[0].MaxAge != -1 || cookies[0].Value != "" {
					t.Fatal("revoked browser cookie was not cleared")
				}
			} else if len(cookies) != 0 {
				t.Fatal("unrelated or retryable credential was removed")
			}
			if tc.status != 200 {
				var usage int
				if err := s.db.QueryRow(`SELECT COUNT(*) FROM guest_usage`).Scan(&usage); err != nil || usage != 0 {
					t.Fatalf("failed authentication spent guest allowance: usage=%d err=%v", usage, err)
				}
			}
		})
	}
}

func TestSharedToolValidatesAndForwardsOnlySupportedArguments(t *testing.T) {
	for _, tc := range []struct {
		name, body, tool string
		args             map[string]any
	}{
		{"list", `{"tool":"cosift_topics","action":"list","topics":["Ignored"]}`, "cosift_topics", map[string]any{"action": "list"}},
		{"add", `{"tool":"cosift_topics","action":"add","topics":["Rust","Open source"]}`, "cosift_topics", map[string]any{"action": "add", "topics": []string{"Rust", "Open source"}}},
		{"remove", `{"tool":"cosift_topics","action":"remove","topics":["Rust"]}`, "cosift_topics", map[string]any{"action": "remove", "topics": []string{"Rust"}}},
		{"lookup", `{"tool":"cosift_lookup","topic":"Rust","why":"Ignored"}`, "cosift_lookup", map[string]any{"topic": "Rust"}},
		{"request with reason", `{"tool":"cosift_request","topic":"Rust","why":"Learning async"}`, "cosift_request", map[string]any{"topic": "Rust", "why": "Learning async"}},
		{"request without reason", `{"tool":"cosift_request","topic":"Rust"}`, "cosift_request", map[string]any{"topic": "Rust"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testServer(t, nil)
			calls := 0
			s.cfg.Shared = &scriptedShared{callFn: func(_ context.Context, token, tool string, args map[string]any) (map[string]any, error) {
				calls++
				if token != sharedTestToken || tool != tc.tool || !reflect.DeepEqual(args, tc.args) {
					t.Fatalf("wrong upstream request: token=%t tool=%s args=%#v", token == sharedTestToken, tool, args)
				}
				return map[string]any{"status": "requested", "already_requested": true}, nil
			}}
			w := sharedJSON(s, "/api/shared", tc.body, sharedTestToken, "")
			expect(t, w, 200)
			if calls != 1 || !strings.Contains(w.Body.String(), `"already_requested":true`) {
				t.Fatal("MCP response was lost or request repeated")
			}
		})
	}
	invalid := []string{
		`{`, `{"tool":"cosift_search","topic":"Rust"}`, `{"tool":"cosift_topics","action":"replace","topics":["Rust"]}`,
		`{"tool":"cosift_topics","action":"add","topics":[]}`, `{"tool":"cosift_topics","action":"remove"}`,
		`{"tool":"cosift_topics","action":"add","topics":[" "]}`,
		fmt.Sprintf(`{"tool":"cosift_topics","action":"add","topics":[%q]}`, strings.Repeat("x", 201)),
		`{"tool":"cosift_lookup","topic":" "}`,
		fmt.Sprintf(`{"tool":"cosift_request","topic":%q}`, strings.Repeat("x", 201)),
		fmt.Sprintf(`{"tool":"cosift_request","topic":"Rust","why":%q}`, strings.Repeat("x", 281)),
		`{"tool":"cosift_request","topic":"Rust","user_id":"someone-else"}`,
	}
	topics, err := json.Marshal(map[string]any{"tool": "cosift_topics", "action": "list", "topics": make([]string, 21)})
	if err != nil {
		t.Fatal(err)
	}
	invalid = append(invalid, string(topics))
	for i, body := range invalid {
		t.Run(fmt.Sprintf("invalid%d", i), func(t *testing.T) {
			s := testServer(t, nil)
			f := &fakeShared{}
			s.cfg.Shared = f
			expect(t, sharedJSON(s, "/api/shared", body, sharedTestToken, ""), 400)
			if f.calls != 0 {
				t.Fatal("invalid tool arguments reached MCP")
			}
		})
	}
}

func TestSharedToolLimitsAndUpstreamFailures(t *testing.T) {
	s := testServer(t, nil)
	f := &scriptedShared{}
	s.cfg.Shared = f
	body := `{"tool":"cosift_topics","action":"list"}`
	for i := 0; i < 30; i++ {
		expect(t, sharedJSON(s, "/api/shared", body, sharedTestToken, ""), 200)
	}
	expect(t, sharedJSON(s, "/api/shared", body, sharedTestToken, "192.0.2.22:1234"), 429)
	expect(t, sharedJSON(s, "/api/shared", body, sharedOtherToken, ""), 200)
	if f.calls != 31 {
		t.Fatalf("per-account tool limit allowed %d upstream calls", f.calls)
	}
	// An upstream outage after successful auth must remain retryable and must
	// not be represented as an empty topic list or a successfully queued article.
	f.callFn = func(context.Context, string, string, map[string]any) (map[string]any, error) {
		return nil, fmt.Errorf("private MCP response: %w", sharedaccount.ErrUnavailable)
	}
	w := sharedJSON(s, "/api/shared", body, sharedOtherToken, "")
	expect(t, w, 503)
	if strings.Contains(w.Body.String(), "private MCP") {
		t.Fatal("upstream service details leaked")
	}
}

func TestSharedIdentityLinkCannotReassignAnExistingAccount(t *testing.T) {
	s := testServer(t, nil)
	identity := sharedaccount.Identity{UID: "0123456789abcdef", Email: "shared@example.com"}
	u, err := s.sharedUser(context.Background(), identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO saved_searches(id,user_id,query,mode,created_at) VALUES('preserved',?,'private saved search','search',0)`, u.ID); err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []sharedaccount.Identity{
		{UID: "INVALID", Email: identity.Email},
		{UID: identity.UID},
		{UID: identity.UID, Email: "changed@example.com"},
		{UID: "fedcba9876543210", Email: identity.Email},
	} {
		if _, err := s.sharedUser(context.Background(), candidate); err == nil {
			t.Fatalf("allowed account reassignment %+v", candidate)
		}
	}
	again, err := s.sharedUser(context.Background(), identity)
	if err != nil || again.ID != u.ID || again.Email != u.Email {
		t.Fatalf("original account changed: %+v, %v", again, err)
	}
	var users, identities, saved int
	for _, q := range []struct {
		sql string
		dst *int
	}{
		{`SELECT COUNT(*) FROM users`, &users},
		{`SELECT COUNT(*) FROM shared_identities`, &identities},
		{`SELECT COUNT(*) FROM saved_searches WHERE user_id=?`, &saved},
	} {
		var err error
		if q.dst == &saved {
			err = s.db.QueryRow(q.sql, u.ID).Scan(q.dst)
		} else {
			err = s.db.QueryRow(q.sql).Scan(q.dst)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if users != 1 || identities != 1 || saved != 1 {
		t.Fatalf("failed links changed local data: users=%d identities=%d saved=%d", users, identities, saved)
	}
	// The same mismatch through optional authentication must not become a guest.
	s.cfg.Shared = &scriptedShared{verifyFn: func(context.Context, string) (sharedaccount.Identity, error) {
		return sharedaccount.Identity{UID: identity.UID, Email: "changed@example.com"}, nil
	}}
	expect(t, bearerRequest(s, "/api/search?q=rust", sharedTestToken), 503)
}

type emptyNamespaceShared struct{ fakeShared }

func (*emptyNamespaceShared) Namespace() string { return "" }

func TestSharedNamespacePersistsAcrossRestarts(t *testing.T) {
	s := testServer(t, nil)
	cfg := s.cfg
	cfg.Shared = &fakeShared{}
	first, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Open(cfg)
	if err != nil {
		t.Fatalf("same provider could not reopen database: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	for _, provider := range []sharedaccount.Provider{nil, &fakeShared{namespace: "test/(default)"}, &emptyNamespaceShared{}} {
		cfg.Shared = provider
		opened, err := Open(cfg)
		if err == nil {
			opened.Close()
			t.Fatal("database opened under different authentication authority")
		}
	}
	// Broken persistence must not silently start with a different auth authority.
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	s.cfg.Shared = &fakeShared{}
	if err := s.bindSharedNamespace(); err == nil {
		t.Fatal("closed database accepted shared namespace")
	}
}
