package v1

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestNames(t *testing.T) {
	scopes := map[Scope]string{
		ScopeArticlesRead:     "articles:read",
		ScopeArticlesReadAll:  "articles:read_all",
		ScopeArticlesWrite:    "articles:write",
		ScopeArticlesStub:     "articles:stub",
		ScopeArticlesModerate: "articles:moderate",
		ScopeArticlesAdmin:    "articles:admin",
		ScopeRetrieveRead:     "retrieve:read",
	}
	for s, name := range scopes {
		if string(s) != name {
			t.Errorf("scope %q, want %q", s, name)
		}
		if !s.Valid() {
			t.Errorf("%q not valid", s)
		}
	}
	for _, s := range []Scope{"", "articles:readall", "Articles:read", "articles:*", "articles:read ", "retrieve:write", "articles"} {
		if s.Valid() {
			t.Errorf("%q valid", s)
		}
	}

	if EnvProd != "prod" || EnvStaging != "staging" {
		t.Errorf("envs = %q, %q", EnvProd, EnvStaging)
	}
	for _, e := range []Env{EnvProd, EnvStaging} {
		if !e.Valid() {
			t.Errorf("%q not valid", e)
		}
	}
	for _, e := range []Env{"", "any", "Prod", "production", "dev"} {
		if e.Valid() {
			t.Errorf("%q valid", e)
		}
	}

	if KindOIDC != "oidc" || KindKey != "key" {
		t.Errorf("kinds = %q, %q", KindOIDC, KindKey)
	}
	for _, k := range []Kind{KindOIDC, KindKey} {
		if !k.Valid() {
			t.Errorf("%q not valid", k)
		}
	}
	for _, k := range []Kind{"", "OIDC", "csk", "token"} {
		if k.Valid() {
			t.Errorf("%q valid", k)
		}
	}
}

func TestScopeChecks(t *testing.T) {
	synth := Principal{Scopes: []Scope{ScopeArticlesRead, ScopeArticlesWrite, ScopeRetrieveRead}}
	dash := Principal{Scopes: []Scope{ScopeArticlesReadAll, ScopeArticlesModerate}}
	cases := []struct {
		name string
		p    Principal
		ask  []Scope
		has  bool
		any  bool
	}{
		{"held", synth, []Scope{ScopeArticlesWrite}, true, true},
		{"not held", synth, []Scope{ScopeArticlesStub}, false, false},
		{"one of two", synth, []Scope{ScopeArticlesStub, ScopeArticlesWrite}, false, true},
		{"read_all is not read", dash, []Scope{ScopeArticlesRead}, false, false},
		{"read or read_all", dash, []Scope{ScopeArticlesRead, ScopeArticlesReadAll}, false, true},
		{"no scopes", Principal{}, []Scope{ScopeArticlesRead}, false, false},
		{"empty ask", synth, nil, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if len(c.ask) == 1 {
				if got := c.p.Has(c.ask[0]); got != c.has {
					t.Errorf("Has = %v, want %v", got, c.has)
				}
			}
			if got := c.p.HasAny(c.ask...); got != c.any {
				t.Errorf("HasAny = %v, want %v", got, c.any)
			}
		})
	}
}

func TestEnvBinding(t *testing.T) {
	cases := []struct {
		env         Env
		wantPrelive bool
		sameProd    bool
		samePrelive bool
	}{
		{EnvProd, false, true, false},
		{EnvStaging, true, false, true},
		{"", false, false, false},
		{"any", false, false, false},
	}
	for _, c := range cases {
		p := Principal{Env: c.env}
		if got := p.Prelive(); got != c.wantPrelive {
			t.Errorf("%q: Prelive = %v, want %v", c.env, got, c.wantPrelive)
		}
		if got := p.SameEnv(false); got != c.sameProd {
			t.Errorf("%q: SameEnv(false) = %v, want %v", c.env, got, c.sameProd)
		}
		if got := p.SameEnv(true); got != c.samePrelive {
			t.Errorf("%q: SameEnv(true) = %v, want %v", c.env, got, c.samePrelive)
		}
	}
}

func TestPrincipalContext(t *testing.T) {
	if _, ok := PrincipalFrom(context.Background()); ok {
		t.Error("principal in an empty context")
	}
	p := Principal{ID: "dash-staging", Kind: KindKey, Env: EnvStaging, Scopes: []Scope{ScopeArticlesReadAll, ScopeArticlesModerate}}
	got, ok := PrincipalFrom(WithPrincipal(context.Background(), p))
	if !ok || !reflect.DeepEqual(got, p) {
		t.Errorf("round trip = %+v, %v", got, ok)
	}
	type otherKey struct{}
	if _, ok := PrincipalFrom(context.WithValue(context.Background(), otherKey{}, p)); ok {
		t.Error("principal read from a foreign key")
	}
}

func TestRequestWithPrincipalThroughMux(t *testing.T) {
	p := Principal{ID: "mcp-prod", Kind: KindOIDC, Env: EnvProd, Scopes: []Scope{ScopeArticlesRead}, CountsReads: true}
	var (
		got     Principal
		ok      bool
		pattern string
		id      string
	)
	rt := Route{Method: http.MethodGet, Path: "/v1/articles/{id}", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok = PrincipalFrom(r.Context())
		pattern, id = r.Pattern, r.PathValue("id")
	})}
	mux := http.NewServeMux()
	mux.Handle(rt.Pattern(), rt.Handler)

	base := httptest.NewRequest(http.MethodGet, "/v1/articles/01J8ZC2Q7W4X9M3K5N6P8R0T2V", nil)
	mux.ServeHTTP(httptest.NewRecorder(), RequestWithPrincipal(base, p))
	if !ok || !reflect.DeepEqual(got, p) {
		t.Errorf("principal = %+v, %v", got, ok)
	}
	if pattern != "GET /v1/articles/{id}" || id != "01J8ZC2Q7W4X9M3K5N6P8R0T2V" {
		t.Errorf("pattern %q, id %q", pattern, id)
	}
	if _, ok := PrincipalFrom(base.Context()); ok {
		t.Error("original request modified")
	}
}

func TestRequireAny(t *testing.T) {
	unauthenticated := `{"error": "Unauthorized", "status": 401, "code": "unauthenticated", "detail": "missing or invalid credential"}`
	missing := func(s Scope) string {
		return `{"error": "Forbidden", "status": 403, "code": "missing_scope", "detail": "requires scope ` + string(s) + `"}`
	}
	retrieveOnly := Principal{ID: "retrieve-only", Kind: KindOIDC, Env: EnvProd, Scopes: []Scope{ScopeRetrieveRead}}
	cases := []struct {
		name    string
		p       *Principal
		scopes  []Scope
		doc     string
		headers map[string]string
	}{
		{"holds the scope", &synthProd, []Scope{ScopeArticlesWrite}, "", nil},
		{"holds the second", &dashStaging, []Scope{ScopeArticlesRead, ScopeArticlesReadAll}, "", nil},
		{"holds one of three", &resolverProd, []Scope{ScopeArticlesWrite, ScopeArticlesStub, ScopeArticlesAdmin}, "", nil},
		{"stub token sending an article", &resolverProd, []Scope{ScopeArticlesWrite},
			`{"error": "Forbidden", "status": 403, "code": "missing_scope", "detail": "requires scope articles:write"}`, nil},
		{"names the first scope", &retrieveOnly, []Scope{ScopeArticlesReadAll, ScopeArticlesRead}, missing(ScopeArticlesReadAll), nil},
		{"read_all is not read", &dashStaging, []Scope{ScopeArticlesRead}, missing(ScopeArticlesRead), nil},
		{"no scopes held", &Principal{ID: "empty"}, []Scope{ScopeArticlesRead}, missing(ScopeArticlesRead), nil},
		{"no principal", nil, []Scope{ScopeArticlesRead}, unauthenticated,
			map[string]string{"WWW-Authenticate": `Bearer realm="cosift-v1"`}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var (
				got    Principal
				passed bool
			)
			mux := http.NewServeMux()
			mux.HandleFunc("POST /v1/articles/{id}/status", func(w http.ResponseWriter, r *http.Request) {
				p, ok := RequireAny(w, r, c.scopes...)
				if !ok {
					return
				}
				got, passed = p, true
				w.WriteHeader(http.StatusNoContent)
			})
			req := httptest.NewRequest(http.MethodPost, "/v1/articles/01J8ZC2Q7W4X9M3K5N6P8R0T2V/status", nil)
			if c.p != nil {
				req = RequestWithPrincipal(req, *c.p)
			}
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if c.doc == "" {
				if !passed || !reflect.DeepEqual(got, *c.p) {
					t.Fatalf("refused or wrong principal: %+v, %v, %s", got, passed, rec.Body.String())
				}
				if rec.Code != http.StatusNoContent || rec.Body.Len() != 0 || len(rec.Header()) != 0 {
					t.Errorf("RequireAny wrote a response on success: %d %v %q", rec.Code, rec.Header(), rec.Body.String())
				}
				return
			}
			if passed {
				t.Fatal("handler ran past a refusal")
			}
			var e Error
			if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil || rec.Code != e.Status {
				t.Errorf("status %d, body %q", rec.Code, rec.Body.String())
			}
			if got, want := rec.Body.String(), wireJSON(t, c.doc); got != want {
				t.Errorf("body\n got %s\nwant %s", got, want)
			}
			checkHeaders(t, rec, c.headers)
		})
	}
}

func TestRequireAnyWithoutScopesPanics(t *testing.T) {
	for _, withPrincipal := range []bool{true, false} {
		req := httptest.NewRequest(http.MethodGet, "/v1/articles/stats", nil)
		if withPrincipal {
			req = RequestWithPrincipal(req, synthProd)
		}
		rec := httptest.NewRecorder()
		func() {
			defer func() {
				if p := recover(); p != "v1.RequireAny: no scopes" {
					t.Errorf("principal %v: recovered %v", withPrincipal, p)
				}
			}()
			RequireAny(rec, req)
			t.Errorf("principal %v: returned instead of panicking", withPrincipal)
		}()
		if rec.Body.Len() != 0 || len(rec.Header()) != 0 {
			t.Errorf("principal %v: wrote a response before panicking", withPrincipal)
		}
	}
}
