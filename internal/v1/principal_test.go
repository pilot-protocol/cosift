package v1

import (
	"context"
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
