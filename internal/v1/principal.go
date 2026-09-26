// Package v1 holds the types shared by the /v1 service listener and the handlers it mounts.
package v1

import (
	"context"
	"net/http"
	"slices"
)

type Kind string

const (
	KindOIDC Kind = "oidc"
	KindKey  Kind = "key"
)

func (k Kind) Valid() bool { return k == KindOIDC || k == KindKey }

type Env string

const (
	EnvProd    Env = "prod"
	EnvStaging Env = "staging"
)

func (e Env) Valid() bool { return e == EnvProd || e == EnvStaging }

type Scope string

const (
	ScopeArticlesRead     Scope = "articles:read"
	ScopeArticlesReadAll  Scope = "articles:read_all"
	ScopeArticlesWrite    Scope = "articles:write"
	ScopeArticlesStub     Scope = "articles:stub"
	ScopeArticlesModerate Scope = "articles:moderate"
	ScopeArticlesAdmin    Scope = "articles:admin"
	ScopeRetrieveRead     Scope = "retrieve:read"
)

func (s Scope) Valid() bool {
	switch s {
	case ScopeArticlesRead, ScopeArticlesReadAll, ScopeArticlesWrite, ScopeArticlesStub,
		ScopeArticlesModerate, ScopeArticlesAdmin, ScopeRetrieveRead:
		return true
	}
	return false
}

// Principal is an authenticated /v1 caller.
type Principal struct {
	ID          string
	Kind        Kind
	Env         Env
	Scopes      []Scope
	CountsReads bool
}

func (p Principal) Has(s Scope) bool { return slices.Contains(p.Scopes, s) }

func (p Principal) HasAny(scopes ...Scope) bool {
	for _, s := range scopes {
		if p.Has(s) {
			return true
		}
	}
	return false
}

// Prelive is the prelive flag of records the principal creates.
func (p Principal) Prelive() bool { return p.Env == EnvStaging }

// SameEnv reports whether a record with the given prelive flag is in the
// principal's environment, the only records it may modify.
func (p Principal) SameEnv(prelive bool) bool {
	switch p.Env {
	case EnvProd:
		return !prelive
	case EnvStaging:
		return prelive
	}
	return false
}

type principalKey struct{}

func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

// RequestWithPrincipal returns a copy of r carrying p, for the listener and for handler tests.
func RequestWithPrincipal(r *http.Request, p Principal) *http.Request {
	return r.WithContext(WithPrincipal(r.Context(), p))
}
