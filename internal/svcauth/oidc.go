package svcauth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"google.golang.org/api/idtoken"
)

// Audience is a constant: idtoken.Validate skips the audience check when
// passed an empty audience.
const Audience = "https://cosift.pilotprotocol.network/v1"

const maxTokenLifetime = 3600

func allowedIssuer(iss string) bool {
	switch iss {
	case "https://accounts.google.com", "accounts.google.com":
		return true
	}
	return false
}

// TokenValidator verifies a Google ID token's signature; *idtoken.Validator
// is the production implementation.
type TokenValidator interface {
	Validate(ctx context.Context, token, audience string) (*idtoken.Payload, error)
}

type certState interface {
	Usable() bool
	HasKid(kid string) bool
	RequestEarly()
}

type oidcVerifier struct {
	validator TokenValidator
	certs     certState
	now       func() time.Time
}

type oidcResult struct {
	p           *principalState
	reason      string
	unavailable bool
}

func (v *oidcVerifier) verify(ctx context.Context, st *state, tok string) oidcResult {
	kid, reason := checkHeader(tok)
	if reason != "" {
		return oidcResult{reason: reason}
	}
	pre, err := idtoken.ParsePayload(tok)
	if err != nil {
		return oidcResult{reason: payloadTypeReason(tok)}
	}
	if _, reason := checkClaims(st, pre, v.now()); reason != "" {
		return oidcResult{reason: reason}
	}
	if !v.certs.Usable() {
		return oidcResult{unavailable: true}
	}
	if !v.certs.HasKid(kid) {
		v.certs.RequestEarly()
		return oidcResult{reason: "bad_signature"}
	}
	verified, err := v.validator.Validate(ctx, tok, Audience)
	if err != nil {
		if errors.Is(err, ErrNoCerts) {
			return oidcResult{unavailable: true}
		}
		return oidcResult{reason: "bad_signature"}
	}
	ps, reason := checkClaims(st, verified, v.now())
	if reason != "" {
		return oidcResult{reason: reason}
	}
	return oidcResult{p: ps}
}

// checkHeader requires three base64url segments whose header and
// payload are JSON objects, and alg RS256 with a kid, before any network I/O.
func checkHeader(tok string) (string, string) {
	segs := strings.Split(tok, ".")
	if len(segs) != 3 {
		return "", "malformed_token"
	}
	var parts [3][]byte
	for i, s := range segs {
		b, err := base64.RawURLEncoding.DecodeString(s)
		if err != nil || len(s) == 0 {
			return "", "malformed_token"
		}
		parts[i] = b
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(parts[0], &obj) != nil || json.Unmarshal(parts[1], &obj) != nil {
		return "", "malformed_token"
	}
	var h struct {
		Algorithm string `json:"alg"`
		Type      string `json:"typ"`
		KeyID     string `json:"kid"`
	}
	if json.Unmarshal(parts[0], &h) != nil {
		return "", "malformed_token"
	}
	if h.Algorithm != "RS256" || h.KeyID == "" {
		return "", "bad_alg"
	}
	return h.KeyID, ""
}

// payloadTypeReason names the claim that made the payload undecodable.
func payloadTypeReason(tok string) string {
	b, err := base64.RawURLEncoding.DecodeString(strings.Split(tok, ".")[1])
	if err != nil {
		return "malformed_token"
	}
	var claims map[string]any
	if json.Unmarshal(b, &claims) != nil {
		return "malformed_token"
	}
	if _, ok := claims["iss"].(string); !ok {
		return "bad_iss"
	}
	if _, ok := claims["aud"].(string); !ok {
		return "bad_aud"
	}
	return "malformed_token"
}

// checkClaims runs on the unverified payload as an early exit
// and again on the payload Validate returned, the only one decided on.
func checkClaims(st *state, p *idtoken.Payload, now time.Time) (*principalState, string) {
	if iss, _ := p.Claims["iss"].(string); iss != p.Issuer || !allowedIssuer(p.Issuer) {
		return nil, "bad_iss"
	}
	if aud, _ := p.Claims["aud"].(string); aud != p.Audience || p.Audience != Audience {
		return nil, "bad_aud"
	}
	if _, ok := p.Claims["exp"]; !ok || now.Unix() > p.Expires {
		return nil, "expired"
	}
	if _, ok := p.Claims["iat"]; !ok || p.IssuedAt < p.Expires-maxTokenLifetime {
		return nil, "bad_lifetime"
	}
	email, _ := p.Claims["email"].(string)
	sub, _ := p.Claims["sub"].(string)
	ps := st.bySub[p.Subject]
	if ps == nil || sub != p.Subject || email != ps.cfg.Email {
		return nil, "unknown_principal"
	}
	if verified, _ := p.Claims["email_verified"].(bool); !verified {
		return nil, "email_unverified"
	}
	return ps, ""
}
