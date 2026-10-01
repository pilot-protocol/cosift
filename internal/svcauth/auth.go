package svcauth

import (
	"net/http"
	"regexp"
	"strings"
)

const maxCredential = 4096

var jwtShape = regexp.MustCompile(`^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$`)

type authOutcome struct {
	ps          *principalState
	reason      string
	unavailable bool
}

// authenticate extracts the one bearer credential and sends it to exactly one
// verifier. No path compares it with any other engine token.
func (s *Service) authenticate(r *http.Request, st *state) authOutcome {
	vals := r.Header.Values("Authorization")
	switch {
	case len(vals) == 0:
		return authOutcome{reason: "missing_credential"}
	case len(vals) > 1:
		return authOutcome{reason: "malformed_credential"}
	}
	cred, ok := bearer(vals[0])
	if !ok {
		return authOutcome{reason: "malformed_credential"}
	}
	switch {
	case strings.HasPrefix(cred, "csk_"):
		ps, reason := verifyKey(st, s.opts.Pepper, s.opts.ClientIPHeader, r, cred)
		return authOutcome{ps: ps, reason: reason}
	case jwtShape.MatchString(cred):
		res := s.oidc.verify(r.Context(), st, cred)
		return authOutcome{ps: res.p, reason: res.reason, unavailable: res.unavailable}
	}
	return authOutcome{reason: "unknown_credential"}
}

// bearer parses "Bearer" (ASCII case-insensitive), one space, then 1-4096
// printable ASCII bytes.
func bearer(v string) (string, bool) {
	const scheme = "bearer "
	if len(v) <= len(scheme) || !asciiEqualFold(v[:len(scheme)-1], scheme[:len(scheme)-1]) || v[len(scheme)-1] != ' ' {
		return "", false
	}
	cred := v[len(scheme):]
	if len(cred) > maxCredential {
		return "", false
	}
	for i := 0; i < len(cred); i++ {
		if c := cred[i]; c <= ' ' || c >= 0x7f {
			return "", false
		}
	}
	return cred, true
}

func asciiEqualFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		x, y := a[i], b[i]
		if 'A' <= x && x <= 'Z' {
			x += 'a' - 'A'
		}
		if 'A' <= y && y <= 'Z' {
			y += 'a' - 'A'
		}
		if x != y {
			return false
		}
	}
	return true
}
