// Package svcauth authenticates and limits callers of the /v1 service listener.
package svcauth

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pilot-protocol/cosift/internal/fileowner"
	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

const (
	DefaultPath   = "/etc/cosift/service-auth.json"
	DefaultListen = "127.0.0.1:7779"

	maxConfigBytes = 1 << 20
	maxPrincipals  = 64
	maxKeys        = 2
)

var (
	ErrAbsent = errors.New("absent")

	idPattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,39}$`)
	subPattern    = regexp.MustCompile(`^[0-9]{6,32}$`)
	emailPattern  = regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]@[a-z][a-z0-9-]{4,28}[a-z0-9]\.iam\.gserviceaccount\.com$`)
	keyIDPattern  = regexp.MustCompile(`^[0-9a-f]{16}$`)
	digestPattern = regexp.MustCompile(`^hmac-sha256:[0-9a-f]{64}$`)
	fieldPattern  = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
)

// Config is a validated service-auth.json.
type Config struct {
	Listen       string
	FailedAuth   FailedAuth
	WritesFrozen bool
	GoliveAt     time.Time
	Principals   []PrincipalConfig
}

type FailedAuth struct {
	PerMinute int
	Burst     int
}

type PrincipalConfig struct {
	ID            string
	Kind          v1.Kind
	Sub           string
	Email         string
	Keys          []KeyConfig
	Env           v1.Env
	Scopes        []v1.Scope
	RPM           int
	Burst         int
	WritesPerHour int
	WritesBurst   int
	WritesPerDay  int
	CountsReads   bool
}

type KeyConfig struct {
	KeyID     string
	Digest    [32]byte
	CreatedAt string
}

// Writer reports whether the principal holds a write or stub scope.
func (p PrincipalConfig) Writer() bool {
	return slices.Contains(p.Scopes, v1.ScopeArticlesWrite) || slices.Contains(p.Scopes, v1.ScopeArticlesStub)
}

func (p PrincipalConfig) principal() v1.Principal {
	return v1.Principal{ID: p.ID, Kind: p.Kind, Env: p.Env, Scopes: slices.Clone(p.Scopes), CountsReads: p.CountsReads}
}

// Checks are the engine tokens no key digest may collide with.
type Checks struct {
	Pepper        []byte
	PeerAuthToken string
	AdminToken    string
	// MainAddr is the engine's main listen address, which listen must not take.
	MainAddr string
}

type rawConfig struct {
	SchemaVersion *int            `json:"schema_version"`
	Listen        *string         `json:"listen"`
	FailedAuth    *rawFailedAuth  `json:"failed_auth"`
	WritesFrozen  *bool           `json:"writes_frozen"`
	GoliveAt      *string         `json:"golive_at"`
	Principals    *[]rawPrincipal `json:"principals"`
}

type rawFailedAuth struct {
	PerMinute *int `json:"per_minute"`
	Burst     *int `json:"burst"`
}

type rawPrincipal struct {
	ID            *string   `json:"id"`
	Kind          *string   `json:"kind"`
	Sub           *string   `json:"sub"`
	Email         *string   `json:"email"`
	Keys          *[]rawKey `json:"keys"`
	Env           *string   `json:"env"`
	Scopes        *[]string `json:"scopes"`
	RPM           *int      `json:"rpm"`
	Burst         *int      `json:"burst"`
	WritesPerHour *int      `json:"writes_per_hour"`
	WritesBurst   *int      `json:"writes_burst"`
	WritesPerDay  *int      `json:"writes_per_day"`
	CountsReads   *bool     `json:"counts_reads"`
}

type rawKey struct {
	KeyID     *string `json:"key_id"`
	Digest    *string `json:"digest"`
	CreatedAt *string `json:"created_at"`
}

// ReadFile reads path after the ownership and mode check: owner uid, and no
// group-write or other permission bits. A missing file is ErrAbsent.
func ReadFile(path string, uid uint32) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrAbsent
		}
		return nil, errors.New("cannot open file")
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, errors.New("cannot stat file")
	}
	if !st.Mode().IsRegular() {
		return nil, errors.New("not a regular file")
	}
	owner, ok := fileowner.UID(st)
	if !ok || owner != uid {
		return nil, fmt.Errorf("owner must be uid %d", uid)
	}
	if st.Mode().Perm()&0o027 != 0 {
		return nil, errors.New("mode must not grant group write or any other access")
	}
	b, err := io.ReadAll(io.LimitReader(f, maxConfigBytes+1))
	if err != nil {
		return nil, errors.New("cannot read file")
	}
	if len(b) > maxConfigBytes {
		return nil, errors.New("file too large")
	}
	return b, nil
}

// Parse decodes and validates a service-auth.json document. The error names
// the first rule that fails and never echoes a value from the file.
func Parse(b []byte, c Checks) (*Config, error) {
	if err := checkFields(b); err != nil {
		return nil, err
	}
	var raw rawConfig
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return nil, decodeError(err)
	}
	return raw.validate(c)
}

func decodeError(err error) error {
	var ute *json.UnmarshalTypeError
	if errors.As(err, &ute) {
		return fmt.Errorf("%s: wrong type", ute.Field)
	}
	var se *json.SyntaxError
	if errors.As(err, &se) {
		return fmt.Errorf("not valid JSON (offset %d)", se.Offset)
	}
	if msg, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
		return fmt.Errorf("unknown field %s", msg)
	}
	return errors.New("not valid JSON")
}

// checkFields walks the document and refuses duplicate keys, nulls and any key
// that encoding/json would only match case-insensitively.
func checkFields(b []byte) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := walkValue(dec, "$"); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("not valid JSON (trailing data)")
	}
	return nil
}

func walkValue(dec *json.Decoder, path string) error {
	tok, err := dec.Token()
	if err != nil {
		return errors.New("not valid JSON")
	}
	switch t := tok.(type) {
	case nil:
		return fmt.Errorf("%s: null is not allowed", path)
	case json.Delim:
		switch t {
		case '{':
			seen := map[string]bool{}
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return errors.New("not valid JSON")
				}
				k, _ := kt.(string)
				if !fieldPattern.MatchString(k) {
					return fmt.Errorf("%s: unknown field %q", path, k)
				}
				if seen[k] {
					return fmt.Errorf("%s.%s: duplicate field", path, k)
				}
				seen[k] = true
				if err := walkValue(dec, path+"."+k); err != nil {
					return err
				}
			}
		case '[':
			for i := 0; dec.More(); i++ {
				if err := walkValue(dec, path+"["+strconv.Itoa(i)+"]"); err != nil {
					return err
				}
			}
		}
		if _, err := dec.Token(); err != nil {
			return errors.New("not valid JSON")
		}
	}
	return nil
}

func (raw *rawConfig) validate(c Checks) (*Config, error) {
	if raw.SchemaVersion == nil {
		return nil, errors.New("schema_version: required")
	}
	if *raw.SchemaVersion != 1 {
		return nil, errors.New("schema_version: must be 1")
	}
	cfg := &Config{Listen: DefaultListen, FailedAuth: FailedAuth{PerMinute: 30, Burst: 10}}
	if raw.Listen != nil {
		if err := checkListen(*raw.Listen); err != nil {
			return nil, fmt.Errorf("listen: %w", err)
		}
		cfg.Listen = *raw.Listen
	}
	if c.MainAddr != "" && samePort(cfg.Listen, c.MainAddr) {
		return nil, errors.New("listen: must differ from the engine's main address")
	}
	if fa := raw.FailedAuth; fa != nil {
		if fa.PerMinute != nil {
			if *fa.PerMinute < 1 || *fa.PerMinute > 600 {
				return nil, errors.New("failed_auth.per_minute: must be 1-600")
			}
			cfg.FailedAuth.PerMinute = *fa.PerMinute
		}
		if fa.Burst != nil {
			if *fa.Burst < 1 || *fa.Burst > 100 {
				return nil, errors.New("failed_auth.burst: must be 1-100")
			}
			cfg.FailedAuth.Burst = *fa.Burst
		}
	}
	if raw.WritesFrozen != nil {
		cfg.WritesFrozen = *raw.WritesFrozen
	}
	if raw.GoliveAt != nil {
		t, err := time.Parse(time.RFC3339, *raw.GoliveAt)
		if err != nil {
			return nil, errors.New("golive_at: must be RFC 3339")
		}
		cfg.GoliveAt = t
	}
	if raw.Principals == nil {
		return nil, errors.New("principals: required")
	}
	ps := *raw.Principals
	if len(ps) < 1 || len(ps) > maxPrincipals {
		return nil, fmt.Errorf("principals: must hold 1-%d entries", maxPrincipals)
	}
	ids, subs, emails, keyIDs := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
	for i, rp := range ps {
		p, err := rp.validate()
		if err != nil {
			return nil, fmt.Errorf("principals[%d].%w", i, err)
		}
		if ids[p.ID] {
			return nil, fmt.Errorf("principals[%d].id: duplicate", i)
		}
		ids[p.ID] = true
		if p.Kind == v1.KindOIDC {
			if subs[p.Sub] {
				return nil, fmt.Errorf("principals[%d].sub: duplicate", i)
			}
			subs[p.Sub] = true
			if emails[p.Email] {
				return nil, fmt.Errorf("principals[%d].email: duplicate", i)
			}
			emails[p.Email] = true
		}
		for j, k := range p.Keys {
			if keyIDs[k.KeyID] {
				return nil, fmt.Errorf("principals[%d].keys[%d].key_id: duplicate", i, j)
			}
			keyIDs[k.KeyID] = true
			if collides(k.Digest, c) {
				return nil, fmt.Errorf("principals[%d].keys[%d].digest: equals the digest of an existing engine token", i, j)
			}
		}
		cfg.Principals = append(cfg.Principals, p)
	}
	return cfg, nil
}

func collides(d [32]byte, c Checks) bool {
	if !PepperOK(c.Pepper) {
		return false
	}
	for _, t := range []string{c.PeerAuthToken, c.AdminToken} {
		if t != "" && Digest(c.Pepper, t) == d {
			return true
		}
	}
	return false
}

func checkListen(s string) error {
	host, port, err := net.SplitHostPort(s)
	if err != nil {
		return errors.New("must be host:port")
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("host must be a loopback IP literal")
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 || strconv.Itoa(n) != port {
		return errors.New("port must be 1-65535")
	}
	return nil
}

// samePort reports whether binding listen could collide with main: the same
// port on the same, a wildcard or an unresolved host.
func samePort(listen, main string) bool {
	lh, lp, err := net.SplitHostPort(listen)
	if err != nil {
		return false
	}
	mh, mp, err := net.SplitHostPort(main)
	if err != nil || lp != mp {
		return false
	}
	mip := net.ParseIP(mh)
	return mip == nil || mip.IsUnspecified() || mip.Equal(net.ParseIP(lh))
}

var (
	oidcScopes = []v1.Scope{v1.ScopeArticlesRead, v1.ScopeArticlesWrite, v1.ScopeArticlesStub, v1.ScopeRetrieveRead}
	keyScopes  = []v1.Scope{v1.ScopeArticlesRead, v1.ScopeArticlesReadAll, v1.ScopeArticlesModerate, v1.ScopeArticlesAdmin}
)

func (rp rawPrincipal) validate() (PrincipalConfig, error) {
	var p PrincipalConfig
	if rp.ID == nil {
		return p, errors.New("id: required")
	}
	if !idPattern.MatchString(*rp.ID) {
		return p, errors.New("id: must match ^[a-z0-9][a-z0-9-]{0,39}$")
	}
	p.ID = *rp.ID
	if rp.Kind == nil {
		return p, errors.New("kind: required")
	}
	p.Kind = v1.Kind(*rp.Kind)
	if !p.Kind.Valid() {
		return p, errors.New("kind: must be oidc or key")
	}
	switch p.Kind {
	case v1.KindOIDC:
		if rp.Keys != nil {
			return p, errors.New("keys: not allowed on an oidc principal")
		}
		if rp.Sub == nil {
			return p, errors.New("sub: required")
		}
		if !subPattern.MatchString(*rp.Sub) {
			return p, errors.New("sub: must match ^[0-9]{6,32}$")
		}
		p.Sub = *rp.Sub
		if rp.Email == nil {
			return p, errors.New("email: required")
		}
		if !emailPattern.MatchString(*rp.Email) {
			return p, errors.New("email: must be a lower-case service-account email")
		}
		p.Email = *rp.Email
	case v1.KindKey:
		if rp.Sub != nil {
			return p, errors.New("sub: not allowed on a key principal")
		}
		if rp.Email != nil {
			return p, errors.New("email: not allowed on a key principal")
		}
		if rp.Keys == nil {
			return p, errors.New("keys: required")
		}
		if n := len(*rp.Keys); n < 1 || n > maxKeys {
			return p, fmt.Errorf("keys: must hold 1-%d entries", maxKeys)
		}
		for j, rk := range *rp.Keys {
			k, err := rk.validate()
			if err != nil {
				return p, fmt.Errorf("keys[%d].%w", j, err)
			}
			p.Keys = append(p.Keys, k)
		}
	}
	if rp.Env == nil {
		return p, errors.New("env: required")
	}
	p.Env = v1.Env(*rp.Env)
	if !p.Env.Valid() {
		return p, errors.New("env: must be prod or staging")
	}
	if rp.Scopes == nil || len(*rp.Scopes) == 0 {
		return p, errors.New("scopes: must be non-empty")
	}
	for _, s := range *rp.Scopes {
		sc := v1.Scope(s)
		if !sc.Valid() {
			return p, errors.New("scopes: unknown scope")
		}
		if slices.Contains(p.Scopes, sc) {
			return p, errors.New("scopes: duplicate scope")
		}
		p.Scopes = append(p.Scopes, sc)
	}
	if err := checkScopes(p.Kind, p.Env, p.Scopes); err != nil {
		return p, err
	}
	if rp.RPM == nil {
		return p, errors.New("rpm: required")
	}
	if *rp.RPM < 1 || *rp.RPM > 6000 {
		return p, errors.New("rpm: must be 1-6000")
	}
	p.RPM = *rp.RPM
	p.Burst = (p.RPM + 3) / 4
	if rp.Burst != nil {
		if *rp.Burst < 1 || *rp.Burst > p.RPM {
			return p, errors.New("burst: must be 1-rpm")
		}
		p.Burst = *rp.Burst
	}
	if p.Writer() {
		p.WritesPerHour, p.WritesBurst, p.WritesPerDay = 30, 10, 150
		if rp.WritesPerHour != nil {
			if *rp.WritesPerHour < 1 || *rp.WritesPerHour > 600 {
				return p, errors.New("writes_per_hour: must be 1-600")
			}
			p.WritesPerHour = *rp.WritesPerHour
		}
		if rp.WritesBurst != nil {
			if *rp.WritesBurst < 1 || *rp.WritesBurst > p.WritesPerHour {
				return p, errors.New("writes_burst: must be 1-writes_per_hour")
			}
			p.WritesBurst = *rp.WritesBurst
		} else {
			p.WritesBurst = min(p.WritesBurst, p.WritesPerHour)
		}
		if rp.WritesPerDay != nil {
			if *rp.WritesPerDay < 1 || *rp.WritesPerDay > 5000 {
				return p, errors.New("writes_per_day: must be 1-5000")
			}
			p.WritesPerDay = *rp.WritesPerDay
		}
	} else if rp.WritesPerHour != nil || rp.WritesBurst != nil || rp.WritesPerDay != nil {
		return p, errors.New("writes_*: only allowed with articles:write or articles:stub")
	}
	if rp.CountsReads != nil && *rp.CountsReads {
		if !slices.Contains(p.Scopes, v1.ScopeArticlesRead) || p.Env != v1.EnvProd {
			return p, errors.New("counts_reads: only allowed with articles:read and env prod")
		}
		p.CountsReads = true
	}
	return p, nil
}

// checkScopes applies the kind and articles:admin rules.
func checkScopes(kind v1.Kind, env v1.Env, scopes []v1.Scope) error {
	allowed := oidcScopes
	if kind == v1.KindKey {
		allowed = keyScopes
	}
	for _, s := range scopes {
		if !slices.Contains(allowed, s) {
			return fmt.Errorf("scopes: %s not allowed on a %s principal", s, kind)
		}
	}
	if slices.Contains(scopes, v1.ScopeArticlesAdmin) && (len(scopes) != 1 || kind != v1.KindKey || env != v1.EnvProd) {
		return errors.New("scopes: articles:admin must be the only scope of a prod key principal")
	}
	return nil
}

func (rk rawKey) validate() (KeyConfig, error) {
	var k KeyConfig
	if rk.KeyID == nil {
		return k, errors.New("key_id: required")
	}
	if !keyIDPattern.MatchString(*rk.KeyID) {
		return k, errors.New("key_id: must match ^[0-9a-f]{16}$")
	}
	k.KeyID = *rk.KeyID
	if rk.Digest == nil {
		return k, errors.New("digest: required")
	}
	if !digestPattern.MatchString(*rk.Digest) {
		return k, errors.New("digest: must match ^hmac-sha256:[0-9a-f]{64}$")
	}
	if _, err := hex.Decode(k.Digest[:], []byte((*rk.Digest)[len(digestPrefix):])); err != nil {
		return k, errors.New("digest: not hex")
	}
	if rk.CreatedAt != nil {
		if _, err := time.Parse(time.DateOnly, *rk.CreatedAt); err != nil {
			return k, errors.New("created_at: must be YYYY-MM-DD")
		}
		k.CreatedAt = *rk.CreatedAt
	}
	return k, nil
}
