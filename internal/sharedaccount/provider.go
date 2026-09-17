// Package sharedaccount connects the community app to cosift-auth and cosift-mcp.
// Accounts and token validity remain authoritative in their shared Firestore.
package sharedaccount

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"errors"
	"net/netip"
	"regexp"
	"strings"
	"sync"
	"time"
)

var (
	ErrUnauthorized = errors.New("invalid or revoked Cosift token")
	ErrBanned       = errors.New("Cosift account suspended")
	ErrUnavailable  = errors.New("shared Cosift service unavailable")
	ErrInvalid      = errors.New("invalid shared Cosift request")
	ErrLimited      = errors.New("shared Cosift service limit reached")
)

type Identity struct{ UID, Email string }
type Challenge struct {
	RequestID string    `json:"request_id"`
	ExpiresAt time.Time `json:"expires_at"`
}
type Issued struct {
	Token string `json:"token"`
	UID   string `json:"account_uid"`
}
type Provider interface {
	Namespace() string
	Verify(context.Context, string) (Identity, error)
	Start(context.Context, string) (Challenge, error)
	Finish(context.Context, string, string) (Issued, error)
	Revoke(context.Context, string) error
	Call(context.Context, string, string, map[string]any) (map[string]any, error)
}

var tokenPattern = regexp.MustCompile(`^ck_([1-9][0-9]{0,5})_([A-Z2-7]{39})$`)
var UIDPattern = regexp.MustCompile(`^[0-9a-f]{16}$`)

func Parse(raw string) (string, error) {
	parts := tokenPattern.FindStringSubmatch(raw)
	if parts == nil {
		return "", ErrUnauthorized
	}
	encoding := base32.StdEncoding.WithPadding(base32.NoPadding)
	decoded, err := encoding.DecodeString(parts[2])
	if err != nil || len(decoded) != 24 || encoding.EncodeToString(decoded) != parts[2] {
		return "", ErrUnauthorized
	}
	return parts[1], nil
}
func TID(key []byte, raw string) string {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(raw))
	return hex.EncodeToString(h.Sum(nil))
}

type tokenRecord struct {
	UID      string
	Revoked  bool
	LastUsed time.Time
}
type identityStore interface {
	Lookup(context.Context, string) (tokenRecord, error)
	Account(context.Context, string) (Identity, error)
	Touch(context.Context, string, string, time.Time) error
}
type verifier struct {
	store  identityStore
	pepper func(context.Context, string) ([]byte, error)
	mu     sync.Mutex
	keys   map[string][]byte
}

func (v *verifier) verify(ctx context.Context, raw string) (Identity, error) {
	id, err := Parse(raw)
	if err != nil {
		return Identity{}, err
	}
	v.mu.Lock()
	key := v.keys[id]
	v.mu.Unlock()
	if key == nil {
		key, err = v.pepper(ctx, id)
		if err != nil {
			return Identity{}, err
		}
		if len(key) != 32 {
			return Identity{}, ErrUnavailable
		}
		v.mu.Lock()
		if v.keys == nil {
			v.keys = map[string][]byte{}
		}
		if len(v.keys) < 128 {
			v.keys[id] = key
		}
		v.mu.Unlock()
	}
	tid := TID(key, raw)
	rec, err := v.store.Lookup(ctx, tid)
	if err != nil {
		return Identity{}, err
	}
	if rec.Revoked || !UIDPattern.MatchString(rec.UID) {
		return Identity{}, ErrUnauthorized
	}
	identity, err := v.store.Account(ctx, rec.UID)
	if err != nil {
		return Identity{}, err
	}
	if identity.UID != rec.UID || identity.Email == "" || len(identity.Email) > 254 || !strings.Contains(identity.Email, "@") {
		return Identity{}, ErrUnavailable
	}
	if time.Since(rec.LastUsed) > 5*time.Minute {
		_ = v.store.Touch(ctx, rec.UID, tid, time.Now())
	}
	return identity, nil
}

// WithClientIP carries the community gateway's already-resolved client address
// to cosift-auth. The auth service must trust this gateway's egress address in
// its CIDR proxy resolver; never increase a global hop count on a public origin.
func WithClientIP(ctx context.Context, ip string) context.Context {
	if parsed, err := netip.ParseAddr(ip); err == nil {
		return context.WithValue(ctx, clientIPKey{}, parsed.String())
	}
	return ctx
}

type clientIPKey struct{}
