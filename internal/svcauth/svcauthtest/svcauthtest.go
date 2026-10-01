// Package svcauthtest mints Google-shaped ID tokens with local keys and fakes
// the certificate endpoint, for tests of the /v1 listener.
package svcauthtest

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	Audience = "https://cosift.pilotprotocol.network/v1"
	Issuer   = "https://accounts.google.com"
)

// Signer is an RS256 key with a key id.
type Signer struct {
	Kid string
	Key *rsa.PrivateKey
}

func NewSigner(kid string) *Signer {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return &Signer{Kid: kid, Key: k}
}

// JWKS is the certificate endpoint body for signers.
func JWKS(signers ...*Signer) []byte {
	keys := make([]map[string]string, 0, len(signers))
	for _, s := range signers {
		keys = append(keys, map[string]string{
			"kty": "RSA", "alg": "RS256", "use": "sig", "kid": s.Kid,
			"n": b64(s.Key.N.Bytes()),
			"e": b64(big.NewInt(int64(s.Key.E)).Bytes()),
		})
	}
	b, _ := json.Marshal(map[string]any{"keys": keys})
	return b
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// Claims returns a valid payload for the service account (sub, email).
func Claims(sub, email string, now time.Time) map[string]any {
	return map[string]any{
		"iss": Issuer, "aud": Audience, "sub": sub, "email": email, "email_verified": true,
		"iat": now.Unix(), "exp": now.Add(time.Hour).Unix(),
	}
}

// Token signs claims with header {"alg":"RS256","kid":s.Kid,"typ":"JWT"}.
func (s *Signer) Token(claims map[string]any) string {
	return s.TokenWithHeader(map[string]any{"alg": "RS256", "kid": s.Kid, "typ": "JWT"}, claims)
}

func (s *Signer) TokenWithHeader(header, claims map[string]any) string {
	h, _ := json.Marshal(header)
	c, _ := json.Marshal(claims)
	signed := b64(h) + "." + b64(c)
	sum := sha256.Sum256([]byte(signed))
	sig, err := rsa.SignPKCS1v15(rand.Reader, s.Key, crypto.SHA256, sum[:])
	if err != nil {
		panic(err)
	}
	return signed + "." + b64(sig)
}

// Unsigned builds a token from raw header and payload JSON and a raw signature.
func Unsigned(header, payload string, sig []byte) string {
	return b64([]byte(header)) + "." + b64([]byte(payload)) + "." + b64(sig)
}

// Response is one canned answer of the fake endpoint.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
	Err    error
}

// Source is a fake certificate endpoint. Block, when set, holds every fetch
// until it is closed or the fetch context ends.
type Source struct {
	mu      sync.Mutex
	resp    Response
	fetches int
	ctxErrs []error
	Block   chan struct{}
	Started chan struct{}
}

func NewSource(body []byte, maxAge int) *Source {
	s := &Source{Started: make(chan struct{}, 64)}
	s.Set(Response{Status: http.StatusOK, Header: http.Header{"Cache-Control": {"public, max-age=" + strconv.Itoa(maxAge)}}, Body: body})
	return s
}

func (s *Source) Set(r Response) {
	s.mu.Lock()
	s.resp = r
	s.mu.Unlock()
}

func (s *Source) SetBlock(ch chan struct{}) {
	s.mu.Lock()
	s.Block = ch
	s.mu.Unlock()
}

func (s *Source) Fetches() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fetches
}

// CtxErrs are the fetch contexts' errors observed when each fetch finished.
func (s *Source) CtxErrs() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.ctxErrs...)
}

func (s *Source) Fetch(ctx context.Context) (*http.Response, error) {
	s.mu.Lock()
	s.fetches++
	block := s.Block
	r := s.resp
	s.mu.Unlock()
	select {
	case s.Started <- struct{}{}:
	default:
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
		}
	}
	s.mu.Lock()
	s.ctxErrs = append(s.ctxErrs, ctx.Err())
	s.mu.Unlock()
	if r.Err != nil {
		return nil, r.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	h := r.Header
	if h == nil {
		h = http.Header{}
	}
	return &http.Response{StatusCode: r.Status, Header: h.Clone(), Body: io.NopCloser(bytes.NewReader(r.Body))}, nil
}
