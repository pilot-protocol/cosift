package svcauth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"net"
	"net/http"
	"regexp"
)

const (
	MinPepperLen = 32
	digestPrefix = "hmac-sha256:"
)

var keyPattern = regexp.MustCompile(`^csk_([0-9a-f]{16})_[A-Z2-7]{52}$`)

// forwardedHeaders are the headers that, with any value, mark a
// request that traversed a proxy.
var forwardedHeaders = canonical(
	"X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "Forwarded", "X-Real-IP",
	"CF-Connecting-IP", "True-Client-IP", "CF-Ray", "CDN-Loop", "Via",
)

func canonical(names ...string) []string {
	for i, n := range names {
		names[i] = http.CanonicalHeaderKey(n)
	}
	return names
}

func PepperOK(pepper []byte) bool { return len(pepper) >= MinPepperLen }

// Digest is HMAC-SHA256(pepper, key). Callers check PepperOK first.
func Digest(pepper []byte, key string) [32]byte {
	m := hmac.New(sha256.New, pepper)
	m.Write([]byte(key))
	var d [32]byte
	copy(d[:], m.Sum(nil))
	return d
}

func FormatDigest(d [32]byte) string { return digestPrefix + hex.EncodeToString(d[:]) }

// NewKey returns a fresh csk_ key and its key id.
func NewKey() (key, keyID string, err error) {
	id := make([]byte, 8)
	secret := make([]byte, 32)
	if _, err := rand.Read(id); err != nil {
		return "", "", err
	}
	if _, err := rand.Read(secret); err != nil {
		return "", "", err
	}
	keyID = hex.EncodeToString(id)
	return "csk_" + keyID + "_" + base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(secret), keyID, nil
}

// forwarded reports whether r carries any header of the forwarded set.
func forwarded(r *http.Request, clientIPHeader string) bool {
	for _, h := range forwardedHeaders {
		if _, ok := r.Header[h]; ok {
			return true
		}
	}
	if clientIPHeader != "" {
		if _, ok := r.Header[http.CanonicalHeaderKey(clientIPHeader)]; ok {
			return true
		}
	}
	return false
}

func loopbackPeer(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

var dummyDigest [32]byte

// verifyKey checks a csk_ credential against the active keys. An unknown
// key id still costs one HMAC.
func verifyKey(st *state, pepper []byte, clientIPHeader string, r *http.Request, cred string) (*principalState, string) {
	if !PepperOK(pepper) {
		return nil, "keys_disabled"
	}
	if !loopbackPeer(r) {
		return nil, "key_not_loopback"
	}
	if forwarded(r, clientIPHeader) {
		return nil, "key_forwarded"
	}
	m := keyPattern.FindStringSubmatch(cred)
	if m == nil {
		return nil, "malformed_credential"
	}
	ref, known := st.byKeyID[m[1]]
	want := dummyDigest
	if known {
		want = ref.digest
	}
	got := Digest(pepper, cred)
	if !hmac.Equal(got[:], want[:]) || !known {
		if !known {
			return nil, "unknown_principal"
		}
		return nil, "bad_key"
	}
	return ref.p, ""
}
