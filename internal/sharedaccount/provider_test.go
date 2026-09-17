package sharedaccount

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"
)

func TestAndreiTokenContractVectors(t *testing.T) {
	data, err := os.ReadFile("testdata/token-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Peppers map[string]string `json:"peppers_hex"`
		Parse   []struct {
			Token string
			Valid bool
			KeyID string
		} `json:"token_parse"`
		TIDs []struct{ Token, KeyID, TID string } `json:"tid"`
	}
	if err = json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	for _, row := range v.Parse {
		id, err := Parse(row.Token)
		if (err == nil) != row.Valid || row.Valid && id != row.KeyID {
			t.Errorf("token vector validity=%v keyid=%s: %v", row.Valid, id, err)
		}
	}
	for _, row := range v.TIDs {
		key, _ := hex.DecodeString(v.Peppers[row.KeyID])
		if TID(key, row.Token) != row.TID {
			t.Error("HMAC differs from official auth fixture")
		}
	}
}

type memoryIdentityStore struct {
	rec                   tokenRecord
	identity              Identity
	lookupErr, accountErr error
	touches               int
	seen                  string
}

func (s *memoryIdentityStore) Lookup(_ context.Context, tid string) (tokenRecord, error) {
	s.seen = tid
	return s.rec, s.lookupErr
}
func (s *memoryIdentityStore) Account(context.Context, string) (Identity, error) {
	return s.identity, s.accountErr
}
func (s *memoryIdentityStore) Touch(context.Context, string, string, time.Time) error {
	s.touches++
	return nil
}
func TestVerifierRevocationBanAndOutageAreNotCached(t *testing.T) {
	s := &memoryIdentityStore{rec: tokenRecord{UID: "0123456789abcdef", LastUsed: time.Now()}, identity: Identity{"0123456789abcdef", "user@example.com"}}
	reads := 0
	key := make([]byte, 32)
	v := verifier{store: s, pepper: func(context.Context, string) ([]byte, error) { reads++; return key, nil }}
	token := "ck_1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	if _, err := v.verify(context.Background(), token); err != nil {
		t.Fatal(err)
	}
	if s.seen != TID(key, token) || s.touches != 0 {
		t.Fatal("wrong token lookup or touch interval")
	}
	s.rec.Revoked = true
	if _, err := v.verify(context.Background(), token); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("revocation bypass", err)
	}
	s.rec.Revoked = false
	s.accountErr = ErrBanned
	if _, err := v.verify(context.Background(), token); !errors.Is(err, ErrBanned) {
		t.Fatal("ban bypass", err)
	}
	s.accountErr = nil
	s.lookupErr = ErrUnavailable
	if _, err := v.verify(context.Background(), token); !errors.Is(err, ErrUnavailable) {
		t.Fatal("outage converted to login", err)
	}
	s.lookupErr = nil
	s.rec.LastUsed = time.Time{}
	if _, err := v.verify(context.Background(), token); err != nil || s.touches != 1 || reads != 1 {
		t.Fatal("pepper caching / touch", err, reads, s.touches)
	}
	if _, err := v.verify(context.Background(), "malformed"); !errors.Is(err, ErrUnauthorized) || reads != 1 {
		t.Fatal("malformed token reached infrastructure")
	}
}
