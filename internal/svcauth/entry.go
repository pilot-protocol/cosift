package svcauth

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"strings"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

// KeyPrincipal describes a key principal for cosift svc-key new. Burst 0 is
// the default, ceil(rpm/4).
type KeyPrincipal struct {
	ID          string
	Env         string
	Scopes      []string
	RPM         int
	Burst       int
	CountsReads bool
}

type keyElement struct {
	KeyID     string `json:"key_id"`
	Digest    string `json:"digest"`
	CreatedAt string `json:"created_at,omitempty"`
}

type keyPrincipalEntry struct {
	ID          string       `json:"id"`
	Kind        string       `json:"kind"`
	Keys        []keyElement `json:"keys"`
	Env         string       `json:"env"`
	Scopes      []string     `json:"scopes"`
	RPM         int          `json:"rpm"`
	Burst       int          `json:"burst,omitempty"`
	CountsReads bool         `json:"counts_reads,omitempty"`
}

// Entries validates kp under the service-auth.json rules and returns its
// config entry and its bare keys[] element, both compact JSON.
func (kp KeyPrincipal) Entries(keyID string, digest [32]byte, createdAt string) (principal, element []byte, err error) {
	el := keyElement{KeyID: keyID, Digest: FormatDigest(digest), CreatedAt: createdAt}
	e := keyPrincipalEntry{ID: kp.ID, Kind: string(v1.KindKey), Keys: []keyElement{el}, Env: kp.Env, Scopes: kp.Scopes, RPM: kp.RPM, Burst: kp.Burst, CountsReads: kp.CountsReads}
	if kp.Burst < 0 {
		return nil, nil, errors.New("burst: must be 1-rpm")
	}
	if principal, err = json.Marshal(e); err != nil {
		return nil, nil, err
	}
	doc, err := json.Marshal(map[string]any{"schema_version": 1, "principals": []json.RawMessage{principal}})
	if err != nil {
		return nil, nil, err
	}
	if _, err := Parse(doc, Checks{}); err != nil {
		msg, _ := strings.CutPrefix(err.Error(), "principals[0].")
		return nil, nil, errors.New(msg)
	}
	if element, err = json.Marshal(el); err != nil {
		return nil, nil, err
	}
	return principal, element, nil
}

// ReadEnvFile returns name's value from a systemd EnvironmentFile: KEY=VALUE
// lines, # and ; comments, optional matching quotes around the value.
func ReadEnvFile(path, name string) (string, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	val, found := "", false
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || line[0] == '#' || line[0] == ';' {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok || strings.TrimSpace(k) != name {
			continue
		}
		v = strings.TrimSpace(v)
		if n := len(v); n >= 2 && (v[0] == '"' || v[0] == '\'') && v[n-1] == v[0] {
			quote := v[0]
			v = v[1 : n-1]
			if quote == '"' {
				v = strings.NewReplacer(`\\`, `\`, `\"`, `"`).Replace(v)
			}
		}
		val, found = v, true
	}
	return val, found
}
