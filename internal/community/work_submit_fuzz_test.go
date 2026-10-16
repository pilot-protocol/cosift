package community

import (
	"encoding/json"
	"strings"
	"testing"
)

const (
	fuzzWorker = "0123456789abcdef0123456789abcdef"
	fuzzNet    = "fedcba9876543210fedcba9876543210"
)

func FuzzBuildSubmit(f *testing.F) {
	for _, seed := range []string{
		`{"lease_id":"a","http_status":403}`,
		`{"lease_id":"a","http_status":200,"text":"hello","title":"t","lang":"en","content_hash":"h","simhash":"s","extract_version":1,"final_url":"https://example.org/"}`,
		`{"worker":"` + strings.Repeat("e", 32) + `","net":"x","lease_id":"a"}`,
		`{"lease_id":"a","lease_id":"b","worker":"w"}`,
		`{"text":"\u0000\ud800 <b>&","artifact":{"model":"m","chunks":[]}}`,
		`{"artifact":null}`, `[]`, `null`, `{}`, `{"lease_id":`, `{"http_status":1e999}`, ``,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		out, refusal := buildSubmit(body, fuzzWorker, fuzzNet)
		if refusal != nil {
			if out != nil || refusal.status < 400 || refusal.code == "" {
				t.Fatalf("a refusal with a body or without a status: %+v %q", refusal, out)
			}
			return
		}
		var sent map[string]json.RawMessage
		if json.Unmarshal(out, &sent) != nil {
			t.Fatalf("the engine body is not an object: %q", out)
		}
		for name := range sent {
			if _, ok := submitMembers[name]; !ok && name != "worker" && name != "net" {
				t.Fatalf("member %q reached the engine", name)
			}
		}
		if string(sent["worker"]) != `"`+fuzzWorker+`"` || string(sent["net"]) != `"`+fuzzNet+`"` {
			t.Fatalf("worker or net came from the client: %s %s", sent["worker"], sent["net"])
		}
	})
}
