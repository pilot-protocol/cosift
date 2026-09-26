package v1

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

const testLimit = 256

type testBody struct {
	Q             string   `json:"q"`
	K             int      `json:"k"`
	Retriever     string   `json:"retriever"`
	TopicID       string   `json:"topic_id"`
	Reader        string   `json:"reader"`
	URLs          []string `json:"urls"`
	Apply         bool     `json:"apply"`
	ExpectRecords *int     `json:"expect_records"`
	Build         *struct {
		SourcesUsed int `json:"sources_used"`
	} `json:"build"`
}

func decodeRequest(body, contentType string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	return r
}

func TestDecodeAccepts(t *testing.T) {
	n := func(v int) *int { return &v }
	cases := []struct {
		name        string
		body        string
		contentType string
		want        testBody
	}{
		{"match", `{"q": "rust async runtimes", "topic_id": "8624225ee5e2b9f9", "reader": "9c1d0f6a2b3e4c5d6e7f8091a2b3c4d5"}`, "application/json",
			testBody{Q: "rust async runtimes", TopicID: "8624225ee5e2b9f9", Reader: "9c1d0f6a2b3e4c5d6e7f8091a2b3c4d5"}},
		{"search", `{"q": "Rust async runtimes", "k": 30, "retriever": "bm25"}`, "application/json",
			testBody{Q: "Rust async runtimes", K: 30, Retriever: "bm25"}},
		{"contents", `{"urls": ["https://tokio.rs/tokio/tutorial", "https://example.org/missing"]}`, "application/json",
			testBody{URLs: []string{"https://tokio.rs/tokio/tutorial", "https://example.org/missing"}}},
		{"purge dry run", `{"apply": false}`, "application/json", testBody{}},
		{"purge apply", `{"apply": true, "expect_records": 23}`, "application/json", testBody{Apply: true, ExpectRecords: n(23)}},
		{"charset parameter", `{"q": "go"}`, "application/json; charset=utf-8", testBody{Q: "go"}},
		{"media type case", `{"q": "go"}`, "Application/JSON", testBody{Q: "go"}},
		{"surrounding whitespace", " \r\n\t{\"q\": \"go\"}\n\n", "application/json", testBody{Q: "go"}},
		{"exactly the limit", `{"q":"` + strings.Repeat("a", 248) + `"}`, "application/json", testBody{Q: strings.Repeat("a", 248)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			var got testBody
			if !Decode(rec, decodeRequest(c.body, c.contentType), &got, testLimit) {
				t.Fatalf("refused: %s", rec.Body.String())
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("decoded %+v, want %+v", got, c.want)
			}
			if rec.Body.Len() != 0 || len(rec.Header()) != 0 {
				t.Errorf("wrote a response on success: %v %q", rec.Header(), rec.Body.String())
			}
		})
	}
}

func TestDecodeRefuses(t *testing.T) {
	const (
		unsupported = `{"error": "Unsupported Media Type", "status": 415, "code": "unsupported_media_type", "detail": "content type must be application/json"}`
		tooLarge    = `{"error": "Request Entity Too Large", "status": 413, "code": "body_too_large", "detail": "request body too large"}`
		invalid     = `{"error": "Bad Request", "status": 400, "code": "invalid_body", "detail": "invalid JSON body"}`
	)
	unknown := func(field string) string {
		return `{"error": "Bad Request", "status": 400, "code": "unknown_field", "detail": "unknown field", "field": "` + field + `"}`
	}
	cases := []struct {
		name        string
		body        string
		contentType string
		doc         string
	}{
		{"no content type", `{"q": "go"}`, "", unsupported},
		{"text/plain", `{"q": "go"}`, "text/plain", unsupported},
		{"form", `q=go`, "application/x-www-form-urlencoded", unsupported},
		{"json lookalike", `{"q": "go"}`, "application/jsonp", unsupported},
		{"malformed parameter", `{"q": "go"}`, "application/json; charset", unsupported},
		{"wrong type and oversized", strings.Repeat("x", 1000), "text/plain", unsupported},

		{"one byte over", `{"q":"` + strings.Repeat("a", 249) + `"}`, "application/json", tooLarge},
		{"oversized garbage", strings.Repeat("x", 1000), "application/json", tooLarge},

		{"search decay", `{"q": "Rust async runtimes", "k": 30, "retriever": "bm25", "decay": 180}`, "application/json", unknown("decay")},
		{"search rerank", `{"q": "go", "rerank": true}`, "application/json", unknown("rerank")},
		{"engine-owned field", `{"prelive": true, "apply": false}`, "application/json", unknown("prelive")},
		{"nested", `{"build": {"sources_used": 9, "cost": 1}}`, "application/json", unknown("cost")},

		{"empty", ``, "application/json", invalid},
		{"whitespace", "  \n", "application/json", invalid},
		{"not json", `q=go`, "application/json", invalid},
		{"null", `null`, "application/json", invalid},
		{"array", `[{"q": "go"}]`, "application/json", invalid},
		{"string", `"go"`, "application/json", invalid},
		{"number", `42`, "application/json", invalid},
		{"truncated", `{"q": `, "application/json", invalid},
		{"trailing object", `{"q": "a"} {"q": "b"}`, "application/json", invalid},
		{"trailing garbage", `{"q": "a"}x`, "application/json", invalid},
		{"trailing brace", `{"q": "a"}}`, "application/json", invalid},
		{"invalid utf-8", "{\"q\": \"\xff\"}", "application/json", invalid},
		{"string for int", `{"k": "30"}`, "application/json", invalid},
		{"fraction for int", `{"k": 1.5}`, "application/json", invalid},
		{"string for array", `{"urls": "https://example.org"}`, "application/json", invalid},
		{"nested type", `{"build": {"sources_used": "9"}}`, "application/json", invalid},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			var got testBody
			if Decode(rec, decodeRequest(c.body, c.contentType), &got, testLimit) {
				t.Fatalf("accepted %+v", got)
			}
			if got, want := rec.Body.String(), wireJSON(t, c.doc); got != want {
				t.Errorf("body\n got %s\nwant %s", got, want)
			}
			checkHeaders(t, rec, nil)
		})
	}
}
