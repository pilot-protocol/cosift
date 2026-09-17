package main

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/pilot-protocol/cosift/internal/store"
)

func TestSynthesisIncludesEvidenceBeyondDisplayExcerpt(t *testing.T) {
	for _, path := range []string{"/answer", "/research", "/research?stream=true"} {
		t.Run(path, func(t *testing.T) {
			f := populatedPebbleStore(t)
			body := strings.Repeat("This introduction describes the scope of the documentation. ", 60) + "To initialize a Go module, run go mod init example.org/project."
			id, err := f.ps.UpsertDocument(context.Background(), &store.Document{URL: "https://docs.example/go-module", Title: "Go module initialization", Text: body, FetchedAt: time.Now()})
			if err != nil {
				t.Fatal(err)
			}
			if err = f.idx.IndexDocument(context.Background(), id, "Go module initialization", body); err != nil {
				t.Fatal(err)
			}
			chat := &capturingChat{fill: "A grounded answer [1].", queue: []string{`["Go module initialization"]`}}
			srv := f.makeServer(nil)
			srv.chat = chat
			sep := "?"
			if strings.Contains(path, "?") {
				sep = "&"
			}
			req := httptest.NewRequest("GET", path+sep+"q=Go+module+initialization&retriever=bm25&rerank=false&judge=false&k=1", nil)
			w := httptest.NewRecorder()
			if strings.HasPrefix(path, "/answer") {
				srv.handleAnswer(w, req)
			} else {
				srv.handleResearch(w, req)
			}
			if w.Code != 200 {
				t.Fatalf("status%d: %s", w.Code, w.Body.String())
			}
			found := false
			for i := range chat.count() {
				_, prompt := chat.call(t, i)
				if strings.Contains(prompt, "go mod init example.org/project") {
					found = true
				}
			}
			if !found {
				t.Fatal("synthesis did not receive the answer-bearing text beyond the 1200-byte display excerpt")
			}
		})
	}
}

func TestSynthesisContextBoundAndUTF8(t *testing.T) {
	text := strings.Repeat("科学研究与技术文档。", 3000)
	for _, sources := range []int{1, 3, 7, 20, 100} {
		context := synthesisContext(text, sources)
		if len(context) > 10000 || len(context)*sources > 30000 || !utf8.ValidString(context) {
			t.Fatalf("invalid context budget for %d sources: %d bytes", sources, len(context))
		}
	}
	if synthesisContext("A concise factual reference.", 3) != "A concise factual reference." {
		t.Fatal("changed short source")
	}
}
