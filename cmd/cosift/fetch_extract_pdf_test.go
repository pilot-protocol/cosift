package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "parse-pdf" {
		crawler.ParsePDFChild(os.Stdin, os.Stdout)
		return
	}
	os.Exit(m.Run())
}

// nestedPDF is a valid PDF whose page tree is n nested arrays, which overflows the parser's stack.
func nestedPDF(n int) []byte {
	objs := []string{
		"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
		"2 0 obj\n<< /Type /Pages /Count 1 /Kids " + strings.Repeat("[", n) + " >>\nendobj\n",
	}
	out := "%PDF-1.4\n"
	var offsets []int
	for _, o := range objs {
		offsets = append(offsets, len(out))
		out += o
	}
	xref := len(out)
	out += fmt.Sprintf("xref\n0 %d\n0000000000 65535 f \n", len(objs)+1)
	for _, off := range offsets {
		out += fmt.Sprintf("%010d 00000 n \n", off)
	}
	return []byte(out + fmt.Sprintf("trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(objs)+1, xref))
}

func TestFetchAndExtractSurvivesAPDFThatOverflowsTheParserStack(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/pdf")
		w.Write(nestedPDF(2<<20 - 200))
	}))
	t.Cleanup(srv.Close)
	_, err := fetchAndExtract(context.Background(), srv.Client(), crawler.BrowserHeaders(), srv.URL+"/hostile.pdf", workerBody)
	if err == nil {
		t.Fatal("a hostile pdf was extracted")
	}
}
