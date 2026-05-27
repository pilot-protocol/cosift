package crawler

import (
	"bufio"
	"io"
	"strings"
	"testing"
)

// Synthetic WET blob — two conversion records sandwiched around a warcinfo
// header that we must skip. Mirrors the actual byte layout CommonCrawl
// emits: CRLF line endings, double-CRLF between body and next header.
const sampleWET = "WARC/1.0\r\n" +
	"WARC-Type: warcinfo\r\n" +
	"Content-Length: 5\r\n" +
	"\r\n" +
	"hello" +
	"\r\n\r\n" +
	"WARC/1.0\r\n" +
	"WARC-Type: conversion\r\n" +
	"WARC-Target-URI: https://example.com/a\r\n" +
	"Content-Length: 11\r\n" +
	"\r\n" +
	"Hello world" +
	"\r\n\r\n" +
	"WARC/1.0\r\n" +
	"WARC-Type: conversion\r\n" +
	"WARC-Target-URI: https://example.com/b\r\n" +
	"Content-Length: 6\r\n" +
	"\r\n" +
	"foobar" +
	"\r\n\r\n"

func TestReadWetRecord(t *testing.T) {
	br := bufio.NewReader(strings.NewReader(sampleWET))
	var got []*WetRecord
	for {
		rec, err := readWetRecord(br)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("readWetRecord: %v", err)
		}
		if rec != nil {
			got = append(got, rec)
		}
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 conversion records, got %d", len(got))
	}
	if got[0].URL != "https://example.com/a" || string(got[0].Body) != "Hello world" {
		t.Errorf("record 0: %+v", got[0])
	}
	if got[1].URL != "https://example.com/b" || string(got[1].Body) != "foobar" {
		t.Errorf("record 1: %+v", got[1])
	}
}
