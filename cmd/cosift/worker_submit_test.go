package main

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestClampTitleCutsOnACharacterBoundary(t *testing.T) {
	long := "a" + strings.Repeat("é", 600)
	got := clampTitle(long)
	if len(got) != 999 || !utf8.ValidString(got) || !strings.HasPrefix(long, got) {
		t.Fatalf("len %d", len(got))
	}
	if clampTitle("short") != "short" {
		t.Fatal("a short title changed")
	}
}

func TestWorkerSubmissionBoundsAreCheckedBeforeSending(t *testing.T) {
	good := &extracted{FinalURL: "http://a.example.com/x", Title: "T", Text: strings.Repeat("word ", 40), Lang: "en", ContentHash: "h", Simhash: "s"}
	if body, why := submissionFor("l", good); why != "" || body["lang"] != "en" {
		t.Fatalf("%v %q", body, why)
	}
	for name, mutate := range map[string]func(*extracted){
		"short": func(e *extracted) { e.Text = "tiny" },
		"large": func(e *extracted) { e.Text = strings.Repeat("x", 512<<10+1) },
		"nul":   func(e *extracted) { e.Text = strings.Repeat("word ", 40) + "\x00" },
		"utf8":  func(e *extracted) { e.Text = strings.Repeat("word ", 40) + "\xff" },
		"url":   func(e *extracted) { e.FinalURL = "http://a.example.com/" + strings.Repeat("x", 2048) },
	} {
		e := *good
		mutate(&e)
		if _, why := submissionFor("l", &e); why == "" {
			t.Errorf("%s was accepted", name)
		}
	}
	e := *good
	for _, lang := range []string{"not a tag!", "en-US-u-co-phonebk-ca-gregory-nu-latn"} {
		e.Lang = lang
		if body, why := submissionFor("l", &e); why != "" || body["lang"] != "" {
			t.Errorf("language tag %q: sent %v, refused %q", lang, body["lang"], why)
		}
	}
	e.Lang = "en-US-u-co-phonebk-ca-gregory-nuxx"
	if body, _ := submissionFor("l", &e); body["lang"] != e.Lang {
		t.Errorf("a 35-byte tag was blanked: %v", body["lang"])
	}
}
