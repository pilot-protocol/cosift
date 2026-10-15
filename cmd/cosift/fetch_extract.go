package main

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

type extracted struct {
	FinalURL    string
	Title       string
	Text        string
	Lang        string
	ContentHash string
	Simhash     string
}

// fetchStatusError is a fetch that answered something other than 200.
type fetchStatusError struct{ Status int }

func (e *fetchStatusError) Error() string { return fmt.Sprintf("http %d", e.Status) }

// fetchAndExtract fetches rawURL with header and returns the text, title and language the server's FetchOne extracts from the same bytes, with the hashes a work submission carries.
func fetchAndExtract(ctx context.Context, client *http.Client, header http.Header, rawURL string, maxBody int64) (*extracted, error) {
	if maxBody <= 0 {
		maxBody = 5 << 20
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Header = header.Clone()
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, &fetchStatusError{Status: res.StatusCode}
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, maxBody))
	if err != nil {
		return nil, err
	}
	finalURL := res.Request.URL.String()
	parsePDF := func(b []byte, u string) (*crawler.ParsedDoc, error) {
		return crawler.ParsePDFSandboxed(ctx, b, u, "", 0)
	}
	parsed, err := crawler.ExtractBodyWith(res.Header.Get("Content-Type"), body, finalURL, parsePDF)
	if err != nil {
		return nil, err
	}
	sum, ok := crawler.Simhash(parsed.Text)
	if !ok {
		return nil, fmt.Errorf("page has no words to hash")
	}
	return &extracted{FinalURL: finalURL, Title: parsed.Title, Text: parsed.Text, Lang: parsed.Lang, ContentHash: crawler.ContentHash(parsed.Text), Simhash: crawler.SimhashHex(sum)}, nil
}
