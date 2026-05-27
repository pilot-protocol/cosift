package crawler

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"strings"
	"time"

	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

// WET (Web Extracted Text) parser — the third leg of the CommonCrawl trio
// alongside WARC (raw HTTP) and WAT (metadata). WET records contain plain
// text already extracted from HTML, so we can skip the HTML parser entirely.
//
// Format spec (https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/):
//
//	WARC/1.0\r\n
//	WARC-Type: conversion\r\n
//	WARC-Target-URI: https://example.com/page\r\n
//	Content-Length: 1234\r\n
//	... more headers ...
//	\r\n
//	<1234 bytes of plain text>
//	\r\n\r\n
//
// One WET file (~150 MB gzipped) holds ~500K records. Streaming one file
// end-to-end through this parser gives us ~500K UpsertDocument calls. At
// ~10ms per upsert + embed, that's ~5K docs/sec sustained — 150K docs/min,
// 100× the open-web crawl throughput.
//
// We don't add a WARC library dep — the format is small enough that a
// stdlib bufio.Reader is fine. Iter 485.

// WetRecord is one entry in a WET file. Body is the extracted plain text.
type WetRecord struct {
	URL  string
	Body []byte
}

// SeedWET streams a remote WET file (gzipped) and indexes every record.
// Reuses the crawler's embedder + passageWriter so docs flow through the
// same chunk → embed → HNSW pipeline as web-crawled content. Returns the
// number of docs successfully indexed.
//
// `dedupeFresh` controls whether we skip URLs already in famDoc within
// the COSIFT_REFETCH_AFTER_HOURS window. Set true for refresh-style
// imports; false for cold-bulk loads where redundant work is acceptable.
//
// `lexicalOnly` (iter 486): when true, skip chunking + embedding entirely.
// Each record gets UpsertDocument + BM25 IndexDocument only — no HNSW
// nodes are added. Unlocks ~30 K docs/min on this box (vs ~2 K with full
// embed pipeline) because the bottleneck shifts from per-chunk embedding
// to just gob-encoding the Document and writing BM25 postings. Pair with
// /admin/embed-backfill later to fill in dense vectors asynchronously.
// Iter 485.
func (c *Crawler) SeedWET(ctx context.Context, wetURL string, dedupeFresh, lexicalOnly bool) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, wetURL, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", c.cfg.UserAgent)
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return 0, fmt.Errorf("wet http %d", resp.StatusCode)
	}

	// WET files at CommonCrawl arrive as .warc.wet.gz — gzip-wrapped WARC.
	// Stream-decompress so we don't load 150 MB into RAM.
	zr, err := gzip.NewReader(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("wet gunzip: %w", err)
	}
	defer zr.Close()

	br := bufio.NewReaderSize(zr, 64<<10)
	t0 := time.Now()
	_ = t0

	var freshWindow time.Duration
	if v := getEnv("COSIFT_REFETCH_AFTER_HOURS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			freshWindow = time.Duration(n) * time.Hour
		}
	}

	// Iter 488b: N parallel writers, each batching M docs per flush.
	// Multiplies the iter-486b parallelism gain by the iter-488 batched-
	// upsert gain. With 8 writers × 32-doc batches, only 8 goroutines
	// contend for p.mu and each one writes 32 docs per acquire.
	batchSize := 32
	if v := getEnv("COSIFT_WET_BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			batchSize = n
		}
	}
	workers := 8
	if v := getEnv("COSIFT_WET_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			workers = n
		}
	}
	recCh := make(chan *WetRecord, workers*batchSize*2)
	var wg sync.WaitGroup
	var indexed atomic.Int64

	// Iter 488c REVERT: batched UpsertDocument helped raw upsert throughput
	// but IndexDocument's per-call locking became the dominant cost when
	// batching meant fewer concurrent goroutines, so net throughput
	// regressed vs iter-486b's per-record parallelism. Back to N workers
	// calling indexWetRecord — gives us the iter-486b 3.4K docs/min ceiling.
	// UpsertDocumentBatch stays in PebbleStore as a callable API; other
	// future call sites can use it (e.g. CLI bulk-import). Iter 488c.
	_ = batchSize
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rec := range recCh {
				if err := c.indexWetRecord(ctx, rec, lexicalOnly); err != nil {
					continue
				}
				indexed.Add(1)
			}
		}()
	}

	// Reader loop — feed the channel until EOF or ctx cancel.
	readErr := func() error {
		defer close(recCh)
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			rec, err := readWetRecord(br)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if rec == nil || rec.URL == "" || len(rec.Body) == 0 {
				continue
			}
			// Optional fresh-dedup: skip if already indexed within refetch window.
			// Done in reader so we don't waste worker capacity on no-ops.
			if dedupeFresh && freshWindow > 0 {
				if prior, _ := c.store.GetDocByURL(ctx, rec.URL); prior != nil && !prior.FetchedAt.IsZero() && time.Since(prior.FetchedAt) < freshWindow {
					continue
				}
			}
			select {
			case recCh <- rec:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}()
	wg.Wait()
	if readErr != nil && readErr != context.Canceled {
		return int(indexed.Load()), readErr
	}
	return int(indexed.Load()), nil
}

// readWetRecord pulls one WARC record from br. Returns (nil, nil) for
// non-conversion records (warcinfo, request, metadata) so callers skip.
// Returns (nil, io.EOF) cleanly at end-of-stream.
func readWetRecord(br *bufio.Reader) (*WetRecord, error) {
	// Skip blank lines between records.
	for {
		b, err := br.Peek(1)
		if err == io.EOF {
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}
		if b[0] == '\r' || b[0] == '\n' {
			_, _ = br.ReadByte()
			continue
		}
		break
	}

	// First line MUST be the WARC version.
	versionLine, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(versionLine, "WARC/") {
		return nil, fmt.Errorf("expected WARC version line, got %q", strings.TrimSpace(versionLine))
	}

	// Headers until empty line.
	headers := make(map[string]string, 12)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		trimmed := strings.TrimRight(line, "\r\n")
		if trimmed == "" {
			break
		}
		idx := strings.Index(trimmed, ":")
		if idx <= 0 {
			continue
		}
		k := strings.TrimSpace(trimmed[:idx])
		v := strings.TrimSpace(trimmed[idx+1:])
		headers[k] = v
	}

	// Read Content-Length bytes for the body.
	clStr := headers["Content-Length"]
	clen, err := strconv.Atoi(clStr)
	if err != nil || clen < 0 {
		return nil, fmt.Errorf("bad Content-Length %q", clStr)
	}
	body := make([]byte, clen)
	if _, err := io.ReadFull(br, body); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	// Trailing \r\n\r\n separator after body — consumed by next call's
	// leading-blank-line skip.

	if headers["WARC-Type"] != "conversion" {
		return nil, nil // skip metadata records
	}
	return &WetRecord{
		URL:  headers["WARC-Target-URI"],
		Body: body,
	}, nil
}

// indexWetRecordsBatch upserts N WET records in a single pebble batch
// via UpsertDocumentBatch, then writes BM25 postings + (optionally)
// embeddings per doc. UpsertDocument is the single-lock-acquire hot path;
// IndexDocument still takes its own lock per doc but those calls are much
// cheaper (a few keys each) than the gob-encode + Commit of UpsertDocument.
// Iter 488.
//
// Returns count successfully indexed. On batch-level error returns 0 + err.
func (c *Crawler) indexWetRecordsBatch(ctx context.Context, recs []*WetRecord, lexicalOnly bool) (int, error) {
	if len(recs) == 0 {
		return 0, nil
	}
	// Build the doc list, dropping records that fail allowedDomain or
	// the URL-parse check. Mirrors indexWetRecord's filtering.
	type item struct {
		rec   *WetRecord
		doc   *store.Document
		title string
		text  string
		host  string
	}
	items := make([]item, 0, len(recs))
	docs := make([]*store.Document, 0, len(recs))
	for _, rec := range recs {
		u, err := url.Parse(rec.URL)
		if err != nil {
			continue
		}
		if !c.allowedDomain(rec.URL) {
			continue
		}
		text := string(rec.Body)
		title := firstNonEmptyLine(text)
		if len(title) > 200 {
			title = title[:200]
		}
		sha := sha256.Sum256(rec.Body)
		doc := &store.Document{
			URL:        rec.URL,
			Domain:     u.Host,
			Title:      title,
			Text:       text,
			Source:     "wet-import",
			Quality:    0.6,
			FetchedAt:  time.Now(),
			ContentSHA: sha[:],
		}
		items = append(items, item{rec: rec, doc: doc, title: title, text: text, host: u.Host})
		docs = append(docs, doc)
	}
	if len(docs) == 0 {
		return 0, nil
	}
	// Cast to the concrete batched path. The interface doesn't expose this
	// yet, so we type-assert — failure means SQLite backend or older Pebble
	// store, in which case we fall back to the per-doc path.
	type batchUpserter interface {
		UpsertDocumentBatch(ctx context.Context, docs []*store.Document) ([]int64, error)
	}
	var ids []int64
	if bu, ok := c.store.(batchUpserter); ok {
		out, err := bu.UpsertDocumentBatch(ctx, docs)
		if err != nil {
			return 0, err
		}
		ids = out
	} else {
		ids = make([]int64, len(docs))
		for i, d := range docs {
			id, err := c.store.UpsertDocument(ctx, d)
			if err != nil {
				continue
			}
			ids[i] = id
		}
	}
	// BM25 + (optional) embed per doc. BM25 takes its own lock per call
	// but each call is cheap relative to the upsert it follows.
	indexed := 0
	for i, it := range items {
		id := ids[i]
		if id == 0 {
			continue
		}
		if err := c.idx.IndexDocument(ctx, id, it.title, it.text); err != nil {
			continue
		}
		indexed++
		if lexicalOnly || c.embedder == nil || c.passageWriter == nil {
			continue
		}
		// Dense path — chunk, embed, store passages. Same as indexWetRecord's
		// tail but inlined to avoid the type-assertion overhead per record.
		chunker := index.NewChunkerWith(c.chunkSizeFor(it.host), c.chunkOverlapFor(it.host))
		chunks := chunker.Chunk(it.text)
		if len(chunks) == 0 {
			continue
		}
		const tokenCap = 1500
		texts := make([]string, len(chunks))
		for j, ch := range chunks {
			texts[j] = truncateForEmbed(ch.Text, tokenCap)
		}
		vecs, embErr := c.embedder.Embed(ctx, texts)
		if embErr != nil || len(vecs) != len(chunks) {
			continue
		}
		if inv, ok := c.passageWriter.(URLInvalidator); ok && getEnv("COSIFT_ZOMBIE_RECLAIM") == "1" {
			_, _ = inv.MarkURLInvalid(ctx, it.rec.URL)
		}
		for j, ch := range chunks {
			p := &store.Passage{DocID: id, Offset: ch.Offset, Length: ch.Length, Model: c.embedder.Model(), Embedding: vecs[j]}
			_ = c.passageWriter.UpsertPassage(ctx, p)
		}
	}
	return indexed, nil
}

// indexWetRecord runs one WET record through UpsertDocument + BM25 +
// chunk + embed + passage-write. Mirrors the crawler's success-path
// flow in processClaimed but skips fetch/parse/robots/sitemap discovery —
// the body is already plain text. When lexicalOnly is true, the chunk +
// embed + passage-write tail is skipped entirely. Iter 485.
func (c *Crawler) indexWetRecord(ctx context.Context, rec *WetRecord, lexicalOnly bool) error {
	if rec.URL == "" || len(rec.Body) == 0 {
		return nil
	}
	u, err := url.Parse(rec.URL)
	if err != nil {
		return err
	}
	if !c.allowedDomain(rec.URL) {
		return nil
	}
	text := string(rec.Body)
	title := firstNonEmptyLine(text)
	if len(title) > 200 {
		title = title[:200]
	}
	sha := sha256.Sum256(rec.Body)
	doc := &store.Document{
		URL:        rec.URL,
		Domain:     u.Host,
		Title:      title,
		Text:       text,
		Lang:       "",
		Source:     "wet-import",
		Quality:    0.6,
		FetchedAt:  time.Now(),
		ContentSHA: sha[:],
	}
	id, err := c.store.UpsertDocument(ctx, doc)
	if err != nil {
		return err
	}
	if err := c.idx.IndexDocument(ctx, id, title, text); err != nil {
		return err
	}
	if lexicalOnly {
		return nil
	}
	// Chunk + embed if embedder is configured.
	if c.embedder == nil || c.passageWriter == nil {
		return nil
	}
	chunker := index.NewChunkerWith(c.chunkSizeFor(u.Host), c.chunkOverlapFor(u.Host))
	chunks := chunker.Chunk(text)
	if len(chunks) == 0 {
		return nil
	}
	const tokenCap = 1500
	texts := make([]string, len(chunks))
	for i, ch := range chunks {
		texts[i] = truncateForEmbed(ch.Text, tokenCap)
	}
	vecs, embErr := c.embedder.Embed(ctx, texts)
	if embErr != nil || len(vecs) != len(chunks) {
		return nil // best-effort — BM25 doc is already indexed
	}
	if inv, ok := c.passageWriter.(URLInvalidator); ok {
		if getEnv("COSIFT_ZOMBIE_RECLAIM") == "1" {
			_, _ = inv.MarkURLInvalid(ctx, rec.URL)
		}
	}
	for i, ch := range chunks {
		p := &store.Passage{
			DocID:     id,
			Offset:    ch.Offset,
			Length:    ch.Length,
			Model:     c.embedder.Model(),
			Embedding: vecs[i],
		}
		_ = c.passageWriter.UpsertPassage(ctx, p)
	}
	return nil
}

// firstNonEmptyLine grabs the first non-blank line for use as a doc title.
// WET files don't carry HTML <title>, so we approximate from the leading
// text — typically the page's main heading.
func firstNonEmptyLine(s string) string {
	sc := bufio.NewScanner(strings.NewReader(s))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line != "" {
			return line
		}
	}
	return ""
}

// getEnv shadows os.Getenv for compactness at call sites. Iter 485.
func getEnv(key string) string {
	return os.Getenv(key)
}
