package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/eval"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

// runIngest loads a corpus.json directly into the local store + indexes —
// useful for evaluation, vertical deployments, and users with a pre-curated
// dataset who don't need a crawler.
//
// The corpus shape matches eval.Corpus: {"docs": [{"url","title","text"}...]}.
// If embeddings are configured, every doc is chunked + embedded + persisted
// to the passages table the same way the crawler does.
// progressReporter logs progress for a long-running CLI loop at most once per
// configured interval. Time-based (vs every-N-iterations) so the cadence is
// consistent regardless of per-item cost variance. Set interval to 0 to
// disable. Designed for ingest but generic enough for other loops.
type progressReporter struct {
	label    string
	total    int
	interval time.Duration
	last     time.Time
}

func newProgressReporter(label string, total int, interval time.Duration) *progressReporter {
	return &progressReporter{
		label:    label,
		total:    total,
		interval: interval,
		// Initialize `last` to now so the first call after construction
		// doesn't immediately log (avoids a noisy first-iter "0/N" line).
		last: time.Now(),
	}
}

// maybeLog emits a progress line only if `interval` has elapsed since the last
// emission. Callers pass the 1-indexed current item count. No-op when interval
// is <= 0 or when total is 0 (avoids divide-by-zero in the percentage).
func (p *progressReporter) maybeLog(current int) {
	if p.interval <= 0 || p.total <= 0 {
		return
	}
	if time.Since(p.last) < p.interval {
		return
	}
	pct := float64(current) / float64(p.total) * 100
	log.Printf("%s: %d/%d (%.1f%%)", p.label, current, p.total, pct)
	p.last = time.Now()
}

// loadCorpusJSONL reads a JSONL file (one eval.CorpusDoc per line) into a
// Corpus. Empty lines are skipped (some tools emit trailing blanks). Pairs
// with's `export -format jsonl` to close the round-trip for ML
// pipelines.
func loadCorpusJSONL(path string) (*eval.Corpus, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	// JSONL lines can be large (full doc text); raise the scanner buffer cap
	// well above the default 64KB. 4MB matches's SSE parser ceiling.
	scanner.Buffer(make([]byte, 0, 64<<10), 4<<20)
	c := &eval.Corpus{}
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" {
			continue
		}
		var d eval.CorpusDoc
		if err := json.Unmarshal([]byte(raw), &d); err != nil {
			return nil, fmt.Errorf("parse %s line %d: %w", path, lineNum, err)
		}
		c.Docs = append(c.Docs, d)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return c, nil
}

func runIngest(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("ingest", flag.ExitOnError)
	corpusPath := fs.String("corpus", "", "path to corpus file (required)")
	source := fs.String("source", "ingest", "source tag for documents")
	// -format selects the loader. Default "auto" infers from file
	// extension (.jsonl → jsonl, anything else → json). Explicit override
	// avoids ambiguity when files have non-standard extensions.
	format := fs.String("format", "auto", "input format: auto (default — infer from extension) | json | jsonl")
	// Operators ingesting large corpora need
	// some signal that work is happening. 0 disables.
	progressInterval := fs.Duration("progress", 5*time.Second, "log per-doc/per-passage progress every N (0 disables)")
	// CLI override for chunker config (mirrors on eval/answer-eval).
	// 0 → fall through to cfg.Crawler.{ChunkSize,ChunkOverlap}, then to NewChunker defaults.
	ingestChunkSize := fs.Int("chunk-size", 0, "passage chunker target words (0 = use cfg.Crawler.ChunkSize, then NewChunker default 320)")
	ingestChunkOverlap := fs.Int("chunk-overlap", 0, "passage chunker overlap words (0 = use cfg.Crawler.ChunkOverlap, then default 64)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *corpusPath == "" {
		return errors.New("ingest: -corpus is required")
	}
	resolvedFormat := *format
	if resolvedFormat == "auto" {
		if strings.HasSuffix(strings.ToLower(*corpusPath), ".jsonl") {
			resolvedFormat = "jsonl"
		} else {
			resolvedFormat = "json"
		}
	}
	var corpus *eval.Corpus
	var err error
	switch resolvedFormat {
	case "json":
		corpus, err = eval.LoadCorpus(*corpusPath)
	case "jsonl":
		corpus, err = loadCorpusJSONL(*corpusPath)
	default:
		return fmt.Errorf("unknown -format %q (want: auto | json | jsonl)", *format)
	}
	if err != nil {
		return err
	}

	s, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer s.Close()
	bm := index.NewBM25(s)

	// Optional embedder — same auto-wire as runServe.
	var emb embed.Embedder
	if cfg.Embeddings.Model != "" {
		apiKey := os.Getenv("OPENAI_API_KEY")
		if apiKey == "" {
			apiKey = os.Getenv("OPENAI")
		}
		if apiKey != "" {
			dim := cfg.Embeddings.Dim
			if dim == 0 {
				dim = 1536
			}
			emb = embed.NewOpenAIClient(apiKey, cfg.Embeddings.URL, cfg.Embeddings.Model, dim)
		} else {
			log.Printf("warning: embeddings configured but no API key; ingesting BM25 only")
		}
	}

	// Pre-compute chunks for the whole corpus so we can batch-embed in one call.
	// CLI flag wins over cfg.Crawler.ChunkSize (only when flag is set).
	chunkSize := cfg.Crawler.ChunkSize
	if *ingestChunkSize > 0 {
		chunkSize = *ingestChunkSize
	}
	chunkOverlap := cfg.Crawler.ChunkOverlap
	if *ingestChunkOverlap > 0 {
		chunkOverlap = *ingestChunkOverlap
	}
	chunker := chunkerWith(chunkSize, chunkOverlap)
	type passageRef struct {
		docIdx int
		chunk  index.Chunk
	}
	var allTexts []string
	var refs []passageRef
	if emb != nil {
		for i, d := range corpus.Docs {
			for _, c := range chunker.Chunk(d.Title + "\n\n" + d.Text) {
				allTexts = append(allTexts, c.Text)
				refs = append(refs, passageRef{docIdx: i, chunk: c})
			}
		}
		log.Printf("ingest: embedding %d passages across %d docs with %s", len(allTexts), len(corpus.Docs), cfg.Embeddings.Model)
	}

	var vecs [][]float32
	if emb != nil && len(allTexts) > 0 {
		vecs, err = emb.Embed(ctx, allTexts)
		if err != nil {
			return fmt.Errorf("embed: %w", err)
		}
	}

	// Map docIdx → docID for the passage writes.
	docIDs := make([]int64, len(corpus.Docs))
	docProgress := newProgressReporter("ingest docs", len(corpus.Docs), *progressInterval)
	for i, d := range corpus.Docs {
		sha := sha256.Sum256([]byte(d.Text))
		id, err := s.UpsertDocument(ctx, &store.Document{
			URL: d.URL, Title: d.Title, Text: d.Text,
			Source: *source, Quality: 0.7, FetchedAt: time.Now(),
			ContentSHA: sha[:],
		})
		if err != nil {
			return fmt.Errorf("upsert %s: %w", d.URL, err)
		}
		if err := bm.IndexDocument(ctx, id, d.Title, d.Text); err != nil {
			return fmt.Errorf("index %s: %w", d.URL, err)
		}
		docIDs[i] = id
		docProgress.maybeLog(i + 1)
	}

	// Write passages.
	if emb != nil && len(vecs) == len(refs) {
		passageProgress := newProgressReporter("ingest passages", len(vecs), *progressInterval)
		for j, v := range vecs {
			ref := refs[j]
			p := &store.Passage{
				DocID:     docIDs[ref.docIdx],
				Offset:    ref.chunk.Offset,
				Length:    ref.chunk.Length,
				Model:     emb.Model(),
				Embedding: v,
			}
			if err := s.UpsertPassage(ctx, p); err != nil {
				return fmt.Errorf("passage %s offset=%d: %w", corpus.Docs[ref.docIdx].URL, ref.chunk.Offset, err)
			}
			passageProgress.maybeLog(j + 1)
		}
	}

	log.Printf("ingest: %d docs, %d passages", len(corpus.Docs), len(vecs))
	return nil
}

// runExport writes the local store's documents to a portable corpus.json,
// shape-compatible with `cosift ingest`. Round-trips cleanly:
//
//	cosift export -output corpus.json
//	cosift -config other.json ingest -corpus corpus.json
//
// Documents only — passages (embeddings) aren't exported because the receiver
// likely uses a different embedding model.
// runMigrateToPebble copies a SQLite-backed cosift data directory into a
// fresh Pebble store. Fifth piece of the path-2 storage rework.
//
// Migrates:
//   - documents (URL, title, text, metadata) via PebbleStore.UpsertDocument
//   - BM25 postings (re-tokenized + re-indexed to preserve title
//     boost; the SQLite postings table is NOT copied directly because
//     re-indexing through PebbleBM25 is the same code path that production
//     uses going forward — eliminates the divergence risk of two
//     posting-write paths)
//
// Does NOT migrate (deferred): frontier rows, query_outcomes feedback,
// paraphrase/HyDE caches, vector embeddings. Operators starting from a
// migrated Pebble store get a working BM25 index immediately; dense + LLM
// caches rebuild from scratch on the new instance.
//
// The destination directory must be empty; the migration refuses to
// overwrite existing data so a mistyped path can't clobber a working store.
func runMigrateToPebble(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("migrate-to-pebble", flag.ExitOnError)
	output := fs.String("output", "", "output directory for the Pebble store (required)")
	progress := fs.Duration("progress", 5*time.Second, "log progress every N (0 disables)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *output == "" {
		return errors.New("migrate-to-pebble: -output is required")
	}
	// Refuse to write into a directory that already contains a Pebble store.
	if entries, err := os.ReadDir(*output); err == nil && len(entries) > 0 {
		return fmt.Errorf("migrate-to-pebble: -output %s is non-empty; refusing to overwrite (move or remove first)", *output)
	}

	src, err := store.Open(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("open source SQLite store at %s: %w", cfg.DataDir, err)
	}
	defer src.Close()

	dst, err := openPebbleOrFriendlyErr(*output)
	if err != nil {
		return err
	}
	defer dst.Close()

	pidx := index.NewPebbleBM25(dst)

	docs, err := src.ListDocuments(ctx, 0)
	if err != nil {
		return fmt.Errorf("list source documents: %w", err)
	}
	log.Printf("migrate-to-pebble: %d documents to copy (source: %s → destination: %s)",
		len(docs), cfg.DataDir, *output)

	reporter := newProgressReporter("migrate docs", len(docs), *progress)
	for i, d := range docs {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Upsert the doc into Pebble — assigns a fresh Pebble-side ID.
		newID, err := dst.UpsertDocument(ctx, d)
		if err != nil {
			return fmt.Errorf("upsert doc %s: %w", d.URL, err)
		}
		// Re-index in Pebble BM25. Uses the same Tokenize + TitleBoost that
		// production reads through, so behavior of the migrated index matches
		// what a fresh crawl would produce.
		if err := pidx.IndexDocument(ctx, newID, d.Title, d.Text); err != nil {
			return fmt.Errorf("index doc %s: %w", d.URL, err)
		}
		reporter.maybeLog(i + 1)
	}

	stats, err := dst.Stats(ctx)
	if err != nil {
		return err
	}
	log.Printf("migrate-to-pebble: %d documents indexed into Pebble at %s", stats.Documents, *output)
	return nil
}

func runExport(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	output := fs.String("output", "", "output path (default depends on -format: corpus-export.{json,jsonl,txt,md})")
	limit := fs.Int("limit", 0, "max docs to export (0 = all, applied AFTER filters)")
	// -format selects the on-disk shape.
	//   json (default — eval.Corpus pretty-printed; backward-compatible with)
	//   jsonl — one {url,title,text} per line; common for ML fine-tuning pipelines
	//   text  — Title/URL header + body + --- separator; for RAG corpora / grep
	//   md    — `# Title` + URL line + body + horizontal rule; for LLM prompt piping
	format := fs.String("format", "json", "output format: json | jsonl | text | md")
	// All applied client-side after ListDocuments.
	includeDomains := fs.String("include-domains", "", "comma-separated allowlist of domains (suffix match on dot boundary)")
	excludeDomains := fs.String("exclude-domains", "", "comma-separated denylist of domains")
	since := fs.String("since", "", "ISO date — only docs with published_at on or after (drops undated docs)")
	until := fs.String("until", "", "ISO date — only docs with published_at on or before (drops undated docs)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := validateExportFormat(*format); err != nil {
		return err
	}
	if *output == "" {
		*output = "corpus-export." + exportExtension(*format)
	}

	sinceT, err := parseExportDate(*since)
	if err != nil {
		return fmt.Errorf("-since: %w", err)
	}
	untilT, err := parseExportDate(*until)
	if err != nil {
		return fmt.Errorf("-until: %w", err)
	}
	includeList := splitDomainCSV(*includeDomains)
	excludeList := splitDomainCSV(*excludeDomains)
	hasDateFilter := !sinceT.IsZero() || !untilT.IsZero()

	s, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer s.Close()

	// limit applies AFTER filters now — operators expect "give me 100
	// docs from example.com" to mean 100 example.com docs, not 100 docs total
	// of which some are from example.com. Pass 0 (no limit) to ListDocuments
	// and apply the limit client-side after filtering.
	rawLimit := 0
	if *limit > 0 && !hasDateFilter && len(includeList) == 0 && len(excludeList) == 0 {
		// No filters → push the limit down to ListDocuments (faster, less memory).
		rawLimit = *limit
	}
	docs, err := s.ListDocuments(ctx, rawLimit)
	if err != nil {
		return err
	}

	docs = filterExportDocs(docs, includeList, excludeList, sinceT, untilT)
	if *limit > 0 && len(docs) > *limit {
		docs = docs[:*limit]
	}

	f, cerr := os.Create(*output)
	if cerr != nil {
		return cerr
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close output: %w", closeErr)
		}
	}()

	switch *format {
	case "json":
		// wire shape: eval.Corpus pretty-printed. Preserved as default.
		corpus := eval.Corpus{Docs: make([]eval.CorpusDoc, 0, len(docs))}
		for _, d := range docs {
			corpus.Docs = append(corpus.Docs, eval.CorpusDoc{URL: d.URL, Title: d.Title, Text: d.Text})
		}
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(corpus); err != nil {
			return fmt.Errorf("encode: %w", err)
		}
	case "jsonl":
		// One {url,title,text} per line. No outer array — caller streams line-by-line.
		enc := json.NewEncoder(f)
		for _, d := range docs {
			if err := enc.Encode(eval.CorpusDoc{URL: d.URL, Title: d.Title, Text: d.Text}); err != nil {
				return fmt.Errorf("encode jsonl: %w", err)
			}
		}
	case "text":
		// Title:/URL: headers + blank + body + --- separator. Matches
		// `contents -text` batch separator convention (LLMs interpret `---` as
		// section breaks).
		for i, d := range docs {
			if i > 0 {
				fmt.Fprintln(f, "---")
				fmt.Fprintln(f)
			}
			if d.Title != "" {
				fmt.Fprintf(f, "Title: %s\n", d.Title)
			}
			fmt.Fprintf(f, "URL: %s\n\n", d.URL)
			fmt.Fprintln(f, d.Text)
		}
	case "md":
		// `# Title` heading + italic URL line + body + horizontal rule. For
		// piping into LLM prompt contexts that render markdown.
		for i, d := range docs {
			if i > 0 {
				fmt.Fprintln(f)
				fmt.Fprintln(f, "---")
				fmt.Fprintln(f)
			}
			if d.Title != "" {
				fmt.Fprintf(f, "# %s\n\n", d.Title)
			}
			fmt.Fprintf(f, "_%s_\n\n", d.URL)
			fmt.Fprintln(f, d.Text)
		}
	}
	log.Printf("export: %d docs → %s (%s)", len(docs), *output, *format)
	return nil
}

// parseExportDate accepts YYYY-MM-DD or full RFC3339 (matches the
// /search?since=/?until= conventions). Empty input returns zero time + nil
// error (no filter).
func parseExportDate(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("could not parse date %q", s)
}

// splitDomainCSV parses a comma-separated list of domains into lowercased
// non-empty entries. (mirrors internal/server's unexported splitCSV).
func splitDomainCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		t := strings.ToLower(strings.TrimSpace(p))
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}

// matchesDomainPattern is the strict suffix-on-dot-boundary match the
// /search filter uses. `example.com` matches `blog.example.com` but NOT
// `evilexample.com`.
func matchesDomainPattern(host string, patterns []string) bool {
	host = strings.ToLower(host)
	for _, p := range patterns {
		if host == p || strings.HasSuffix(host, "."+p) {
			return true
		}
	}
	return false
}

// hostFromURL extracts the lowercased host without port from a URL. Returns
// empty string when parsing fails so callers can treat unparseable URLs
// uniformly (typically: excluded from include-filtered exports).
func hostFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
}

// filterExportDocs applies the export filters to a doc slice
// in-memory. Returns the filtered slice (in input order — preserves whatever
// ordering ListDocuments returned).
func filterExportDocs(docs []*store.Document, include, exclude []string, since, until time.Time) []*store.Document {
	hasDateFilter := !since.IsZero() || !until.IsZero()
	hasDomainFilter := len(include) > 0 || len(exclude) > 0
	if !hasDateFilter && !hasDomainFilter {
		return docs
	}
	out := make([]*store.Document, 0, len(docs))
	for _, d := range docs {
		// Date filter: undated docs are excluded when ANY date bound is set
		// (matches /search?since=/?until= semantics).
		if hasDateFilter {
			if d.PublishedAt.IsZero() {
				continue
			}
			if !since.IsZero() && d.PublishedAt.Before(since) {
				continue
			}
			if !until.IsZero() && d.PublishedAt.After(until) {
				continue
			}
		}
		if hasDomainFilter {
			host := hostFromURL(d.URL)
			if len(include) > 0 && !matchesDomainPattern(host, include) {
				continue
			}
			if len(exclude) > 0 && matchesDomainPattern(host, exclude) {
				continue
			}
		}
		out = append(out, d)
	}
	return out
}

// validateExportFormat is the export-side counterpart to's
// validateFormat. Different accept-list because export supports json/jsonl
// (file-shaped) which the synth-output CLIs don't, and synth CLIs support
// markdown aliases which export doesn't need (single canonical name reduces
// confusion when picking a file extension).
func validateExportFormat(v string) error {
	switch v {
	case "json", "jsonl", "text", "md":
		return nil
	default:
		return fmt.Errorf("export format must be one of: json, jsonl, text, md (got %q)", v)
	}
}

// exportExtension picks a conventional file extension for each export format.
// Used to derive a default -output path when the operator doesn't specify one.
func exportExtension(format string) string {
	switch format {
	case "jsonl":
		return "jsonl"
	case "text":
		return "txt"
	case "md":
		return "md"
	default: // json
		return "json"
	}
}

// runReembed walks every document, chunks it, embeds the chunks with the
// configured model, and writes them as new passages. The new rows coexist with
// the old (different model_id) until `-drop-old` is passed.
//
// Recommended sequence:
//
//	cosift reembed                       # write new passages alongside old
//	# verify search quality on new model …
//	cosift reembed -drop-old             # drop the others
//
// Or just `cosift reembed -drop-old` if you're confident.
func runReembed(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("reembed", flag.ExitOnError)
	dropOld := fs.Bool("drop-old", false, "after re-embed, delete passages with a different model_id")
	batchSize := fs.Int("batch", 256, "embeddings per API call (provider-dependent cap)")
	// time-based progress reporting (shared helper from).
	progressInterval := fs.Duration("progress", 5*time.Second, "log re-embed progress every N (0 disables)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if cfg.Embeddings.Model == "" {
		return errors.New("reembed: cfg.Embeddings.Model must be set")
	}
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		apiKey = os.Getenv("OPENAI")
	}
	if apiKey == "" {
		return errors.New("reembed: no OPENAI key in env")
	}
	dim := cfg.Embeddings.Dim
	if dim == 0 {
		dim = 1536
	}
	emb := embed.NewOpenAIClient(apiKey, cfg.Embeddings.URL, cfg.Embeddings.Model, dim)

	s, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer s.Close()

	docs, err := s.ListDocuments(ctx, 0)
	if err != nil {
		return err
	}
	if len(docs) == 0 {
		log.Printf("reembed: 0 docs in store — nothing to do")
		return nil
	}

	// Pre-existing models (informational; useful for the operator).
	models, _ := s.PassageModels(ctx)
	log.Printf("reembed: %d docs · target model=%s · existing models=%v · batch=%d",
		len(docs), cfg.Embeddings.Model, models, *batchSize)

	chunker := chunkerWith(cfg.Crawler.ChunkSize, cfg.Crawler.ChunkOverlap)
	type ref struct {
		docID int64
		chunk index.Chunk
	}
	var (
		texts   []string
		refs    []ref
		total   int
		flush   func() error
		written int
	)
	flush = func() error {
		if len(texts) == 0 {
			return nil
		}
		vecs, err := emb.Embed(ctx, texts)
		if err != nil {
			return fmt.Errorf("embed batch (%d texts): %w", len(texts), err)
		}
		if len(vecs) != len(texts) {
			return fmt.Errorf("embed count mismatch: %d vs %d", len(vecs), len(texts))
		}
		for i, v := range vecs {
			if err := s.UpsertPassage(ctx, &store.Passage{
				DocID:     refs[i].docID,
				Offset:    refs[i].chunk.Offset,
				Length:    refs[i].chunk.Length,
				Model:     emb.Model(),
				Embedding: v,
			}); err != nil {
				return fmt.Errorf("write passage: %w", err)
			}
			written++
		}
		texts = texts[:0]
		refs = refs[:0]
		return nil
	}

	docProgress := newProgressReporter("reembed docs", len(docs), *progressInterval)
	for docIdx, d := range docs {
		for _, c := range chunker.Chunk(d.Title + "\n\n" + d.Text) {
			texts = append(texts, c.Text)
			refs = append(refs, ref{docID: d.ID, chunk: c})
			total++
			if len(texts) >= *batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		docProgress.maybeLog(docIdx + 1)
	}
	if err := flush(); err != nil {
		return err
	}
	log.Printf("reembed: wrote %d passages with model=%s", written, cfg.Embeddings.Model)

	if *dropOld {
		n, err := s.DropPassagesNotModel(ctx, cfg.Embeddings.Model)
		if err != nil {
			return fmt.Errorf("drop old: %w", err)
		}
		log.Printf("reembed: dropped %d passages from other models", n)
	} else if len(models) > 0 && !contains(models, cfg.Embeddings.Model) {
		log.Printf("reembed: %d other-model passages remain — run with -drop-old to remove", len(models))
	}
	return nil
}

// runCompactIndex drops orphaned passages and terms, then VACUUMs.
// Companion to `cosift gc` — gc cleans the frontier; compact-index cleans
// the index proper.
func runCompactIndex(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("compact-index", flag.ExitOnError)
	vacuum := fs.Bool("vacuum", true, "run VACUUM after deletes to reclaim disk")
	if err := fs.Parse(args); err != nil {
		return err
	}
	s, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer s.Close()

	dropped, err := s.DropOrphanedPassages(ctx)
	if err != nil {
		return fmt.Errorf("drop orphan passages: %w", err)
	}
	log.Printf("compact-index: dropped %d orphan passages", dropped)

	terms, err := s.DropOrphanedTerms(ctx)
	if err != nil {
		return fmt.Errorf("drop orphan terms: %w", err)
	}
	log.Printf("compact-index: dropped %d orphan terms", terms)

	if *vacuum {
		log.Printf("compact-index: vacuuming...")
		if err := s.Vacuum(ctx); err != nil {
			return fmt.Errorf("vacuum: %w", err)
		}
		log.Printf("compact-index: vacuum complete")
	}
	return nil
}

// batchEmbed chunks a large text slice into provider-safe batches before
// calling the embedder. OpenAI's text-embedding-3-* caps at 2048 inputs OR
// 300k tokens per request; 1000 is a safe default that respects both at
// our typical chunk sizes. Returns vectors in the input order.
//
// Between batches we sleep 200ms — keeps multi-batch runs under OpenAI's
// 1M-TPM org limit for text-embedding-3-small, which hit at ~50k inputs.
// Provider rate-limit errors get one bounded retry with the suggested delay.
func batchEmbed(ctx context.Context, e embed.Embedder, texts []string, batchSize int) ([][]float32, error) {
	if batchSize <= 0 {
		batchSize = 256
	}
	// 1000 is the published OpenAI per-call cap, but large-dim models
	// (text-embedding-3-large @ 3072) return malformed JSON well before that
	// in practice. 256 is safe across the model lineup.
	if batchSize > 1000 {
		batchSize = 1000
	}
	if len(texts) <= batchSize {
		return e.Embed(ctx, texts)
	}
	out := make([][]float32, 0, len(texts))
	for i := 0; i < len(texts); i += batchSize {
		end := i + batchSize
		if end > len(texts) {
			end = len(texts)
		}
		vecs, err := embedWithRetry(ctx, e, texts[i:end])
		if err != nil {
			return nil, fmt.Errorf("batch %d-%d: %w", i, end, err)
		}
		if len(vecs) != end-i {
			return nil, fmt.Errorf("batch %d-%d: %d returned, expected %d", i, end, len(vecs), end-i)
		}
		out = append(out, vecs...)
		if end < len(texts) {
			select {
			case <-ctx.Done():
				return out, ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	return out, nil
}

// embedWithRetry retries once on rate-limit errors with a fixed backoff.
// Crude but effective at our scale — keeps full distractor runs from failing
// when a single batch slips past the TPM bucket. Production deployments with
// higher request volumes want exponential backoff + provider-supplied delay
// parsing; deferred until traffic warrants.
func embedWithRetry(ctx context.Context, e embed.Embedder, texts []string) ([][]float32, error) {
	vecs, err := e.Embed(ctx, texts)
	if err == nil {
		return vecs, nil
	}
	msg := err.Error()
	if !strings.Contains(msg, "rate_limit") && !strings.Contains(msg, "429") {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(2 * time.Second):
	}
	return e.Embed(ctx, texts)
}
