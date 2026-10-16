// Package work leases fetch work to workers, verifies what they attest and indexes what is verified.
package work

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

const extractVersion = crawler.ExtractVersion

type Class string

const (
	ClassPushed   Class = "pushed"
	ClassErrored  Class = "errored"
	ClassFrontier Class = "frontier"
)

var classOrder = []Class{ClassPushed, ClassErrored, ClassFrontier}

type State string

const (
	StateAwaiting   State = "awaiting"
	StateModerating State = "moderating"
	StateDeferred   State = "deferred"
	StateDone       State = "done"
	StateMismatch   State = "mismatch"
	StateRejected   State = "rejected"
	StateExpired    State = "expired"
)

type CopyState string

const (
	CopyNone       CopyState = "none"
	CopyModerating CopyState = "moderating"
	CopyDeferred   CopyState = "deferred"
	CopyDone       CopyState = "done"
	CopyRejected   CopyState = "rejected"
)

// Attestation is one worker's claim about a page; it keeps hashes, never text. TitleSha, Lang and Len
// are kept for an errored record only (WORK 6.5).
type Attestation struct {
	Worker   string    `json:"worker"`
	Net      string    `json:"net"`
	Hash     string    `json:"hash"`
	Simhash  string    `json:"simhash"`
	At       time.Time `json:"at"`
	TitleSha string    `json:"title_sha,omitempty"`
	Lang     string    `json:"lang,omitempty"`
	Len      int       `json:"len,omitempty"`
}

// Group holds attestations that agree with its anchor, each from a distinct worker and network.
// The part hashes of an errored record's anchor are in W p (WORK 5); PartsN is how many parts its text had
// before the cap.
type Group struct {
	Anchor  string        `json:"anchor"`
	PartsN  int           `json:"parts_n,omitempty"`
	Members []Attestation `json:"members"`
}

// Record is the durable state of one URL's attestations.
type Record struct {
	V                  int       `json:"v"`
	State              State     `json:"state"`
	Copy               CopyState `json:"copy"`
	ModerationFailures int       `json:"moderation_failures,omitempty"`
	RetryAt            time.Time `json:"retry_at"`
	CopyFailures       int       `json:"copy_failures,omitempty"`
	CopyRetryAt        time.Time `json:"copy_retry_at"`
	Class              Class     `json:"class"`
	FirstAt            time.Time `json:"first_at"`
	WindowUntil        time.Time `json:"window_until"`
	RefetchAt          time.Time `json:"refetch_at"`
	RefetchFailures    int       `json:"refetch_failures,omitempty"`
	FinalURL           string    `json:"final_url,omitempty"`
	Groups             []Group   `json:"groups"`
	VerifiedGroup      int       `json:"verified_group"`
	VerifiedBy         string    `json:"verified_by,omitempty"`
	ContentHash        string    `json:"content_hash,omitempty"`
	ContentNovel       bool      `json:"content_novel,omitempty"`
	RejectReason       string    `json:"reject_reason,omitempty"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// Text is a verified or copied document waiting for moderation and indexing.
type Text struct {
	FinalURL string    `json:"final_url"`
	Title    string    `json:"title"`
	Text     string    `json:"text"`
	Lang     string    `json:"lang"`
	Artifact *Artifact `json:"artifact,omitempty"`
}

// Artifact is a worker's embedding vectors for the chunks of its text.
type Artifact struct {
	Model  string                 `json:"model"`
	Chunks []crawler.IndexedChunk `json:"chunks"`
}

// Event is one settled outcome for the broker to credit.
type Event struct {
	Seq          uint64    `json:"seq"`
	URLSha       string    `json:"url_sha"`
	Verdict      string    `json:"verdict"`
	Workers      []string  `json:"workers"`
	ContentNovel bool      `json:"content_novel"`
	At           time.Time `json:"at"`
}

// Page is the server's own fetch of a URL.
type Page struct {
	FinalURL, Title, Text, Lang string
}

// ModerationDoc is a page put to the content-safety check, in full.
type ModerationDoc struct {
	URL, Title, Text string
}

// Verdict is the content-safety decision on a page.
type Verdict struct {
	Decision, Category string
}

// FrontierRow is a frontier row as a feeder reads it.
type FrontierRow struct {
	URL, LastError string
}

// FrontierSource reads frontier rows without claiming them.
type FrontierSource interface {
	// Errored returns errored rows after the cursor in URL order and the last URL scanned;
	// end is true when the scan reached the end of the range.
	Errored(ctx context.Context, after string, scan int) (rows []FrontierRow, next string, end bool, err error)
	// Queued is Errored for queued rows.
	Queued(ctx context.Context, after string, scan int) (rows []FrontierRow, next string, end bool, err error)
}

// Deps are the engine services the work routes lean on.
type Deps struct {
	// Canonical returns the pool's canonical form of a public web URL or an error.
	Canonical func(raw string) (string, error)
	// Allowed applies the crawler's domain policy and the adult filter to a canonical URL.
	Allowed func(canonical string) bool
	// InCorpus reports whether a document is stored under the canonical URL.
	InCorpus func(ctx context.Context, canonical string) (bool, error)
	Frontier FrontierSource
	// Fetch is the server's own copy of a URL, through the crawl path.
	Fetch func(ctx context.Context, url string) (Page, error)
	// BotCheck reports a page that is an error, login or bot-check page.
	BotCheck func(title, text string) bool
	// Moderate decides a page's safety; reserve must run just before a model call.
	Moderate func(ctx context.Context, doc ModerationDoc, reserve func() error) (Verdict, error)
	Index    func(ctx context.Context, d crawler.AttestedDoc) (indexed, contentNovel bool, err error)
	// Purge soft-deletes the attested document stored under url, and counts a crawled one as kept.
	Purge func(ctx context.Context, url string, apply bool) (deleted, kept int, err error)
}

// Error is a refusal with the status, code and detail of the /v1 error envelope.
type Error struct {
	Status         int
	Code           string
	Field, Rule    string
	ExtractVersion int
	RetryAfter     time.Duration
}

func (e *Error) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("%s: %s %s", e.Code, e.Field, e.Rule)
	}
	return e.Code
}

func invalidField(field, rule string) *Error {
	return &Error{Status: http.StatusUnprocessableEntity, Code: "invalid_field", Field: field, Rule: rule}
}

var (
	errLeaseExpired  = &Error{Status: http.StatusConflict, Code: "lease_expired"}
	errLeaseNotOwner = &Error{Status: http.StatusConflict, Code: "lease_not_owner"}
	errUnavailable   = &Error{Status: http.StatusServiceUnavailable, Code: "index_unavailable"}
)

// CeilingError reports the daily moderation ceiling; Retry is the time to 00:00 UTC.
type CeilingError struct{ Retry time.Duration }

func (e *CeilingError) Error() string { return "daily moderation ceiling reached" }
