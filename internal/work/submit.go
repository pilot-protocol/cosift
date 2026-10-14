package work

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

const (
	minTextBytes  = 80
	maxTextBytes  = 512 << 10
	maxTitleBytes = 1000
	maxLangBytes  = 35
	maxURLBytes   = 2048
)

var (
	langPattern = regexp.MustCompile(`^[A-Za-z]{2,3}(-[A-Za-z0-9]{1,8})*$`)
	hash64      = regexp.MustCompile(`^[0-9a-f]{64}$`)
	hash16      = regexp.MustCompile(`^[0-9a-f]{16}$`)
)

// SubmitRequest is a lease's attestation or failure report; a nil member was absent from the body.
type SubmitRequest struct {
	LeaseID, Worker, Net                              string
	HTTPStatus                                        *int
	FinalURL, Title, Text, Lang, ContentHash, Simhash *string
	ExtractVersion                                    *int
	Artifact                                          *Artifact
}

// SubmitResult is the answer to a submission.
type SubmitResult struct {
	Verdict         string
	Agreeing        []string
	Indexed         bool
	WindowExpiresAt *time.Time
	Reason          string
}

// attestation is a validated 200 submission.
type attestation struct {
	finalURL string
	text     Text
	hash     string
	simhash  string
	split    *splitText
}

// parts cuts the text into parts once; only an errored record needs them.
func (a *attestation) parts() *splitText {
	if a.split == nil {
		a.split = splitParts(a.text.Text)
	}
	return a.split
}

// Submit takes the attestation or failure report for a lease.
func (s *Service) Submit(req SubmitRequest) (SubmitResult, error) {
	if !hexID32.MatchString(req.LeaseID) {
		return SubmitResult{}, invalidField("lease_id", "pattern")
	}
	if !hexID32.MatchString(req.Worker) {
		return SubmitResult{}, invalidField("worker", "pattern")
	}
	if !hexID32.MatchString(req.Net) {
		return SubmitResult{}, invalidField("net", "pattern")
	}
	if req.HTTPStatus == nil {
		return SubmitResult{}, invalidField("http_status", "required")
	}
	if *req.HTTPStatus < 100 || *req.HTTPStatus > 599 {
		return SubmitResult{}, invalidField("http_status", "range")
	}
	failure := *req.HTTPStatus != 200
	var att *attestation
	if failure {
		if err := reportShape(req); err != nil {
			return SubmitResult{}, err
		}
	} else {
		var err error
		if att, err = s.checkAttestation(req); err != nil {
			return SubmitResult{}, err
		}
		if s.leaseClass(req.LeaseID) == ClassErrored {
			att.parts()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.expireLeases(now)
	l, c := s.leases[req.LeaseID], s.consumed[req.LeaseID]
	switch {
	case l != nil && (l.worker != req.Worker || l.net != req.Net), c != nil && (c.worker != req.Worker || c.net != req.Net):
		return SubmitResult{}, errLeaseNotOwner
	case l == nil && c == nil:
		return SubmitResult{}, errLeaseExpired
	case l == nil:
		if failure != c.failure || (!failure && att.hash != c.hash) {
			return SubmitResult{}, errLeaseExpired
		}
		if failure {
			return SubmitResult{Verdict: "released", Agreeing: []string{}}, nil
		}
		return s.currentVerdict(c.url, req.Worker, now)
	}
	if failure {
		return s.failLocked(l, now), nil
	}
	if err := s.checkFinalURL(l.url, att); err != nil {
		return SubmitResult{}, err
	}
	res, err := s.attestLocked(l, Attestation{Worker: req.Worker, Net: req.Net, Hash: att.hash, Simhash: att.simhash, At: now.UTC()}, att, now)
	if err != nil {
		return SubmitResult{}, err
	}
	s.metrics.submissions.add(res.Verdict)
	return res, nil
}

// leaseClass is the class of a live lease, so the parts of an errored text are cut outside the lock.
func (s *Service) leaseClass(id string) Class {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l := s.leases[id]; l != nil {
		return l.class
	}
	return ""
}

func reportShape(req SubmitRequest) error {
	for _, f := range []struct {
		name string
		set  bool
	}{
		{"title", req.Title != nil}, {"text", req.Text != nil}, {"lang", req.Lang != nil}, {"content_hash", req.ContentHash != nil},
		{"simhash", req.Simhash != nil}, {"extract_version", req.ExtractVersion != nil}, {"artifact", req.Artifact != nil},
	} {
		if f.set {
			return invalidField(f.name, "field_not_allowed")
		}
	}
	if req.FinalURL != nil && len(*req.FinalURL) > maxURLBytes {
		return invalidField("final_url", "length")
	}
	return nil
}

// checkAttestation validates a 200 submission's members and recomputes its hashes.
func (s *Service) checkAttestation(req SubmitRequest) (*attestation, error) {
	for _, f := range []struct {
		name string
		set  bool
	}{
		{"final_url", req.FinalURL != nil}, {"title", req.Title != nil}, {"text", req.Text != nil}, {"lang", req.Lang != nil},
		{"content_hash", req.ContentHash != nil}, {"simhash", req.Simhash != nil}, {"extract_version", req.ExtractVersion != nil},
	} {
		if !f.set {
			return nil, invalidField(f.name, "required")
		}
	}
	if *req.ExtractVersion != extractVersion {
		return nil, &Error{Status: 409, Code: "extract_version_mismatch", ExtractVersion: extractVersion}
	}
	title, text, lang := *req.Title, *req.Text, *req.Lang
	switch {
	case len(title) > maxTitleBytes:
		return nil, invalidField("title", "length")
	case !utf8.ValidString(title):
		return nil, invalidField("title", "char_class")
	case len(text) < minTextBytes || len(text) > maxTextBytes:
		return nil, invalidField("text", "length")
	case !utf8.ValidString(text) || strings.IndexByte(text, 0) >= 0:
		return nil, invalidField("text", "char_class")
	case lang != "" && (len(lang) > maxLangBytes || !langPattern.MatchString(lang)):
		return nil, invalidField("lang", "pattern")
	case !hash64.MatchString(*req.ContentHash):
		return nil, invalidField("content_hash", "pattern")
	case !hash16.MatchString(*req.Simhash):
		return nil, invalidField("simhash", "pattern")
	case len(*req.FinalURL) > maxURLBytes:
		return nil, invalidField("final_url", "length")
	}
	sum, ok := crawler.Simhash(text)
	if !ok {
		return nil, invalidField("text", "required")
	}
	digest := sha256.Sum256([]byte(text))
	hash := hex.EncodeToString(digest[:])
	if *req.ContentHash != hash {
		return nil, invalidField("content_hash", "mismatch")
	}
	if *req.Simhash != crawler.SimhashHex(sum) {
		return nil, invalidField("simhash", "mismatch")
	}
	doc := Text{Title: title, Text: text, Lang: lang}
	if a := req.Artifact; a != nil && (a.Model != "" || len(a.Chunks) > 0) {
		la := crawler.LocalArtifact{Title: title, Text: text, Model: a.Model, Chunks: a.Chunks}
		if la.ValidateVectors() != nil {
			return nil, invalidField("artifact", "artifact_invalid")
		}
		doc.Artifact = a
	}
	return &attestation{finalURL: *req.FinalURL, text: doc, hash: hash, simhash: crawler.SimhashHex(sum)}, nil
}

// checkFinalURL canonicalises the worker's final URL into att and holds it to the leased site and the policy.
func (s *Service) checkFinalURL(leaseURL string, att *attestation) error {
	canon, err := s.deps.Canonical(att.finalURL)
	if err != nil {
		return invalidField("final_url", "url")
	}
	if !sameSite(leaseURL, canon) {
		return invalidField("final_url", "off_host")
	}
	if !s.deps.Allowed(canon) {
		return invalidField("final_url", "policy")
	}
	att.finalURL, att.text.FinalURL = canon, canon
	return nil
}

// sameSite holds when the hosts are equal after lower-casing, or differ only by a leading www.
func sameSite(a, b string) bool {
	ha, hb := siteHost(a), siteHost(b)
	return ha != "" && ha == hb
}

func siteHost(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.TrimPrefix(strings.ToLower(u.Hostname()), "www.")
}

// failLocked records a failure report: the lease returns to the pool and the pair is counted once.
func (s *Service) failLocked(l *lease, now time.Time) SubmitResult {
	e := s.pool[l.url]
	s.consume(l, "", true)
	if e != nil {
		s.noteFailure(e, pair{l.worker, l.net}, true, now)
	}
	s.metrics.submissions.add("released")
	return SubmitResult{Verdict: "released", Agreeing: []string{}}
}

// consume moves a lease to the consumed set for the rest of its TTL.
func (s *Service) consume(l *lease, hash string, failure bool) {
	s.dropLease(l.id)
	s.consumed[l.id] = &consumedLease{url: l.url, worker: l.worker, net: l.net, hash: hash, failure: failure, until: l.expiry}
}
