package articles

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

var (
	qualityTiers = set("strong", "ok", "thin")
	verticals    = set("dev-docs", "technology-news", "research", "other")
	sensClasses  = set("none", "named_private_person", "medical_advice", "legal_advice", "financial_advice", "allegation", "harmful_adult")
	sha64        = regexp.MustCompile(`^[0-9a-f]{64}$`)
	markerRE     = regexp.MustCompile(`\[([0-9]+)\]`)
)

// putBody is the article PUT; engine-owned fields are accepted and ignored.
type putBody struct {
	Status          *string      `json:"status"`
	ExpectedVersion *int         `json:"expected_version"`
	Title           string       `json:"title"`
	Lead            string       `json:"lead"`
	BodyMD          string       `json:"body_md"`
	Citations       []Citation   `json:"citations"`
	QualityTier     string       `json:"quality_tier"`
	Vertical        string       `json:"vertical"`
	Sensitivity     *Sensitivity `json:"sensitivity"`
	Aliases         []string     `json:"aliases"`
	TopicIDs        []string     `json:"topic_ids"`
	ClusterIDs      []string     `json:"cluster_ids"`
	Models          *Models      `json:"models"`
	Build           *Build       `json:"build"`
	AIGenerated     *bool        `json:"ai_generated"`

	Schema          json.RawMessage `json:"schema"`
	ID              json.RawMessage `json:"id"`
	Slug            json.RawMessage `json:"slug"`
	BaseSlug        json.RawMessage `json:"base_slug"`
	Promoted        json.RawMessage `json:"promoted"`
	Version         json.RawMessage `json:"version"`
	ContentSHA256   json.RawMessage `json:"content_sha256"`
	CreatedAt       json.RawMessage `json:"created_at"`
	UpdatedAt       json.RawMessage `json:"updated_at"`
	StatusChangedAt json.RawMessage `json:"status_changed_at"`
	Moderation      json.RawMessage `json:"moderation"`
	Prelive         json.RawMessage `json:"prelive"`
	Residue         json.RawMessage `json:"residue"`
	ReasonCode      json.RawMessage `json:"reason_code"`
	TombstonedAt    json.RawMessage `json:"tombstoned_at"`
}

// stubBody is the stub PUT, decoded strictly.
type stubBody struct {
	Status   string    `json:"status"`
	Title    string    `json:"title"`
	Vertical string    `json:"vertical"`
	TopicIDs *[]string `json:"topic_ids"`
}

// plainText refuses Cc, Cf, Co, Cs, Zl and Zp; allowed lists the Cc exceptions.
func plainText(s string, allowed string) bool {
	for _, r := range s {
		if strings.ContainsRune(allowed, r) {
			continue
		}
		if unicode.In(r, unicode.Cc, unicode.Cf, unicode.Co, unicode.Cs, unicode.Zl, unicode.Zp) {
			return false
		}
	}
	return true
}

func runes(s string) int { return utf8.RuneCountInString(s) }

func checkText(field, s string, lo, hi int) *v1.Error {
	if n := runes(s); n < lo || n > hi {
		return fail(v1.InvalidField(field, "length"))
	}
	if !plainText(s, "") {
		return fail(v1.InvalidField(field, "char_class"))
	}
	return nil
}

func checkTitleField(field, s string, alias bool) *v1.Error {
	if rule := CheckTitle(s, alias); rule != "" {
		return fail(v1.InvalidField(field, rule))
	}
	return nil
}

func checkTopics(field string, ids []string, limit int) *v1.Error {
	if len(ids) > limit {
		return fail(v1.InvalidField(field, "too_many"))
	}
	for i, t := range ids {
		if !topicPattern.MatchString(t) {
			return fail(v1.InvalidField(fmt.Sprintf("%s[%d]", field, i), "pattern"))
		}
	}
	return nil
}

func validateArticle(b *putBody) *v1.Error {
	switch {
	case b.Status == nil:
		return fail(v1.InvalidField("status", "required"))
	case *b.Status != StatusPublished && *b.Status != StatusHeld:
		return fail(v1.InvalidField("status", "enum"))
	}
	if e := checkTitleField("title", b.Title, false); e != nil {
		return e
	}
	if e := checkText("lead", b.Lead, 1, 600); e != nil {
		return e
	}
	if e := validateCitations(b.Citations); e != nil {
		return e
	}
	if e := validateBody(b); e != nil {
		return e
	}
	if !qualityTiers[b.QualityTier] {
		return fail(v1.InvalidField("quality_tier", "enum"))
	}
	if !verticals[b.Vertical] {
		return fail(v1.InvalidField("vertical", "enum"))
	}
	switch {
	case b.Sensitivity == nil:
		return fail(v1.InvalidField("sensitivity", "required"))
	case !sensClasses[b.Sensitivity.Class]:
		return fail(v1.InvalidField("sensitivity.class", "enum"))
	}
	if e := checkText("sensitivity.reason", b.Sensitivity.Reason, 0, 200); e != nil {
		return e
	}
	if b.AIGenerated != nil && !*b.AIGenerated {
		return fail(v1.InvalidField("ai_generated", "must_be_true"))
	}
	if len(b.Aliases) > 32 {
		return fail(v1.InvalidField("aliases", "too_many"))
	}
	for i, a := range b.Aliases {
		if e := checkTitleField(fmt.Sprintf("aliases[%d]", i), a, true); e != nil {
			return e
		}
	}
	if e := checkTopics("topic_ids", b.TopicIDs, 256); e != nil {
		return e
	}
	if len(b.ClusterIDs) > 64 {
		return fail(v1.InvalidField("cluster_ids", "too_many"))
	}
	for i, c := range b.ClusterIDs {
		if !clusterPattern.MatchString(c) {
			return fail(v1.InvalidField(fmt.Sprintf("cluster_ids[%d]", i), "pattern"))
		}
	}
	if b.Models == nil {
		return fail(v1.InvalidField("models", "required"))
	}
	for _, m := range [][2]string{{"models.triage", b.Models.Triage}, {"models.writer", b.Models.Writer}} {
		if e := checkText(m[0], m[1], 0, 100); e != nil {
			return e
		}
	}
	if e := validateBuild(b.Build); e != nil {
		return e
	}
	if b.Sensitivity.Class != "none" && *b.Status == StatusPublished {
		return fail(v1.InvalidField("status", "sensitive_must_hold"))
	}
	return nil
}

func validateBody(b *putBody) *v1.Error {
	body := b.BodyMD
	if len(body) > 60000 {
		return fail(v1.InvalidField("body_md", "length"))
	}
	if !plainText(body, "\n\t") {
		return fail(v1.InvalidField("body_md", "char_class"))
	}
	keyFacts := false
	for _, line := range strings.Split(body, "\n") {
		if line == "## Key facts" {
			keyFacts = true
		}
		if strings.HasPrefix(line, "# ") {
			return fail(v1.InvalidField("body_md", "h1"))
		}
	}
	if !keyFacts {
		return fail(v1.InvalidField("body_md", "key_facts"))
	}
	n := len(b.Citations)
	cited := make([]bool, n+1)
	for _, m := range markerRE.FindAllStringSubmatch(body, -1) {
		k, err := parseSmallInt(m[1])
		if err != nil || k < 1 || k > n {
			return fail(v1.InvalidField("body_md", "citation_marker"))
		}
		cited[k] = true
	}
	for k := 1; k <= n; k++ {
		if !cited[k] {
			return fail(v1.InvalidField(fmt.Sprintf("citations[%d]", k-1), "unreferenced"))
		}
	}
	if words := len(strings.Fields(b.Lead)) + len(strings.Fields(body)); words < 300 || words > 2000 {
		return fail(v1.InvalidField("body_md", "word_count"))
	}
	return nil
}

func parseSmallInt(s string) (int, error) {
	if len(s) > 4 {
		return 0, fmt.Errorf("out of range")
	}
	n := 0
	for _, c := range s {
		n = n*10 + int(c-'0')
	}
	return n, nil
}

func validateCitations(cs []Citation) *v1.Error {
	if len(cs) < 1 {
		return fail(v1.InvalidField("citations", "required"))
	}
	if len(cs) > 30 {
		return fail(v1.InvalidField("citations", "too_many"))
	}
	for i, c := range cs {
		f := func(name string) string { return fmt.Sprintf("citations[%d].%s", i, name) }
		if c.N != i+1 {
			return fail(v1.InvalidField(f("n"), "sequence"))
		}
		u, err := url.Parse(c.URL)
		if len(c.URL) > 2048 || err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fail(v1.InvalidField(f("url"), "url"))
		}
		if c.Host != strings.ToLower(u.Hostname()) {
			return fail(v1.InvalidField(f("host"), "host_mismatch"))
		}
		if e := checkText(f("title"), c.Title, 0, 300); e != nil {
			return e
		}
		if e := checkText(f("quote"), c.Quote, 1, 500); e != nil {
			return e
		}
		if !sha64.MatchString(c.ContentSHA) {
			return fail(v1.InvalidField(f("content_sha"), "pattern"))
		}
	}
	return nil
}

func validateBuild(b *Build) *v1.Error {
	switch {
	case b == nil:
		return fail(v1.InvalidField("build", "required"))
	case b.SourcesConsidered < 0:
		return fail(v1.InvalidField("build.sources_considered", "range"))
	case b.SourcesUsed < 0:
		return fail(v1.InvalidField("build.sources_used", "range"))
	case b.CostUSD < 0:
		return fail(v1.InvalidField("build.cost_usd", "range"))
	}
	if len(b.RetrievalParams) > 0 {
		t := bytes.TrimSpace(b.RetrievalParams)
		if len(t) > 2048 {
			return fail(v1.InvalidField("build.retrieval_params", "length"))
		}
		var obj map[string]json.RawMessage
		if len(t) == 0 || t[0] != '{' || json.Unmarshal(t, &obj) != nil {
			return fail(v1.InvalidField("build.retrieval_params", "object"))
		}
		b.RetrievalParams = json.RawMessage(t)
	}
	return nil
}

// checkCorpus refuses a citation whose URL is not a corpus document.
func (s *Store) checkCorpus(ctx context.Context, cs []Citation) (*v1.Error, error) {
	if s.corpus == nil {
		return nil, fmt.Errorf("no corpus configured")
	}
	for i, c := range cs {
		ok, err := s.corpus.HasDocument(ctx, c.URL)
		if err != nil {
			return nil, err
		}
		if !ok {
			return fail(v1.InvalidField(fmt.Sprintf("citations[%d].url", i), "not_in_corpus")), nil
		}
	}
	return nil, nil
}

func validateStub(b *stubBody) *v1.Error {
	if e := checkTitleField("title", b.Title, false); e != nil {
		return e
	}
	if !verticals[b.Vertical] {
		return fail(v1.InvalidField("vertical", "enum"))
	}
	if b.TopicIDs == nil || len(*b.TopicIDs) == 0 {
		return fail(v1.InvalidField("topic_ids", "required"))
	}
	return checkTopics("topic_ids", *b.TopicIDs, 256)
}
