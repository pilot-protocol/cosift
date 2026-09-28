package articles

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
)

const (
	StatusPending    = "pending"
	StatusPublished  = "published"
	StatusHeld       = "held"
	StatusRejected   = "rejected"
	StatusTombstoned = "tombstoned"
)

type Citation struct {
	N          int    `json:"n"`
	URL        string `json:"url"`
	Title      string `json:"title"`
	Host       string `json:"host"`
	Quote      string `json:"quote"`
	ContentSHA string `json:"content_sha"`
}

type Sensitivity struct {
	Class  string `json:"class"`
	Reason string `json:"reason"`
}

type Models struct {
	Triage string `json:"triage"`
	Writer string `json:"writer"`
}

type Build struct {
	SourcesConsidered int             `json:"sources_considered"`
	SourcesUsed       int             `json:"sources_used"`
	RetrievalParams   json.RawMessage `json:"retrieval_params"`
	CostUSD           float64         `json:"cost_usd"`
	DenseDegraded     bool            `json:"dense_degraded"`
}

type Moderation struct {
	Action     string `json:"action"`
	By         string `json:"by"`
	ReasonCode string `json:"reason_code,omitempty"`
	At         string `json:"at"`
}

// Record is an 'R a' value. Its stored shape depends on whether it is an
// article, a stub or a residue; records are never mutated once published
// to the index.
type Record struct {
	Schema          int         `json:"schema"`
	ID              string      `json:"id"`
	Slug            string      `json:"slug"`
	BaseSlug        string      `json:"base_slug"`
	Title           string      `json:"title"`
	Lead            string      `json:"lead"`
	BodyMD          string      `json:"body_md"`
	Citations       []Citation  `json:"citations"`
	QualityTier     string      `json:"quality_tier"`
	Promoted        bool        `json:"promoted"`
	Vertical        string      `json:"vertical"`
	Status          string      `json:"status"`
	Sensitivity     Sensitivity `json:"sensitivity"`
	AIGenerated     bool        `json:"ai_generated"`
	Aliases         []string    `json:"aliases"`
	TopicIDs        []string    `json:"topic_ids"`
	ClusterIDs      []string    `json:"cluster_ids"`
	Models          Models      `json:"models"`
	Build           Build       `json:"build"`
	Version         int         `json:"version"`
	ContentSHA256   string      `json:"content_sha256"`
	CreatedAt       string      `json:"created_at"`
	UpdatedAt       string      `json:"updated_at"`
	StatusChangedAt string      `json:"status_changed_at"`
	Moderation      *Moderation `json:"moderation"`
	Prelive         bool        `json:"prelive"`

	Residue      bool   `json:"residue"`
	ReasonCode   string `json:"reason_code"`
	TombstonedAt string `json:"tombstoned_at"`
}

type stubJSON struct {
	Schema          int      `json:"schema"`
	ID              string   `json:"id"`
	Slug            string   `json:"slug"`
	BaseSlug        string   `json:"base_slug"`
	Title           string   `json:"title"`
	Vertical        string   `json:"vertical"`
	Status          string   `json:"status"`
	TopicIDs        []string `json:"topic_ids"`
	Version         int      `json:"version"`
	ContentSHA256   string   `json:"content_sha256"`
	CreatedAt       string   `json:"created_at"`
	UpdatedAt       string   `json:"updated_at"`
	StatusChangedAt string   `json:"status_changed_at"`
	Prelive         bool     `json:"prelive"`
}

type residueJSON struct {
	Schema          int      `json:"schema"`
	ID              string   `json:"id"`
	Slug            string   `json:"slug"`
	BaseSlug        string   `json:"base_slug"`
	Status          string   `json:"status"`
	Residue         bool     `json:"residue"`
	ReasonCode      string   `json:"reason_code"`
	TopicIDs        []string `json:"topic_ids"`
	Prelive         bool     `json:"prelive"`
	TombstonedAt    string   `json:"tombstoned_at,omitempty"`
	StatusChangedAt string   `json:"status_changed_at"`
}

type articleJSON struct {
	Schema          int         `json:"schema"`
	ID              string      `json:"id"`
	Slug            string      `json:"slug"`
	BaseSlug        string      `json:"base_slug"`
	Title           string      `json:"title"`
	Lead            string      `json:"lead"`
	BodyMD          string      `json:"body_md"`
	Citations       []Citation  `json:"citations"`
	QualityTier     string      `json:"quality_tier"`
	Promoted        bool        `json:"promoted"`
	Vertical        string      `json:"vertical"`
	Status          string      `json:"status"`
	Sensitivity     Sensitivity `json:"sensitivity"`
	AIGenerated     bool        `json:"ai_generated"`
	Aliases         []string    `json:"aliases"`
	TopicIDs        []string    `json:"topic_ids"`
	ClusterIDs      []string    `json:"cluster_ids"`
	Models          Models      `json:"models"`
	Build           Build       `json:"build"`
	Version         int         `json:"version"`
	ContentSHA256   string      `json:"content_sha256"`
	CreatedAt       string      `json:"created_at"`
	UpdatedAt       string      `json:"updated_at"`
	StatusChangedAt string      `json:"status_changed_at"`
	Moderation      *Moderation `json:"moderation"`
	Prelive         bool        `json:"prelive"`
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

func (r *Record) MarshalJSON() ([]byte, error) {
	switch {
	case r.Residue:
		return json.Marshal(residueJSON{r.Schema, r.ID, r.Slug, r.BaseSlug, r.Status, true, r.ReasonCode,
			nonNil(r.TopicIDs), r.Prelive, r.TombstonedAt, r.StatusChangedAt})
	case r.Status == StatusPending:
		return json.Marshal(stubJSON{r.Schema, r.ID, r.Slug, r.BaseSlug, r.Title, r.Vertical, r.Status,
			nonNil(r.TopicIDs), r.Version, r.ContentSHA256, r.CreatedAt, r.UpdatedAt, r.StatusChangedAt, r.Prelive})
	}
	b := r.Build
	if len(b.RetrievalParams) == 0 {
		b.RetrievalParams = json.RawMessage("{}")
	}
	return json.Marshal(articleJSON{r.Schema, r.ID, r.Slug, r.BaseSlug, r.Title, r.Lead, r.BodyMD,
		nonNil(r.Citations), r.QualityTier, r.Promoted, r.Vertical, r.Status, r.Sensitivity, r.AIGenerated,
		nonNil(r.Aliases), nonNil(r.TopicIDs), nonNil(r.ClusterIDs), r.Models, b, r.Version, r.ContentSHA256,
		r.CreatedAt, r.UpdatedAt, r.StatusChangedAt, r.Moderation, r.Prelive})
}

func decodeRecord(b []byte) (*Record, error) {
	var r Record
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (r *Record) clone() *Record {
	c := *r
	c.Citations = slices.Clone(r.Citations)
	c.Aliases = slices.Clone(r.Aliases)
	c.TopicIDs = slices.Clone(r.TopicIDs)
	c.ClusterIDs = slices.Clone(r.ClusterIDs)
	if r.Moderation != nil {
		m := *r.Moderation
		c.Moderation = &m
	}
	return &c
}

func (r *Record) setStatus(status, at string) {
	r.Status = status
	r.StatusChangedAt = at
	r.Promoted = status == StatusPublished && r.QualityTier == "strong"
}

// toResidue reduces r to what a takedown keeps.
func (r *Record) toResidue(status, reason, at string) *Record {
	res := &Record{Schema: 1, ID: r.ID, Slug: r.Slug, BaseSlug: r.BaseSlug, Status: status, Residue: true,
		ReasonCode: reason, TopicIDs: slices.Clone(r.TopicIDs), Prelive: r.Prelive, StatusChangedAt: r.StatusChangedAt}
	if status == StatusTombstoned {
		res.TombstonedAt = at
		res.StatusChangedAt = at
	} else if r.Residue {
		res.TombstonedAt = r.TombstonedAt
	}
	return res
}

// blocksTitle reports whether r refuses re-creation under its base slug.
func (r *Record) blocksTitle() bool {
	return r.Status == StatusHeld || r.Status == StatusRejected || r.Status == StatusTombstoned
}

func contentHash(r *Record) string {
	aliases, topics := slices.Clone(r.Aliases), slices.Clone(r.TopicIDs)
	slices.Sort(aliases)
	slices.Sort(topics)
	b, _ := json.Marshal(struct {
		Title       string      `json:"title"`
		Lead        string      `json:"lead"`
		BodyMD      string      `json:"body_md"`
		Citations   []Citation  `json:"citations"`
		QualityTier string      `json:"quality_tier"`
		Vertical    string      `json:"vertical"`
		Sensitivity Sensitivity `json:"sensitivity"`
		Aliases     []string    `json:"aliases"`
		TopicIDs    []string    `json:"topic_ids"`
	}{r.Title, r.Lead, r.BodyMD, nonNil(r.Citations), r.QualityTier, r.Vertical, r.Sensitivity, nonNil(aliases), nonNil(topics)})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

type publicView struct {
	ID          string     `json:"id"`
	Slug        string     `json:"slug"`
	Title       string     `json:"title"`
	Lead        string     `json:"lead"`
	BodyMD      string     `json:"body_md"`
	Citations   []Citation `json:"citations"`
	QualityTier string     `json:"quality_tier"`
	Promoted    bool       `json:"promoted"`
	Vertical    string     `json:"vertical"`
	Status      string     `json:"status"`
	AIGenerated bool       `json:"ai_generated"`
	Version     int        `json:"version"`
	CreatedAt   string     `json:"created_at"`
	UpdatedAt   string     `json:"updated_at"`
}

func (r *Record) public(promoted bool) publicView {
	return publicView{r.ID, r.Slug, r.Title, r.Lead, r.BodyMD, nonNil(r.Citations), r.QualityTier, promoted,
		r.Vertical, r.Status, true, r.Version, r.CreatedAt, r.UpdatedAt}
}

type stubView struct {
	ID        string `json:"id"`
	Slug      string `json:"slug"`
	Title     string `json:"title"`
	Vertical  string `json:"vertical"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
}

func (r *Record) stub() stubView {
	return stubView{r.ID, r.Slug, r.Title, r.Vertical, r.Status, r.CreatedAt}
}

type writeView struct {
	ID            string `json:"id"`
	Slug          string `json:"slug"`
	Status        string `json:"status"`
	Version       int    `json:"version"`
	Prelive       bool   `json:"prelive"`
	ContentSHA256 string `json:"content_sha256"`
	CreatedAt     string `json:"created_at"`
	UpdatedAt     string `json:"updated_at"`
}

func (r *Record) written() writeView {
	return writeView{r.ID, r.Slug, r.Status, r.Version, r.Prelive, r.ContentSHA256, r.CreatedAt, r.UpdatedAt}
}

// full is the read_all projection: every stored field plus the read-side extras.
func full(r *Record, promoted bool, readers7d int, byDay map[string]int, hasFingerprint bool) json.RawMessage {
	b, _ := r.MarshalJSON()
	var m map[string]json.RawMessage
	_ = json.Unmarshal(b, &m)
	if _, ok := m["promoted"]; ok {
		m["promoted"], _ = json.Marshal(promoted)
	}
	m["readers_7d"], _ = json.Marshal(readers7d)
	if byDay == nil {
		byDay = map[string]int{}
	}
	m["readers_by_day"], _ = json.Marshal(byDay)
	m["has_fingerprint"], _ = json.Marshal(hasFingerprint)
	out, _ := json.Marshal(m)
	return out
}
