package articles

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/pilot-protocol/cosift/internal/fileowner"
)

const DefaultThresholdsPath = "/etc/cosift/articles.json"

// Thresholds are the match cut-offs; Related is also the fingerprint threshold.
// PromotionMinTier is the lowest quality tier a published article is promoted at.
type Thresholds struct {
	Covered          float64
	Related          float64
	Measured         bool
	PromotionMinTier string
}

var DefaultThresholds = Thresholds{Covered: 0.78, Related: 0.74, PromotionMinTier: "ok"}

var tierRank = map[string]int{"thin": 1, "ok": 2, "strong": 3}

// promotedAt is AR §3.4 with a configurable floor; thin is never promoted.
func promotedAt(r *Record, minTier string) bool {
	floor := max(tierRank[minTier], tierRank["ok"])
	return r.Status == StatusPublished && !r.Residue && tierRank[r.QualityTier] >= floor
}

// thresholdsOwner is the uid the file must belong to.
var thresholdsOwner uint32 = 0

// LoadThresholds reads path; an absent file yields the defaults.
func LoadThresholds(path string) (Thresholds, error) {
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return DefaultThresholds, nil
	}
	if err != nil {
		return Thresholds{}, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return Thresholds{}, err
	}
	owner, ok := fileowner.UID(fi)
	switch {
	case !fi.Mode().IsRegular():
		return Thresholds{}, errors.New("not a regular file")
	case !ok || owner != thresholdsOwner:
		return Thresholds{}, fmt.Errorf("owner must be uid %d", thresholdsOwner)
	case fi.Mode().Perm()&0o027 != 0:
		return Thresholds{}, errors.New("mode must not grant group write or any other access")
	}
	b, err := io.ReadAll(io.LimitReader(f, 64<<10))
	if err != nil {
		return Thresholds{}, err
	}
	var doc struct {
		SchemaVersion *int     `json:"schema_version"`
		ThetaCovered  *float64 `json:"theta_covered"`
		ThetaRelated  *float64 `json:"theta_related"`
		Measured      *bool    `json:"measured"`
		Promotion     *string  `json:"promotion_min_tier"`
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil {
		return Thresholds{}, fmt.Errorf("parse: %w", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return Thresholds{}, errors.New("parse: trailing data")
	}
	switch {
	case doc.SchemaVersion == nil || *doc.SchemaVersion != 1:
		return Thresholds{}, errors.New("schema_version must be 1")
	case doc.ThetaCovered == nil || doc.ThetaRelated == nil:
		return Thresholds{}, errors.New("theta_covered and theta_related are required")
	case !(0 < *doc.ThetaRelated && *doc.ThetaRelated < *doc.ThetaCovered && *doc.ThetaCovered <= 1):
		return Thresholds{}, errors.New("need 0 < theta_related < theta_covered <= 1")
	case doc.Promotion != nil && *doc.Promotion != "strong" && *doc.Promotion != "ok":
		return Thresholds{}, errors.New("promotion_min_tier must be strong or ok")
	}
	t := Thresholds{Covered: *doc.ThetaCovered, Related: *doc.ThetaRelated, PromotionMinTier: DefaultThresholds.PromotionMinTier}
	if doc.Promotion != nil {
		t.PromotionMinTier = *doc.Promotion
	}
	if doc.Measured != nil {
		t.Measured = *doc.Measured
	}
	return t, nil
}
