package articles

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
)

const (
	kindTitle = "title"
	kindLead  = "lead"
	kindAlias = "alias"
)

// row is one stored vector of an article.
type row struct {
	kind  string
	alias int
	vec   []float32
}

type storedVector struct {
	Kind string `json:"kind"`
	I    *int   `json:"i,omitempty"`
	Vec  string `json:"vec"`
}

type embeddingsJSON struct {
	V       int            `json:"v"`
	Model   string         `json:"model"`
	Dim     int            `json:"dim"`
	Vectors []storedVector `json:"vectors"`
}

type fingerprintJSON struct {
	V     int    `json:"v"`
	Model string `json:"model"`
	Dim   int    `json:"dim"`
	Vec   string `json:"vec"`
}

func encodeVec(v []float32) string {
	b := make([]byte, 4*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[4*i:], math.Float32bits(f))
	}
	return base64.StdEncoding.EncodeToString(b)
}

func decodeVec(s string, dim int) ([]float32, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	if len(b) != 4*dim {
		return nil, errors.New("vector length does not match dim")
	}
	v := make([]float32, dim)
	for i := range v {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[4*i:]))
	}
	return v, nil
}

// normalize scales v to unit length in place; a zero vector is an error.
func normalize(v []float32) error {
	var sum float64
	for _, f := range v {
		sum += float64(f) * float64(f)
	}
	if sum == 0 || math.IsNaN(sum) || math.IsInf(sum, 0) {
		return errors.New("degenerate vector")
	}
	inv := float32(1 / math.Sqrt(sum))
	for i := range v {
		v[i] *= inv
	}
	return nil
}

func dot(a, b []float32) float32 {
	var s float32
	for i := range a {
		s += a[i] * b[i]
	}
	return s
}

func encodeEmbeddings(model string, dim int, rows []row) []byte {
	out := embeddingsJSON{V: 1, Model: model, Dim: dim, Vectors: make([]storedVector, 0, len(rows))}
	for _, r := range rows {
		sv := storedVector{Kind: r.kind, Vec: encodeVec(r.vec)}
		if r.kind == kindAlias {
			i := r.alias
			sv.I = &i
		}
		out.Vectors = append(out.Vectors, sv)
	}
	b, _ := json.Marshal(out)
	return b
}

func decodeEmbeddings(b []byte) (model string, dim int, rows []row, err error) {
	var e embeddingsJSON
	if err := json.Unmarshal(b, &e); err != nil {
		return "", 0, nil, err
	}
	for _, sv := range e.Vectors {
		v, err := decodeVec(sv.Vec, e.Dim)
		if err != nil {
			return "", 0, nil, err
		}
		r := row{kind: sv.Kind, vec: v}
		if sv.I != nil {
			r.alias = *sv.I
		}
		rows = append(rows, r)
	}
	return e.Model, e.Dim, rows, nil
}

func encodeFingerprint(model string, dim int, v []float32) []byte {
	b, _ := json.Marshal(fingerprintJSON{V: 1, Model: model, Dim: dim, Vec: encodeVec(v)})
	return b
}

func decodeFingerprint(b []byte) (model string, dim int, v []float32, err error) {
	var f fingerprintJSON
	if err := json.Unmarshal(b, &f); err != nil {
		return "", 0, nil, err
	}
	v, err = decodeVec(f.Vec, f.Dim)
	return f.Model, f.Dim, v, err
}
