package articles

import (
	"encoding/binary"
	"regexp"
)

const (
	famArticles byte = 'R'

	subRecord      byte = 'a'
	subVersion     byte = 'v'
	subSlug        byte = 's'
	subTopic       byte = 't'
	subEmbedding   byte = 'e'
	subFingerprint byte = 'f'
	subCounters    byte = 'c'
	subMeta        byte = 'm'
)

var (
	idPattern      = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)
	slugPattern    = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	topicPattern   = regexp.MustCompile(`^[0-9a-f]{16}$`)
	clusterPattern = regexp.MustCompile(`^c[0-9]{6,}$`)
	readerPattern  = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

func validID(id string) bool { return idPattern.MatchString(id) }

func validSlug(s string) bool { return len(s) <= 80 && slugPattern.MatchString(s) }

func key(sub byte, suffix string) []byte {
	k := make([]byte, 0, 2+len(suffix))
	return append(append(k, famArticles, sub), suffix...)
}

func recordKey(id string) []byte      { return key(subRecord, id) }
func slugKey(slug string) []byte      { return key(subSlug, slug) }
func topicKey(topic string) []byte    { return key(subTopic, topic) }
func embeddingKey(id string) []byte   { return key(subEmbedding, id) }
func fingerprintKey(id string) []byte { return key(subFingerprint, id) }
func countersKey(id string) []byte    { return key(subCounters, id) }
func metaKey(name string) []byte      { return key(subMeta, name) }

func versionKey(id string, version int) []byte {
	return binary.BigEndian.AppendUint64(key(subVersion, id), uint64(version))
}

// versionPrefix bounds the R v keys of one article.
func versionPrefix(id string) (lower, upper []byte) {
	lower = key(subVersion, id)
	upper = append(key(subVersion, id), 0xff)
	return lower, upper
}

// familyBounds is the whole 'R' range.
func familyBounds() (lower, upper []byte) { return []byte{famArticles}, []byte{famArticles + 1} }

// parsedKey is a decoded 'R' key.
type parsedKey struct {
	sub     byte
	suffix  string // id, slug, topic id or meta name
	version int
}

func parseKey(k []byte) (parsedKey, bool) {
	if len(k) < 3 || k[0] != famArticles {
		return parsedKey{}, false
	}
	p := parsedKey{sub: k[1]}
	rest := k[2:]
	switch p.sub {
	case subRecord, subEmbedding, subFingerprint, subCounters:
		p.suffix = string(rest)
		return p, validID(p.suffix)
	case subVersion:
		if len(rest) != 26+8 {
			return parsedKey{}, false
		}
		p.suffix = string(rest[:26])
		p.version = int(binary.BigEndian.Uint64(rest[26:]))
		return p, validID(p.suffix)
	case subSlug:
		p.suffix = string(rest)
		return p, true
	case subTopic:
		p.suffix = string(rest)
		return p, topicPattern.MatchString(p.suffix)
	case subMeta:
		p.suffix = string(rest)
		return p, true
	}
	return parsedKey{}, false
}
