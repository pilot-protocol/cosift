package v1

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Decode reads r's JSON body, at most limit bytes, strictly into dst. A value
// of the wrong JSON type is invalid_body, like a syntax error or trailing data.
// When the body is refused Decode writes the error and returns false.
func Decode(w http.ResponseWriter, r *http.Request, dst any, limit int64) bool {
	if e, ok := decode(w, r, dst, limit); !ok {
		WriteError(w, e)
		return false
	}
	return true
}

func decode(w http.ResponseWriter, r *http.Request, dst any, limit int64) (Error, bool) {
	if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mt != "application/json" {
		return UnsupportedMediaType(), false
	}
	b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return BodyTooLarge(), false
		}
		return InvalidBody(), false
	}
	if t := bytes.TrimLeft(b, " \t\r\n"); len(t) == 0 || t[0] != '{' || !utf8.Valid(b) {
		return InvalidBody(), false
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		if quoted, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
			if name, err := strconv.Unquote(quoted); err == nil {
				return UnknownField(name), false
			}
		}
		return InvalidBody(), false
	}
	if _, err := dec.Token(); err != io.EOF {
		return InvalidBody(), false
	}
	return Error{}, true
}
