package community

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"maps"
	"mime"
	"net/http"
	"slices"
)

const submitRoute = "/v1/work/submit"

type memberKind int

const (
	memberString memberKind = iota
	memberInt
	memberObject
)

type memberRule struct {
	kind memberKind
	max  int
}

// submitMembers is the envelope a worker may send; worker and net are never among them, the app adds both.
var submitMembers = map[string]memberRule{
	"lease_id": {memberString, 64}, "final_url": {memberString, 2048}, "http_status": {memberInt, 0},
	"title": {memberString, 1000}, "text": {memberString, 512 << 10}, "lang": {memberString, 35},
	"content_hash": {memberString, 64}, "simhash": {memberString, 16}, "extract_version": {memberInt, 0},
	"artifact": {memberObject, 0},
}

type submitRefusal struct {
	status      int
	code        string
	field, rule string
}

// buildSubmit checks a worker's envelope and returns the engine's body: the same members plus the app's worker and net.
func buildSubmit(body []byte, worker, net string) ([]byte, *submitRefusal) {
	dec := json.NewDecoder(bytes.NewReader(body))
	var members map[string]json.RawMessage
	if dec.Decode(&members) != nil || members == nil || dec.Decode(new(any)) != io.EOF {
		return nil, &submitRefusal{status: http.StatusBadRequest, code: "invalid_body"}
	}
	for _, name := range slices.Sorted(maps.Keys(members)) {
		rule, known := submitMembers[name]
		if !known {
			return nil, &submitRefusal{status: http.StatusBadRequest, code: "unknown_field"}
		}
		raw := members[name]
		switch rule.kind {
		case memberString:
			var v string
			if json.Unmarshal(raw, &v) != nil {
				return nil, &submitRefusal{status: http.StatusBadRequest, code: "invalid_body"}
			}
			if len(v) > rule.max {
				return nil, &submitRefusal{http.StatusUnprocessableEntity, "invalid_field", name, "length"}
			}
		case memberInt:
			var v int
			if json.Unmarshal(raw, &v) != nil {
				return nil, &submitRefusal{status: http.StatusBadRequest, code: "invalid_body"}
			}
		case memberObject:
			var v map[string]json.RawMessage
			if json.Unmarshal(raw, &v) != nil {
				return nil, &submitRefusal{status: http.StatusBadRequest, code: "invalid_body"}
			}
		}
	}
	members["worker"], _ = json.Marshal(worker)
	members["net"], _ = json.Marshal(net)
	out, err := json.Marshal(members)
	if err != nil {
		return nil, &submitRefusal{status: http.StatusBadRequest, code: "invalid_body"}
	}
	return out, nil
}

type submitAnswer struct {
	Verdict         string   `json:"verdict"`
	Agreeing        []string `json:"agreeing"`
	Indexed         bool     `json:"indexed"`
	WindowExpiresAt string   `json:"window_expires_at"`
	Reason          string   `json:"reason"`
}

func (s *Server) submitWork(w http.ResponseWriter, r *http.Request, u User) {
	if !s.allowRoute(w, "work_submit", u.ID, workSubmitPerMinute) {
		return
	}
	ctx := r.Context()
	member, refusal := s.joinGate(ctx, u.ID)
	if refusal != nil {
		problemCode(w, refusal.status, refusalMessages[refusal.code], refusal.code, nil)
		return
	}
	if kind, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); kind != "application/json" {
		problemCode(w, http.StatusBadRequest, "invalid request body", "invalid_body", nil)
		return
	}
	raw, err := io.ReadAll(r.Body)
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		problemCode(w, http.StatusRequestEntityTooLarge, "request body too large", "body_too_large", nil)
		return
	}
	if err != nil {
		problemCode(w, http.StatusBadRequest, "invalid request body", "invalid_body", nil)
		return
	}
	worker, net, ok := s.mintIdentity(w, r, u.ID)
	if !ok {
		return
	}
	body, bad := buildSubmit(raw, worker, net)
	if bad != nil {
		extra := map[string]any{}
		if bad.field != "" {
			extra["field"], extra["rule"] = bad.field, bad.rule
		}
		problemCode(w, bad.status, workMessages[bad.code], bad.code, extra)
		return
	}
	if !s.registerWorker(w, ctx, u.ID, worker, member) {
		return
	}
	res, err := s.engine.post(ctx, submitRoute, body, workRouteTimeout)
	if err != nil {
		log.Printf("community: POST %s failed", submitRoute)
		problemCode(w, http.StatusServiceUnavailable, workUnavailable, "engine_unavailable", nil)
		return
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		s.relayWorkError(w, submitRoute, res)
		return
	}
	var in submitAnswer
	if json.NewDecoder(io.LimitReader(res.Body, workReplyBytes)).Decode(&in) != nil || !workTokenPattern.MatchString(in.Verdict) || (in.Reason != "" && !workTokenPattern.MatchString(in.Reason)) {
		log.Printf("community: POST %s returned an unreadable body", submitRoute)
		problemCode(w, http.StatusServiceUnavailable, workUnavailable, "engine_unavailable", nil)
		return
	}
	if in.Verdict != "released" {
		s.bumpFetch(member.cid, fetchSubmitted, 1)
	}
	if in.Verdict == "deferred" && in.Reason == "moderation_ceiling" {
		s.noteCeiling(ceilingWork)
	}
	out := map[string]any{"verdict": in.Verdict, "agreeing_count": len(in.Agreeing), "indexed": in.Indexed}
	if in.WindowExpiresAt != "" {
		out["window_expires_at"] = in.WindowExpiresAt
	}
	if in.Reason != "" {
		out["reason"] = in.Reason
	}
	respond(w, http.StatusOK, out)
}
