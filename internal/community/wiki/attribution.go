package wiki

import (
	"context"
	"regexp"
)

var (
	cidPattern    = regexp.MustCompile(`^[0-9a-f]{16}$`)
	handlePattern = regexp.MustCompile(`^[a-z0-9_-]{3,24}$`)
)

const maxContributors = 64

// contributorCIDs reads the contributors member: at most 64 16-hex strings, else none.
func contributorCIDs(raw []any) []string {
	if len(raw) > maxContributors {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		s, ok := v.(string)
		if !ok || !cidPattern.MatchString(s) {
			return nil
		}
		out = append(out, s)
	}
	return out
}

// displayName is "@handle" for a live contributor with a well-formed handle, else the anonymous label.
func displayName(h Handle, known bool) string {
	if !known || h.Banned || !handlePattern.MatchString(h.Name) {
		return anonymous
	}
	return "@" + h.Name
}

// names returns one display name per cid, in order; degraded means the source failed, so the page should be served only briefly.
func (w *Wiki) names(cids []string) (out []string, degraded bool) {
	now := w.now()
	resolved := map[string]string{}
	var missing []string
	for _, cid := range cids {
		if _, seen := resolved[cid]; seen {
			continue
		}
		if name, ok := w.handles.get(cid, now); ok {
			resolved[cid] = name
			continue
		}
		resolved[cid] = anonymous
		missing = append(missing, cid)
	}
	if len(missing) > 0 && w.cfg.Handles != nil {
		ctx, cancel := context.WithTimeout(context.Background(), handleTimeout)
		found, err := w.cfg.Handles.Handles(ctx, missing)
		cancel()
		if err != nil {
			w.logOnce("handles", "wiki: WARN contributor handles unavailable; names shown as a community member")
			degraded = true
		} else {
			fresh := map[string]string{}
			for _, cid := range missing {
				h, known := found[cid]
				resolved[cid] = displayName(h, known)
				fresh[cid] = resolved[cid]
			}
			w.handles.put(fresh, now)
		}
	}
	for _, cid := range cids {
		out = append(out, resolved[cid])
	}
	return out, degraded
}
