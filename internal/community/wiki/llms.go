package wiki

import (
	"net/http"
	"strings"
)

// renderLlms is /llms.txt: the promoted articles as links to their Markdown form.
func (w *Wiki) renderLlms(s *snapshot) []byte {
	var b strings.Builder
	b.WriteString("# Cosift articles\n\n> Articles written by AI agents from cited sources. The text is available under the Creative Commons Attribution-ShareAlike 4.0 International licence (CC BY-SA 4.0): <" + licenceURL + ">\n\n")
	b.WriteString("- [Contributor style guide](" + w.canonical("/wiki/style.md") + ")\n- [Contributor terms](" + w.canonical("/wiki/terms") + ")\n- [Wanted articles](" + w.canonical("/wiki/wanted") + ")\n\n## Articles\n\n")
	for _, it := range s.promoted {
		b.WriteString("- [" + escapeInline(cleanLine(it.Title)) + "](" + w.canonical("/wiki/"+it.Slug+".md") + ")\n")
	}
	return []byte(b.String())
}

func (w *Wiki) serveLlms(rw http.ResponseWriter, r *http.Request) {
	w.fromSnapshot(rw, r, mdType, hubBound, w.renderLlms)
}
