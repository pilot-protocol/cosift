package work

import "github.com/pilot-protocol/cosift/internal/crawler"

// request builds the attestation a correct worker sends for text.
func request(l LeaseGrant, worker, net, finalURL, text string) SubmitRequest {
	return requestAs(l, worker, net, finalURL, "A page", "en", text)
}

func requestAs(l LeaseGrant, worker, net, finalURL, title, lang, text string) SubmitRequest {
	sum, _ := crawler.Simhash(text)
	status, hash, simhash, version := 200, sha(text), crawler.SimhashHex(sum), crawler.ExtractVersion
	return SubmitRequest{LeaseID: l.ID, Worker: worker, Net: net, HTTPStatus: &status, FinalURL: &finalURL, Title: &title, Text: &text,
		Lang: &lang, ContentHash: &hash, Simhash: &simhash, ExtractVersion: &version}
}

// attest claims a lease for a fresh pair and submits text for it.
func (h *harness) attest(text string) (SubmitResult, string, string, error) {
	h.t.Helper()
	return h.attestAs("A page", "en", text)
}

func (h *harness) attestAs(title, lang, text string) (SubmitResult, string, string, error) {
	h.t.Helper()
	w, n := h.nextPair()
	l, ok := h.claimFor(w, n)
	if !ok {
		h.t.Fatal("no lease for a fresh pair")
	}
	res, err := h.s.Submit(requestAs(l, w, n, l.URL, title, lang, text))
	return res, w, n, err
}

func report(h *harness, l LeaseGrant, w, n string, status int) (SubmitResult, error) {
	st := status
	return h.s.Submit(SubmitRequest{LeaseID: l.ID, Worker: w, Net: n, HTTPStatus: &st})
}
