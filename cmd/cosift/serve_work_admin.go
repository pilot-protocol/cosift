package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/pilot-protocol/cosift/internal/work"
)

const workAdminBodyLimit = 4 << 10

// registerWorkAdmin adds the operator's two work routes to the :7777 mux behind the admin gate.
func (s *pebbleHTTP) registerWorkAdmin(mux *http.ServeMux, wrap func(http.HandlerFunc) http.HandlerFunc) {
	mux.HandleFunc("POST /admin/work-purge", workAdminLog(wrap(s.handleWorkPurge)))
	mux.HandleFunc("POST /admin/work-void", workAdminLog(wrap(s.handleWorkVoid)))
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// workAdminLog logs a refused or failed call with its status and nothing of the request.
func workAdminLog(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		h(rec, r)
		switch {
		case rec.status >= 500:
			log.Printf("work_admin_failed status=%d", rec.status)
		case rec.status >= 400:
			log.Printf("work_admin_refused status=%d", rec.status)
		}
	}
}

// decodeWorkAdmin reads a strict body of at most 4 KiB with no trailing data.
func decodeWorkAdmin(w http.ResponseWriter, r *http.Request, dst any) bool {
	b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, workAdminBodyLimit))
	if err != nil {
		writeProblem(w, http.StatusRequestEntityTooLarge, "request body too large")
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil || dec.More() {
		writeProblem(w, http.StatusBadRequest, "invalid body")
		return false
	}
	if _, err := dec.Token(); err != io.EOF {
		writeProblem(w, http.StatusBadRequest, "invalid body")
		return false
	}
	return true
}

func (s *pebbleHTTP) workAdminService(w http.ResponseWriter) *work.Service {
	if s.work == nil || s.work.svc == nil {
		writeProblem(w, http.StatusServiceUnavailable, "work unavailable")
		return nil
	}
	return s.work.svc
}

func workAdminError(w http.ResponseWriter, err error) {
	var we *work.Error
	if errors.As(err, &we) && we.Status < 500 {
		writeProblem(w, we.Status, we.Code)
		return
	}
	writeProblem(w, http.StatusInternalServerError, "work store error")
}

func (s *pebbleHTTP) handleWorkPurge(w http.ResponseWriter, r *http.Request) {
	svc := s.workAdminService(w)
	if svc == nil {
		return
	}
	var req struct {
		URL   string `json:"url"`
		Apply bool   `json:"apply"`
	}
	if !decodeWorkAdmin(w, r, &req) {
		return
	}
	res, err := svc.Purge(r.Context(), req.URL, req.Apply)
	if err != nil {
		workAdminError(w, err)
		return
	}
	if req.Apply {
		log.Printf("work_purge documents_deleted=%d documents_kept=%d records_deleted=%d texts_deleted=%d workers_settled=%d",
			res.DocumentsDeleted, res.DocumentsKept, res.RecordsDeleted, res.TextsDeleted, res.WorkersSettled)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		DocumentsDeleted int       `json:"documents_deleted"`
		DocumentsKept    int       `json:"documents_kept"`
		RecordsDeleted   int       `json:"records_deleted"`
		TextsDeleted     int       `json:"texts_deleted"`
		WorkersSettled   int       `json:"workers_settled"`
		ParkedUntil      time.Time `json:"parked_until"`
	}{res.DocumentsDeleted, res.DocumentsKept, res.RecordsDeleted, res.TextsDeleted, res.WorkersSettled, res.ParkedUntil})
}

func (s *pebbleHTTP) handleWorkVoid(w http.ResponseWriter, r *http.Request) {
	svc := s.workAdminService(w)
	if svc == nil {
		return
	}
	var req struct {
		Worker string `json:"worker"`
		Apply  bool   `json:"apply"`
	}
	if !decodeWorkAdmin(w, r, &req) {
		return
	}
	res, err := svc.Void(r.Context(), req.Worker, req.Apply)
	if err != nil {
		workAdminError(w, err)
		return
	}
	if req.Apply {
		log.Printf("work_void records_changed=%d attestations_dropped=%d records_deleted=%d urls_parked=%d",
			res.RecordsChanged, res.AttestationsDropped, res.RecordsDeleted, res.URLsParked)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		RecordsChanged      int `json:"records_changed"`
		AttestationsDropped int `json:"attestations_dropped"`
		RecordsDeleted      int `json:"records_deleted"`
		URLsParked          int `json:"urls_parked"`
	}{res.RecordsChanged, res.AttestationsDropped, res.RecordsDeleted, res.URLsParked})
}
