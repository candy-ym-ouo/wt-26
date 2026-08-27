package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"tsdb/internal/ingest"
	"tsdb/internal/model"
	"tsdb/internal/query"
	"tsdb/internal/storage"
)

type handler struct {
	engine *storage.Engine
	writer *ingest.Writer
	query  *query.Executor
}

func (h *handler) write(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		methodNotAllowed(response)
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, 1<<20)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var batch model.IngestBatch
	if err := decoder.Decode(&batch); err != nil {
		if errors.As(err, new(*http.MaxBytesError)) {
			writeError(response, http.StatusRequestEntityTooLarge, "too_large", "request body too large")
		} else {
			writeError(response, http.StatusBadRequest, "bad_request", "invalid JSON body")
		}
		return
	}
	if err := ensureEOF(decoder); err != nil {
		writeError(response, http.StatusBadRequest, "bad_request", "invalid JSON body")
		return
	}
	h.engine.SetRequestContext(request.Context())
	written, err := h.writer.Write(batch)
	if err != nil {
		writeAppError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"status": "success", "written": written})
}

func (h *handler) queryRange(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(response)
		return
	}
	parsed, err := query.ParseRange(request.URL.Query())
	if err != nil {
		writeAppError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"status": "success", "data": h.query.Range(parsed)})
}

func (h *handler) queryInstant(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(response)
		return
	}
	parsed, aggregate, err := query.ParseInstant(request.URL.Query(), time.Now())
	if err != nil {
		writeAppError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"status": "success", "data": h.query.Instant(parsed, aggregate)})
}

func (h *handler) metrics(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(response)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"status": "success", "data": h.engine.ListMetrics()})
}

func (h *handler) tags(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(response)
		return
	}
	metric := request.URL.Query().Get("metric")
	if metric == "" {
		writeAppError(response, model.BadRequest("metric is required"))
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"status": "success", "data": h.engine.Tags(metric)})
}

func (h *handler) status(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(response)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"status": "success", "data": h.engine.Status()})
}

func (h *handler) health(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(response)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"status": "success", "data": map[string]bool{"healthy": true}})
}

func (h *handler) ready(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(response)
		return
	}
	if !h.engine.Ready() {
		writeError(response, http.StatusServiceUnavailable, "unavailable", "service is not ready")
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"status": "success", "data": map[string]bool{"ready": true}})
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

func writeAppError(response http.ResponseWriter, err error) {
	errorType, message, status := model.ErrorInfo(err)
	writeError(response, status, errorType, message)
}

func writeError(response http.ResponseWriter, status int, errorType, message string) {
	writeJSON(response, status, map[string]any{"status": "error", "errorType": errorType, "error": message})
}

func methodNotAllowed(response http.ResponseWriter) {
	writeError(response, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
}

func ensureEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return errors.New("extra JSON value")
}
