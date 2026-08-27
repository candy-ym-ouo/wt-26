package api

import (
	"log"
	"net/http"
	"time"

	"tsdb/internal/storage"
)

type responseRecorder struct {
	http.ResponseWriter
	status int
}

func (r *responseRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// Logging records method, path, status code, and request duration.
func Logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		start := time.Now()
		recorder := &responseRecorder{ResponseWriter: response, status: http.StatusOK}
		next.ServeHTTP(recorder, request)
		log.Printf("method=%s path=%s status=%d duration=%s", request.Method, request.URL.Path, recorder.status, time.Since(start))
	})
}

// Recover converts escaped panics to a JSON internal error.
func Recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Printf("panic: %v", recovered)
				writeError(response, http.StatusInternalServerError, "internal_error", "internal server error")
			}
		}()
		next.ServeHTTP(response, request)
	})
}

// ReadyGate prevents data traffic before engine recovery finishes.
func ReadyGate(engine *storage.Engine, next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if !engine.Ready() && request.URL.Path != "/" && request.URL.Path != "/api/v1/ready" && request.URL.Path != "/api/v1/health" {
			writeError(response, http.StatusServiceUnavailable, "unavailable", "service is not ready")
			return
		}
		next.ServeHTTP(response, request)
	})
}
