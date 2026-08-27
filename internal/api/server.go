// Package api exposes the TSDB engine over HTTP and embeds its demonstration UI.
package api

import (
	"context"
	"embed"
	"io/fs"
	"net/http"
	"time"

	"tsdb/internal/ingest"
	"tsdb/internal/query"
	"tsdb/internal/storage"
)

//go:embed web/*
var webFiles embed.FS

// Server owns routing and HTTP lifecycle.
type Server struct {
	engine *storage.Engine
	http   *http.Server
	router http.Handler
}

// NewServer wires storage services to all API and static routes.
func NewServer(engine *storage.Engine, address string) *Server {
	handler := &handler{
		engine: engine,
		writer: ingest.NewWriter(engine),
		query:  query.NewExecutor(engine),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/write", handler.write)
	mux.HandleFunc("/api/v1/query_range", handler.queryRange)
	mux.HandleFunc("/api/v1/query", handler.queryInstant)
	mux.HandleFunc("/api/v1/metrics", handler.metrics)
	mux.HandleFunc("/api/v1/tags", handler.tags)
	mux.HandleFunc("/api/v1/status", handler.status)
	mux.HandleFunc("/api/v1/health", handler.health)
	mux.HandleFunc("/api/v1/ready", handler.ready)
	assets, _ := fs.Sub(webFiles, "web")
	static := http.FileServer(http.FS(assets))
	mux.Handle("/static/", http.StripPrefix("/static/", static))
	mux.HandleFunc("/", func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/" {
			writeError(response, http.StatusNotFound, "not_found", "endpoint not found")
			return
		}
		if request.Method != http.MethodGet {
			methodNotAllowed(response)
			return
		}
		raw, err := webFiles.ReadFile("web/index.html")
		if err != nil {
			writeError(response, http.StatusInternalServerError, "internal_error", "frontend unavailable")
			return
		}
		response.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = response.Write(raw)
	})
	router := Logging(Recover(ReadyGate(engine, mux)))
	return &Server{
		engine: engine,
		router: router,
		http:   &http.Server{Addr: address, Handler: router, ReadHeaderTimeout: 5 * time.Second},
	}
}

// Handler returns the complete router for tests and embedding.
func (s *Server) Handler() http.Handler { return s.router }

// ListenAndServe starts serving until shutdown.
func (s *Server) ListenAndServe() error { return s.http.ListenAndServe() }

// Shutdown gracefully stops the HTTP listener.
func (s *Server) Shutdown(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			err = nil
		}
	}()
	return s.http.Shutdown(ctx)
}
