package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"tsdb/internal/config"
	"tsdb/internal/storage"
)

func TestHTTPWriteQueryAndMetadata(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.Retention = 48 * time.Hour
	cfg.MaintenanceInterval = time.Hour
	engine, err := storage.NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	router := NewServer(engine, ":0").Handler()
	now := time.Now().UnixMilli()
	body := fmt.Sprintf(`{"metric":"cpu.usage","tags":{"host":"web-01"},"points":[{"ts":%d,"value":1},{"ts":%d,"value":3}]}`, now-1000, now)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/write", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("write status=%d body=%s", response.Code, response.Body.String())
	}
	queryURL := fmt.Sprintf("/api/v1/query_range?metric=cpu.usage&tags=host=web-01&start=%d&end=%d", now-2000, now+1)
	response = httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, queryURL, nil))
	var decoded map[string]any
	_ = json.Unmarshal(response.Body.Bytes(), &decoded)
	if response.Code != http.StatusOK || decoded["status"] != "success" {
		t.Fatalf("query status=%d body=%s", response.Code, response.Body.String())
	}
	for _, path := range []string{"/", "/api/v1/metrics", "/api/v1/tags?metric=cpu.usage", "/api/v1/status", "/api/v1/health", "/api/v1/ready"} {
		response = httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", path, response.Code, response.Body.String())
		}
	}
}
