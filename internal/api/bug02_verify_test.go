package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"tsdb/internal/config"
	"tsdb/internal/model"
	"tsdb/internal/storage"
)

func TestBug02CanceledRequestDoesNotPoisonNextQuery(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.MaintenanceInterval = time.Hour
	engine, err := storage.NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })

	now := time.Now().UnixMilli()
	series := engine.RegisterSeries("context.clean", map[string]string{"host": "new-client"})
	if err := engine.AddPoints(series, []model.Point{{Ts: now, Value: 42}}); err != nil {
		t.Fatal(err)
	}
	router := NewServer(engine, ":0").Handler()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	body := fmt.Sprintf(`{"metric":"canceled.write","tags":{"host":"old-client"},"points":[{"ts":%d,"value":1}]}`, now)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/write", strings.NewReader(body)).WithContext(canceled)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	queryURL := fmt.Sprintf("/api/v1/query_range?metric=context.clean&tags=host=new-client&start=%d&end=%d", now-1, now+1)
	response = httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, queryURL, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("query status=%d body=%s", response.Code, response.Body.String())
	}
	var decoded struct {
		Data struct {
			Result []json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Data.Result) != 1 {
		t.Fatalf("unrelated query returned %d series after cancellation: %s", len(decoded.Data.Result), response.Body.String())
	}
}
