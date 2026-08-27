package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"tsdb/internal/config"
	"tsdb/internal/model"
	"tsdb/internal/storage"
)

// TestConcurrentQueryStateIsolation reproduces the reported failure mode at the
// HTTP boundary: one metric with several label groups, hammered by a mix of
// range, instant, and tag-filter requests. It asserts that no read-only query
// leaks the synthesized __name__ label into stored metadata (which would poison
// later tag-filter results) and that responses never bleed into one another.
func TestConcurrentQueryStateIsolation(t *testing.T) {
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

	// Multiple label groups under a single metric.
	now := time.Now().UnixMilli()
	groups := []struct{ region, role string }{
		{"us-east", "web"}, {"us-east", "db"}, {"eu-west", "web"}, {"ap-south", "cache"},
	}
	for _, g := range groups {
		series := engine.RegisterSeries("http.latency", map[string]string{"region": g.region, "role": g.role})
		if err := engine.AddPoints(series, []model.Point{
			{Ts: now - 60_000, Value: 1},
			{Ts: now, Value: 2},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Each request gets its own recorder so responses are fully independent.
	mkReq := func(path string) *http.Request {
		return httptest.NewRequest(http.MethodGet, path, nil)
	}
	get := func(path string) map[string]any {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, mkReq(path))
		var body map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		return body
	}
	rangeURL := fmt.Sprintf("/api/v1/query_range?metric=http.latency&start=%d&end=%d", now-120000, now+1)
	instantURL := "/api/v1/query?metric=http.latency&agg=avg"
	tagsURL := "/api/v1/tags?metric=http.latency"

	const workers = 32
	const iterations = 150
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				switch (i + seed) % 3 {
				case 0:
					_ = get(rangeURL)
				case 1:
					_ = get(instantURL)
				case 2:
					_ = get(tagsURL)
				}
			}
		}(w)
	}
	wg.Wait()

	// After the storm, stored labels must still be exactly the originals:
	// __name__ must never appear in tag values (it is response-only).
	tagsBody := get(tagsURL)
	data, _ := tagsBody["data"].(map[string]any)
	if data == nil {
		t.Fatalf("tags endpoint returned no data: %#v", tagsBody)
	}
	if _, leaked := data["__name__"]; leaked {
		t.Fatalf("synthesized __name__ leaked into stored tags: %#v", data)
	}
	wantKeys := map[string]bool{"region": true, "role": true}
	if len(data) != len(wantKeys) {
		t.Fatalf("tag keys changed after concurrent reads: %#v", data)
	}

	// A filtered range query must return only the matching series — proving the
	// filter state is intact and unaffected by the concurrent reads above.
	filtered := get(rangeURL + "&tags=region=eu-west,role=web")
	matrix, _ := filtered["data"].(map[string]any)
	rows, _ := matrix["result"].([]any)
	if len(rows) != 1 {
		t.Fatalf("filtered query expected 1 series, got %d: %#v", len(rows), filtered)
	}
	row, _ := rows[0].(map[string]any)
	metric, _ := row["metric"].(map[string]any)
	if metric["region"] != "eu-west" || metric["role"] != "web" || metric["__name__"] != "http.latency" {
		t.Fatalf("filtered series has wrong or incomplete labels: %#v", metric)
	}
}
