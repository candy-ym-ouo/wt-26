package query

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"tsdb/internal/config"
	"tsdb/internal/model"
	"tsdb/internal/storage"
)

// TestConcurrentQueryIsolation exercises the scenario from the bug report:
// multiple label sets under one metric, with a high-frequency mix of range,
// instant, and tag-filter queries running concurrently. It asserts that
//
//   - no read-only query leaves __name__ (or any response-only label) behind
//     in the index, so later tag filters are never polluted, and
//   - every response carries a complete, independently-owned label map, so
//     concurrent results never bleed into one another (no race, no crash).
func TestConcurrentQueryIsolation(t *testing.T) {
	engine := newIsolationEngine(t)
	exec := NewExecutor(engine)

	now := time.Now().UnixMilli()
	req := model.QueryReq{Metric: "cpu.usage", Start: now - 2*60*1000, End: now + 1}

	const workers = 48
	const iterations = 200

	var wg sync.WaitGroup
	results := make([][]model.MatrixSeries, workers)
	var mu sync.Mutex
	failures := 0

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			local := make([]model.MatrixSeries, 0, iterations)
			for i := 0; i < iterations; i++ {
				switch i % 3 {
				case 0:
					matrix := exec.Range(req)
					local = append(local, matrix.Result...)
				case 1:
					exec.Instant(req, true)
				case 2:
					exec.Instant(req, false)
				}
			}
			mu.Lock()
			results[id] = local
			mu.Unlock()
		}(w)
	}
	wg.Wait()

	// Every returned series must expose a complete, owned label set: the
	// original series tags plus exactly one synthesized __name__ entry.
	for _, seriesList := range results {
		for _, series := range seriesList {
			if got := series.Metric["__name__"]; got != "cpu.usage" {
				mu.Lock()
				failures++
				mu.Unlock()
				t.Errorf("missing/wrong __name__ in result: %#v", series.Metric)
				break
			}
			if _, leaked := series.Metric["__name__"]; !leaked {
				continue
			}
			for key := range series.Metric {
				if key != "__name__" && key != "host" && key != "region" {
					t.Errorf("unexpected label %q in result: %#v", key, series.Metric)
				}
			}
		}
	}

	// After thousands of concurrent read-only queries, the index must still
	// report exactly the original label keys — never __name__.
	tags := engine.Tags("cpu.usage")
	if _, leaked := tags["__name__"]; leaked {
		t.Fatalf("index tags polluted after concurrent reads: %#v", tags)
	}
	wantKeys := map[string]bool{"host": true, "region": true}
	if len(tags) != len(wantKeys) {
		t.Fatalf("index tag keys changed: %#v", tags)
	}
	for key := range wantKeys {
		if len(tags[key]) == 0 {
			t.Fatalf("index lost label values for %q: %#v", key, tags)
		}
	}

	// A follow-up filtered query must still match exactly the series whose
	// host tag equals the filter — proving the filter state is uncorrupted.
	filtered := engine.FindSeries("cpu.usage", map[string]string{"region": "us-east"}, 0)
	for _, series := range filtered {
		if series.Tags["region"] != "us-east" {
			t.Fatalf("filter returned mismatched series: %#v", series)
		}
	}
	if len(filtered) == 0 {
		t.Fatal("filtered query returned no series after concurrent reads")
	}

	if failures > 0 {
		t.Fatalf("%d responses had incomplete label sets", failures)
	}
}

func newIsolationEngine(t *testing.T) *storage.Engine {
	t.Helper()
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.Retention = 48 * time.Hour
	cfg.MaintenanceInterval = time.Hour
	engine, err := storage.NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	now := time.Now().UnixMilli()
	regions := []string{"us-east", "us-west", "eu-central"}
	for r, region := range regions {
		for h := 0; h < 2; h++ {
			series := engine.RegisterSeries("cpu.usage", map[string]string{
				"region": region,
				"host":   fmt.Sprintf("%s-web-%02d", region, h),
			})
			if err := engine.AddPoints(series, []model.Point{
				{Ts: now - 60_000, Value: float64(r*10 + h)},
				{Ts: now, Value: float64(r*10 + h + 1)},
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	return engine
}
