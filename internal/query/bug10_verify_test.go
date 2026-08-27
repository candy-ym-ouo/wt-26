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

func TestBug10ConcurrentQueriesKeepLabelsIsolated(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.MaintenanceInterval = time.Hour
	cfg.WALSync = false
	engine, err := storage.NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })

	now := time.Now().UnixMilli()
	for region := 0; region < 4; region++ {
		for host := 0; host < 3; host++ {
			series := engine.RegisterSeries("query.isolation", map[string]string{
				"region": fmt.Sprintf("region-%d", region),
				"host":   fmt.Sprintf("host-%d", host),
			})
			if err := engine.AddPoints(series, []model.Point{{Ts: now - 1, Value: float64(region)}, {Ts: now, Value: float64(host)}}); err != nil {
				t.Fatal(err)
			}
		}
	}

	executor := NewExecutor(engine)
	request := model.QueryReq{Metric: "query.isolation", Start: now - 10, End: now + 1, Limit: 100}
	const workers = 40
	const iterations = 120
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			<-start
			for iteration := 0; iteration < iterations; iteration++ {
				if (iteration+seed)%2 == 0 {
					result := executor.Range(request)
					for _, row := range result.Result {
						if row.Metric["__name__"] != "query.isolation" || row.Metric["region"] == "" || row.Metric["host"] == "" {
							t.Errorf("incomplete labels: %#v", row.Metric)
							return
						}
					}
				} else {
					executor.Instant(request, false)
				}
			}
		}(worker)
	}
	close(start)
	wg.Wait()

	if tags := engine.Tags("query.isolation"); tags["__name__"] != nil {
		t.Fatalf("response-only label polluted stored tags: %#v", tags)
	}
	filtered := engine.FindSeries("query.isolation", map[string]string{"region": "region-2"}, 0)
	if len(filtered) != 3 {
		t.Fatalf("filtered series count=%d want=3", len(filtered))
	}
}
