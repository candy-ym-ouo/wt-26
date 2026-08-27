package ingest

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"tsdb/internal/config"
	"tsdb/internal/model"
	"tsdb/internal/storage"
)

func TestBug01ConcurrentBatchesRemainIsolated(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.MaintenanceInterval = time.Hour
	cfg.WALSync = false
	engine, err := storage.NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })

	writer := NewWriter(engine)
	const workers = 32
	const pointsPerBatch = 64
	base := time.Now().UnixMilli() - 10_000
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			points := make([]model.Point, pointsPerBatch)
			for point := range points {
				points[point] = model.Point{Ts: base + int64(point), Value: float64(id*1000 + point)}
			}
			_, err := writer.Write(model.IngestBatch{
				Metric: fmt.Sprintf("batch.%02d", id),
				Tags:   map[string]string{"worker": fmt.Sprintf("%02d", id)},
				Points: points,
			})
			errs <- err
		}(worker)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent write failed: %v", err)
		}
	}

	for worker := 0; worker < workers; worker++ {
		metric := fmt.Sprintf("batch.%02d", worker)
		series := engine.FindSeries(metric, map[string]string{"worker": fmt.Sprintf("%02d", worker)}, 0)
		if len(series) != 1 {
			t.Fatalf("%s resolved to %d series", metric, len(series))
		}
		points := engine.QueryPoints(series[0].ID, base, base+pointsPerBatch)
		if len(points) != pointsPerBatch {
			t.Fatalf("%s has %d points, want %d", metric, len(points), pointsPerBatch)
		}
		for point, got := range points {
			want := float64(worker*1000 + point)
			if got.Value != want {
				t.Fatalf("%s point %d value=%v want=%v", metric, point, got.Value, want)
			}
		}
	}
}
