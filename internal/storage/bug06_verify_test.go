package storage

import (
	"runtime"
	"testing"
	"time"

	"tsdb/internal/config"
	"tsdb/internal/model"
)

func TestBug06GoroutinesConvergeAfterEngineClose(t *testing.T) {
	baseline := runtime.NumGoroutine()
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.ShardDuration = time.Hour
	cfg.Retention = 48 * time.Hour
	cfg.MaintenanceInterval = time.Hour
	cfg.FlushBytes = 1024
	cfg.WALSync = false

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	series := engine.RegisterSeries("goroutine.convergence", nil)
	start := time.Now().Truncate(time.Hour).UnixMilli()
	for point := 0; point < 1200; point++ {
		if err := engine.AddPoints(series, []model.Point{{Ts: start + int64(point), Value: float64(point)}}); err != nil {
			t.Fatal(err)
		}
	}
	engine.maintain(time.Now().Add(2 * time.Hour).UnixMilli())
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if runtime.NumGoroutine() <= baseline+5 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("goroutines did not converge: baseline=%d current=%d", baseline, runtime.NumGoroutine())
}
