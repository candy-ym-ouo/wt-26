package storage

import (
	"runtime"
	"testing"
	"time"

	"tsdb/internal/config"
	"tsdb/internal/model"
)

// TestEngineGoroutinesConvergeAfterRounds verifies that flushes and compactions
// do not leave parked goroutines behind: after each start/write/flush/compact/close
// cycle the live goroutine count must return near its baseline instead of growing
// monotonically across rounds.
func TestEngineGoroutinesConvergeAfterRounds(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.ShardDuration = time.Hour
	cfg.Retention = 48 * time.Hour
	cfg.MaintenanceInterval = 5 * time.Millisecond
	cfg.FlushBytes = 1024
	cfg.WALSync = false

	baseline := runtime.NumGoroutine()
	const rounds = 6
	for i := 0; i < rounds; i++ {
		engine, err := NewEngine(cfg)
		if err != nil {
			t.Fatal(err)
		}
		series := engine.RegisterSeries("converge", map[string]string{"host": "h"})
		start := time.Now().Truncate(time.Hour).UnixMilli()
		// Write enough points across multiple flush thresholds so Flush fires
		// several times this round; each previous leak would add a goroutine.
		for j := 0; j < 80; j++ {
			if err := engine.AddPoints(series, []model.Point{{Ts: start + int64(j), Value: float64(j)}}); err != nil {
				t.Fatal(err)
			}
		}
		// Force the active shard through close -> compact via maintenance using a
		// future timestamp so the window is considered closed.
		engine.maintain(time.Now().Add(2 * time.Hour).UnixMilli())
		started := time.Now()
		if err := engine.Close(); err != nil {
			t.Fatal(err)
		}
		// Close must return well inside any caller deadline; before the fix parked
		// goroutines accumulated and the maintenance loop's teardown could drag.
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("close took too long: %s", elapsed)
		}
	}

	// Give any asynchronous work a moment to drain.
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()
	t.Logf("goroutines baseline=%d after=%d rounds=%d", baseline, after, rounds)

	// Each round performs several flushes and one compact; before the fix every
	// flush and compact parked a goroutine forever, so `after` grew without bound.
	// Allow a small slack for runtime/test harness bookkeeping but require that the
	// count does not grow with the number of rounds.
	if after > baseline+rounds {
		t.Fatalf("goroutines did not converge: baseline=%d after=%d (rounds=%d)", baseline, after, rounds)
	}
}
