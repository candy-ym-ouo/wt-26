package storage

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"tsdb/internal/config"
	"tsdb/internal/model"
)

func testConfig(directory string) config.Config {
	cfg := config.Default()
	cfg.DataDir = directory
	cfg.ShardDuration = time.Hour
	cfg.Retention = 48 * time.Hour
	cfg.MaintenanceInterval = time.Hour
	cfg.FlushBytes = 1024
	return cfg
}

func TestEngineCrossShardAndRestart(t *testing.T) {
	directory := t.TempDir()
	engine, err := NewEngine(testConfig(directory))
	if err != nil {
		t.Fatal(err)
	}
	series := engine.RegisterSeries("cpu.usage", map[string]string{"host": "web-01"})
	now := time.Now().Truncate(time.Hour).UnixMilli()
	points := []model.Point{{Ts: now - 1, Value: 1}, {Ts: now + 1, Value: 2}}
	if err := engine.AddPoints(series, points); err != nil {
		t.Fatal(err)
	}
	got := engine.QueryPoints(series.ID, now-10, now+10)
	if len(got) != 2 {
		t.Fatalf("cross-shard query got %d points", len(got))
	}
	if engine.Status().Shards["total"] != 2 {
		t.Fatalf("expected two shards: %#v", engine.Status().Shards)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewEngine(testConfig(directory))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	seriesList := reopened.FindSeries("cpu.usage", map[string]string{"host": "web-01"}, 10)
	if len(seriesList) != 1 || len(reopened.QueryPoints(seriesList[0].ID, now-10, now+10)) != 2 {
		t.Fatal("persisted query did not survive restart")
	}
}

func TestEngineReplaysCrashWAL(t *testing.T) {
	directory := t.TempDir()
	cfg := testConfig(directory)
	wal, err := OpenWAL(filepath.Join(directory, "wal.log"), false)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	series := model.Series{ID: 9, Name: "recovered", Tags: map[string]string{"host": "one"}}
	start := shardStart(now, cfg.ShardDuration.Milliseconds())
	if err := wal.Append(WALRecord{ShardID: uint64(start), Series: series, Points: []model.Point{{Ts: now, Value: 7}}}); err != nil {
		t.Fatal(err)
	}
	_ = wal.Close()
	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	if got := engine.QueryPoints(series.ID, now, now); len(got) != 1 || got[0].Value != 7 {
		t.Fatalf("unexpected recovered points: %#v", got)
	}
}

func TestMaintenancePersistsCompactedSegmentsBeforeCrash(t *testing.T) {
	directory := t.TempDir()
	cfg := testConfig(directory)
	now := time.Now().UnixMilli()
	timestamp := now - 2*time.Hour.Milliseconds()
	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	series := engine.RegisterSeries("maintenance", nil)
	if err := engine.AddPoints(series, []model.Point{{Ts: timestamp, Value: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	engine, err = NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.AddPoints(series, []model.Point{{Ts: timestamp + 1, Value: 2}}); err != nil {
		t.Fatal(err)
	}
	start := shardStart(timestamp, cfg.ShardDuration.Milliseconds())
	if err := engine.shards[start].Flush(); err != nil {
		t.Fatal(err)
	}
	engine.maintain(now)
	engine.ready.Store(false)
	close(engine.stop)
	<-engine.done
	if err := engine.wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.QueryPoints(series.ID, timestamp, timestamp+1); len(got) != 2 {
		t.Fatalf("unexpected points after maintenance crash: %#v", got)
	}
}

// TestRestartPreservesFinishedLifecycle asserts that a shard already advanced
// past the active stage keeps that stage across restart: the on-disk lifecycle
// is authoritative, never reset to active by replay.
func TestRestartPreservesFinishedLifecycle(t *testing.T) {
	directory := t.TempDir()
	cfg := testConfig(directory)
	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	timestamp := now - 2*time.Hour.Milliseconds()
	series := engine.RegisterSeries("lifecycle", nil)
	if err := engine.AddPoints(series, []model.Point{{Ts: timestamp, Value: 1}}); err != nil {
		t.Fatal(err)
	}
	start := shardStart(timestamp, cfg.ShardDuration.Milliseconds())
	if err := engine.shards[start].Flush(); err != nil {
		t.Fatal(err)
	}
	// Stage the shard as closed and persist that lifecycle to disk.
	engine.shards[start].State = ShardClosed
	if err := engine.saveMeta(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.shards[start].State; got != ShardClosed {
		t.Fatalf("persisted closed stage lost on restart: got %q", got)
	}
	// A second restart keeps the same stage (idempotent recovery).
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	reopened2, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened2.Close()
	if got := reopened2.shards[start].State; got != ShardClosed {
		t.Fatalf("stage drifted on second restart: got %q", got)
	}
}

// TestLateWriteToFinishedWindowSurvivesRestarts reproduces the reported
// scenario: write, let maintenance finish the old window, restart, write late
// into the old window, then restart repeatedly and verify the late write is
// durable and idempotent under last-write-wins.
func TestLateWriteToFinishedWindowSurvivesRestarts(t *testing.T) {
	directory := t.TempDir()
	cfg := testConfig(directory)
	cfg.ShardDuration = 200 * time.Millisecond
	cfg.MaintenanceInterval = 50 * time.Millisecond

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	start := shardStart(now, cfg.ShardDuration.Milliseconds())
	series := engine.RegisterSeries("late", map[string]string{"host": "a"})
	if err := engine.AddPoints(series, []model.Point{{Ts: now, Value: 1}}); err != nil {
		t.Fatal(err)
	}
	// Let maintenance close the now-elapsed window.
	time.Sleep(500 * time.Millisecond)
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart and perform a legal late write into the old window.
	engine2, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	late := engine2.FindSeries("late", map[string]string{"host": "a"}, 1)[0]
	if err := engine2.AddPoints(late, []model.Point{{Ts: now, Value: 5}}); err != nil {
		t.Fatalf("late write to finished window failed: %v", err)
	}
	if err := engine2.Close(); err != nil {
		t.Fatal(err)
	}

	// Repeated restarts must converge to the same result.
	for i := 0; i < 3; i++ {
		e, err := NewEngine(cfg)
		if err != nil {
			t.Fatalf("restart %d: %v", i, err)
		}
		got := e.QueryPoints(late.ID, start, start+cfg.ShardDuration.Milliseconds())
		if len(got) != 1 || got[0].Value != 5 {
			t.Fatalf("restart %d: want last-write-wins value 5, got %#v", i, got)
		}
		if err := e.Close(); err != nil {
			t.Fatalf("restart %d close: %v", i, err)
		}
	}
}

// TestLateWriteConcurrentWithMaintenance guards against the time-of-check/
// time-of-use "shard is not active" failure when maintenance closes a window
// at the same moment a legal late write targets it.
func TestLateWriteConcurrentWithMaintenance(t *testing.T) {
	directory := t.TempDir()
	cfg := testConfig(directory)
	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	now := time.Now().UnixMilli()
	old := now - 2*time.Hour.Milliseconds()
	start := shardStart(old, cfg.ShardDuration.Milliseconds())
	series := engine.RegisterSeries("race", nil)
	// Build a shard with two segments so Compact keeps it finished.
	if err := engine.AddPoints(series, []model.Point{{Ts: old, Value: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := engine.shards[start].Flush(); err != nil {
		t.Fatal(err)
	}
	if err := engine.AddPoints(series, []model.Point{{Ts: old + 1, Value: 2}}); err != nil {
		t.Fatal(err)
	}
	if err := engine.shards[start].Flush(); err != nil {
		t.Fatal(err)
	}
	nowDrive := now + 3*time.Hour.Milliseconds()

	var wg sync.WaitGroup
	errs := make([]error, 200)
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = engine.AddPoints(series, []model.Point{{Ts: old + int64(i%5), Value: float64(i)}})
		}(i)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for j := 0; j < 2000; j++ {
			engine.maintain(nowDrive)
		}
	}()
	wg.Wait()
	<-done
	failed := 0
	for _, er := range errs {
		if er != nil {
			failed++
		}
	}
	if failed != 0 {
		t.Fatalf("%d legal late writes failed under concurrent maintenance", failed)
	}
}
