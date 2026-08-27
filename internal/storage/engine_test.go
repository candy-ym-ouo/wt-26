package storage

import (
	"path/filepath"
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
