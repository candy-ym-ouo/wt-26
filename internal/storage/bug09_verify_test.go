package storage

import (
	"testing"
	"time"

	"tsdb/internal/config"
	"tsdb/internal/model"
)

func TestBug09RestartPreservesFinishedShardLifecycle(t *testing.T) {
	directory := t.TempDir()
	cfg := config.Default()
	cfg.DataDir = directory
	cfg.ShardDuration = time.Hour
	cfg.Retention = 48 * time.Hour
	cfg.MaintenanceInterval = time.Hour
	cfg.WALSync = false
	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}

	timestamp := time.Now().Add(-2 * time.Hour).UnixMilli()
	series := engine.RegisterSeries("restart.lifecycle", nil)
	if err := engine.AddPoints(series, []model.Point{{Ts: timestamp, Value: 1}}); err != nil {
		t.Fatal(err)
	}
	start := shardStart(timestamp, cfg.ShardDuration.Milliseconds())
	shard := engine.shards[start]
	if shard == nil {
		t.Fatal("expected shard was not created")
	}
	if err := shard.Flush(); err != nil {
		t.Fatal(err)
	}
	shard.State = ShardClosed
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restored := reopened.shards[start]
	if restored == nil {
		t.Fatal("persisted shard was not restored")
	}
	if restored.State != ShardClosed {
		t.Fatalf("restored lifecycle=%q want=%q", restored.State, ShardClosed)
	}
}
