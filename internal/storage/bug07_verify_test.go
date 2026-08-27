package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"tsdb/internal/config"
	"tsdb/internal/model"
)

func TestBug07CloseReportsAndRetainsMetadataFailure(t *testing.T) {
	directory := t.TempDir()
	cfg := config.Default()
	cfg.DataDir = directory
	cfg.MaintenanceInterval = time.Hour
	cfg.WALSync = false
	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	series := engine.RegisterSeries("close.failure", nil)
	if err := engine.AddPoints(series, []model.Point{{Ts: time.Now().UnixMilli(), Value: 7}}); err != nil {
		t.Fatal(err)
	}
	for _, shard := range engine.shards {
		if err := shard.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(directory, "meta.json.tmp"), 0o755); err != nil {
		t.Fatal(err)
	}

	first := engine.Close()
	if first == nil {
		t.Fatal("Close returned nil after metadata publication failed")
	}
	second := engine.Close()
	if second == nil {
		t.Fatal("a repeated Close call lost the first failure")
	}
	info, err := os.Stat(filepath.Join(directory, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatal("WAL was truncated after an earlier close step failed")
	}
}
