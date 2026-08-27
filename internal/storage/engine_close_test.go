package storage

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"tsdb/internal/model"
)

// TestCloseReportsPersistedFailure replicates a stable shutdown-path failure:
// shards flush successfully, then the data directory is made read-only so the
// metadata publish and WAL truncation cannot succeed. Close must surface a
// non-nil error rather than report success, which would leave a stale WAL to
// be replayed and missing metadata to be loaded on the next start.
func TestCloseReportsPersistedFailure(t *testing.T) {
	// Read-only dirs are honored as write failures on unix; skip elsewhere.
	if runtime.GOOS == "windows" {
		t.Skip("read-only directory simulation is unix-specific")
	}
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions; cannot simulate read-only failure")
	}

	directory := t.TempDir()
	cfg := testConfig(directory)
	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	series := engine.RegisterSeries("cpu.usage", map[string]string{"host": "web-01"})
	now := time.Now().Truncate(time.Hour).UnixMilli()
	if err := engine.AddPoints(series, []model.Point{{Ts: now + 1, Value: 9}}); err != nil {
		t.Fatal(err)
	}

	// Flush the shard while the directory is still writable so Close's flush
	// step succeeds. The remaining close-time work — publishing meta.json and
	// truncating the WAL — is exactly what must fail once the dir is read-only.
	for _, shard := range engine.shards {
		if err := shard.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	// Deny writes to the data directory so metadata publish and WAL truncation
	// both error on the read-only filesystem.
	if err := os.Chmod(directory, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(directory, 0o755) })

	// Close must not lie: it returns the real failure instead of nil.
	closeErr := engine.Close()
	if closeErr == nil {
		t.Fatalf("Close returned nil despite read-only data directory; " +
			"restart would replay a stale WAL or load missing metadata")
	}
}

// TestCloseIdempotent keeps the "repeated close calls are safe" contract:
// the first call performs the work, and later calls return the same outcome
// (nil on success, the retained error on failure) instead of a fresh nil that
// would hide a real failure from a caller that calls Close twice.
func TestCloseIdempotent(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		directory := t.TempDir()
		engine, err := NewEngine(testConfig(directory))
		if err != nil {
			t.Fatal(err)
		}
		if err := engine.Close(); err != nil {
			t.Fatalf("first close: %v", err)
		}
		if err := engine.Close(); err != nil {
			t.Fatalf("second close should be a no-op returning nil, got %v", err)
		}
	})

	t.Run("failure_retained", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("read-only directory simulation is unix-specific")
		}
		if os.Geteuid() == 0 {
			t.Skip("root bypasses directory permissions; cannot simulate read-only failure")
		}
		directory := t.TempDir()
		engine, err := NewEngine(testConfig(directory))
		if err != nil {
			t.Fatal(err)
		}
		for _, shard := range engine.shards {
			if err := shard.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.Chmod(directory, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(directory, 0o755) })
		first := engine.Close()
		second := engine.Close()
		if first == nil {
			t.Fatal("first close should report the failure")
		}
		if second == nil {
			t.Fatal("second close should retain the failure, not report fresh nil")
		}
	})
}

// TestClosePreservesWALWhenFlushFails simulates a shard flush failure during
// close and asserts that the WAL truncation is skipped so the records survive
// for replay on the next start. A successful truncation here would drop the
// only durable copy of points that never reached a segment, corrupting recovery.
func TestClosePreservesWALWhenFlushFails(t *testing.T) {
	directory := t.TempDir()
	cfg := testConfig(directory)
	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	series := engine.RegisterSeries("cpu.usage", map[string]string{"host": "web-01"})
	now := time.Now().Truncate(time.Hour).UnixMilli()
	if err := engine.AddPoints(series, []model.Point{{Ts: now + 1, Value: 9}}); err != nil {
		t.Fatal(err)
	}
	// Pin the shard so Flush publishes into a path whose parent is a regular
	// file: MkdirAll then errors with "not a directory", a stable flush failure
	// that does not depend on filesystem permission quirks.
	start := shardStart(now+1, cfg.ShardDuration.Milliseconds())
	shard := engine.shards[start]
	if shard == nil {
		t.Fatalf("shard not created for start %d", start)
	}
	blocker := filepath.Join(directory, "blocker")
	if err := os.WriteFile(blocker, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	shard.Dir = blocker

	closeErr := engine.Close()
	if closeErr == nil {
		t.Fatalf("Close returned nil despite shard flush failure")
	}

	// WAL must still hold the record because truncation was skipped.
	info, statErr := os.Stat(filepath.Join(directory, "wal.log"))
	if statErr != nil {
		t.Fatalf("wal stat: %v", statErr)
	}
	if info.Size() == 0 {
		t.Fatalf("WAL was truncated despite an earlier flush failure; " +
			"restart would lose the unflushed points")
	}
}
