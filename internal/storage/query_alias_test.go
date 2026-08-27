package storage

import (
	"sort"
	"testing"
	"time"

	"tsdb/internal/config"
	"tsdb/internal/model"
)

// TestReadDoesNotAliasStorage guards against the regression where a read
// returned a slice sharing the memtable backing array, so a later query that
// sorted points by value (p95) or appended in place could corrupt in-process
// data: scrambling time order or zeroing the last point.
func TestReadDoesNotAliasStorage(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.ShardDuration = time.Hour
	cfg.Retention = 48 * time.Hour
	cfg.MaintenanceInterval = time.Hour
	cfg.FlushBytes = 1 << 30 // keep everything in the memtable
	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	series := engine.RegisterSeries("metric", map[string]string{"host": "a"})
	// Time order intentionally differs from value order; the last point (40)
	// is NOT the max value, so a corruption that reorders by value is visible.
	points := []model.Point{
		{Ts: 10, Value: 5},
		{Ts: 20, Value: 1},
		{Ts: 30, Value: 9},
		{Ts: 40, Value: 2},
	}
	if err := engine.AddPoints(series, points); err != nil {
		t.Fatal(err)
	}

	// Read a sub-range that excludes the tail point, then reorder the result by
	// value in place. This mirrors the p95 aggregation path, which previously
	// received slices aliasing the memtable backing array.
	got := engine.QueryPoints(series.ID, 0, 30)
	sortByValue(got)

	// A fresh full read must still match the written order: the read must not
	// have scrambled the stored points or zeroed the tail.
	fresh := engine.QueryPoints(series.ID, 0, 1<<40)
	if len(fresh) != len(points) {
		t.Fatalf("length changed: got %d want %d (%#v)", len(fresh), len(points), fresh)
	}
	for i := range points {
		if fresh[i] != points[i] {
			t.Fatalf("storage corrupted by read at index %d: got %#v want %#v (full=%#v)", i, fresh[i], points[i], fresh)
		}
	}
}

// TestSegmentReadDoesNotAlias exercises the same aliasing risk through the
// persisted-segment read path.
func TestSegmentReadDoesNotAlias(t *testing.T) {
	shard := NewShard(1, 0, 60000, t.TempDir())
	points := []model.Point{
		{Ts: 10, Value: 5},
		{Ts: 20, Value: 1},
		{Ts: 30, Value: 9},
		{Ts: 40, Value: 2},
	}
	if err := shard.Insert(7, points); err != nil {
		t.Fatal(err)
	}
	if err := shard.Flush(); err != nil { // force data into a segment
		t.Fatal(err)
	}
	// Read a sub-range excluding the tail and reorder by value in place.
	got := shard.Read(7, 0, 30)
	sortByValue(got)
	again := shard.Read(7, 0, 60000)
	if len(again) != len(points) {
		t.Fatalf("length changed: got %d want %d (%#v)", len(again), len(points), again)
	}
	for i := range points {
		if again[i] != points[i] {
			t.Fatalf("segment data corrupted by read at %d: got %#v want %#v (full=%#v)", i, again[i], points[i], again)
		}
	}
}

// TestSubrangeReadKeepsTailPoint guards the specific "last value changes with no
// new write" symptom: reading a window that excludes the newest point and then
// appending to the result (as the executor used to) must not zero the stored
// tail point.
func TestSubrangeReadKeepsTailPoint(t *testing.T) {
	m := NewMemtable()
	points := []model.Point{
		{Ts: 10, Value: 5},
		{Ts: 20, Value: 1},
		{Ts: 30, Value: 9},
		{Ts: 40, Value: 2}, // tail, not the max
	}
	for _, p := range points {
		m.Insert(1, p)
	}
	// Sub-range read returns a slice with spare capacity when it aliases
	// storage; appending to it must not touch the stored tail.
	got := m.Read(1, 10, 30)
	_ = append(got, model.Point{})

	full := m.Read(1, 0, 100)
	if len(full) != len(points) {
		t.Fatalf("length changed: got %d want %d (%#v)", len(full), len(points), full)
	}
	for i := range points {
		if full[i] != points[i] {
			t.Fatalf("tail corrupted by sub-range read at %d: got %#v want %#v (full=%#v)", i, full[i], points[i], full)
		}
	}
}

func sortByValue(points []model.Point) {
	sort.Slice(points, func(i, j int) bool { return points[i].Value < points[j].Value })
}
