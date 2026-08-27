package storage

import (
	"os"
	"path/filepath"
	"testing"

	"tsdb/internal/model"
)

func TestSegmentRoundTripAndChecksum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shard", "seg-1.seg")
	segment, err := WriteSegment(path, 100, map[uint64][]model.Point{7: {{Ts: 2, Value: 2}, {Ts: 1, Value: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	points := segment.ReadSeries(7, 1, 2)
	if len(points) != 2 || points[0].Ts != 1 {
		t.Fatalf("unexpected segment data: %#v", points)
	}
	raw, _ := os.ReadFile(path)
	raw[len(raw)-1] ^= 0xff
	_ = os.WriteFile(path, raw, 0o644)
	if _, err := OpenSegment(path); err == nil {
		t.Fatal("expected checksum failure")
	}
}

func TestDownsampleReplacesSegmentsWithAverages(t *testing.T) {
	shard := NewShard(1, 120000, 180000, t.TempDir())
	if err := shard.Insert(7, []model.Point{{Ts: 120001, Value: 10}, {Ts: 120002, Value: 20}}); err != nil {
		t.Fatal(err)
	}
	if err := shard.Flush(); err != nil {
		t.Fatal(err)
	}
	oldPath := shard.segments[0].Path
	if err := Downsample(shard, 60000); err != nil {
		t.Fatal(err)
	}
	points := shard.Read(7, 120000, 179999)
	if shard.State != ShardDownsampled || len(shard.segments) != 1 {
		t.Fatalf("unexpected downsample state: %s, segments: %d", shard.State, len(shard.segments))
	}
	if len(points) != 1 || points[0].Ts != 120000 || points[0].Value != 15 {
		t.Fatalf("unexpected downsampled points: %#v", points)
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("original segment still exists: %v", err)
	}
}

func TestRestoredShardDoesNotOverwriteExistingSegment(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "seg-000002.seg")
	existing, err := WriteSegment(path, 1, map[uint64][]model.Point{7: {{Ts: 1, Value: 10}}})
	if err != nil {
		t.Fatal(err)
	}
	shard := NewShard(1, 0, 60000, directory)
	shard.AddSegment(existing)
	if err := shard.Insert(7, []model.Point{{Ts: 2, Value: 20}}); err != nil {
		t.Fatal(err)
	}
	if err := shard.Flush(); err != nil {
		t.Fatal(err)
	}
	paths := shard.SegmentPaths()
	if len(paths) != 2 || paths[0] == paths[1] {
		t.Fatalf("restored segment sequence was reused: %#v", paths)
	}
	reopened, err := OpenSegment(path)
	if err != nil {
		t.Fatal(err)
	}
	points := reopened.ReadSeries(7, 1, 1)
	if len(points) != 1 || points[0].Value != 10 {
		t.Fatalf("existing segment was overwritten: %#v", points)
	}
}
