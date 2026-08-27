package storage

import (
	"os"
	"path/filepath"
	"testing"

	"tsdb/internal/model"
)

func TestWALAppendReplayAndTruncate(t *testing.T) {
	wal, err := OpenWAL(filepath.Join(t.TempDir(), "wal.log"), false)
	if err != nil {
		t.Fatal(err)
	}
	record := WALRecord{ShardID: 10, Series: model.Series{ID: 1, Name: "cpu"}, Points: []model.Point{{Ts: 11, Value: 2}}}
	if err := wal.Append(record); err != nil {
		t.Fatal(err)
	}
	seen := 0
	if err := wal.Replay(func(got WALRecord) error { seen += len(got.Points); return nil }); err != nil {
		t.Fatal(err)
	}
	if seen != 1 {
		t.Fatalf("replayed %d points", seen)
	}
	if err := wal.Truncate(); err != nil {
		t.Fatal(err)
	}
	seen = 0
	if err := wal.Replay(func(WALRecord) error { seen++; return nil }); err != nil || seen != 0 {
		t.Fatalf("truncate replay: seen=%d err=%v", seen, err)
	}
	_ = wal.Close()
}

func TestWALReplayRemovesPartialTailBeforeAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	wal, err := OpenWAL(path, false)
	if err != nil {
		t.Fatal(err)
	}
	record := WALRecord{ShardID: 10, Series: model.Series{ID: 1, Name: "cpu"}, Points: []model.Point{{Ts: 11, Value: 2}}}
	if err := wal.Append(record); err != nil {
		t.Fatal(err)
	}
	if _, err := wal.file.Write([]byte("WA")); err != nil {
		t.Fatal(err)
	}
	if err := wal.Replay(func(WALRecord) error { return nil }); err != nil {
		t.Fatal(err)
	}
	record.Points[0].Ts = 12
	if err := wal.Append(record); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenWAL(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	seen := 0
	if err := reopened.Replay(func(got WALRecord) error { seen += len(got.Points); return nil }); err != nil {
		t.Fatal(err)
	}
	if seen != 2 {
		t.Fatalf("replayed %d points after partial tail recovery", seen)
	}
}
