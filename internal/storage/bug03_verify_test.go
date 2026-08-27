package storage

import (
	"errors"
	"fmt"
	"path/filepath"
	"syscall"
	"testing"

	"tsdb/internal/model"
)

func TestBug03ReplayPreservesOriginalErrorIdentity(t *testing.T) {
	wal, err := OpenWAL(filepath.Join(t.TempDir(), "wal.log"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	record := WALRecord{
		ShardID: 1,
		Series:  model.Series{ID: 1, Name: "disk.failure"},
		Points:  []model.Point{{Ts: 1, Value: 1}},
	}
	if err := wal.Append(record); err != nil {
		t.Fatal(err)
	}
	sentinel := fmt.Errorf("persist segment: %w", syscall.ENOSPC)
	err = wal.Replay(func(WALRecord) error { return sentinel })
	if err == nil {
		t.Fatal("replay unexpectedly succeeded")
	}
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("original error identity was lost: %v", err)
	}
}
