package storage_test

import (
	"testing"

	"tsdb/internal/model"
	"tsdb/internal/query"
	"tsdb/internal/storage"
)

func TestBug05P95DoesNotMutateLaterQueryResults(t *testing.T) {
	shard := storage.NewShard(1, 0, 100, t.TempDir())
	points := []model.Point{
		{Ts: 10, Value: 5},
		{Ts: 20, Value: 1},
		{Ts: 30, Value: 9},
		{Ts: 40, Value: 2},
	}
	if err := shard.Insert(7, points); err != nil {
		t.Fatal(err)
	}

	read := shard.Read(7, 0, 100)
	if value, ok := query.Aggregate("p95", read); !ok || value != 9 {
		t.Fatalf("p95=(%v,%v) want=(9,true)", value, ok)
	}

	fresh := shard.Read(7, 0, 100)
	if len(fresh) != len(points) {
		t.Fatalf("fresh read has %d points, want %d", len(fresh), len(points))
	}
	for index, point := range points {
		if fresh[index] != point {
			t.Fatalf("point %d changed to %#v, want %#v", index, fresh[index], point)
		}
	}
}
