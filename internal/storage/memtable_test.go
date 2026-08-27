package storage

import (
	"testing"

	"tsdb/internal/model"
)

func TestMemtableOrdersAndOverwrites(t *testing.T) {
	table := NewMemtable()
	table.Insert(1, model.Point{Ts: 20, Value: 2})
	table.Insert(1, model.Point{Ts: 10, Value: 1})
	table.Insert(1, model.Point{Ts: 20, Value: 9})
	points := table.Read(1, 0, 30)
	if len(points) != 2 || points[0].Ts != 10 || points[1].Value != 9 {
		t.Fatalf("unexpected memtable data: %#v", points)
	}
	if table.Count() != 2 {
		t.Fatalf("count=%d, want 2", table.Count())
	}
}
