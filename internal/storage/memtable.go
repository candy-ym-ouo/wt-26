package storage

import (
	"sort"
	"sync"

	"tsdb/internal/model"
)

// Memtable stores active points ordered within each series.
type Memtable struct {
	mu     sync.RWMutex
	points map[uint64][]model.Point
	count  int64
	size   int64
}

// NewMemtable creates an empty in-memory table.
func NewMemtable() *Memtable {
	return &Memtable{points: make(map[uint64][]model.Point)}
}

// Insert adds a point in timestamp order and overwrites an existing timestamp.
func (m *Memtable) Insert(seriesID uint64, point model.Point) {
	m.mu.Lock()
	defer m.mu.Unlock()
	points := m.points[seriesID]
	position := sort.Search(len(points), func(i int) bool { return points[i].Ts >= point.Ts })
	if position < len(points) && points[position].Ts == point.Ts {
		points[position] = point
		m.points[seriesID] = points
		return
	}
	points = append(points, model.Point{})
	copy(points[position+1:], points[position:])
	points[position] = point
	m.points[seriesID] = points
	m.count++
	m.size += 24
}

// InsertBatch adds multiple points while retaining last-write-wins semantics.
func (m *Memtable) InsertBatch(seriesID uint64, points []model.Point) {
	for _, point := range points {
		m.Insert(seriesID, point)
	}
}

// Read returns a defensive copy of points inside the inclusive interval.
func (m *Memtable) Read(seriesID uint64, start, end int64) []model.Point {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return model.FilterPoints(m.points[seriesID], start, end)
}

// Snapshot creates an independent copy of all series and their points.
func (m *Memtable) Snapshot() map[uint64][]model.Point {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[uint64][]model.Point, len(m.points))
	for id, points := range m.points {
		result[id] = append([]model.Point(nil), points...)
	}
	return result
}

// Count reports the number of unique series/timestamp pairs.
func (m *Memtable) Count() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.count
}

// Size reports the approximate memory used by stored points.
func (m *Memtable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// Empty reports whether the table has no points.
func (m *Memtable) Empty() bool { return m.Count() == 0 }
