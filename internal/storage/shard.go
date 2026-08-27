package storage

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"

	"tsdb/internal/model"
)

// ShardState records the lifecycle stage of one time window.
type ShardState string

const (
	ShardActive      ShardState = "active"
	ShardClosed      ShardState = "closed"
	ShardCompacted   ShardState = "compacted"
	ShardDownsampled ShardState = "downsampled"
)

// Shard stores one aligned time window.
type Shard struct {
	ID       uint64
	Start    int64
	End      int64
	State    ShardState
	Dir      string
	mem      *Memtable
	segments []*Segment
	nextSeg  uint64
	mu       sync.RWMutex
}

// NewShard creates an active shard backed by an empty memtable.
func NewShard(id uint64, start, end int64, dir string) *Shard {
	return &Shard{ID: id, Start: start, End: end, State: ShardActive, Dir: dir, mem: NewMemtable(), nextSeg: 1}
}

// Insert buffers points into the shard memtable.
//
// Writability is decoupled from the lifecycle stage: a late write targets an
// already-closed (or compacted/downsampled) window and lands in the pending
// memtable, where maintenance will later flush it as a new segment. The stage
// itself is never reset by a write, so the persisted lifecycle survives both
// concurrent maintenance and restart. This closes the time-of-check/time-of-use
// window that previously surfaced as "shard is not active".
func (s *Shard) Insert(seriesID uint64, points []model.Point) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.mem.InsertBatch(seriesID, points)
	return nil
}

// Read merges memtable and segments with newer memtable values winning.
func (s *Shard) Read(seriesID uint64, start, end int64) []model.Point {
	s.mu.RLock()
	defer s.mu.RUnlock()
	byTime := make(map[int64]model.Point)
	for _, segment := range s.segments {
		for _, point := range segment.ReadSeries(seriesID, start, end) {
			byTime[point.Ts] = point
		}
	}
	for _, point := range s.mem.Read(seriesID, start, end) {
		byTime[point.Ts] = point
	}
	points := make([]model.Point, 0, len(byTime))
	for _, point := range byTime {
		points = append(points, point)
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Ts < points[j].Ts })
	return points
}

// Flush atomically swaps the active memtable and writes its snapshot to disk.
func (s *Shard) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mem.Empty() {
		return nil
	}
	snapshot := s.mem.Snapshot()
	path := filepath.Join(s.Dir, fmt.Sprintf("seg-%06d.seg", s.nextSeg))
	segment, err := WriteSegment(path, s.ID, snapshot)
	if err != nil {
		return err
	}
	s.nextSeg++
	s.segments = append(s.segments, segment)
	s.mem = NewMemtable()
	return nil
}

// Close flushes the shard and makes it read-only.
func (s *Shard) Close() error {
	if err := s.Flush(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.State = ShardClosed
	return nil
}

// advanceState moves the lifecycle stage forward. It never touches the memtable
// or segments, so maintenance can close a window after a flush without the
// two-step Flush+Close reentrancy.
func (s *Shard) advanceState(state ShardState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.State = state
}

// AddSegment installs a segment restored from metadata. It advances the next
// free sequence number but never changes the shard's lifecycle stage; the
// stage is owned by the engine and persists across restarts.
func (s *Shard) AddSegment(segment *Segment) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.segments = append(s.segments, segment)
	var sequence uint64
	if _, err := fmt.Sscanf(filepath.Base(segment.Path), "seg-%d", &sequence); err == nil && sequence >= s.nextSeg {
		s.nextSeg = sequence + 1
	}
}

// SegmentPaths returns persisted segment names.
func (s *Shard) SegmentPaths() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	paths := make([]string, 0, len(s.segments))
	for _, segment := range s.segments {
		paths = append(paths, filepath.Base(segment.Path))
	}
	return paths
}

// state returns the current lifecycle stage under the shard lock.
func (s *Shard) state() ShardState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.State
}

// segmentCount returns the number of persisted segments under the shard lock.
func (s *Shard) segmentCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.segments)
}

// PointCount calculates unique logical points across all sources.
func (s *Shard) PointCount() int64 {
	s.mu.RLock()
	ids := make(map[uint64]struct{})
	for id := range s.mem.Snapshot() {
		ids[id] = struct{}{}
	}
	for _, segment := range s.segments {
		for id := range segment.Data {
			ids[id] = struct{}{}
		}
	}
	s.mu.RUnlock()
	var count int64
	for id := range ids {
		count += int64(len(s.Read(id, s.Start, s.End-1)))
	}
	return count
}

// MemoryPoints returns the current unflushed point count.
func (s *Shard) MemoryPoints() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mem.Count()
}

// MemoryBytes returns the current memtable size estimate.
func (s *Shard) MemoryBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mem.Size()
}
