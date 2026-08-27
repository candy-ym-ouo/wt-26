package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"tsdb/internal/model"
)

// Compact rewrites overlapping segments into one deduplicated segment.
func Compact(shard *Shard) error {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if len(shard.segments) < 2 {
		if shard.State == ShardClosed {
			shard.State = ShardCompacted
		}
		return nil
	}
	merged := make(map[uint64]map[int64]float64)
	for _, segment := range shard.segments {
		for id, points := range segment.Data {
			if merged[id] == nil {
				merged[id] = make(map[int64]float64)
			}
			for _, point := range points {
				merged[id][point.Ts] = point.Value
			}
		}
	}
	data := materializePoints(merged)
	path := filepath.Join(shard.Dir, fmt.Sprintf("seg-%06d.seg", shard.nextSeg))
	segment, err := WriteSegment(path, shard.ID, data)
	if err != nil {
		return err
	}
	old := shard.segments
	shard.segments = []*Segment{segment}
	shard.nextSeg++
	shard.State = ShardCompacted
	for _, item := range old {
		if item.Path != segment.Path {
			_ = os.Remove(item.Path)
		}
	}
	return nil
}

// Downsample replaces compacted raw points with fixed-step averages.
func Downsample(shard *Shard, step int64) error {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if step <= 0 || len(shard.segments) == 0 {
		return nil
	}
	type total struct{ sum, count float64 }
	buckets := make(map[uint64]map[int64]total)
	for _, segment := range shard.segments {
		for id, points := range segment.Data {
			if buckets[id] == nil {
				buckets[id] = make(map[int64]total)
			}
			for _, point := range points {
				start := point.Ts - point.Ts%step
				value := buckets[id][start]
				value.sum += point.Value
				value.count++
				buckets[id][start] = value
			}
		}
	}
	data := make(map[uint64][]model.Point, len(buckets))
	for id, seriesBuckets := range buckets {
		for start, value := range seriesBuckets {
			data[id] = append(data[id], model.Point{Ts: start, Value: value.sum / value.count})
		}
		data[id] = model.SortPoints(data[id])
	}
	path := filepath.Join(shard.Dir, fmt.Sprintf("seg-%06d-down.seg", shard.nextSeg))
	segment, err := WriteSegment(path, shard.ID, data)
	if err != nil {
		return err
	}
	old := shard.segments
	shard.segments = []*Segment{segment}
	shard.nextSeg++
	shard.State = ShardDownsampled
	for _, item := range old {
		_ = os.Remove(item.Path)
	}
	return nil
}

func materializePoints(values map[uint64]map[int64]float64) map[uint64][]model.Point {
	result := make(map[uint64][]model.Point, len(values))
	for id, points := range values {
		for timestamp, value := range points {
			result[id] = append(result[id], model.Point{Ts: timestamp, Value: value})
		}
		result[id] = model.SortPoints(result[id])
	}
	return result
}
