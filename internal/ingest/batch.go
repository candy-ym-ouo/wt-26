// Package ingest validates and writes public metric batches.
package ingest

import (
	"sort"
	"time"

	"tsdb/internal/model"
)

// Batch is the normalized internal write request.
type Batch struct {
	Metric string
	Tags   map[string]string
	Points []model.Point
}

// FromModel defensively copies an HTTP request model.
func FromModel(input model.IngestBatch) Batch {
	return Batch{
		Metric: input.Metric,
		Tags:   model.CloneTags(input.Tags),
		Points: append([]model.Point(nil), input.Points...),
	}
}

// Count returns the number of submitted points.
func (b Batch) Count() int { return len(b.Points) }

// GroupByShard partitions points using UTC-aligned duration boundaries.
func (b Batch) GroupByShard(duration time.Duration) map[int64][]model.Point {
	millis := duration.Milliseconds()
	groups := make(map[int64][]model.Point)
	for _, point := range b.Points {
		start := point.Ts - point.Ts%millis
		groups[start] = append(groups[start], point)
	}
	return groups
}

// SortedShardStarts returns group keys in chronological order.
func SortedShardStarts(groups map[int64][]model.Point) []int64 {
	starts := make([]int64, 0, len(groups))
	for start := range groups {
		starts = append(starts, start)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	return starts
}
