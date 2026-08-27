// Package model contains dependency-free data structures shared by TSDB layers.
package model

import "sort"

// Point is one millisecond timestamp and floating-point observation.
type Point struct {
	Ts    int64   `json:"ts"`
	Value float64 `json:"value"`
}

// Series identifies one metric and its exact set of labels.
type Series struct {
	ID   uint64            `json:"id"`
	Name string            `json:"name"`
	Tags map[string]string `json:"tags,omitempty"`
}

// CloneTags returns a defensive copy suitable for crossing package boundaries.
func CloneTags(tags map[string]string) map[string]string {
	copyTags := make(map[string]string, len(tags))
	for key, value := range tags {
		copyTags[key] = value
	}
	return copyTags
}

// SortPoints orders points by timestamp and removes duplicates, keeping the last value.
func SortPoints(points []Point) []Point {
	if len(points) < 2 {
		return points
	}
	sort.SliceStable(points, func(i, j int) bool { return points[i].Ts < points[j].Ts })
	out := points[:0]
	for _, point := range points {
		if len(out) > 0 && out[len(out)-1].Ts == point.Ts {
			out[len(out)-1] = point
			continue
		}
		out = append(out, point)
	}
	return out
}

// FilterPoints returns points in the inclusive interval.
func FilterPoints(points []Point, start, end int64) []Point {
	left := sort.Search(len(points), func(i int) bool { return points[i].Ts >= start })
	right := sort.Search(len(points), func(i int) bool { return points[i].Ts > end })
	return points[left:right]
}
