package query

import (
	"math"
	"sort"

	"tsdb/internal/model"
)

// Aggregate reduces one non-empty point bucket to a single value.
func Aggregate(name string, points []model.Point) (float64, bool) {
	if len(points) == 0 {
		return 0, false
	}
	sum := 0.0
	minimum := points[0].Value
	maximum := points[0].Value
	for _, point := range points {
		sum += point.Value
		minimum = math.Min(minimum, point.Value)
		maximum = math.Max(maximum, point.Value)
	}
	switch name {
	case "avg":
		return sum / float64(len(points)), true
	case "sum":
		return sum, true
	case "count":
		return float64(len(points)), true
	case "min":
		return minimum, true
	case "max":
		return maximum, true
	case "last":
		return points[len(points)-1].Value, true
	case "p95":
		sort.Slice(points, func(i, j int) bool { return points[i].Value < points[j].Value })
		position := int(math.Ceil(float64(len(points))*0.95)) - 1
		if position < 0 {
			position = 0
		}
		return points[position].Value, true
	default:
		return 0, false
	}
}

// Bucket aggregates points into start-aligned step windows and applies fill.
func Bucket(points []model.Point, request model.QueryReq) []model.Point {
	if request.Step <= 0 {
		if len(points) > 10000 {
			points = points[:10000]
		}
		return append([]model.Point(nil), points...)
	}
	result := make([]model.Point, 0)
	position := 0
	previous := 0.0
	hasPrevious := false
	for bucketStart := request.Start; bucketStart <= request.End; bucketStart += request.Step {
		bucketEnd := bucketStart + request.Step
		begin := position
		for position < len(points) && points[position].Ts < bucketEnd {
			if points[position].Ts >= bucketStart {
				position++
				continue
			}
			position++
			begin = position
		}
		value, ok := Aggregate(request.Agg, points[begin:position])
		if ok {
			previous, hasPrevious = value, true
			result = append(result, model.Point{Ts: bucketStart, Value: value})
			continue
		}
		if request.Agg == "count" || request.Fill == "zero" {
			result = append(result, model.Point{Ts: bucketStart, Value: 0})
		} else if request.Fill == "prev" && hasPrevious {
			result = append(result, model.Point{Ts: bucketStart, Value: previous})
		}
		if bucketStart > request.End-request.Step {
			break
		}
	}
	return result
}
