package ingest

import (
	"math"
	"regexp"
	"time"
	"unicode/utf8"

	"tsdb/internal/model"
)

var metricPattern = regexp.MustCompile(`^[A-Za-z0-9_.\-/]+$`)

// Validator enforces public write limits and timestamp tolerance.
type Validator struct {
	Retention time.Duration
	Now       func() time.Time
}

// NewValidator creates a validator tied to the configured retention period.
func NewValidator(retention time.Duration) Validator {
	return Validator{Retention: retention, Now: time.Now}
}

// Validate checks metric, tags, batch size, timestamps, values, and ordering.
func (v Validator) Validate(batch Batch) error {
	if batch.Metric == "" || len(batch.Metric) > 128 || !metricPattern.MatchString(batch.Metric) {
		return model.BadRequest("invalid metric name")
	}
	if len(batch.Tags) > 16 {
		return model.BadRequest("invalid tags")
	}
	for key, value := range batch.Tags {
		if key == "" || key == "__name__" || len(key) > 64 || len(value) > 256 || !utf8.ValidString(key) || !utf8.ValidString(value) {
			return model.BadRequest("invalid tags")
		}
	}
	if len(batch.Points) == 0 {
		return model.BadRequest("points cannot be empty")
	}
	if len(batch.Points) > 5000 {
		return model.TooLarge("batch too large")
	}
	now := v.Now().UnixMilli()
	oldest := now - v.Retention.Milliseconds()
	latest := now + time.Hour.Milliseconds()
	var previous int64
	for index, point := range batch.Points {
		if point.Ts < oldest || point.Ts > latest {
			return model.BadRequest("timestamp out of range")
		}
		if math.IsNaN(point.Value) || math.IsInf(point.Value, 0) {
			return model.BadRequest("invalid value")
		}
		if index > 0 && point.Ts < previous {
			return model.BadRequest("points out of order")
		}
		previous = point.Ts
	}
	return nil
}
