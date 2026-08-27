package query

import (
	"net/url"
	"testing"

	"tsdb/internal/model"
)

func TestParseRangeAndBucket(t *testing.T) {
	request, err := ParseRange(url.Values{
		"metric": {"cpu"}, "start": {"1001"}, "end": {"5000"},
		"step": {"1000"}, "agg": {"avg"}, "fill": {"zero"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if request.Start != 1000 {
		t.Fatalf("start=%d, want aligned 1000", request.Start)
	}
	points := []model.Point{{Ts: 1100, Value: 2}, {Ts: 1500, Value: 4}, {Ts: 3100, Value: 8}}
	result := Bucket(points, request)
	if len(result) != 5 || result[0].Value != 3 || result[1].Value != 0 || result[2].Value != 8 {
		t.Fatalf("unexpected buckets: %#v", result)
	}
}

func TestAggregations(t *testing.T) {
	points := []model.Point{{Ts: 1, Value: 1}, {Ts: 2, Value: 2}, {Ts: 3, Value: 10}}
	checks := map[string]float64{"sum": 13, "count": 3, "min": 1, "max": 10, "last": 10, "p95": 10}
	for name, want := range checks {
		got, ok := Aggregate(name, points)
		if !ok || got != want {
			t.Fatalf("%s=%v,%v want %v", name, got, ok, want)
		}
	}
}
