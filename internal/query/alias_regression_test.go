package query

import (
	"testing"
	"time"

	"tsdb/internal/config"
	"tsdb/internal/model"
	"tsdb/internal/storage"
)

// TestQueryOrderDoesNotCorruptStorage is a positive end-to-end stability check
// for the reported scenario: running p95 instant, then range and last queries
// repeatedly on one series must keep producing identical, correctly-ordered
// results. The p95 path sorts points by value in place, so any leak of a slice
// aliasing in-process storage would scramble later reads. The structural guard
// against that aliasing lives at the storage read boundary (see
// storage.TestSubrangeReadKeepsTailPoint); this test pins the user-visible
// contract that query order never changes results.
func TestQueryOrderDoesNotCorruptStorage(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.ShardDuration = time.Hour
	cfg.Retention = 48 * time.Hour
	cfg.MaintenanceInterval = time.Hour
	cfg.FlushBytes = 1 << 20
	engine, err := storage.NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	series := engine.RegisterSeries("metric", map[string]string{"host": "a"})
	points := []model.Point{
		{Ts: 10, Value: 5},
		{Ts: 20, Value: 1},
		{Ts: 30, Value: 9},
		{Ts: 40, Value: 2}, // last point is not the max
	}
	if err := engine.AddPoints(series, points); err != nil {
		t.Fatal(err)
	}

	exec := NewExecutor(engine)
	tags := map[string]string{"host": "a"}

	// Snapshot the canonical range result BEFORE any p95 query runs.
	req := model.QueryReq{Metric: "metric", Tags: tags, Start: 0, End: 1 << 40, Limit: 1000}
	baseline := exec.Range(req)
	if len(baseline.Result) != 1 || len(baseline.Result[0].Values) != 4 {
		t.Fatalf("baseline range got %d series / %d points", len(baseline.Result), len(baseline.Result[0].Values))
	}
	baselineLast := baseline.Result[0].Values[3]

	// Run several cycles of p95 instant -> range -> last, exactly the order
	// that surfaced the corruption. The p95 path sorts points by value in
	// place, which previously aliased storage.
	for round := 0; round < 5; round++ {
		vector := exec.Instant(model.QueryReq{Metric: "metric", Tags: tags, Start: 0, End: 1 << 40, Agg: "p95", Limit: 1000}, true)
		if len(vector.Result) != 1 {
			t.Fatalf("round %d: instant got %d series", round, len(vector.Result))
		}
		// p95 of {1,2,5,9} is 9 (ceil(4*0.95)=4 -> index 3 of value-sorted).
		if got := vector.Result[0].Value.Value; got != 9 {
			t.Fatalf("round %d: p95=%v want 9", round, got)
		}

		matrix := exec.Range(req)
		got := matrix.Result[0].Values
		if len(got) != 4 {
			t.Fatalf("round %d: range got %d points", round, len(got))
		}
		// Time order must be preserved and equal the written order.
		for i := range points {
			if got[i].Ts != points[i].Ts {
				t.Fatalf("round %d: time order scrambled at %d: got %#v", round, i, got)
			}
			if got[i].Value != points[i].Value {
				t.Fatalf("round %d: value changed at %d: got %v want %v", round, i, got[i].Value, points[i].Value)
			}
		}
		if got[3].Value != baselineLast.Value {
			t.Fatalf("round %d: last value drifted: got %v want %v", round, got[3].Value, baselineLast.Value)
		}

		// last instant (no aggregation label) returns the latest point value.
		last := exec.Instant(model.QueryReq{Metric: "metric", Tags: tags, Start: 0, End: 1 << 40, Limit: 1000}, false)
		if got := last.Result[0].Value.Value; got != 2 {
			t.Fatalf("round %d: last=%v want 2", round, got)
		}
	}

	// Skipping the p95 query must yield the same range result as before — i.e.
	// the canonical answer does not depend on which queries ran first.
	final := exec.Range(req)
	if len(final.Result[0].Values) != len(baseline.Result[0].Values) {
		t.Fatalf("final length differs from baseline")
	}
	for i := range baseline.Result[0].Values {
		if final.Result[0].Values[i] != baseline.Result[0].Values[i] {
			t.Fatalf("final result diverged from baseline at %d: got %v want %v", i, final.Result[0].Values[i], baseline.Result[0].Values[i])
		}
	}
}
