package model

import "testing"

func TestBug04NilTagsProduceValidLabels(t *testing.T) {
	series := Series{ID: 1, Name: "untagged.metric", Tags: nil}
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("Labels panicked for a nil tag map: %v", recovered)
		}
	}()
	labels := Labels(series)
	if labels == nil {
		t.Fatal("Labels returned a nil map")
	}
	if got := labels["__name__"]; got != series.Name {
		t.Fatalf("metric label=%q want=%q", got, series.Name)
	}
}
