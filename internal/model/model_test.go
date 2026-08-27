package model

import "testing"

func TestSeriesKeyAndTags(t *testing.T) {
	one := SeriesKey("cpu.usage", map[string]string{"region": "tokyo", "host": "web-01"})
	two := SeriesKey("cpu.usage", map[string]string{"host": "web-01", "region": "tokyo"})
	if one != two {
		t.Fatalf("series key depends on map order: %q != %q", one, two)
	}
	tags, err := ParseTags("host=web-01,region=tokyo")
	if err != nil || tags["host"] != "web-01" || tags["region"] != "tokyo" {
		t.Fatalf("unexpected parsed tags: %#v, %v", tags, err)
	}
	if _, err := ParseTags("host"); err == nil {
		t.Fatal("expected malformed tags to fail")
	}
}

func TestSortPointsLastWriteWins(t *testing.T) {
	points := []Point{{Ts: 2, Value: 2}, {Ts: 1, Value: 1}, {Ts: 2, Value: 3}}
	points = SortPoints(points)
	if len(points) != 2 || points[0].Ts != 1 || points[1].Value != 3 {
		t.Fatalf("unexpected points: %#v", points)
	}
}
