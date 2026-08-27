package ingest

import (
	"testing"
	"time"

	"tsdb/internal/model"
)

func TestValidatorRules(t *testing.T) {
	now := time.UnixMilli(2_000_000)
	validator := NewValidator(time.Hour)
	validator.Now = func() time.Time { return now }
	valid := Batch{Metric: "cpu.usage", Tags: map[string]string{"host": "one"}, Points: []model.Point{{Ts: 1_999_000, Value: 1}}}
	if err := validator.Validate(valid); err != nil {
		t.Fatal(err)
	}
	cases := []Batch{
		{Metric: "bad metric", Points: valid.Points},
		{Metric: "cpu", Tags: map[string]string{"__name__": "bad"}, Points: valid.Points},
		{Metric: "cpu", Points: []model.Point{{Ts: 1_999_000, Value: 1}, {Ts: 1_998_000, Value: 2}}},
		{Metric: "cpu", Points: []model.Point{{Ts: now.Add(-time.Hour - time.Millisecond).UnixMilli(), Value: 1}}},
	}
	for index, batch := range cases {
		if err := validator.Validate(batch); err == nil {
			t.Fatalf("case %d should fail", index)
		}
	}
}
