package model

import (
	"encoding/json"
	"strconv"
)

// QueryReq describes a range query after URL parsing and validation.
type QueryReq struct {
	Metric string
	Tags   map[string]string
	Start  int64
	End    int64
	Step   int64
	Agg    string
	Fill   string
	Limit  int
}

// Sample preserves the API convention of [timestamp,"value"].
type Sample struct {
	Ts    int64
	Value float64
}

// MarshalJSON emits a compact Prometheus-style sample tuple.
func (s Sample) MarshalJSON() ([]byte, error) {
	return json.Marshal([2]any{s.Ts, strconv.FormatFloat(s.Value, 'g', -1, 64)})
}

// MatrixSeries is one labeled time series in a range response.
type MatrixSeries struct {
	Metric map[string]string `json:"metric"`
	Values []Sample          `json:"values"`
}

// MatrixData wraps range query results.
type MatrixData struct {
	ResultType string         `json:"resultType"`
	Result     []MatrixSeries `json:"result"`
}

// VectorSeries is one labeled instant result.
type VectorSeries struct {
	Metric map[string]string `json:"metric"`
	Value  Sample            `json:"value"`
}

// VectorData wraps instant query results.
type VectorData struct {
	ResultType string         `json:"resultType"`
	Result     []VectorSeries `json:"result"`
}

// MetricInfo summarizes stored cardinality and point volume.
type MetricInfo struct {
	Name   string `json:"name"`
	Series int    `json:"series"`
	Points int64  `json:"points"`
}

// IngestBatch is the HTTP write request body.
type IngestBatch struct {
	Metric string            `json:"metric"`
	Tags   map[string]string `json:"tags"`
	Points []Point           `json:"points"`
}
