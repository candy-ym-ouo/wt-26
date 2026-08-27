package query

import (
	"tsdb/internal/model"
	"tsdb/internal/storage"
)

// Executor resolves candidate series and gathers their shard data.
type Executor struct {
	engine *storage.Engine
}

// NewExecutor creates a query service backed by an engine.
func NewExecutor(engine *storage.Engine) *Executor { return &Executor{engine: engine} }

// Range executes a normalized range request.
func (e *Executor) Range(request model.QueryReq) model.MatrixData {
	series, data := e.collect(request)
	return BuildMatrix(series, data, request)
}

// Instant executes a last-value or window-aggregation request.
func (e *Executor) Instant(request model.QueryReq, aggregate bool) model.VectorData {
	series, data := e.collect(request)
	return BuildVector(series, data, request, aggregate)
}

func (e *Executor) collect(request model.QueryReq) ([]model.Series, map[uint64][]model.Point) {
	series := e.engine.FindSeries(request.Metric, request.Tags, request.Limit)
	data := make(map[uint64][]model.Point, len(series))
	for _, item := range series {
		points := e.engine.QueryPoints(item.ID, request.Start, request.End)
		if len(points) > 0 {
			data[item.ID] = points
		}
	}
	return series, data
}
