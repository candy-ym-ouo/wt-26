package query

import "tsdb/internal/model"

// BuildMatrix converts collected point maps into the public matrix shape.
func BuildMatrix(series []model.Series, data map[uint64][]model.Point, request model.QueryReq) model.MatrixData {
	result := model.MatrixData{ResultType: "matrix", Result: make([]model.MatrixSeries, 0, len(series))}
	for _, item := range series {
		points := Bucket(data[item.ID], request)
		if len(points) == 0 {
			continue
		}
		values := make([]model.Sample, len(points))
		for index, point := range points {
			values[index] = model.Sample{Ts: point.Ts, Value: point.Value}
		}
		result.Result = append(result.Result, model.MatrixSeries{Metric: resultLabels(item), Values: values})
	}
	return result
}

// BuildVector selects or aggregates one instant value per matching series.
func BuildVector(series []model.Series, data map[uint64][]model.Point, request model.QueryReq, aggregate bool) model.VectorData {
	result := model.VectorData{ResultType: "vector", Result: make([]model.VectorSeries, 0, len(series))}
	for _, item := range series {
		points := data[item.ID]
		if len(points) == 0 {
			continue
		}
		point := points[len(points)-1]
		if aggregate {
			value, ok := Aggregate(request.Agg, points)
			if !ok {
				continue
			}
			point = model.Point{Ts: request.End, Value: value}
		}
		result.Result = append(result.Result, model.VectorSeries{
			Metric: resultLabels(item),
			Value:  model.Sample{Ts: point.Ts, Value: point.Value},
		})
	}
	return result
}

func resultLabels(series model.Series) map[string]string {
	labels := series.Tags
	labels["__name__"] = series.Name
	return labels
}
