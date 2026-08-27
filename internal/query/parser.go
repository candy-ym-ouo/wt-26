// Package query parses and executes HTTP query semantics.
package query

import (
	"fmt"
	"net/url"
	"strconv"
	"time"

	"tsdb/internal/model"
)

var validAggregations = map[string]bool{"avg": true, "max": true, "min": true, "sum": true, "count": true, "last": true, "p95": true}
var validFills = map[string]bool{"none": true, "zero": true, "prev": true}

// ParseRange validates URL values and returns a normalized range request.
func ParseRange(values url.Values) (model.QueryReq, error) {
	request := model.QueryReq{Metric: values.Get("metric"), Agg: values.Get("agg"), Fill: values.Get("fill"), Limit: 1000}
	if request.Metric == "" {
		return request, model.BadRequest("metric is required")
	}
	var err error
	request.Tags, err = model.ParseTags(values.Get("tags"))
	if err != nil {
		return request, model.BadRequest(err.Error())
	}
	if request.Start, err = parseTime(values.Get("start")); err != nil {
		return request, model.BadRequest("invalid start")
	}
	if request.End, err = parseTime(values.Get("end")); err != nil {
		return request, model.BadRequest("invalid end")
	}
	if request.End < request.Start {
		return request, model.BadRequest("end must not be before start")
	}
	if raw := values.Get("step"); raw != "" {
		request.Step, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || request.Step < 0 {
			return request, model.BadRequest("invalid step")
		}
	}
	if request.Agg == "" {
		request.Agg = "avg"
	}
	if !validAggregations[request.Agg] {
		return request, model.BadRequest("invalid aggregation")
	}
	if request.Fill == "" {
		request.Fill = "none"
	}
	if !validFills[request.Fill] {
		return request, model.BadRequest("invalid fill")
	}
	if raw := values.Get("limit"); raw != "" {
		request.Limit, err = strconv.Atoi(raw)
		if err != nil || request.Limit < 1 || request.Limit > 10000 {
			return request, model.BadRequest("invalid limit")
		}
	}
	if request.Step > 0 {
		request.Start = align(request.Start, request.Step)
	}
	return request, nil
}

// ParseInstant converts instant-query values into a range request.
func ParseInstant(values url.Values, now time.Time) (model.QueryReq, bool, error) {
	end := now.UnixMilli()
	var err error
	if raw := values.Get("start"); raw != "" {
		end, err = parseTime(raw)
		if err != nil {
			return model.QueryReq{}, false, model.BadRequest("invalid start")
		}
	}
	window := int64(60000)
	if raw := values.Get("window"); raw != "" {
		window, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || window <= 0 {
			return model.QueryReq{}, false, model.BadRequest("invalid window")
		}
	}
	copyValues := make(url.Values)
	for key, entries := range values {
		copyValues[key] = append([]string(nil), entries...)
	}
	copyValues.Set("start", strconv.FormatInt(end-window, 10))
	copyValues.Set("end", strconv.FormatInt(end, 10))
	copyValues.Set("step", "0")
	request, err := ParseRange(copyValues)
	return request, values.Get("agg") != "", err
}

func parseTime(raw string) (int64, error) {
	if raw == "" {
		return 0, fmt.Errorf("time is required")
	}
	if millis, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return millis, nil
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return 0, err
	}
	return parsed.UnixMilli(), nil
}

func align(value, step int64) int64 { return value - value%step }
