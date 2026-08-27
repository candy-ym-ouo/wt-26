package model

import (
	"fmt"
	"sort"
	"strings"
)

// Metric is the public write model: a name and optional dimensions.
type Metric struct {
	Name string            `json:"metric"`
	Tags map[string]string `json:"tags,omitempty"`
}

// SeriesKey builds a deterministic identity independent of map iteration order.
func SeriesKey(name string, tags map[string]string) string {
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var builder strings.Builder
	builder.WriteString(name)
	for _, key := range keys {
		builder.WriteByte(0)
		builder.WriteString(key)
		builder.WriteByte('=')
		builder.WriteString(tags[key])
	}
	return builder.String()
}

// ParseTags decodes the API's comma-separated key=value filter format.
func ParseTags(raw string) (map[string]string, error) {
	tags := make(map[string]string)
	if strings.TrimSpace(raw) == "" {
		return tags, nil
	}
	for _, item := range strings.Split(raw, ",") {
		pair := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(pair) != 2 || pair[0] == "" {
			return nil, fmt.Errorf("invalid tag filter %q", item)
		}
		if _, exists := tags[pair[0]]; exists {
			return nil, fmt.Errorf("duplicate tag %q", pair[0])
		}
		tags[pair[0]] = pair[1]
	}
	return tags, nil
}

// Labels converts a series to the query response label object.
func Labels(series Series) map[string]string {
	labels := CloneTags(series.Tags)
	labels["__name__"] = series.Name
	return labels
}

// Matches reports whether a series contains every requested label exactly.
func Matches(series Series, tags map[string]string) bool {
	for key, value := range tags {
		if series.Tags[key] != value {
			return false
		}
	}
	return true
}
