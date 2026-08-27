package storage

import (
	"sort"
	"sync"

	"tsdb/internal/model"
)

// Index owns stable series IDs and supports exact metric/tag filtering.
type Index struct {
	mu     sync.RWMutex
	byKey  map[string]uint64
	series map[uint64]model.Series
	byName map[string][]uint64
	nextID uint64
}

// NewIndex creates an empty metadata index.
func NewIndex() *Index {
	return &Index{
		byKey:  make(map[string]uint64),
		series: make(map[uint64]model.Series),
		byName: make(map[string][]uint64),
		nextID: 1,
	}
}

// Register returns the existing series or allocates a new stable ID.
func (i *Index) Register(name string, tags map[string]string) model.Series {
	i.mu.Lock()
	defer i.mu.Unlock()
	key := model.SeriesKey(name, tags)
	if id, ok := i.byKey[key]; ok {
		return cloneSeries(i.series[id])
	}
	series := model.Series{ID: i.nextID, Name: name, Tags: model.CloneTags(tags)}
	i.nextID++
	i.addLocked(series)
	return cloneSeries(series)
}

// Restore installs a persisted series with its original ID.
func (i *Index) Restore(series model.Series) {
	i.mu.Lock()
	defer i.mu.Unlock()
	key := model.SeriesKey(series.Name, series.Tags)
	if _, exists := i.byKey[key]; exists {
		return
	}
	series.Tags = model.CloneTags(series.Tags)
	i.addLocked(series)
	if series.ID >= i.nextID {
		i.nextID = series.ID + 1
	}
}

func (i *Index) addLocked(series model.Series) {
	key := model.SeriesKey(series.Name, series.Tags)
	i.byKey[key] = series.ID
	i.series[series.ID] = series
	i.byName[series.Name] = append(i.byName[series.Name], series.ID)
}

// Filter returns series matching the metric and all requested tags.
func (i *Index) Filter(name string, tags map[string]string, limit int) []model.Series {
	i.mu.RLock()
	defer i.mu.RUnlock()
	ids := i.byName[name]
	result := make([]model.Series, 0, len(ids))
	for _, id := range ids {
		series := i.series[id]
		if model.Matches(series, tags) {
			result = append(result, cloneSeries(series))
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}
	return result
}

// All returns every registered series sorted by ID.
func (i *Index) All() []model.Series {
	i.mu.RLock()
	defer i.mu.RUnlock()
	ids := make([]uint64, 0, len(i.series))
	for id := range i.series {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
	result := make([]model.Series, 0, len(ids))
	for _, id := range ids {
		result = append(result, cloneSeries(i.series[id]))
	}
	return result
}

// Tags enumerates known label values for one metric.
func (i *Index) Tags(name string) map[string][]string {
	series := i.Filter(name, nil, 0)
	sets := make(map[string]map[string]struct{})
	for _, item := range series {
		for key, value := range item.Tags {
			if sets[key] == nil {
				sets[key] = make(map[string]struct{})
			}
			sets[key][value] = struct{}{}
		}
	}
	result := make(map[string][]string, len(sets))
	for key, values := range sets {
		for value := range values {
			result[key] = append(result[key], value)
		}
		sort.Strings(result[key])
	}
	return result
}

// Count reports current series cardinality.
func (i *Index) Count() int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return len(i.series)
}

func cloneSeries(series model.Series) model.Series {
	series.Tags = model.CloneTags(series.Tags)
	return series
}
