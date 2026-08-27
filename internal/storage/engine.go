package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"tsdb/internal/config"
	"tsdb/internal/model"
)

type metaFile struct {
	Series []model.Series `json:"series"`
	Shards []metaShard    `json:"shards"`
}

type metaShard struct {
	ID       uint64     `json:"id"`
	Start    int64      `json:"start"`
	End      int64      `json:"end"`
	State    ShardState `json:"state"`
	Segments []string   `json:"segments"`
}

// Status is a stable snapshot used by the status endpoint.
type Status struct {
	UptimeSeconds int64          `json:"uptime_seconds"`
	Config        map[string]any `json:"config"`
	Shards        map[string]int `json:"shards"`
	Series        int            `json:"series"`
	PointsWritten int64          `json:"points_written"`
	PointsMemory  int64          `json:"points_in_memory"`
	QueriesTotal  int64          `json:"queries_total"`
	WriteErrors   int64          `json:"write_errors"`
	DiskBytes     int64          `json:"disk_bytes"`
}

// Engine coordinates the series index, time shards, WAL, and maintenance.
type Engine struct {
	cfg       config.Config
	index     *Index
	wal       *WAL
	mu        sync.RWMutex
	shards    map[int64]*Shard
	starts    []int64
	started   time.Time
	ready     atomic.Bool
	written   atomic.Int64
	queries   atomic.Int64
	writeErrs atomic.Int64
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// NewEngine opens persisted metadata, replays the WAL, and starts maintenance.
func NewEngine(cfg config.Config) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	wal, err := OpenWAL(filepath.Join(cfg.DataDir, "wal.log"), cfg.WALSync)
	if err != nil {
		return nil, err
	}
	engine := &Engine{
		cfg: cfg, index: NewIndex(), wal: wal, shards: make(map[int64]*Shard),
		started: time.Now(), stop: make(chan struct{}), done: make(chan struct{}),
	}
	if err := engine.loadMeta(); err != nil {
		_ = wal.Close()
		return nil, err
	}
	if err := wal.Replay(engine.replayRecord); err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("replay wal: %w", err)
	}
	engine.ready.Store(true)
	go engine.maintenanceLoop()
	return engine, nil
}

// Config returns a copy of engine settings.
func (e *Engine) Config() config.Config { return e.cfg }

// Ready reports whether startup and recovery completed.
func (e *Engine) Ready() bool { return e.ready.Load() }

// RegisterSeries resolves a metric and tag set to a stable series.
func (e *Engine) RegisterSeries(name string, tags map[string]string) model.Series {
	return e.index.Register(name, tags)
}

// AddPoints writes a validated series batch using WAL-before-memory ordering.
func (e *Engine) AddPoints(series model.Series, points []model.Point) error {
	groups := make(map[int64][]model.Point)
	for _, point := range points {
		start := shardStart(point.Ts, e.cfg.ShardDuration.Milliseconds())
		groups[start] = append(groups[start], point)
	}
	starts := make([]int64, 0, len(groups))
	for start := range groups {
		starts = append(starts, start)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	for _, start := range starts {
		shard := e.getOrCreateShard(start)
		record := WALRecord{ShardID: shard.ID, Series: series, Points: groups[start]}
		if err := e.wal.Append(record); err != nil {
			e.writeErrs.Add(1)
			return err
		}
		if err := shard.Insert(series.ID, groups[start]); err != nil {
			e.writeErrs.Add(1)
			return err
		}
		if shard.MemoryBytes() >= e.cfg.FlushBytes {
			if err := shard.Flush(); err != nil {
				e.writeErrs.Add(1)
				return err
			}
		}
		e.written.Add(int64(len(groups[start])))
	}
	return nil
}

// QueryPoints reads and merges a series across every overlapping shard.
func (e *Engine) QueryPoints(seriesID uint64, start, end int64) []model.Point {
	e.queries.Add(1)
	e.mu.RLock()
	shards := make([]*Shard, 0, len(e.starts))
	for _, shardStart := range e.starts {
		shard := e.shards[shardStart]
		if shard.End > start && shard.Start <= end {
			shards = append(shards, shard)
		}
	}
	e.mu.RUnlock()
	points := make([]model.Point, 0)
	for _, shard := range shards {
		points = append(points, shard.Read(seriesID, start, end)...)
	}
	return model.SortPoints(points)
}

// FindSeries uses exact metric and tag matching.
func (e *Engine) FindSeries(name string, tags map[string]string, limit int) []model.Series {
	return e.index.Filter(name, tags, limit)
}

// ListMetrics returns exact point totals grouped by metric name.
func (e *Engine) ListMetrics() []model.MetricInfo {
	all := e.index.All()
	byName := make(map[string]*model.MetricInfo)
	for _, series := range all {
		item := byName[series.Name]
		if item == nil {
			item = &model.MetricInfo{Name: series.Name}
			byName[series.Name] = item
		}
		item.Series++
		item.Points += e.seriesPointCount(series.ID)
	}
	result := make([]model.MetricInfo, 0, len(byName))
	for _, item := range byName {
		result = append(result, *item)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// Tags returns all known label values for a metric.
func (e *Engine) Tags(metric string) map[string][]string { return e.index.Tags(metric) }

// Status gathers engine counters and storage usage.
func (e *Engine) Status() Status {
	counts := map[string]int{"total": 0, "active": 0, "closed": 0, "compacted": 0, "dropped": 0}
	var memory int64
	e.mu.RLock()
	for _, shard := range e.shards {
		counts["total"]++
		counts[string(shard.State)]++
		memory += shard.MemoryPoints()
	}
	e.mu.RUnlock()
	return Status{
		UptimeSeconds: int64(time.Since(e.started).Seconds()),
		Config:        map[string]any{"data_dir": e.cfg.DataDir, "shard_duration_ms": e.cfg.ShardDuration.Milliseconds(), "retention": e.cfg.Retention.String()},
		Shards:        counts, Series: e.index.Count(), PointsWritten: e.written.Load(),
		PointsMemory: memory, QueriesTotal: e.queries.Load(), WriteErrors: e.writeErrs.Load(), DiskBytes: directorySize(e.cfg.DataDir),
	}
}

// Close flushes all shards, persists metadata, clears the WAL, and stops workers.
func (e *Engine) Close() error {
	var closeErr error
	e.closeOnce.Do(func() {
		e.ready.Store(false)
		close(e.stop)
		<-e.done
		e.mu.RLock()
		shards := append([]*Shard(nil), e.orderedShardsLocked()...)
		e.mu.RUnlock()
		for _, shard := range shards {
			if err := shard.Flush(); err != nil && closeErr == nil {
				closeErr = err
			}
		}
		if closeErr == nil {
			closeErr = e.saveMeta()
		}
		if closeErr == nil {
			closeErr = e.wal.Truncate()
		}
		if err := e.wal.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	})
	return closeErr
}

func (e *Engine) getOrCreateShard(start int64) *Shard {
	e.mu.Lock()
	defer e.mu.Unlock()
	if shard := e.shards[start]; shard != nil {
		if shard.State != ShardActive {
			shard.Activate()
		}
		return shard
	}
	end := start + e.cfg.ShardDuration.Milliseconds()
	shard := NewShard(uint64(start), start, end, filepath.Join(e.cfg.DataDir, fmt.Sprintf("shard-%d", start)))
	e.shards[start] = shard
	e.starts = append(e.starts, start)
	sort.Slice(e.starts, func(i, j int) bool { return e.starts[i] < e.starts[j] })
	return shard
}

func (e *Engine) replayRecord(record WALRecord) error {
	e.index.Restore(record.Series)
	shard := e.getOrCreateShard(int64(record.ShardID))
	return shard.Insert(record.Series.ID, record.Points)
}

func (e *Engine) maintenanceLoop() {
	ticker := time.NewTicker(e.cfg.MaintenanceInterval)
	defer func() { ticker.Stop(); close(e.done) }()
	for {
		select {
		case <-ticker.C:
			e.maintain(time.Now().UnixMilli())
		case <-e.stop:
			return
		}
	}
}

func (e *Engine) maintain(now int64) {
	e.mu.RLock()
	shards := append([]*Shard(nil), e.orderedShardsLocked()...)
	e.mu.RUnlock()
	for _, shard := range shards {
		if shard.State == ShardActive && shard.End <= now {
			_ = shard.Close()
		}
		if shard.State == ShardClosed {
			_ = Compact(shard)
		}
		if shard.State == ShardCompacted && shard.End < now-e.cfg.Retention.Milliseconds()/2 {
			_ = Downsample(shard, time.Minute.Milliseconds())
		}
		if shard.End < now-e.cfg.Retention.Milliseconds() {
			e.dropShard(shard)
		}
	}
	_ = e.saveMeta()
}

func (e *Engine) dropShard(target *Shard) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.shards, target.Start)
	for index, start := range e.starts {
		if start == target.Start {
			e.starts = append(e.starts[:index], e.starts[index+1:]...)
			break
		}
	}
	_ = os.RemoveAll(target.Dir)
}

func (e *Engine) seriesPointCount(id uint64) int64 {
	e.mu.RLock()
	shards := e.orderedShardsLocked()
	e.mu.RUnlock()
	var count int64
	for _, shard := range shards {
		count += int64(len(shard.Read(id, shard.Start, shard.End-1)))
	}
	return count
}

func (e *Engine) orderedShardsLocked() []*Shard {
	result := make([]*Shard, 0, len(e.starts))
	for _, start := range e.starts {
		result = append(result, e.shards[start])
	}
	return result
}

func (e *Engine) loadMeta() error {
	raw, err := os.ReadFile(filepath.Join(e.cfg.DataDir, "meta.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read metadata: %w", err)
	}
	var meta metaFile
	if err := json.Unmarshal(raw, &meta); err != nil {
		return fmt.Errorf("decode metadata: %w", err)
	}
	for _, series := range meta.Series {
		e.index.Restore(series)
	}
	for _, saved := range meta.Shards {
		shard := NewShard(saved.ID, saved.Start, saved.End, filepath.Join(e.cfg.DataDir, fmt.Sprintf("shard-%d", saved.Start)))
		shard.State = saved.State
		for _, name := range saved.Segments {
			segment, err := OpenSegment(filepath.Join(shard.Dir, name))
			if err != nil {
				return fmt.Errorf("open segment %s: %w", name, err)
			}
			shard.AddSegment(segment)
		}
		e.shards[shard.Start] = shard
		e.starts = append(e.starts, shard.Start)
	}
	sort.Slice(e.starts, func(i, j int) bool { return e.starts[i] < e.starts[j] })
	return nil
}

func (e *Engine) saveMeta() error {
	meta := metaFile{Series: e.index.All()}
	e.mu.RLock()
	for _, shard := range e.orderedShardsLocked() {
		meta.Shards = append(meta.Shards, metaShard{ID: shard.ID, Start: shard.Start, End: shard.End, State: shard.State, Segments: shard.SegmentPaths()})
	}
	e.mu.RUnlock()
	raw, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	temporary := filepath.Join(e.cfg.DataDir, "meta.json.tmp")
	if err := os.WriteFile(temporary, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(temporary, filepath.Join(e.cfg.DataDir, "meta.json"))
}

func shardStart(timestamp, duration int64) int64 {
	start := timestamp - timestamp%duration
	if timestamp < 0 && timestamp%duration != 0 {
		start -= duration
	}
	return start
}

func directorySize(root string) int64 {
	var total int64
	_ = filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}
