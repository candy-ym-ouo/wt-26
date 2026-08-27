package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"

	"tsdb/internal/model"
)

var segmentMagic = [4]byte{'T', 'S', 'D', 'B'}

// Segment is an immutable on-disk snapshot loaded into a compact read map.
type Segment struct {
	Path    string
	ShardID uint64
	Data    map[uint64][]model.Point
}

// WriteSegment atomically publishes a binary segment file.
func WriteSegment(path string, shardID uint64, data map[uint64][]model.Point) (*Segment, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create segment directory: %w", err)
	}
	var payload bytes.Buffer
	payload.Write(segmentMagic[:])
	payload.WriteByte(1)
	_ = binary.Write(&payload, binary.BigEndian, shardID)
	ids := sortedSeriesIDs(data)
	_ = binary.Write(&payload, binary.BigEndian, uint32(len(ids)))
	for _, id := range ids {
		points := model.SortPoints(append([]model.Point(nil), data[id]...))
		_ = binary.Write(&payload, binary.BigEndian, id)
		_ = binary.Write(&payload, binary.BigEndian, uint32(len(points)))
		for _, point := range points {
			_ = binary.Write(&payload, binary.BigEndian, point.Ts)
			_ = binary.Write(&payload, binary.BigEndian, point.Value)
		}
	}
	checksum := crc32.ChecksumIEEE(payload.Bytes())
	_ = binary.Write(&payload, binary.BigEndian, checksum)
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, payload.Bytes(), 0o644); err != nil {
		return nil, fmt.Errorf("write segment: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return nil, fmt.Errorf("publish segment: %w", err)
	}
	return OpenSegment(path)
}

// OpenSegment validates and loads a segment from disk.
func OpenSegment(path string) (*Segment, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(raw) < 21 {
		return nil, errors.New("segment is too short")
	}
	want := binary.BigEndian.Uint32(raw[len(raw)-4:])
	if crc32.ChecksumIEEE(raw[:len(raw)-4]) != want {
		return nil, errors.New("segment checksum mismatch")
	}
	reader := bytes.NewReader(raw[:len(raw)-4])
	var magic [4]byte
	_, _ = io.ReadFull(reader, magic[:])
	if magic != segmentMagic {
		return nil, errors.New("invalid segment magic")
	}
	version, _ := reader.ReadByte()
	if version != 1 {
		return nil, fmt.Errorf("unsupported segment version %d", version)
	}
	segment := &Segment{Path: path, Data: make(map[uint64][]model.Point)}
	var seriesCount uint32
	if err := binary.Read(reader, binary.BigEndian, &segment.ShardID); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.BigEndian, &seriesCount); err != nil {
		return nil, err
	}
	for n := uint32(0); n < seriesCount; n++ {
		var id uint64
		var pointCount uint32
		if err := binary.Read(reader, binary.BigEndian, &id); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.BigEndian, &pointCount); err != nil {
			return nil, err
		}
		points := make([]model.Point, pointCount)
		for index := range points {
			if err := binary.Read(reader, binary.BigEndian, &points[index].Ts); err != nil {
				return nil, err
			}
			if err := binary.Read(reader, binary.BigEndian, &points[index].Value); err != nil {
				return nil, err
			}
		}
		segment.Data[id] = points
	}
	return segment, nil
}

// ReadSeries returns segment points within an inclusive interval.
// The returned slice is a defensive copy so callers cannot mutate the
// segment's in-memory data through it.
func (s *Segment) ReadSeries(id uint64, start, end int64) []model.Point {
	return model.CopyPoints(model.FilterPoints(s.Data[id], start, end))
}

// PointCount reports the number of entries in this segment.
func (s *Segment) PointCount() int64 {
	var count int64
	for _, points := range s.Data {
		count += int64(len(points))
	}
	return count
}

func sortedSeriesIDs(data map[uint64][]model.Point) []uint64 {
	ids := make([]uint64, 0, len(data))
	for id := range data {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
