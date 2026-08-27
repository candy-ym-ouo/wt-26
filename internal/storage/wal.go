package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"tsdb/internal/model"
)

var walMagic = [4]byte{'W', 'A', 'L', '1'}

// WALRecord contains enough metadata to restore an index after a crash.
//
// The shard lifecycle stage is not carried by WAL records: it is the
// authority of the on-disk metadata, and replay only restores points into the
// covering shard. Carrying the write-time stage here previously let a late
// write's "active" stamp overwrite a persisted "closed"/"compacted" stage on
// replay, breaking the lifecycle across restart. The State field is retained
// for decoding older log files but is no longer written or applied.
type WALRecord struct {
	ShardID uint64        `json:"shard"`
	State   ShardState    `json:"state,omitempty"`
	Series  model.Series  `json:"series"`
	Points  []model.Point `json:"points"`
}

// WAL is a length-delimited, checksummed append-only log.
type WAL struct {
	mu   sync.Mutex
	file *os.File
	sync bool
}

// OpenWAL opens or creates the log at path.
func OpenWAL(path string, syncWrites bool) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &WAL{file: file, sync: syncWrites}, nil
}

// Append writes one durable record before its points enter the memtable.
func (w *WAL) Append(record WALRecord) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal wal record: %w", err)
	}
	if len(payload) > 64<<20 {
		return errors.New("wal record exceeds maximum size")
	}
	var frame bytes.Buffer
	frame.Write(walMagic[:])
	_ = binary.Write(&frame, binary.BigEndian, uint32(len(payload)))
	frame.Write(payload)
	_ = binary.Write(&frame, binary.BigEndian, crc32.ChecksumIEEE(payload))
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.file.Write(frame.Bytes()); err != nil {
		return fmt.Errorf("append wal: %w", err)
	}
	if w.sync {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("sync wal: %w", err)
		}
	}
	return nil
}

// Replay reads valid records and removes a partial trailing frame.
func (w *WAL) Replay(visit func(WALRecord) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	reader, validEnd := bufio.NewReader(w.file), int64(0)
	for {
		var magic [4]byte
		if _, err := io.ReadFull(reader, magic[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}
		if magic != walMagic {
			return errors.New("invalid wal magic")
		}
		var length uint32
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			break
		}
		if length > 64<<20 {
			return errors.New("invalid wal record length")
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			break
		}
		var checksum uint32
		if err := binary.Read(reader, binary.BigEndian, &checksum); err != nil {
			break
		}
		if crc32.ChecksumIEEE(payload) != checksum {
			return errors.New("wal checksum mismatch")
		}
		var record WALRecord
		if err := json.Unmarshal(payload, &record); err != nil {
			return fmt.Errorf("decode wal: %w", err)
		}
		if err := visit(record); err != nil {
			return err
		}
		validEnd += int64(length) + 12
	}
	if err := w.file.Truncate(validEnd); err != nil {
		return err
	}
	_, err := w.file.Seek(0, io.SeekEnd)
	return err
}

// Truncate clears records after all active data has been flushed and catalogued.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	_, err := w.file.Seek(0, io.SeekStart)
	return err
}

// Close releases the log file.
func (w *WAL) Close() error { w.mu.Lock(); defer w.mu.Unlock(); return w.file.Close() }
