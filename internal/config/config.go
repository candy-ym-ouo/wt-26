// Package config defines command-line configuration for the TSDB server.
package config

import (
	"errors"
	"flag"
	"fmt"
	"time"
)

// Config contains all runtime settings shared by the server and engine.
type Config struct {
	DataDir             string
	Listen              string
	ShardDuration       time.Duration
	Retention           time.Duration
	FlushBytes          int64
	MaintenanceInterval time.Duration
	WALSync             bool
}

// Default returns a production-friendly local configuration.
func Default() Config {
	return Config{
		DataDir:             "./data",
		Listen:              ":8080",
		ShardDuration:       2 * time.Hour,
		Retention:           7 * 24 * time.Hour,
		FlushBytes:          8 << 20,
		MaintenanceInterval: time.Minute,
		WALSync:             true,
	}
}

// Load parses process flags and validates the result.
func Load() (Config, error) {
	cfg := Default()
	flag.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "directory used for database files")
	flag.StringVar(&cfg.Listen, "listen", cfg.Listen, "HTTP listen address")
	flag.DurationVar(&cfg.ShardDuration, "shard-duration", cfg.ShardDuration, "time covered by one shard")
	flag.DurationVar(&cfg.Retention, "retention", cfg.Retention, "raw data retention period")
	flag.Int64Var(&cfg.FlushBytes, "flush-bytes", cfg.FlushBytes, "memtable flush threshold")
	flag.DurationVar(&cfg.MaintenanceInterval, "maintenance-interval", cfg.MaintenanceInterval, "background maintenance interval")
	flag.BoolVar(&cfg.WALSync, "wal-sync", cfg.WALSync, "fsync every WAL record")
	flag.Parse()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate rejects settings that could make storage boundaries ambiguous.
func (c Config) Validate() error {
	if c.DataDir == "" {
		return errors.New("data-dir cannot be empty")
	}
	if c.Listen == "" {
		return errors.New("listen cannot be empty")
	}
	if c.ShardDuration <= 0 {
		return errors.New("shard-duration must be positive")
	}
	if c.Retention < c.ShardDuration {
		return fmt.Errorf("retention must be at least one shard duration")
	}
	if c.FlushBytes < 1024 {
		return errors.New("flush-bytes must be at least 1024")
	}
	if c.MaintenanceInterval <= 0 {
		return errors.New("maintenance-interval must be positive")
	}
	return nil
}
