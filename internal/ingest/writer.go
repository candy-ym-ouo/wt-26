package ingest

import (
	"context"
	"fmt"

	"tsdb/internal/model"
	"tsdb/internal/storage"
)

// Writer is the service boundary for validated metric ingestion.
type Writer struct {
	engine    *storage.Engine
	validator Validator
}

// NewWriter creates a write service backed by an engine.
func NewWriter(engine *storage.Engine) *Writer {
	return &Writer{engine: engine, validator: NewValidator(engine.Config().Retention)}
}

// Write validates and persists an entire batch.
func (w *Writer) Write(input model.IngestBatch) (int, error) {
	if err := w.engine.RequestContextErr(); err != nil {
		return 0, err
	}
	batch := FromModel(input)
	if err := w.validator.Validate(batch); err != nil {
		return 0, err
	}
	series := w.engine.RegisterSeries(batch.Metric, batch.Tags)
	if err := w.engine.AddPoints(series, batch.Points); err != nil {
		return 0, fmt.Errorf("write points: %w", err)
	}
	return batch.Count(), nil
}

func (w *Writer) WriteContext(ctx context.Context, input model.IngestBatch) (int, error) {
	w.engine.SetRequestContext(ctx)
	return w.Write(input)
}

// Validator exposes the configured validator for focused tests and diagnostics.
func (w *Writer) Validator() Validator { return w.validator }
