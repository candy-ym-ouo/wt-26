package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tsdb/internal/api"
	"tsdb/internal/config"
	"tsdb/internal/storage"
)

func main() {
	if err := run(); err != nil {
		log.Printf("tsdb stopped with error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	engine, err := storage.NewEngine(cfg)
	if err != nil {
		return err
	}
	server := api.NewServer(engine, cfg.Listen)
	serveErrors := make(chan error, 1)
	go func() {
		log.Printf("TSDB listening on %s with data directory %s", cfg.Listen, cfg.DataDir)
		serveErrors <- server.ListenAndServe()
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	select {
	case signal := <-signals:
		log.Printf("received %s, shutting down", signal)
	case err := <-serveErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			_ = engine.Close()
			return err
		}
	}
	context, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(context); err != nil {
		return err
	}
	return engine.Close()
}
