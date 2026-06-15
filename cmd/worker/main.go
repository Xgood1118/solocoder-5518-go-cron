package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"

	"github.com/solo-coder/go-cron/internal/worker"
	"github.com/solo-coder/go-cron/pkg/config"
	"github.com/solo-coder/go-cron/pkg/logger"
)

func main() {
	logger.Init("worker")
	cfg := config.LoadWorkerConfig()

	w := worker.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Info().Msg("shutdown signal received")
		cancel()
	}()

	if err := w.Start(ctx); err != nil {
		log.Fatal().Err(err).Msg("worker exited with error")
	}

	log.Info().Msg("worker shut down gracefully")
}
