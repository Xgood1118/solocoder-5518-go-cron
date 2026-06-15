package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/solo-coder/go-cron/internal/api"
	"github.com/solo-coder/go-cron/internal/auth"
	"github.com/solo-coder/go-cron/internal/scheduler"
	"github.com/solo-coder/go-cron/internal/storage"
	"github.com/solo-coder/go-cron/pkg/config"
	"github.com/solo-coder/go-cron/pkg/logger"
)

func main() {
	logger.Init("scheduler")
	cfg := config.LoadSchedulerConfig()

	store, err := storage.NewStore(cfg.DBPath)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize database")
	}
	log.Info().Str("db_path", cfg.DBPath).Msg("database initialized")

	authSvc := auth.NewService(cfg.JWTSecret)
	sched := scheduler.New(store, cfg)

	server := api.NewServer(store, sched, authSvc, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := sched.Start(ctx); err != nil {
			log.Error().Err(err).Msg("scheduler exited with error")
		}
	}()

	httpServer := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: server.Handler(),
	}

	go func() {
		log.Info().Str("port", cfg.Port).Msg("scheduler HTTP server starting")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("HTTP server failed")
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Info().Msg("shutdown signal received")

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("HTTP server shutdown error")
	}

	log.Info().Msg("scheduler shut down gracefully")
}
