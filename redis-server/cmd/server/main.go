// cmd/server/main.go

package main

import (
	"fmt"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"syscall"

	"go-redis/redis-server/internal/config"
	"go-redis/redis-server/internal/logger"
	"go-redis/redis-server/internal/server"
)

func main() {
	// Parse configuration
	cfg, err := config.ParseFlags()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing config: %v\n", err)
		os.Exit(1)
	}

	// Setup logger
	log, err := logger.Setup(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error setting up logger: %v\n", err)
		os.Exit(1)
	}
	defer log.Sync()

	// Create and start server - passing cfg and logger
	srv := server.NewServer(cfg, log)
	if err := srv.Start(); err != nil {
		log.Fatal("Failed to start server",
			zap.Error(err),
		)
	}

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	<-sigChan
	log.Info("Shutting down server...")

	// Graceful shutdown
	if err := srv.Stop(); err != nil {
		log.Error("Error during shutdown",
			zap.Error(err),
		)
		os.Exit(1)
	}
}
