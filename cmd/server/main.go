package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/cypherlabdev/data-normalizer-service/internal/messaging"
)

func main() {
	logger := zerolog.New(os.Stdout).
		With().
		Timestamp().
		Str("service", "data-normalizer-service").
		Logger()

	log.Logger = logger
	logger.Info().Msg("data-normalizer-service starting")

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Kafka configuration
	kafkaBrokers := []string{"localhost:9092"}
	inputTopic := "raw_odds"        // From odds-adapter
	outputTopic := "normalized_odds" // To odds-optimizer
	groupID := "data-normalizer"

	// Create Kafka producer (to odds-optimizer)
	producer := messaging.NewKafkaProducer(kafkaBrokers, outputTopic, logger)
	defer producer.Close()

	// Create Kafka consumer (from odds-adapter)
	consumer := messaging.NewKafkaConsumer(
		kafkaBrokers,
		inputTopic,
		groupID,
		producer,
		logger,
	)
	defer consumer.Close()

	// Start Kafka consumer in goroutine
	go func() {
		if err := consumer.Start(ctx); err != nil {
			logger.Error().Err(err).Msg("Kafka consumer failed")
		}
	}()

	// Start HTTP server for health checks and metrics
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/ready", readyHandler)
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:         ":9093",
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		logger.Info().Int("port", 9093).Msg("HTTP server listening")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("HTTP server failed")
		}
	}()

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	logger.Info().Msg("shutting down gracefully...")

	// Cancel context to stop consumer
	cancel()

	// Shutdown HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("HTTP server shutdown failed")
	}

	logger.Info().Msg("shutdown complete")
}

// healthHandler returns 200 if service is running
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// readyHandler returns 200 if service is ready to accept traffic
func readyHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("READY"))
}
