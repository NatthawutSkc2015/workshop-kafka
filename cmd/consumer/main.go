package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-message/internal/config"
	"go-message/internal/kafka"
	"go-message/internal/observability"
	"go-message/internal/service"
)

func main() {
	// Load configuration
	cfg := config.Load()

	// Initialize logger
	observability.InitLogger(cfg.Logging.Level)
	logger := observability.GetLogger()

	logger.Info("Starting Kafka Consumer")

	// Initialize metrics
	metrics := observability.NewInMemoryMetrics()

	// Create Kafka client for health checks
	client := kafka.NewKafkaClient(cfg.Kafka.Brokers, 5)

	// Verify connectivity
	ctx := context.Background()
	if err := client.HealthCheck(ctx); err != nil {
		logger.WithError(err).Fatal("Failed to connect to Kafka")
	}
	logger.Info("Successfully connected to Kafka")

	// Create producer for retry/DLQ
	producerCfg := kafka.ProducerConfig{
		Brokers:     cfg.Kafka.Brokers,
		Acks:        -1,
		Retries:     3,
		Idempotent:  true,
		MaxRetries:  3,
		BaseBackoff: 100 * time.Millisecond,
		Metrics:     metrics,
	}
	producer := kafka.NewProducer(producerCfg)
	defer producer.Close()

	// Create consumer
	consumerCfg := kafka.ConsumerConfig{
		Brokers:          cfg.Kafka.Brokers,
		Topic:            cfg.Consumer.Topic,
		GroupID:          cfg.Consumer.GroupID,
		Workers:          cfg.Consumer.Workers,
		RetryMax:         cfg.Consumer.RetryMax,
		FetchMinBytes:    cfg.Consumer.FetchMinBytes,
		FetchMaxBytes:    cfg.Consumer.FetchMaxBytes,
		RetryTopicPrefix: cfg.Consumer.RetryTopicPrefix,
		DLQTopic:         cfg.Consumer.DLQTopic,
		Metrics:          metrics,
		DedupeStore:      kafka.NewInMemoryDedupeStore(1 * time.Hour),
	}

	consumer := kafka.NewConsumer(consumerCfg, producer)
	defer consumer.Close()

	// Create message processor
	processor := service.NewMessageProcessor()

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info("Received shutdown signal, initiating graceful shutdown")
		cancel()
	}()

	// Start health check loop in background
	go client.HealthCheckLoop(ctx, 30*time.Second, func() error {
		logger.Info("Reconnection callback triggered")
		return nil
	})

	// Start metrics reporter
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				logger.WithFields(map[string]interface{}{
					"received":    metrics.GetReceived(),
					"processed":   metrics.GetProcessed(),
					"failed":      metrics.GetFailed(),
					"retried":     metrics.GetRetried(),
					"sent_to_dlq": metrics.GetSentToDLQ(),
				}).Info("Consumer metrics")
			}
		}
	}()

	// Start consuming
	logger.Info("Starting message consumption")
	if err := consumer.Start(ctx, processor.Process); err != nil {
		logger.WithError(err).Error("Consumer error")
	}

	logger.Info("Consumer stopped")

	// Print final metrics
	logger.WithFields(map[string]interface{}{
		"received":    metrics.GetReceived(),
		"processed":   metrics.GetProcessed(),
		"failed":      metrics.GetFailed(),
		"retried":     metrics.GetRetried(),
		"sent_to_dlq": metrics.GetSentToDLQ(),
	}).Info("Final metrics")
}
