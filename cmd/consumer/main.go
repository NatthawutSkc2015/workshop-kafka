package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-message/internal/config"
	"go-message/internal/kafka"
	"go-message/internal/observability"
	"go-message/internal/service"

	"go.uber.org/zap"
)

func main() {
	// Load configuration
	cfg := config.Load()

	// Initialize logger
	logger, err := observability.NewLogger(cfg.Env)
	if err != nil {
		log.Printf("Error logger : %+v", err.Error())
		return
	}

	logger.Info("Starting Kafka Consumer")

	// Initialize metrics
	metrics := observability.NewInMemoryMetrics()

	// Create Kafka client for health checks
	client := kafka.NewKafkaClient(cfg.Kafka.Brokers, 5, logger)

	// Verify connectivity
	ctx := context.Background()
	if err := client.HealthCheck(ctx); err != nil {
		logger.Error("Failed to connect to Kafka")
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
		Logger:      logger,
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
		Logger:           logger,
	}

	consumer := kafka.NewConsumer(consumerCfg, producer)
	defer consumer.Close()

	// Create message processor
	processor := service.NewMessageProcessor(logger)

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
				logger.Info("Consumer metrics",
					zap.Any("received", metrics.GetReceived()),
					zap.Any("processed", metrics.GetProcessed()),
					zap.Any("failed", metrics.GetFailed()),
					zap.Any("retried", metrics.GetRetried()),
					zap.Any("sent_to_dlq", metrics.GetSentToDLQ()),
				)
			}
		}
	}()

	// Start consuming
	logger.Info("Starting message consumption")
	if err := consumer.Start(ctx, processor.Process); err != nil {
		logger.Error("Consumer error", zap.Error(err))
	}

	logger.Info("Consumer stopped")

	// Print final metrics
	logger.Info("Final metrics",
		zap.Any("received", metrics.GetReceived()),
		zap.Any("processed", metrics.GetProcessed()),
		zap.Any("failed", metrics.GetFailed()),
		zap.Any("retried", metrics.GetRetried()),
		zap.Any("sent_to_dlq", metrics.GetSentToDLQ()),
	)
}
