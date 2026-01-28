package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-message/internal/config"
	"go-message/internal/kafka"
	"go-message/internal/observability"
	"go-message/pkg/models"

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

	logger.Info("Starting Kafka Producer")

	// Initialize metrics
	metrics := observability.NewInMemoryMetrics()

	// Create Kafka client for health checks
	client := kafka.NewKafkaClient(cfg.Kafka.Brokers, 5, logger)

	// Verify connectivity
	ctx := context.Background()
	if err := client.HealthCheck(ctx); err != nil {
		logger.Error("Failed to connect to Kafka", zap.Error(err))
	}
	logger.Info("Successfully connected to Kafka")

	// Create producer
	producerCfg := kafka.ProducerConfig{
		Brokers:     cfg.Kafka.Brokers,
		Acks:        cfg.Producer.Acks,
		Retries:     cfg.Producer.Retries,
		Idempotent:  cfg.Producer.Idempotent,
		MaxRetries:  cfg.Producer.Retries,
		BaseBackoff: 100 * time.Millisecond,
		Metrics:     metrics,
		Logger:      logger,
	}

	producer := kafka.NewProducer(producerCfg)
	defer producer.Close()

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info("Received shutdown signal")
		cancel()
	}()

	// Produce messages periodically
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	messageCount := 0

	for {
		select {
		case <-ctx.Done():
			logger.Info("Shutting down producer")

			// Print metrics
			logger.Info("Final metrics",
				zap.Any("", metrics.GetPublished()),
				zap.Any("failed", metrics.GetPublishFailed()),
			)

			return

		case <-ticker.C:
			messageCount++

			// Create sample message
			data := map[string]interface{}{
				"user_id":   messageCount,
				"action":    "sample_event",
				"timestamp": time.Now().Unix(),
				"metadata": map[string]string{
					"version": "1.0",
					"source":  "producer-cli",
				},
			}

			value, err := json.Marshal(data)
			if err != nil {
				logger.Error("Failed to marshal message", zap.Error(err))
				continue
			}

			// Generate message ID for idempotency
			messageID := fmt.Sprintf("msg-%d-%d", time.Now().UnixNano(), messageCount)

			headers := map[string]string{
				models.HeaderMessageID: messageID,
				"producer":             "cli-producer",
				"timestamp":            time.Now().Format(time.RFC3339),
			}

			// Publish message
			key := fmt.Sprintf("user-%d", messageCount)
			err = producer.Publish(ctx, cfg.Producer.Topic, key, value, headers)
			if err != nil {
				logger.Error("Failed to publish message", zap.Error(err))
			} else {
				logger.Info("Message published",
					zap.Any("topic", cfg.Producer.Topic),
					zap.Any("key", key),
					zap.Any("message_id", messageID),
				)
			}

			// Print current metrics
			logger.Debug("Message published",
				zap.Any("published", metrics.GetPublished()),
				zap.Any("failed", metrics.GetPublishFailed()),
			)
		}
	}
}
