package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-message/internal/config"
	"go-message/internal/kafka"
	"go-message/internal/observability"
	"go-message/pkg/models"
)

func main() {
	// Load configuration
	cfg := config.Load()

	// Initialize logger
	observability.InitLogger(cfg.Logging.Level)
	logger := observability.GetLogger()

	logger.Info("Starting Kafka Producer")

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

	// Create producer
	producerCfg := kafka.ProducerConfig{
		Brokers:     cfg.Kafka.Brokers,
		Acks:        cfg.Producer.Acks,
		Retries:     cfg.Producer.Retries,
		Idempotent:  cfg.Producer.Idempotent,
		MaxRetries:  cfg.Producer.Retries,
		BaseBackoff: 100 * time.Millisecond,
		Metrics:     metrics,
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
			logger.WithFields(map[string]interface{}{
				"published": metrics.GetPublished(),
				"failed":    metrics.GetPublishFailed(),
			}).Info("Final metrics")

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
				logger.WithError(err).Error("Failed to marshal message")
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
				logger.WithError(err).Error("Failed to publish message")
			} else {
				logger.WithFields(map[string]interface{}{
					"topic":      cfg.Producer.Topic,
					"key":        key,
					"message_id": messageID,
				}).Info("Message published")
			}

			// Print current metrics
			logger.WithFields(map[string]interface{}{
				"published": metrics.GetPublished(),
				"failed":    metrics.GetPublishFailed(),
			}).Debug("Current metrics")
		}
	}
}
