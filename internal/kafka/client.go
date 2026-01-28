package kafka

import (
	"context"
	"fmt"
	"math"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// KafkaClient manages Kafka connection with automatic reconnection
type KafkaClient struct {
	brokers     []string
	logger      *zap.Logger
	maxRetries  int
	baseBackoff time.Duration
	maxBackoff  time.Duration
}

func NewKafkaClient(brokers []string, maxRetries int, logger *zap.Logger) *KafkaClient {
	return &KafkaClient{
		brokers:     brokers,
		logger:      logger,
		maxRetries:  maxRetries,
		baseBackoff: 1 * time.Second,
		maxBackoff:  30 * time.Second,
	}
}

// HealthCheck verifies connectivity to Kafka brokers
func (c *KafkaClient) HealthCheck(ctx context.Context) error {
	conn, err := kafka.DialContext(ctx, "tcp", c.brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()

	// Fetch metadata to verify broker health
	_, err = conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("failed to read partitions: %w", err)
	}

	return nil
}

// HealthCheckLoop runs health checks periodically with reconnection logic
func (c *KafkaClient) HealthCheckLoop(ctx context.Context, interval time.Duration, onReconnect func() error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Health check loop stopped")
			return
		case <-ticker.C:
			if err := c.HealthCheck(ctx); err != nil {
				c.logger.Error("Health check failed, attempting reconnection", zap.Error(err))
				if err := c.reconnectWithBackoff(ctx, onReconnect); err != nil {
					c.logger.Error("Reconnection failed", zap.Error(err))
				}
			}
		}
	}
}

// reconnectWithBackoff implements exponential backoff reconnection strategy
func (c *KafkaClient) reconnectWithBackoff(ctx context.Context, onReconnect func() error) error {
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Calculate exponential backoff
		backoff := time.Duration(math.Min(
			float64(c.baseBackoff)*math.Pow(2, float64(attempt)),
			float64(c.maxBackoff),
		))

		c.logger.Info("Attempting reconnection",
			zap.Any("attempt", attempt+1),
			zap.Any("backoff", backoff),
		)

		// Wait before retry
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		// Try health check
		if err := c.HealthCheck(ctx); err != nil {
			c.logger.Error("Reconnection attempt failed", zap.Error(err))
			continue
		}

		// Call reconnect callback if provided
		if onReconnect != nil {
			if err := onReconnect(); err != nil {
				c.logger.Error("Reconnect callback failed", zap.Error(err))
				continue
			}
		}

		c.logger.Info("Reconnection successful")
		return nil
	}

	return fmt.Errorf("failed to reconnect after %d attempts", c.maxRetries)
}

// GetBrokers returns the list of brokers
func (c *KafkaClient) GetBrokers() []string {
	return c.brokers
}
