package kafka

import (
	"context"
	"fmt"
	"math"
	"time"

	"go-message/internal/observability"

	kafka "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// ProducerClient defines the interface for Kafka producer operations
type ProducerClient interface {
	Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error
	Close() error
}

// Producer implements ProducerClient with delivery guarantees and retry logic
type Producer struct {
	writer      *kafka.Writer
	logger      *zap.Logger
	metrics     observability.MetricsCollector
	maxRetries  int
	baseBackoff time.Duration
}

type ProducerConfig struct {
	Brokers     []string
	Acks        int // -1 for all, 0 for none, 1 for leader
	Retries     int
	Idempotent  bool
	MaxRetries  int
	BaseBackoff time.Duration
	Metrics     observability.MetricsCollector
	Logger      *zap.Logger
}

func NewProducer(cfg ProducerConfig) *Producer {
	if cfg.Metrics == nil {
		cfg.Metrics = observability.NewInMemoryMetrics()
	}
	if cfg.BaseBackoff == 0 {
		cfg.BaseBackoff = 100 * time.Millisecond
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}

	// Configure writer with delivery guarantees
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequiredAcks(cfg.Acks),
		MaxAttempts:            cfg.Retries,
		WriteTimeout:           10 * time.Second,
		ReadTimeout:            10 * time.Second,
		AllowAutoTopicCreation: false,
		Async:                  false, // Synchronous for reliable error handling
	}

	// Enable idempotent producer if requested
	if cfg.Idempotent {
		writer.RequiredAcks = kafka.RequireAll // Idempotent requires acks=all
		writer.MaxAttempts = 10                // Higher retries for idempotency
	}

	return &Producer{
		writer:      writer,
		logger:      cfg.Logger,
		metrics:     cfg.Metrics,
		maxRetries:  cfg.MaxRetries,
		baseBackoff: cfg.BaseBackoff,
	}
}

// Publish sends a message to Kafka with configurable retry logic
func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
		Time:  time.Now(),
	}

	// Convert headers
	if headers != nil {
		msg.Headers = make([]kafka.Header, 0, len(headers))
		for k, v := range headers {
			msg.Headers = append(msg.Headers, kafka.Header{
				Key:   k,
				Value: []byte(v),
			})
		}
	}

	// Retry logic with exponential backoff
	var lastErr error
	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		if attempt > 0 {
			// Calculate backoff
			backoff := time.Duration(math.Min(
				float64(p.baseBackoff)*math.Pow(2, float64(attempt-1)),
				float64(5*time.Second),
			))

			p.logger.Info("Retrying message publish",
				zap.Any("attempt", attempt),
				zap.Any("topic", topic),
				zap.Any("key", key),
				zap.Any("backoff", backoff),
			)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}

		err := p.writer.WriteMessages(ctx, msg)
		if err == nil {
			p.metrics.IncPublished()
			p.logger.Info("Message published successfully",
				zap.Any("topic", topic),
				zap.Any("key", key),
				zap.Any("attempt", attempt+1),
			)

			return nil
		}

		lastErr = err
		p.logger.Info("Failed to publish message",
			zap.Any("topic", topic),
			zap.Any("key", key),
			zap.Any("attempt", attempt+1),
			zap.String("error", err.Error()),
		)
	}

	p.metrics.IncPublishFailed()
	return fmt.Errorf("failed to publish message after %d attempts: %w", p.maxRetries+1, lastErr)
}

// Close gracefully shuts down the producer
func (p *Producer) Close() error {
	p.logger.Info("Closing producer")
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("failed to close producer: %w", err)
	}
	return nil
}
