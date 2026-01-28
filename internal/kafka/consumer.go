package kafka

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"go-message/internal/observability"

	"go-message/pkg/models"

	kafka "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// MessageHandler processes consumed messages
type MessageHandler func(ctx context.Context, msg *models.Message) error

// ConsumerClient defines the interface for Kafka consumer operations
type ConsumerClient interface {
	Start(ctx context.Context, handler MessageHandler) error
	Close() error
}

// Consumer implements ConsumerClient with worker pool and retry/DLQ logic
type Consumer struct {
	reader           *kafka.Reader
	producer         ProducerClient
	logger           *zap.Logger
	metrics          observability.MetricsCollector
	workers          int
	retryMax         int
	retryTopicPrefix string
	dlqTopic         string
	dedupeStore      DedupeStore
	wg               sync.WaitGroup
}

type ConsumerConfig struct {
	Brokers          []string
	Topic            string
	GroupID          string
	Workers          int
	RetryMax         int
	FetchMinBytes    int
	FetchMaxBytes    int
	RetryTopicPrefix string
	DLQTopic         string
	Metrics          observability.MetricsCollector
	DedupeStore      DedupeStore
	Logger           *zap.Logger
}

// DedupeStore provides interface for message deduplication
type DedupeStore interface {
	Exists(messageID string) bool
	Add(messageID string) error
}

// InMemoryDedupeStore is a simple in-memory implementation
type InMemoryDedupeStore struct {
	mu    sync.RWMutex
	store map[string]time.Time
	ttl   time.Duration
}

func NewInMemoryDedupeStore(ttl time.Duration) *InMemoryDedupeStore {
	store := &InMemoryDedupeStore{
		store: make(map[string]time.Time),
		ttl:   ttl,
	}
	go store.cleanup()
	return store
}

func (s *InMemoryDedupeStore) Exists(messageID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.store[messageID]
	return exists
}

func (s *InMemoryDedupeStore) Add(messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[messageID] = time.Now().Add(s.ttl)
	return nil
}

func (s *InMemoryDedupeStore) cleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		now := time.Now()
		for id, expiry := range s.store {
			if now.After(expiry) {
				delete(s.store, id)
			}
		}
		s.mu.Unlock()
	}
}

func NewConsumer(cfg ConsumerConfig, producer ProducerClient) *Consumer {
	if cfg.Metrics == nil {
		cfg.Metrics = observability.NewInMemoryMetrics()
	}
	if cfg.DedupeStore == nil {
		cfg.DedupeStore = NewInMemoryDedupeStore(1 * time.Hour)
	}
	if cfg.Workers == 0 {
		cfg.Workers = 5
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       cfg.FetchMinBytes,
		MaxBytes:       cfg.FetchMaxBytes,
		CommitInterval: 0, // Manual commits
		StartOffset:    kafka.LastOffset,
	})

	return &Consumer{
		reader:           reader,
		producer:         producer,
		logger:           cfg.Logger,
		metrics:          cfg.Metrics,
		workers:          cfg.Workers,
		retryMax:         cfg.RetryMax,
		retryTopicPrefix: cfg.RetryTopicPrefix,
		dlqTopic:         cfg.DLQTopic,
		dedupeStore:      cfg.DedupeStore,
	}
}

// Start begins consuming messages with worker pool
func (c *Consumer) Start(ctx context.Context, handler MessageHandler) error {
	c.logger.Info("Starting consumer", zap.Any("workers", c.workers))

	// Create worker pool
	msgChan := make(chan kafka.Message, c.workers*2)

	// Start workers
	for i := 0; i < c.workers; i++ {
		c.wg.Add(1)
		go c.worker(ctx, i, msgChan, handler)
	}

	// Start message fetcher
	c.wg.Add(1)
	go c.fetcher(ctx, msgChan)

	// Wait for all workers to finish
	c.wg.Wait()
	return nil
}

// fetcher reads messages from Kafka and sends to worker pool
func (c *Consumer) fetcher(ctx context.Context, msgChan chan<- kafka.Message) {
	defer c.wg.Done()
	defer close(msgChan)

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Fetcher stopping due to context cancellation")
			return
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if err == context.Canceled {
				return
			}
			c.logger.Error("Failed to fetch message", zap.Error(err))
			continue
		}

		c.metrics.IncReceived()

		select {
		case msgChan <- msg:
		case <-ctx.Done():
			return
		}
	}
}

// worker processes messages from the channel
func (c *Consumer) worker(ctx context.Context, id int, msgChan <-chan kafka.Message, handler MessageHandler) {
	defer c.wg.Done()
	c.logger.Info("Worker started", zap.Any("worker_id", id))

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Worker stopping due to context cancellation")
			return
		case msg, ok := <-msgChan:
			if !ok {
				c.logger.Info("Worker stopping - channel closed")
				return
			}

			c.processMessage(ctx, msg, handler, id)
		}
	}
}

// processMessage handles message processing with retry and DLQ logic
func (c *Consumer) processMessage(ctx context.Context, kafkaMsg kafka.Message, handler MessageHandler, workderID int) {
	// Convert to internal message format
	msg := c.toInternalMessage(kafkaMsg)

	logger := c.logger.With(
		zap.Any("topic", kafkaMsg.Topic),
		zap.Any("partition", kafkaMsg.Partition),
		zap.Any("offset", kafkaMsg.Offset),
		zap.Any("message_id", msg.Headers[models.HeaderMessageID]),
		zap.Any("worker_id", workderID),
	)

	// Check for duplicates using message ID
	if msgID, ok := msg.Headers[models.HeaderMessageID]; ok {
		if c.dedupeStore.Exists(msgID) {
			logger.Info("Duplicate message detected, skipping")
			c.commitMessage(kafkaMsg)
			return
		}
	}

	// Get current retry count
	retryCount := c.getRetryCount(msg)

	// Process message
	err := handler(ctx, msg)
	if err == nil {
		c.metrics.IncProcessed()
		logger.Debug("Message processed successfully")

		// Add to dedupe store
		if msgID, ok := msg.Headers[models.HeaderMessageID]; ok {
			c.dedupeStore.Add(msgID)
		}

		c.commitMessage(kafkaMsg)
		return
	}

	// Processing failed
	c.metrics.IncFailed()
	logger.Error("Message processing failed", zap.Error(err))

	// Check if we should retry or send to DLQ
	if retryCount < c.retryMax {
		c.sendToRetry(ctx, msg, retryCount+1, err)
		c.commitMessage(kafkaMsg) // Commit original message
	} else {
		c.sendToDLQ(ctx, msg, err)
		c.commitMessage(kafkaMsg) // Commit original message
	}
}

// commitMessage commits the message offset
func (c *Consumer) commitMessage(msg kafka.Message) {
	if err := c.reader.CommitMessages(context.Background(), msg); err != nil {
		c.logger.Error("Failed to commit message", zap.Error(err))
	}
}

// sendToRetry sends message to retry topic
func (c *Consumer) sendToRetry(ctx context.Context, msg *models.Message, retryCount int, failureErr error) {
	c.metrics.IncRetried()

	retryTopic := fmt.Sprintf("%s-%d", c.retryTopicPrefix, retryCount)

	// Add retry metadata to headers
	if msg.Headers == nil {
		msg.Headers = make(map[string]string)
	}
	msg.Headers[models.HeaderRetryCount] = strconv.Itoa(retryCount)
	msg.Headers[models.HeaderRetryAttempt] = strconv.Itoa(retryCount)
	msg.Headers[models.HeaderFailureReason] = failureErr.Error()

	err := c.producer.Publish(ctx, retryTopic, msg.Key, msg.Value, msg.Headers)
	if err != nil {
		c.logger.Error("Failed to send message to retry topic",
			zap.Any("topic", retryTopic),
			zap.Any("retry_count", retryCount),
			zap.Any("error", err.Error()),
		)
	} else {
		c.logger.Error("Message sent to retry topic",
			zap.Any("topic", retryTopic),
			zap.Any("retry_count", retryCount),
		)
	}
}

// sendToDLQ sends message to dead letter queue
func (c *Consumer) sendToDLQ(ctx context.Context, msg *models.Message, failureErr error) {
	c.metrics.IncSentToDLQ()

	// Add DLQ metadata to headers
	if msg.Headers == nil {
		msg.Headers = make(map[string]string)
	}
	msg.Headers[models.HeaderFailureReason] = failureErr.Error()
	msg.Headers[models.HeaderProcessedAt] = time.Now().Format(time.RFC3339)

	err := c.producer.Publish(ctx, c.dlqTopic, msg.Key, msg.Value, msg.Headers)
	if err != nil {
		c.logger.Error("Failed to send message to DLQ",
			zap.Any("topic", c.dlqTopic),
			zap.Any("error", err.Error()),
		)
	} else {
		c.logger.Info("Message sent to DLQ", zap.Any("topic", c.dlqTopic))
	}
}

// toInternalMessage converts Kafka message to internal format
func (c *Consumer) toInternalMessage(kafkaMsg kafka.Message) *models.Message {
	headers := make(map[string]string)
	for _, h := range kafkaMsg.Headers {
		headers[h.Key] = string(h.Value)
	}

	return &models.Message{
		Key:       string(kafkaMsg.Key),
		Value:     kafkaMsg.Value,
		Headers:   headers,
		Timestamp: kafkaMsg.Time,
	}
}

// getRetryCount extracts retry count from message headers
func (c *Consumer) getRetryCount(msg *models.Message) int {
	if countStr, ok := msg.Headers[models.HeaderRetryCount]; ok {
		if count, err := strconv.Atoi(countStr); err == nil {
			return count
		}
	}
	return 0
}

// Close gracefully shuts down the consumer
func (c *Consumer) Close() error {
	c.logger.Info("Closing consumer")
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("failed to close consumer: %w", err)
	}
	return nil
}
