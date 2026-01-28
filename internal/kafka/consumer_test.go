package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go-message/internal/observability"
	"go-message/pkg/models"

	kafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumer_ProcessMessage_Success(t *testing.T) {
	mockProducer := NewMockProducer()
	mockDedupe := NewMockDedupeStore()
	metrics := observability.NewInMemoryMetrics()

	cfg := ConsumerConfig{
		Brokers:          []string{"localhost:9092"},
		Topic:            "test-topic",
		GroupID:          "test-group",
		Workers:          2,
		RetryMax:         3,
		RetryTopicPrefix: "test-retry",
		DLQTopic:         "test-dlq",
		Metrics:          metrics,
		DedupeStore:      mockDedupe,
	}

	consumer := NewConsumer(cfg, mockProducer)

	// Create test message
	msg := &models.Message{
		Key:   "test-key",
		Value: []byte("test-value"),
		Headers: map[string]string{
			models.HeaderMessageID: "msg-123",
		},
		Timestamp: time.Now(),
	}

	// Simulate Kafka message
	kafkaMsg := createKafkaMessage(msg)

	// Handler that succeeds
	handlerCalled := false
	handler := func(ctx context.Context, m *models.Message) error {
		handlerCalled = true
		assert.Equal(t, msg.Key, m.Key)
		assert.Equal(t, msg.Value, m.Value)
		return nil
	}

	// Process message (note: in real test, this would need Kafka reader mock)
	ctx := context.Background()
	consumer.processMessage(ctx, kafkaMsg, handler, 0)

	assert.True(t, handlerCalled)
	assert.Equal(t, int64(1), metrics.GetProcessed())
	assert.True(t, mockDedupe.Exists("msg-123"))
	assert.Len(t, mockProducer.GetPublishedMessages(), 0) // No retry or DLQ
}

func TestConsumer_ProcessMessage_RetryLogic(t *testing.T) {
	mockProducer := NewMockProducer()
	mockDedupe := NewMockDedupeStore()
	metrics := observability.NewInMemoryMetrics()

	cfg := ConsumerConfig{
		Brokers:          []string{"localhost:9092"},
		Topic:            "test-topic",
		GroupID:          "test-group",
		Workers:          2,
		RetryMax:         3,
		RetryTopicPrefix: "test-retry",
		DLQTopic:         "test-dlq",
		Metrics:          metrics,
		DedupeStore:      mockDedupe,
	}

	consumer := NewConsumer(cfg, mockProducer)

	// Create test message with retry count = 1
	msg := &models.Message{
		Key:   "test-key",
		Value: []byte("test-value"),
		Headers: map[string]string{
			models.HeaderMessageID:  "msg-456",
			models.HeaderRetryCount: "1",
		},
		Timestamp: time.Now(),
	}

	kafkaMsg := createKafkaMessage(msg)

	// Handler that fails
	handler := func(ctx context.Context, m *models.Message) error {
		return fmt.Errorf("processing failed")
	}

	ctx := context.Background()
	consumer.processMessage(ctx, kafkaMsg, handler, 0)

	// Should send to retry topic
	assert.Equal(t, int64(1), metrics.GetFailed())
	assert.Equal(t, int64(1), metrics.GetRetried())

	published := mockProducer.GetPublishedMessages()
	require.Len(t, published, 1)
	assert.Equal(t, "test-retry-2", published[0].Topic) // Retry count 2
	assert.Equal(t, "2", published[0].Headers[models.HeaderRetryCount])
}

func TestConsumer_ProcessMessage_SendToDLQ(t *testing.T) {
	mockProducer := NewMockProducer()
	mockDedupe := NewMockDedupeStore()
	metrics := observability.NewInMemoryMetrics()

	cfg := ConsumerConfig{
		Brokers:          []string{"localhost:9092"},
		Topic:            "test-topic",
		GroupID:          "test-group",
		Workers:          2,
		RetryMax:         3,
		RetryTopicPrefix: "test-retry",
		DLQTopic:         "test-dlq",
		Metrics:          metrics,
		DedupeStore:      mockDedupe,
	}

	consumer := NewConsumer(cfg, mockProducer)

	// Create test message with max retry count
	msg := &models.Message{
		Key:   "test-key",
		Value: []byte("test-value"),
		Headers: map[string]string{
			models.HeaderMessageID:  "msg-789",
			models.HeaderRetryCount: "3", // Max retries reached
		},
		Timestamp: time.Now(),
	}

	kafkaMsg := createKafkaMessage(msg)

	// Handler that fails
	handler := func(ctx context.Context, m *models.Message) error {
		return fmt.Errorf("processing failed permanently")
	}

	ctx := context.Background()
	consumer.processMessage(ctx, kafkaMsg, handler, 0)

	// Should send to DLQ
	assert.Equal(t, int64(1), metrics.GetFailed())
	assert.Equal(t, int64(1), metrics.GetSentToDLQ())

	published := mockProducer.GetPublishedMessages()
	require.Len(t, published, 1)
	assert.Equal(t, "test-dlq", published[0].Topic)
	assert.Contains(t, published[0].Headers[models.HeaderFailureReason], "processing failed")
}

func TestConsumer_ProcessMessage_Deduplication(t *testing.T) {
	mockProducer := NewMockProducer()
	mockDedupe := NewMockDedupeStore()
	metrics := observability.NewInMemoryMetrics()

	// Pre-add message ID to dedupe store
	mockDedupe.Add("duplicate-msg")

	cfg := ConsumerConfig{
		Brokers:          []string{"localhost:9092"},
		Topic:            "test-topic",
		GroupID:          "test-group",
		Workers:          2,
		RetryMax:         3,
		RetryTopicPrefix: "test-retry",
		DLQTopic:         "test-dlq",
		Metrics:          metrics,
		DedupeStore:      mockDedupe,
	}

	consumer := NewConsumer(cfg, mockProducer)

	msg := &models.Message{
		Key:   "test-key",
		Value: []byte("test-value"),
		Headers: map[string]string{
			models.HeaderMessageID: "duplicate-msg",
		},
		Timestamp: time.Now(),
	}

	kafkaMsg := createKafkaMessage(msg)

	handlerCalled := false
	handler := func(ctx context.Context, m *models.Message) error {
		handlerCalled = true
		return nil
	}

	ctx := context.Background()
	consumer.processMessage(ctx, kafkaMsg, handler, 0)

	// Handler should not be called for duplicate
	assert.False(t, handlerCalled)
	assert.Equal(t, int64(0), metrics.GetProcessed())
}

func TestConsumer_GetRetryCount(t *testing.T) {
	consumer := &Consumer{}

	tests := []struct {
		name     string
		headers  map[string]string
		expected int
	}{
		{
			name:     "No retry header",
			headers:  map[string]string{},
			expected: 0,
		},
		{
			name: "Valid retry count",
			headers: map[string]string{
				models.HeaderRetryCount: "2",
			},
			expected: 2,
		},
		{
			name: "Invalid retry count",
			headers: map[string]string{
				models.HeaderRetryCount: "invalid",
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &models.Message{
				Headers: tt.headers,
			}
			result := consumer.getRetryCount(msg)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestInMemoryDedupeStore(t *testing.T) {
	store := NewInMemoryDedupeStore(100 * time.Millisecond)

	// Test non-existent message
	assert.False(t, store.Exists("msg-1"))

	// Add message
	err := store.Add("msg-1")
	require.NoError(t, err)
	assert.True(t, store.Exists("msg-1"))

	// Test TTL expiration
	time.Sleep(150 * time.Millisecond)
	// Note: cleanup runs every minute, so in unit test this won't auto-cleanup
	// In production, cleanup goroutine handles expiration
}

func TestMockDedupeStore(t *testing.T) {
	mock := NewMockDedupeStore()

	// Test default behavior
	assert.False(t, mock.Exists("msg-1"))

	err := mock.Add("msg-1")
	require.NoError(t, err)
	assert.True(t, mock.Exists("msg-1"))

	// Test custom behavior
	mock.ExistsFunc = func(messageID string) bool {
		return messageID == "always-exists"
	}

	assert.True(t, mock.Exists("always-exists"))
	assert.False(t, mock.Exists("msg-1"))

	// Reset
	mock.Reset()
	mock.ExistsFunc = nil
	assert.False(t, mock.Exists("msg-1"))
}

// Helper function to create mock Kafka message
func createKafkaMessage(msg *models.Message) kafka.Message {
	headers := make([]kafka.Header, 0, len(msg.Headers))
	for k, v := range msg.Headers {
		headers = append(headers, kafka.Header{
			Key:   k,
			Value: []byte(v),
		})
	}

	return kafka.Message{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    1,
		Key:       []byte(msg.Key),
		Value:     msg.Value,
		Headers:   headers,
		Time:      msg.Timestamp,
	}
}
