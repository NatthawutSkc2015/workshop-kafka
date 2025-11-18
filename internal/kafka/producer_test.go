package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go-message/internal/observability"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProducer_PublishSuccess(t *testing.T) {
	// This test demonstrates the expected behavior but cannot run without real Kafka
	// In a real scenario, you would use a mock writer or test against a test Kafka instance
	t.Skip("Requires real Kafka or mocked kafka.Writer")

	metrics := observability.NewInMemoryMetrics()
	cfg := ProducerConfig{
		Brokers:     []string{"localhost:9092"},
		Acks:        -1, // all
		Retries:     3,
		Idempotent:  true,
		MaxRetries:  3,
		BaseBackoff: 10 * time.Millisecond,
		Metrics:     metrics,
	}

	producer := NewProducer(cfg)
	defer producer.Close()

	ctx := context.Background()
	err := producer.Publish(ctx, "test-topic", "test-key", []byte("test-value"), map[string]string{
		"header1": "value1",
	})

	assert.NoError(t, err)
	assert.Equal(t, int64(1), metrics.GetPublished())
}

func TestProducer_PublishWithRetries(t *testing.T) {
	// This test demonstrates retry behavior conceptually
	t.Skip("Requires mocked kafka.Writer to simulate failures")

	metrics := observability.NewInMemoryMetrics()
	cfg := ProducerConfig{
		Brokers:     []string{"localhost:9092"},
		Acks:        -1,
		Retries:     3,
		Idempotent:  true,
		MaxRetries:  2,
		BaseBackoff: 10 * time.Millisecond,
		Metrics:     metrics,
	}

	producer := NewProducer(cfg)
	defer producer.Close()

	// In a real test, you would mock the writer to fail twice then succeed
	ctx := context.Background()
	err := producer.Publish(ctx, "test-topic", "test-key", []byte("test-value"), nil)

	// Should eventually succeed after retries
	assert.NoError(t, err)
}

func TestProducer_PublishExceedsMaxRetries(t *testing.T) {
	// This test demonstrates behavior when retries are exhausted
	t.Skip("Requires mocked kafka.Writer to simulate persistent failures")

	metrics := observability.NewInMemoryMetrics()
	cfg := ProducerConfig{
		Brokers:     []string{"localhost:9092"},
		Acks:        -1,
		Retries:     3,
		MaxRetries:  2,
		BaseBackoff: 10 * time.Millisecond,
		Metrics:     metrics,
	}

	producer := NewProducer(cfg)
	defer producer.Close()

	// Mock would fail all attempts
	ctx := context.Background()
	err := producer.Publish(ctx, "test-topic", "test-key", []byte("test-value"), nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to publish message after")
	assert.Equal(t, int64(1), metrics.GetPublishFailed())
}

func TestProducer_IdempotentConfiguration(t *testing.T) {
	metrics := observability.NewInMemoryMetrics()
	cfg := ProducerConfig{
		Brokers:     []string{"localhost:9092"},
		Acks:        1,
		Retries:     3,
		Idempotent:  true, // Should override acks to -1
		MaxRetries:  3,
		BaseBackoff: 10 * time.Millisecond,
		Metrics:     metrics,
	}

	producer := NewProducer(cfg)
	defer producer.Close()

	// Verify idempotent producer sets acks to all (-1)
	assert.NotNil(t, producer.writer)
	assert.Equal(t, -1, int(producer.writer.RequiredAcks))
	assert.Equal(t, 10, producer.writer.MaxAttempts)
}

func TestProducer_ContextCancellation(t *testing.T) {
	metrics := observability.NewInMemoryMetrics()
	cfg := ProducerConfig{
		Brokers:     []string{"localhost:9092"},
		Acks:        -1,
		Retries:     3,
		MaxRetries:  5,
		BaseBackoff: 100 * time.Millisecond,
		Metrics:     metrics,
	}

	producer := NewProducer(cfg)
	defer producer.Close()

	// Create a context that we'll cancel
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := producer.Publish(ctx, "test-topic", "test-key", []byte("test-value"), nil)

	// Should return context error
	assert.Error(t, err)
}

// MockProducerIntegrationTest demonstrates how the Producer would be tested with mocks
func TestMockProducer_Behavior(t *testing.T) {
	mock := NewMockProducer()

	ctx := context.Background()

	// Test successful publish
	err := mock.Publish(ctx, "test-topic", "key1", []byte("value1"), map[string]string{
		"header1": "value1",
	})
	require.NoError(t, err)

	messages := mock.GetPublishedMessages()
	assert.Len(t, messages, 1)
	assert.Equal(t, "test-topic", messages[0].Topic)
	assert.Equal(t, "key1", messages[0].Key)
	assert.Equal(t, []byte("value1"), messages[0].Value)
	assert.Equal(t, "value1", messages[0].Headers["header1"])
}

func TestMockProducer_SimulateFailures(t *testing.T) {
	mock := NewMockProducer()
	mock.FailCount = 2 // Fail first 2 attempts

	ctx := context.Background()

	// First two attempts should fail
	err := mock.Publish(ctx, "test-topic", "key1", []byte("value1"), nil)
	assert.Error(t, err)

	err = mock.Publish(ctx, "test-topic", "key1", []byte("value1"), nil)
	assert.Error(t, err)

	// Third attempt should succeed
	err = mock.Publish(ctx, "test-topic", "key1", []byte("value1"), nil)
	assert.NoError(t, err)

	messages := mock.GetPublishedMessages()
	assert.Len(t, messages, 1)
}

func TestMockProducer_CustomPublishFunc(t *testing.T) {
	mock := NewMockProducer()

	callCount := 0
	mock.PublishFunc = func(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
		callCount++
		if callCount < 3 {
			return fmt.Errorf("temporary error")
		}
		return nil
	}

	ctx := context.Background()

	// Should fail twice
	err := mock.Publish(ctx, "test-topic", "key1", []byte("value1"), nil)
	assert.Error(t, err)
	assert.Equal(t, 1, callCount)

	err = mock.Publish(ctx, "test-topic", "key1", []byte("value1"), nil)
	assert.Error(t, err)
	assert.Equal(t, 2, callCount)

	// Should succeed on third attempt
	err = mock.Publish(ctx, "test-topic", "key1", []byte("value1"), nil)
	assert.NoError(t, err)
	assert.Equal(t, 3, callCount)
}
