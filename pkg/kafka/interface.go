package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// ============================================================================
// Logger Interface
// ============================================================================

type Logger interface {
	Info(msg string, fields ...interface{})
	Error(msg string, fields ...interface{})
	Warn(msg string, fields ...interface{})
}

type DefaultLogger struct{}

func (l *DefaultLogger) Info(msg string, fields ...interface{}) {
	log.Printf("[INFO] %s %v", msg, fields)
}

func (l *DefaultLogger) Error(msg string, fields ...interface{}) {
	log.Printf("[ERROR] %s %v", msg, fields)
}

func (l *DefaultLogger) Warn(msg string, fields ...interface{}) {
	log.Printf("[WARN] %s %v", msg, fields)
}

// ============================================================================
// Message and Config Structures
// ============================================================================

type Message struct {
	Topic     string      `json:"topic"`
	Key       string      `json:"key,omitempty"`
	Message   interface{} `json:"message"`
	RawValue  []byte      `json:"-"`
	Partition int         `json:"partition,omitempty"`
	Offset    int64       `json:"offset,omitempty"`
}

type RetryPolicy struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	BackoffFactor  float64
	Jitter         bool
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxRetries:     3,
		InitialBackoff: time.Second,
		MaxBackoff:     30 * time.Second,
		BackoffFactor:  2.0,
		Jitter:         true,
	}
}

type ProducerConfig struct {
	Brokers      []string
	Topic        string
	WriteTimeout time.Duration
	ReadTimeout  time.Duration
	RequiredAcks kafka.RequiredAcks
	Balancer     kafka.Balancer
	Compression  kafka.Compression
	Logger       Logger
}

type ConsumerConfig struct {
	Brokers        []string
	Topic          string
	GroupID        string
	MinBytes       int
	MaxBytes       int
	MaxWait        time.Duration // เพิ่ม: หลีกเลี่ยง infinite blocking
	CommitInterval time.Duration
	StartOffset    int64
	HandlerTimeout time.Duration
	MaxRetries     int
	DLQTopic       string
	DLQProducer    Producer // ใช้ interface แทน
	Logger         Logger
	RetryPolicy    RetryPolicy
}

// ============================================================================
// Custom Error Types
// ============================================================================

// RetryableError indicates the operation should be retried
type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("retryable error: %v", e.Err)
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}

// PermanentError indicates the operation should not be retried
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string {
	return fmt.Sprintf("permanent error: %v", e.Err)
}

func (e *PermanentError) Unwrap() error {
	return e.Err
}

// IsRetryable checks if error is retryable
func IsRetryable(err error) bool {
	var retryableErr *RetryableError
	return errors.As(err, &retryableErr)
}

// IsPermanent checks if error is permanent
func IsPermanent(err error) bool {
	var permanentErr *PermanentError
	return errors.As(err, &permanentErr)
}

// ============================================================================
// Interfaces for Testing
// ============================================================================

type Producer interface {
	SendMessage(msg Message) error
	SendMessageWithContext(ctx context.Context, msg Message) error
	SendMessageWithRetry(msg Message, policy RetryPolicy) error
	SendMessages(msgs []Message) error
	Close() error
	CloseGracefully(timeout time.Duration) error
}

type Consumer interface {
	ReceiveMessage(handler MessageHandler, config ConsumerConfig) error
	ReceiveMessageWithContext(ctx context.Context, handler MessageHandler, config ConsumerConfig) error
	ReceiveBatch(ctx context.Context, handler BatchMessageHandler, config ConsumerConfig, batchSize int, batchTimeout time.Duration) error
	Close() error
	CloseGracefully(timeout time.Duration) error
	Metrics() (processed, errors, dlq int64, lastProcessed time.Time, lastError error)
	HealthCheck(ctx context.Context) error
}

// ============================================================================
// Enhanced Consumer Metrics
// ============================================================================

type ConsumerMetrics struct {
	ProcessedCount      int64
	ErrorCount          int64
	DLQCount            int64
	RetryCount          int64 // เพิ่ม: นับจำนวน retry
	LastError           error
	LastProcessed       time.Time
	TotalProcessingTime time.Duration // เพิ่ม: รวมเวลาประมวลผล
	mu                  sync.RWMutex
}

func (m *ConsumerMetrics) IncrementProcessed(duration time.Duration) {
	atomic.AddInt64(&m.ProcessedCount, 1)
	m.mu.Lock()
	m.LastProcessed = time.Now()
	m.TotalProcessingTime += duration
	m.mu.Unlock()
}

func (m *ConsumerMetrics) IncrementError(err error) {
	atomic.AddInt64(&m.ErrorCount, 1)
	m.mu.Lock()
	m.LastError = err
	m.mu.Unlock()
}

func (m *ConsumerMetrics) IncrementDLQ() {
	atomic.AddInt64(&m.DLQCount, 1)
}

func (m *ConsumerMetrics) IncrementRetry() {
	atomic.AddInt64(&m.RetryCount, 1)
}

func (m *ConsumerMetrics) GetMetrics() (
	processed,
	errors,
	dlq,
	retries int64,
	avgDuration time.Duration,
	lastProcessed time.Time,
	lastError error,
) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	p := atomic.LoadInt64(&m.ProcessedCount)
	var avg time.Duration
	if p > 0 {
		avg = m.TotalProcessingTime / time.Duration(p)
	}

	return p,
		atomic.LoadInt64(&m.ErrorCount),
		atomic.LoadInt64(&m.DLQCount),
		atomic.LoadInt64(&m.RetryCount),
		avg,
		m.LastProcessed,
		m.LastError
}
