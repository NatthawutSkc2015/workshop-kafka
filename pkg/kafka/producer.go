package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

// ============================================================================
// Producer Implementation
// ============================================================================

type KafkaProducer struct {
	writer *kafka.Writer
	logger Logger
}

func OpenKafkaProducer(config ProducerConfig) (*KafkaProducer, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid producer config: %w", err)
	}

	if config.WriteTimeout == 0 {
		config.WriteTimeout = 10 * time.Second
	}
	if config.ReadTimeout == 0 {
		config.ReadTimeout = 10 * time.Second
	}
	if config.RequiredAcks == 0 {
		config.RequiredAcks = kafka.RequireOne
	}
	if config.Balancer == nil {
		config.Balancer = &kafka.LeastBytes{}
	}
	if config.Logger == nil {
		config.Logger = &DefaultLogger{}
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.Brokers...),
		Topic:        config.Topic,
		Balancer:     config.Balancer,
		WriteTimeout: config.WriteTimeout,
		ReadTimeout:  config.ReadTimeout,
		RequiredAcks: config.RequiredAcks,
		Compression:  config.Compression,
	}

	return &KafkaProducer{
		writer: writer,
		logger: config.Logger,
	}, nil
}

func (kp *KafkaProducer) Close() error {
	if kp.writer == nil {
		return errors.New("writer is nil")
	}
	return kp.writer.Close()
}

func (kp *KafkaProducer) CloseGracefully(timeout time.Duration) error {
	if kp.writer == nil {
		return errors.New("writer is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- kp.writer.Close()
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (kp *KafkaProducer) SendMessage(msg Message) error {
	return kp.SendMessageWithContext(context.Background(), msg)
}

func (kp *KafkaProducer) SendMessageWithContext(ctx context.Context, msg Message) error {
	messageBytes, err := json.Marshal(msg.Message)
	if err != nil {
		return &PermanentError{Err: err}
	}

	message := kafka.Message{
		Key:   []byte(msg.Key),
		Value: messageBytes,
		Time:  time.Now(),
	}

	if err := kp.writer.WriteMessages(ctx, message); err != nil {
		return &RetryableError{Err: err}
	}
	return nil
}

func calculateBackoff(policy RetryPolicy, attempt int) time.Duration {
	backoff := time.Duration(float64(policy.InitialBackoff) * math.Pow(policy.BackoffFactor, float64(attempt)))

	// จำกัดไม่ให้เกิน MaxBackoff
	if backoff > policy.MaxBackoff {
		backoff = policy.MaxBackoff
	}

	// เพิ่ม jitter (ถ้ามี) แต่ยังคงไม่เกิน MaxBackoff
	if policy.Jitter && backoff > 0 {
		maxJitter := backoff / 4
		if maxJitter > 0 {
			jitter := time.Duration(rand.Int63n(int64(maxJitter)))
			backoff += jitter

			// ตรวจสอบอีกครั้งหลังเพิ่ม jitter
			if backoff > policy.MaxBackoff {
				backoff = policy.MaxBackoff
			}
		}
	}

	return backoff
}

func (kp *KafkaProducer) SendMessageWithRetry(msg Message, policy RetryPolicy) error {
	var lastErr error

	for i := 0; i < policy.MaxRetries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := kp.SendMessageWithContext(ctx, msg)
		cancel()

		if err == nil {
			return nil
		}

		// ถ้าเป็น permanent error ไม่ต้อง retry
		if IsPermanent(err) {
			return err
		}

		lastErr = err
		kp.logger.Warn("failed to send message",
			"attempt", i+1,
			"max_retries", policy.MaxRetries,
			"error", err)

		if i < policy.MaxRetries-1 {
			backoff := calculateBackoff(policy, i)
			time.Sleep(backoff)
		}
	}
	return lastErr
}

func (kp *KafkaProducer) SendMessages(msgs []Message) error {
	return kp.SendMessagesWithContext(context.Background(), msgs)
}

func (kp *KafkaProducer) SendMessagesWithContext(ctx context.Context, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}

	kafkaMessages := make([]kafka.Message, len(msgs))
	for i, msg := range msgs {
		messageBytes, err := json.Marshal(msg.Message)
		if err != nil {
			return &PermanentError{Err: err}
		}
		kafkaMessages[i] = kafka.Message{
			Key:   []byte(msg.Key),
			Value: messageBytes,
			Time:  time.Now(),
		}
	}

	if err := kp.writer.WriteMessages(ctx, kafkaMessages...); err != nil {
		return &RetryableError{Err: err}
	}
	return nil
}
