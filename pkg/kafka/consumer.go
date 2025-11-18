package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// ============================================================================
// Consumer Implementation
// ============================================================================

type KafkaConsumer struct {
	reader  *kafka.Reader
	logger  Logger
	metrics *ConsumerMetrics
}

type MessageHandler func(ctx context.Context, msg Message) error
type BatchMessageHandler func(ctx context.Context, msgs []Message) error

func OpenKafkaConsumer(config ConsumerConfig) (*KafkaConsumer, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid consumer config: %w", err)
	}

	if config.MinBytes == 0 {
		config.MinBytes = 10e3
	}
	if config.MaxBytes == 0 {
		config.MaxBytes = 10e6
	}
	if config.MaxWait == 0 {
		config.MaxWait = 10 * time.Second
	}
	if config.CommitInterval == 0 {
		config.CommitInterval = time.Second
	}
	if config.StartOffset == 0 {
		config.StartOffset = kafka.LastOffset
	}
	if config.HandlerTimeout == 0 {
		config.HandlerTimeout = 30 * time.Second
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	if config.Logger == nil {
		config.Logger = &DefaultLogger{}
	}
	if config.RetryPolicy.MaxRetries == 0 {
		config.RetryPolicy = DefaultRetryPolicy()
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        config.Brokers,
		GroupID:        config.GroupID,
		Topic:          config.Topic,
		MinBytes:       config.MinBytes,
		MaxBytes:       config.MaxBytes,
		MaxWait:        config.MaxWait,
		CommitInterval: config.CommitInterval,
		StartOffset:    config.StartOffset,
	})

	return &KafkaConsumer{
		reader:  reader,
		logger:  config.Logger,
		metrics: &ConsumerMetrics{},
	}, nil
}

func (kc *KafkaConsumer) Close() error {
	if kc.reader == nil {
		return errors.New("reader is nil")
	}
	return kc.reader.Close()
}

func (kc *KafkaConsumer) CloseGracefully(timeout time.Duration) error {
	if kc.reader == nil {
		return errors.New("reader is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- kc.reader.Close()
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// getStackTrace captures stack trace for panic situations
func getStackTrace() string {
	return string(debug.Stack())
}

// sendToDLQ sends failed message to Dead Letter Queue with enhanced metadata
func (kc *KafkaConsumer) sendToDLQ(msg Message, originalError error, retryCount int, config ConsumerConfig) {
	if config.DLQProducer == nil || config.DLQTopic == "" {
		return
	}

	// ใช้ context แยกเพื่อไม่ให้ถูกยกเลิกจาก parent context
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dlqMsg := Message{
		Topic: config.DLQTopic,
		Key:   msg.Key,
		Message: map[string]interface{}{
			"original_topic":     msg.Topic,
			"original_key":       msg.Key,
			"original_offset":    msg.Offset,
			"original_partition": msg.Partition,
			"error":              originalError.Error(),
			"retry_count":        retryCount,
			"timestamp":          time.Now().Unix(),
			"message":            msg.Message,
			"raw_value":          string(msg.RawValue),
		},
	}

	if err := config.DLQProducer.SendMessageWithContext(ctx, dlqMsg); err != nil {
		kc.logger.Error("failed to send message to DLQ", "error", err)
	} else {
		kc.metrics.IncrementDLQ()
		kc.logger.Info("message sent to DLQ", "key", msg.Key, "retry_count", retryCount)
	}
}

func (kc *KafkaConsumer) ReceiveMessage(handler MessageHandler, config ConsumerConfig) error {
	return kc.ReceiveMessageWithContext(context.Background(), handler, config)
}

func (kc *KafkaConsumer) ReceiveMessageWithContext(ctx context.Context, handler MessageHandler, config ConsumerConfig) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	/*
		Graceful shutdown handler
		งานที่เกี่ยวกับการ โปรแกรมถูกขัดจังหวะ Error
	*/
	shutdownChan := make(chan struct{})
	go func() {
		select {
		case sig := <-sigChan: //เคสกด Ctrol + C เพื่อ Shutdown server
			kc.logger.Info("received shutdown signal", "signal", sig)
			close(shutdownChan)
			cancel()
		case <-ctx.Done():
			return
		}
	}()

	// Track in-flight messages for graceful shutdown
	var inflightWg sync.WaitGroup

	defer func() {
		kc.logger.Info("waiting for in-flight messages to complete...")
		done := make(chan struct{})
		go func() {
			inflightWg.Wait()
			close(done)
		}()

		select {
		case <-done: //กด Control + C
			kc.logger.Info("all in-flight messages completed")
		case <-time.After(30 * time.Second):
			kc.logger.Warn("timeout waiting for in-flight messages")
		}

		// เตียมข้อมูล ผลลัพ การ process, error ต่างๆ ไว้ log ที่ terminal console
		processed, errors, dlq, retries, avgDuration, lastErr, lastProc := kc.metrics.GetMetrics()
		kc.logger.Info("consumer stopped",
			"processed", processed,
			"errors", errors,
			"dlq", dlq,
			"retries", retries,
			"avg_duration", avgDuration,
			"last_error", lastErr,
			"last_processed", lastProc)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-shutdownChan:
			return nil
		default:
			m, err := kc.reader.FetchMessage(ctx)
			if err != nil {
				if err == context.Canceled {
					return nil
				}
				kc.logger.Error("error reading message", "error", err)
				kc.metrics.IncrementError(err)
				time.Sleep(time.Second)
				continue
			}

			inflightWg.Add(1)
			go func(kafkaMsg kafka.Message) {
				defer inflightWg.Done()

				startTime := time.Now()

				var payload interface{}
				unmarshalErr := json.Unmarshal(kafkaMsg.Value, &payload)

				msg := Message{
					Topic:     kafkaMsg.Topic,
					Key:       string(kafkaMsg.Key),
					Message:   payload,
					RawValue:  kafkaMsg.Value,
					Partition: kafkaMsg.Partition,
					Offset:    kafkaMsg.Offset,
				}

				if unmarshalErr != nil {
					kc.logger.Warn("failed to unmarshal message, passing raw value",
						"error", unmarshalErr,
						"key", msg.Key)
				}

				// Process with retry logic
				var handlerErr error
				var actualRetries int

				for attempt := 0; attempt < config.MaxRetries; attempt++ {
					handlerCtx, handlerCancel := context.WithTimeout(ctx, config.HandlerTimeout)

					handlerErr = func() (err error) {
						defer func() {
							if r := recover(); r != nil {
								kc.logger.Error("panic in handler",
									"panic", r,
									"key", msg.Key,
									"stack", getStackTrace())
								err = &PermanentError{Err: fmt.Errorf("handler panicked: %v", r)}
							}
						}()
						return handler(handlerCtx, msg)
					}()

					handlerCancel()

					if handlerErr == nil {
						break
					}

					actualRetries = attempt + 1

					// ถ้าเป็น permanent error หยุด retry ทันที
					if IsPermanent(handlerErr) {
						kc.logger.Warn("permanent error, skipping retries",
							"error", handlerErr,
							"key", msg.Key)
						break
					}

					kc.metrics.IncrementRetry()
					kc.logger.Warn("handler error",
						"attempt", attempt+1,
						"max_retries", config.MaxRetries,
						"error", handlerErr,
						"key", msg.Key)

					if attempt < config.MaxRetries-1 {
						backoff := calculateBackoff(config.RetryPolicy, attempt)
						time.Sleep(backoff)
					}
				}

				// Handle final result
				if handlerErr != nil {
					kc.metrics.IncrementError(handlerErr)
					kc.sendToDLQ(msg, handlerErr, actualRetries, config)
					return // ไม่ commit message ที่ล้มเหลว
				}

				// Commit on success
				if err := kc.reader.CommitMessages(ctx, kafkaMsg); err != nil {
					kc.logger.Error("failed to commit message", "error", err)
					kc.metrics.IncrementError(err)
				} else {
					duration := time.Since(startTime)
					kc.metrics.IncrementProcessed(duration)

					if atomic.LoadInt64(&kc.metrics.ProcessedCount)%100 == 0 {
						processed, errors, dlq, retries, avgDuration, _, _ := kc.metrics.GetMetrics()
						kc.logger.Info("progress update",
							"processed", processed,
							"errors", errors,
							"dlq", dlq,
							"retries", retries,
							"avg_duration", avgDuration)
					}
				}
			}(m)
		}
	}
}

func (kc *KafkaConsumer) ReceiveBatch(ctx context.Context, handler BatchMessageHandler, config ConsumerConfig, batchSize int, batchTimeout time.Duration) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	go func() {
		select {
		case sig := <-sigChan:
			kc.logger.Info("received shutdown signal", "signal", sig)
			cancel()
		case <-ctx.Done():
			return
		}
	}()

	batch := make([]Message, 0, batchSize)
	kafkaMsgs := make([]kafka.Message, 0, batchSize)

	batchTimer := time.NewTimer(batchTimeout)
	defer func() {
		if !batchTimer.Stop() {
			select {
			case <-batchTimer.C:
			default:
			}
		}
	}()

	processBatch := func() {
		if len(batch) == 0 {
			return
		}

		startTime := time.Now()
		handlerCtx, handlerCancel := context.WithTimeout(ctx, config.HandlerTimeout)

		var handlerErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					kc.logger.Error("panic in batch handler",
						"panic", r,
						"batch_size", len(batch),
						"stack", getStackTrace())
					handlerErr = fmt.Errorf("batch handler panicked: %v", r)
				}
			}()
			handlerErr = handler(handlerCtx, batch)
		}()

		handlerCancel()

		if handlerErr != nil {
			kc.logger.Error("batch handler error", "error", handlerErr, "batch_size", len(batch))
			kc.metrics.IncrementError(handlerErr)
		} else {
			if err := kc.reader.CommitMessages(ctx, kafkaMsgs...); err != nil {
				kc.logger.Error("failed to commit batch", "error", err)
			} else {
				duration := time.Since(startTime)
				for range batch {
					kc.metrics.IncrementProcessed(duration / time.Duration(len(batch)))
				}
			}
		}

		batch = batch[:0]
		kafkaMsgs = kafkaMsgs[:0]

		// Reset timer properly
		if !batchTimer.Stop() {
			select {
			case <-batchTimer.C:
			default:
			}
		}
		batchTimer.Reset(batchTimeout)
	}

	defer processBatch() // Process remaining messages on exit

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-batchTimer.C:
			processBatch()
		default:
			m, err := kc.reader.FetchMessage(ctx)
			if err != nil {
				if err == context.Canceled {
					return nil
				}
				kc.logger.Error("error reading message", "error", err)
				time.Sleep(time.Second)
				continue
			}

			var payload interface{}
			json.Unmarshal(m.Value, &payload)

			msg := Message{
				Topic:     m.Topic,
				Key:       string(m.Key),
				Message:   payload,
				RawValue:  m.Value,
				Partition: m.Partition,
				Offset:    m.Offset,
			}

			batch = append(batch, msg)
			kafkaMsgs = append(kafkaMsgs, m)

			if len(batch) >= batchSize {
				processBatch()
			}
		}
	}
}

func (kc *KafkaConsumer) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		stats := kc.reader.Stats()
		if stats.Errors > 0 {
			return fmt.Errorf("consumer has %d errors", stats.Errors)
		}
		return nil
	}
}

func (kc *KafkaConsumer) Stats() kafka.ReaderStats {
	return kc.reader.Stats()
}

func (kc *KafkaConsumer) Metrics() (
	processed,
	errors,
	dlq int64,
	lastProcessed time.Time,
	lastError error,
) {
	p, e, d, _, _, lp, le := kc.metrics.GetMetrics()
	return p, e, d, lp, le
}
