package service

import (
	"context"
	"encoding/json"
	"fmt"

	"go-message/pkg/models"

	"go.uber.org/zap"
)

// MessageProcessor handles business logic for processing messages
type MessageProcessor struct {
	logger *zap.Logger
}

func NewMessageProcessor(logger *zap.Logger) *MessageProcessor {
	return &MessageProcessor{
		logger: logger,
	}
}

// Process handles the business logic for a consumed message
func (p *MessageProcessor) Process(ctx context.Context, msg *models.Message) error {
	p.logger.Info("Processing message",
		zap.String("key", msg.Key),
		zap.String("message_id", msg.Headers[models.HeaderMessageID]),
	)

	// Example: Parse message value as JSON
	var data map[string]interface{}
	if err := json.Unmarshal(msg.Value, &data); err != nil {
		return fmt.Errorf("failed to parse message: %w", err)
	}

	// Example business logic
	// In a real application, this would:
	// - Validate data
	// - Store in database
	// - Call external APIs
	// - Transform data
	// - etc.
	p.logger.Debug("Message processed successfully",
		zap.Any("key", msg.Key),
		zap.Any("data", data),
	)

	return nil
}
