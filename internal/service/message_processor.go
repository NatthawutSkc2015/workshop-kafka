package service

import (
	"context"
	"encoding/json"
	"fmt"

	"go-message/internal/observability"
	"go-message/pkg/models"

	"github.com/sirupsen/logrus"
)

// MessageProcessor handles business logic for processing messages
type MessageProcessor struct {
	logger *logrus.Logger
}

func NewMessageProcessor() *MessageProcessor {
	return &MessageProcessor{
		logger: observability.GetLogger(),
	}
}

// Process handles the business logic for a consumed message
func (p *MessageProcessor) Process(ctx context.Context, msg *models.Message) error {
	p.logger.WithFields(logrus.Fields{
		"key":        msg.Key,
		"message_id": msg.Headers[models.HeaderMessageID],
	}).Info("Processing message")

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

	p.logger.WithFields(logrus.Fields{
		"key":  msg.Key,
		"data": data,
	}).Debug("Message processed successfully")

	return nil
}
