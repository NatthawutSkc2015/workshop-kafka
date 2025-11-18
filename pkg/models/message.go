package models

import "time"

// Message represents a message in the system
type Message struct {
	ID        string            `json:"id"`
	Key       string            `json:"key"`
	Value     []byte            `json:"value"`
	Headers   map[string]string `json:"headers"`
	Timestamp time.Time         `json:"timestamp"`
}

// MessageHeader constants
const (
	HeaderMessageID     = "message-id"
	HeaderRetryCount    = "retry-count"
	HeaderRetryAttempt  = "retry-attempt"
	HeaderOriginalTopic = "original-topic"
	HeaderFailureReason = "failure-reason"
	HeaderProcessedAt   = "processed-at"
)
