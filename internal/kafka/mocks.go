package kafka

import (
	"context"
	"fmt"
	"sync"
)

// MockProducer is a mock implementation of ProducerClient for testing
type MockProducer struct {
	mu                sync.RWMutex
	PublishedMessages []PublishedMessage
	PublishFunc       func(ctx context.Context, topic, key string, value []byte, headers map[string]string) error
	CloseFunc         func() error
	FailCount         int
	failureCounter    int
}

type PublishedMessage struct {
	Topic   string
	Key     string
	Value   []byte
	Headers map[string]string
}

func NewMockProducer() *MockProducer {
	return &MockProducer{
		PublishedMessages: make([]PublishedMessage, 0),
	}
}

func (m *MockProducer) Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Use custom publish function if provided
	if m.PublishFunc != nil {
		return m.PublishFunc(ctx, topic, key, value, headers)
	}

	// Simulate failures for testing retry logic
	if m.FailCount > 0 {
		m.failureCounter++
		if m.failureCounter <= m.FailCount {
			return fmt.Errorf("simulated publish failure %d", m.failureCounter)
		}
	}

	// Record published message
	m.PublishedMessages = append(m.PublishedMessages, PublishedMessage{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: headers,
	})

	return nil
}

func (m *MockProducer) Close() error {
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return nil
}

func (m *MockProducer) GetPublishedMessages() []PublishedMessage {
	m.mu.RLock()
	defer m.mu.RUnlock()

	messages := make([]PublishedMessage, len(m.PublishedMessages))
	copy(messages, m.PublishedMessages)
	return messages
}

func (m *MockProducer) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.PublishedMessages = make([]PublishedMessage, 0)
	m.failureCounter = 0
}

// MockDedupeStore is a mock implementation of DedupeStore for testing
type MockDedupeStore struct {
	mu          sync.RWMutex
	ExistsFunc  func(messageID string) bool
	AddFunc     func(messageID string) error
	existingIDs map[string]bool
}

func NewMockDedupeStore() *MockDedupeStore {
	return &MockDedupeStore{
		existingIDs: make(map[string]bool),
	}
}

func (m *MockDedupeStore) Exists(messageID string) bool {
	if m.ExistsFunc != nil {
		return m.ExistsFunc(messageID)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.existingIDs[messageID]
}

func (m *MockDedupeStore) Add(messageID string) error {
	if m.AddFunc != nil {
		return m.AddFunc(messageID)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.existingIDs[messageID] = true
	return nil
}

func (m *MockDedupeStore) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.existingIDs = make(map[string]bool)
}
