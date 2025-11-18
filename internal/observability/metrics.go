package observability

import (
	"sync/atomic"
)

// MetricsCollector provides hooks for metrics collection
// Can be implemented to integrate with Prometheus, StatsD, etc.
type MetricsCollector interface {
	IncPublished()
	IncPublishFailed()
	IncReceived()
	IncProcessed()
	IncFailed()
	IncRetried()
	IncSentToDLQ()
}

// InMemoryMetrics is a simple in-memory implementation for testing/demo
type InMemoryMetrics struct {
	Published     atomic.Int64
	PublishFailed atomic.Int64
	Received      atomic.Int64
	Processed     atomic.Int64
	Failed        atomic.Int64
	Retried       atomic.Int64
	SentToDLQ     atomic.Int64
}

func NewInMemoryMetrics() *InMemoryMetrics {
	return &InMemoryMetrics{}
}

func (m *InMemoryMetrics) IncPublished() {
	m.Published.Add(1)
}

func (m *InMemoryMetrics) IncPublishFailed() {
	m.PublishFailed.Add(1)
}

func (m *InMemoryMetrics) IncReceived() {
	m.Received.Add(1)
}

func (m *InMemoryMetrics) IncProcessed() {
	m.Processed.Add(1)
}

func (m *InMemoryMetrics) IncFailed() {
	m.Failed.Add(1)
}

func (m *InMemoryMetrics) IncRetried() {
	m.Retried.Add(1)
}

func (m *InMemoryMetrics) IncSentToDLQ() {
	m.SentToDLQ.Add(1)
}

func (m *InMemoryMetrics) GetPublished() int64 {
	return m.Published.Load()
}

func (m *InMemoryMetrics) GetPublishFailed() int64 {
	return m.PublishFailed.Load()
}

func (m *InMemoryMetrics) GetReceived() int64 {
	return m.Received.Load()
}

func (m *InMemoryMetrics) GetProcessed() int64 {
	return m.Processed.Load()
}

func (m *InMemoryMetrics) GetFailed() int64 {
	return m.Failed.Load()
}

func (m *InMemoryMetrics) GetRetried() int64 {
	return m.Retried.Load()
}

func (m *InMemoryMetrics) GetSentToDLQ() int64 {
	return m.SentToDLQ.Load()
}
