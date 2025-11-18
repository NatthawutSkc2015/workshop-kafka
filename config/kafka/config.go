package kafka

import "time"

type Config struct {
	RabbitMQURL      string
	Exchange         string
	Queue            string
	RoutingKey       string
	RetryQueue       string
	DLQ              string
	PrefetchCount    int
	WorkerCount      int
	MaxRetries       int
	RetryDelay       time.Duration
	ReconnectBackoff time.Duration
	MaxReconnectWait time.Duration
}
