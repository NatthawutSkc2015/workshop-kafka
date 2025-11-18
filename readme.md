# Production-Ready Kafka Go Example

A production-ready Apache Kafka application in Go demonstrating best practices for both Producer and Consumer implementations with comprehensive testing.

## Features

### Producer
- ✅ Configurable delivery guarantees (acks: 0, 1, all)
- ✅ Idempotent producer support
- ✅ Automatic retry with exponential backoff
- ✅ Graceful shutdown
- ✅ Structured logging with request/message IDs
- ✅ Metrics collection hooks

### Consumer
- ✅ Consumer group support
- ✅ Manual offset commits
- ✅ Concurrent worker pool
- ✅ Automatic retry logic with retry topics
- ✅ Dead Letter Queue (DLQ) for failed messages
- ✅ Message deduplication
- ✅ Graceful shutdown with proper cleanup
- ✅ Backpressure management

### Infrastructure
- ✅ Automatic reconnection with exponential backoff
- ✅ Health checks with non-blocking reconnect loop
- ✅ Comprehensive unit tests with mocks
- ✅ Clean architecture with separated layers
- ✅ Interface-based design for testability

## Project Structure

```
kafka-example/
├── cmd/
│   ├── producer/
│   │   └── main.go              # Producer CLI application
│   └── consumer/
│       └── main.go              # Consumer CLI application
├── internal/
│   ├── config/
│   │   └── config.go            # Configuration management
│   ├── kafka/
│   │   ├── client.go            # Kafka client with health checks
│   │   ├── producer.go          # Producer implementation
│   │   ├── producer_test.go     # Producer tests
│   │   ├── consumer.go          # Consumer implementation
│   │   ├── consumer_test.go     # Consumer tests
│   │   └── mocks.go             # Mock implementations
│   ├── service/
│   │   ├── message_processor.go # Business logic
│   │   └── message_processor_test.go
│   └── observability/
│       ├── logger.go            # Structured logging
│       └── metrics.go           # Metrics collection
├── pkg/
│   └── models/
│       └── message.go           # Message models
├── go.mod
├── go.sum
├── docker-compose.yml           # Local Kafka setup
└── README.md
```

## Prerequisites

- Go 1.21 or higher
- Docker and Docker Compose (for local Kafka)

## Environment Variables

### Kafka Connection
```bash
KAFKA_BROKERS=localhost:9092   # Comma-separated broker list
```

### Producer Settings
```bash
KAFKA_PRODUCER_TOPIC=events
KAFKA_PRODUCER_ACKS=all         # all, 0, or 1
KAFKA_PRODUCER_RETRIES=3
KAFKA_PRODUCER_IDEMPOTENT=true
```

### Consumer Settings
```bash
KAFKA_CONSUMER_TOPIC=events
KAFKA_CONSUMER_GROUP_ID=event-processor-group
KAFKA_CONSUMER_WORKERS=5
KAFKA_CONSUMER_RETRY_MAX=3
KAFKA_CONSUMER_FETCH_MIN_BYTES=1024
KAFKA_CONSUMER_FETCH_MAX_BYTES=10485760
```

### Retry/DLQ Topics
```bash
KAFKA_RETRY_TOPIC_PREFIX=events-retry
KAFKA_DLQ_TOPIC=events-dlq
```

### Logging
```bash
LOG_LEVEL=info  # debug, info, warn, error
```

## Quick Start

### 1. Start Kafka Locally

```bash
docker-compose up -d
```

Wait for Kafka to be ready (~30 seconds).

### 2. Create Topics

```bash
# Create main topic
docker exec -it kafka-example-kafka-1 kafka-topics \
  --create --topic events \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

# Create retry topics
docker exec -it kafka-example-kafka-1 kafka-topics \
  --create --topic events-retry-1 \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

docker exec -it kafka-example-kafka-1 kafka-topics \
  --create --topic events-retry-2 \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

docker exec -it kafka-example-kafka-1 kafka-topics \
  --create --topic events-retry-3 \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

# Create DLQ topic
docker exec -it kafka-example-kafka-1 kafka-topics \
  --create --topic events-dlq \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

### 3. Install Dependencies

```bash
go mod download
```

### 4. Run Tests

```bash
# Run all tests
go test ./...

# Run with coverage
go test ./... -cover -v

# Run specific package
go test ./internal/kafka/... -v

# Run with race detector
go test ./... -race
```

### 5. Run Producer

```bash
export KAFKA_BROKERS=localhost:9092
export KAFKA_PRODUCER_TOPIC=events
export LOG_LEVEL=info

go run cmd/producer/main.go
```

### 6. Run Consumer (in another terminal)

```bash
export KAFKA_BROKERS=localhost:9092
export KAFKA_CONSUMER_TOPIC=events
export KAFKA_CONSUMER_GROUP_ID=event-processor-group
export LOG_LEVEL=info

go run cmd/consumer/main.go
```

## Docker Compose Configuration

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: kafka-example-zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka-example-kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'false'
```

## Architecture

### Layers

1. **Transport Layer** (`internal/kafka/`)
   - Kafka client abstraction with interfaces
   - Connection management with health checks
   - Automatic reconnection with exponential backoff
   - Producer with delivery guarantees
   - Consumer with worker pool and manual commits

2. **Service Layer** (`internal/service/`)
   - Business logic for message processing
   - Independent of transport implementation
   - Easy to test with mock data

3. **Config Layer** (`internal/config/`)
   - Centralized configuration management
   - Environment variable parsing
   - Default values

4. **Observability** (`internal/observability/`)
   - Structured logging (logrus)
   - Metrics collection interfaces
   - Ready for Prometheus integration

### Key Design Patterns

#### Producer Reliability
```go
// Idempotent producer with acks=all
producerCfg := kafka.ProducerConfig{
    Acks:       -1,  // Wait for all replicas
    Retries:    3,
    Idempotent: true,
}
```

#### Consumer Worker Pool
```go
// Concurrent processing with manual commits
consumerCfg := kafka.ConsumerConfig{
    Workers:   5,  // 5 concurrent workers
    RetryMax:  3,  // Retry 3 times before DLQ
}
```

#### Retry Logic
```
Message Processing Failed
         ↓
   Retry Count < Max?
         ↓
    Yes: Send to retry-topic-N
         ↓
    No: Send to DLQ
```

#### Message Deduplication
```go
// Use message ID for idempotency
headers := map[string]string{
    models.HeaderMessageID: generateUniqueID(),
}

// Consumer checks dedupe store before processing
if dedupeStore.Exists(messageID) {
    // Skip duplicate
}
```

## Testing

### Unit Tests

All components have comprehensive unit tests using mocks:

```bash
# Run specific test
go test -run TestProducer_PublishSuccess ./internal/kafka/

# Run with verbose output
go test -v ./internal/kafka/

# Run with coverage
go test -cover ./internal/kafka/
```

### Test Coverage

- ✅ Producer retry logic
- ✅ Producer idempotent configuration
- ✅ Consumer message processing
- ✅ Consumer retry → DLQ flow
- ✅ Consumer deduplication
- ✅ Message processor business logic
- ✅ Mock implementations

### Integration Testing

While this project focuses on unit tests, the architecture supports integration testing:

```go
// Use testcontainers-go for integration tests
func TestIntegration_ProducerConsumer(t *testing.T) {
    // Start Kafka container
    // Create real producer and consumer
    // Verify end-to-end flow
}
```

## Monitoring & Observability

### Metrics

The application exposes metrics through the `MetricsCollector` interface:

```go
type MetricsCollector interface {
    IncPublished()
    IncPublishFailed()
    IncReceived()
    IncProcessed()
    IncFailed()
    IncRetried()
    IncSentToDLQ()
}
```

Implement this interface to integrate with:
- Prometheus
- StatsD
- CloudWatch
- Datadog

### Logging

Structured JSON logs with fields:
```json
{
  "level": "info",
  "topic": "events",
  "key": "user-123",
  "message_id": "msg-456",
  "msg": "Message published",
  "timestamp": "2025-01-01T10:00:00Z"
}
```

### Health Checks

```go
// Check Kafka connectivity
client := kafka.NewKafkaClient(brokers, 5)
if err := client.HealthCheck(ctx); err != nil {
    log.Fatal(err)
}

// Continuous health monitoring
go client.HealthCheckLoop(ctx, 30*time.Second, onReconnect)
```

## Production Considerations

### Security
- **TLS/SSL**: Configure secure connections (not included in example)
- **SASL Authentication**: Add username/password or SCRAM (extend config)
- **ACLs**: Set up appropriate topic permissions

### Performance
- **Batch Size**: Tune `FetchMinBytes` and `FetchMaxBytes`
- **Worker Count**: Adjust based on CPU and processing time
- **Partitions**: Scale partitions for parallelism

### Reliability
- **Replication Factor**: Use RF >= 3 for production
- **Min ISR**: Set `min.insync.replicas >= 2`
- **Acks**: Use `acks=all` for critical data

### Scaling
- **Horizontal**: Add more consumer instances (same group ID)
- **Vertical**: Increase workers per consumer
- **Partitions**: Add partitions for higher throughput

## Troubleshooting

### Consumer Lag
```bash
# Check consumer group lag
docker exec kafka-example-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group event-processor-group \
  --describe
```

### View Messages
```bash
# Consume from topic
docker exec kafka-example-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic events \
  --from-beginning
```

### Check DLQ
```bash
# View dead letter queue
docker exec kafka-example-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic events-dlq \
  --from-beginning
```

## Dependencies

- **github.com/segmentio/kafka-go** (v0.4.47): Kafka client library
  - Well-maintained, production-ready
  - Clean API, easy to test
  - Supports all modern Kafka features

- **github.com/sirupsen/logrus** (v1.9.3): Structured logging
  - JSON output for log aggregation
  - Flexible field support

- **github.com/stretchr/testify** (v1.9.0): Testing utilities
  - Assertions and require functions
  - Mock support

## License

MIT License

## Contributing

1. Fork the repository
2. Create your feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## References

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [kafka-go Library](https://github.com/segmentio/kafka-go)
- [Go Testing](https://golang.org/pkg/testing/)
