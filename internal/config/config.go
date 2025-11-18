package config

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	Kafka    KafkaConfig
	Logging  LoggingConfig
	Consumer ConsumerConfig
	Producer ProducerConfig
}

type KafkaConfig struct {
	Brokers []string
}

type LoggingConfig struct {
	Level string
}

type ConsumerConfig struct {
	Topic            string
	GroupID          string
	Workers          int
	RetryMax         int
	FetchMinBytes    int
	FetchMaxBytes    int
	RetryTopicPrefix string
	DLQTopic         string
}

type ProducerConfig struct {
	Topic      string
	Acks       int
	Retries    int
	Idempotent bool
}

func Load() *Config {
	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: .env file not found")
		os.Exit(1)
	}
	return &Config{
		Kafka: KafkaConfig{
			Brokers: parseBrokers(getEnv("KAFKA_BROKERS", "localhost:9092")),
		},
		Logging: LoggingConfig{
			Level: getEnv("LOG_LEVEL", "info"),
		},
		Consumer: ConsumerConfig{
			Topic:            getEnv("KAFKA_CONSUMER_TOPIC", "events"),
			GroupID:          getEnv("KAFKA_CONSUMER_GROUP_ID", "event-processor-group"),
			Workers:          getEnvInt("KAFKA_CONSUMER_WORKERS", 5),
			RetryMax:         getEnvInt("KAFKA_CONSUMER_RETRY_MAX", 3),
			FetchMinBytes:    getEnvInt("KAFKA_CONSUMER_FETCH_MIN_BYTES", 1024),
			FetchMaxBytes:    getEnvInt("KAFKA_CONSUMER_FETCH_MAX_BYTES", 10485760),
			RetryTopicPrefix: getEnv("KAFKA_RETRY_TOPIC_PREFIX", "events-retry"),
			DLQTopic:         getEnv("KAFKA_DLQ_TOPIC", "events-dlq"),
		},
		Producer: ProducerConfig{
			Topic:      getEnv("KAFKA_PRODUCER_TOPIC", "events"),
			Acks:       parseAcks(getEnv("KAFKA_PRODUCER_ACKS", "all")),
			Retries:    getEnvInt("KAFKA_PRODUCER_RETRIES", 3),
			Idempotent: getEnvBool("KAFKA_PRODUCER_IDEMPOTENT", true),
		},
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

func parseBrokers(brokers string) []string {
	parts := strings.Split(brokers, ",")
	result := make([]string, 0, len(parts))
	for _, broker := range parts {
		if trimmed := strings.TrimSpace(broker); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func parseAcks(acks string) int {
	switch strings.ToLower(acks) {
	case "all", "-1":
		return -1
	case "0":
		return 0
	case "1":
		return 1
	default:
		return -1 // default to all
	}
}
