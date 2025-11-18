package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	pkg "go-message/pkg/kafka"
	"log"
	"time"
)

func main() {
	serviceName := flag.String("service", "", "service name as string")
	flag.Parse()
	fmt.Println("🚀 Starting Kafka Consumer... ", *serviceName)

	kc, err := pkg.OpenKafkaConsumer(pkg.ConsumerConfig{
		Brokers:        []string{"localhost:9092"},
		Topic:          "Order",
		GroupID:        *serviceName,
		HandlerTimeout: 1000,
	})
	if err != nil {
		log.Fatal(err)
	}

	defer kc.CloseGracefully(5 * time.Second)
	handler := func(ctx context.Context, msg pkg.Message) error {
		// ทำอะไรก็ได้กับข้อความที่ได้รับ
		jsonBytes, err := json.MarshalIndent(msg.Message, "", "  ")
		if err != nil {
			fmt.Printf("📥 Received message from topic %s: key=%s, [unmarshal error: %v]\n", msg.Topic, msg.Key, err)
		}
		fmt.Printf("📥 Received message from topic %s\nKey: %s\nValue:\n%s\n", msg.Topic, msg.Key, string(jsonBytes))
		return nil
	}
	policy := pkg.ConsumerConfig{
		HandlerTimeout: 30 * time.Second,
		MaxRetries:     3,
	}
	if err := kc.ReceiveMessage(handler, policy); err != nil {
		log.Printf("Consumer error: %v", err)
	}
}
