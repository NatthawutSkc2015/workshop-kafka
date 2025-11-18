package main

import (
	pkg "go-message/pkg/kafka"
	"log"
	"time"

	"github.com/google/uuid"
)

func main() {
	msg := pkg.Message{
		Key: uuid.NewString(),
		Message: map[string]interface{}{
			"event_type":  "order_created",
			"timestamp":   "2025-11-07T10:30:45.123Z",
			"order_id":    "ORD-2025-001234",
			"customer_id": "CUST-567890",
			"items": []interface{}{
				map[string]interface{}{
					"product_id": "PROD-111",
					"name":       "iPhone 15 Pro",
					"quantity":   1,
					"price":      42900.00,
				},
				map[string]interface{}{
					"product_id": "PROD-222",
					"name":       "AirPods Pro",
					"quantity":   1,
					"price":      8990.00,
				},
			},
			"total_amount": "51890.00",
			"currency":     "THB",
			"shipping_address": map[string]interface{}{
				"province":    "Bangkok",
				"district":    "Chatuchak",
				"postal_code": "10900",
			},
			"payment_method": "credit_card",
			"status":         "pending",
		},
	}

	kp, err := pkg.OpenKafkaProducer(pkg.ProducerConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "Order",
	})
	if err != nil {
		log.Fatal(err)
	}
	policy := pkg.RetryPolicy{
		MaxRetries:     5,
		InitialBackoff: time.Second,
		MaxBackoff:     30 * time.Second,
		BackoffFactor:  2.0,
		Jitter:         true,
	}
	if err := kp.SendMessageWithRetry(msg, policy); err != nil {
		log.Println(err.Error())
	}
	log.Println("Send message to kafka success.")
	defer kp.CloseGracefully(5 * time.Second)
}
