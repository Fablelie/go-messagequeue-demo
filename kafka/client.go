package kafka

import (
	"context"
	"os"

	"github.com/segmentio/kafka-go"
)

var writer *kafka.Writer

func InitKafka() {
	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	writer = &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    "web-kafka-topic",
		Balancer: &kafka.LeastBytes{},
	}
}

func Publish(msg string) error {
	return writer.WriteMessages(context.Background(),
		kafka.Message{Value: []byte(msg)},
	)
}
