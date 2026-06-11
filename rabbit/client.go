package rabbit

import (
	"context"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

var conn *amqp.Connection
var ch *amqp.Channel

func InitRabbit() {
	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@localhost:5672/"
	}

	var err error
	conn, err = amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("RabbitMQ Connect Fail: %v", err)
	}
	ch, err = conn.Channel()
	if err != nil {
		log.Fatalf("RabbitMQ Channel Fail: %v", err)
	}
	_, err = ch.QueueDeclare("web-rabbit-queue", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("RabbitMQ Queue Declare Fail: %v", err)
	}
}

func Publish(msg string) error {
	return ch.PublishWithContext(context.Background(),
		"", "web-rabbit-queue", false, false,
		amqp.Publishing{ContentType: "text/plain", Body: []byte(msg)},
	)
}
