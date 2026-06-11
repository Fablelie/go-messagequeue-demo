package kafka

import (
	"context"
	"os"
	"sync"

	"github.com/fasthttp/websocket"
	"github.com/segmentio/kafka-go"
)

type Hub struct {
	clients map[*websocket.Conn]string
	mu      sync.Mutex
	groupID string
}

func NewHub(groupID string) *Hub {
	return &Hub{
		clients: make(map[*websocket.Conn]string),
		groupID: groupID,
	}
}

func (h *Hub) Register(ws *websocket.Conn) {
	h.mu.Lock()
	h.clients[ws] = "active"
	h.mu.Unlock()
}

func (h *Hub) Unregister(ws *websocket.Conn) {
	h.mu.Lock()
	delete(h.clients, ws)
	h.mu.Unlock()
}

func (h *Hub) StartConsuming() {
	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   "web-kafka-topic",
		GroupID: h.groupID,
	})

	go func() {
		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				break
			}
			h.mu.Lock()
			// จุดสำคัญของ Kafka Pub-Sub: ทุกคนที่อยู่ใน Group นี้จะได้ข้อความกระจายไปครบถ้วน
			for ws := range h.clients {
				_ = ws.WriteJSON(map[string]string{
					"group":   h.groupID,
					"message": string(m.Value),
				})
			}
			h.mu.Unlock()
		}
	}()
}
