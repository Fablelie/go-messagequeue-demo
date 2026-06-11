package rabbit

import (
	"log"
	"sync"

	"github.com/fasthttp/websocket"
)

type Hub struct {
	clients map[*websocket.Conn]string // เก็บ conn และชื่อ Worker
	mu      sync.Mutex
}

func NewHub() *Hub {
	return &Hub{clients: make(map[*websocket.Conn]string)}
}

func (h *Hub) Register(ws *websocket.Conn, name string) {
	h.mu.Lock()
	h.clients[ws] = name
	h.mu.Unlock()
}

func (h *Hub) Unregister(ws *websocket.Conn) {
	h.mu.Lock()
	delete(h.clients, ws)
	h.mu.Unlock()
}

func (h *Hub) StartConsuming() {
	msgs, err := ch.Consume("web-rabbit-queue", "", true, false, false, false, nil)
	if err != nil {
		log.Printf("RabbitMQ Consume Error: %v", err)
		return
	}

	go func() {
		for d := range msgs {
			h.mu.Lock()
			// จุดสำคัญของ RabbitMQ: วนลูปส่งให้ "แค่ลูกค้าคนเดียว" ที่ว่างอยู่ (Competing Consumer)
			for ws, name := range h.clients {
				err := ws.WriteJSON(map[string]string{
					"worker":  name,
					"message": string(d.Body),
				})
				if err == nil {
					// ส่งให้คนนี้แล้ว เบรกออกเพื่อไม่ให้คนอื่นได้ข้อความนี้ซ้ำ
					break
				}
			}
			h.mu.Unlock()
		}
	}()
}
