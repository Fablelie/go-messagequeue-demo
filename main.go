package main

import (
	"log"
	"sync"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/static"
)

// ขยายโครงสร้างข้อมูล Kafka ให้รองรับฟิลด์ TargetGroup (ระบุผู้รับชิ้นนั้นๆ)
type KafkaMessage struct {
	Offset      int64  `json:"offset"`
	TargetGroup string `json:"target_group"` // ฟิลด์ระบุกลุ่มผู้รับ: "analytics" หรือ "email"
	Value       string `json:"value"`
}

var (
	rabbitQueue = []string{}

	kafkaLog                 = []KafkaMessage{}
	kafkaOffsetCounter int64 = 0

	// เข็มชี้ตำแหน่งอ่านแยกอิสระตัวเดิม (อ่านไปข้างหน้าเรื่อยๆ ไม่ย้อนกลับ)
	analyticsCurrentOffset int64 = 0
	emailCurrentOffset     int64 = 0

	mu sync.Mutex
)

func main() {
	app := fiber.New()
	app.Use("/*", static.New("./web"))

	app.Get("/api/queues-status", func(c fiber.Ctx) error {
		mu.Lock()
		defer mu.Unlock()
		return c.JSON(fiber.Map{
			"rabbit_queue": rabbitQueue,
			"kafka_log":    kafkaLog,
		})
	})

	// ==========================================
	// 📨 ฝั่งส่งข้อมูล (PRODUCER)
	// ==========================================
	app.Post("/api/send-rabbit", func(c fiber.Ctx) error {
		msg := c.FormValue("message")
		mu.Lock()
		rabbitQueue = append(rabbitQueue, msg)
		mu.Unlock()
		return c.JSON(fiber.Map{"status": "success"})
	})

	app.Post("/api/send-kafka", func(c fiber.Ctx) error {
		msg := c.FormValue("message")
		targetGroup := c.FormValue("target_group") // รับค่าว่าตั้งใจส่งให้ใคร ("analytics" หรือ "email")

		mu.Lock()
		kafkaLog = append(kafkaLog, KafkaMessage{
			Offset:      kafkaOffsetCounter,
			TargetGroup: targetGroup,
			Value:       msg,
		})
		kafkaOffsetCounter++
		mu.Unlock()
		return c.JSON(fiber.Map{"status": "success"})
	})

	// ==========================================
	// 📥 ฝั่งดึงข้อมูล (CONSUMER)
	// ==========================================
	app.Get("/api/get-rabbit", func(c fiber.Ctx) error {
		mu.Lock()
		defer mu.Unlock()
		if len(rabbitQueue) == 0 {
			return c.JSON(fiber.Map{"message": nil})
		}
		work := rabbitQueue[0]
		rabbitQueue = rabbitQueue[1:]
		return c.JSON(fiber.Map{"message": work})
	})

	// ตรรกะคัดกรองตามความต้องการ: ถ้าเปิดมาไม่ใช่ของกลุ่มตัวเอง -> จะขึ้นอ่านไม่เจอ
	app.Get("/api/get-kafka", func(c fiber.Ctx) error {
		targetGroup := c.Query("target") // แผนกที่กดดึงข้อมูลมา: "analytics" หรือ "email"
		mu.Lock()
		defer mu.Unlock()

		var currentOffset int64
		if targetGroup == "analytics" {
			currentOffset = analyticsCurrentOffset
		} else {
			currentOffset = emailCurrentOffset
		}

		// ค้นหาข้อความที่ตรงตาม Offset ปัจจุบันของกลุ่มนั้นๆ
		var foundMessage *KafkaMessage
		for _, logItem := range kafkaLog {
			if logItem.Offset == currentOffset {
				foundMessage = &logItem
				break
			}
		}

		// เคสที่ 1: ดึงไปจนหมดสมุดบันทึกแล้วจริงๆ ไม่มีข้อมูลชิ้นใดเหลือ
		if foundMessage == nil {
			return c.JSON(fiber.Map{"message": nil})
		}

		// ไม่ว่าจะอ่านเจอของกลุ่มตัวเองหรือของคนอื่น เข็มตำแหน่งอ่าน (Offset) ต้องบวกเลื่อนไปข้างหน้า 1 เสมอ (ข้ามหน้าไปถัดไป)
		if targetGroup == "analytics" {
			analyticsCurrentOffset++
		} else {
			emailCurrentOffset++
		}

		// 🎯 เคสสำคัญ: ตรวจสอบเงื่อนไขกลุ่มเป้าหมาย (Group Filtering)
		// ถ้ากลุ่มเป้าหมายใน Event ไม่ตรงกับกลุ่มคนที่กดดึงงาน -> ส่งสัญญลักษณ์บอกว่าข้าม/อ่านไม่เจอ
		if foundMessage.TargetGroup != targetGroup {
			return c.JSON(fiber.Map{"message": nil}) // จะไปขึ้น ❌ ที่หน้าบ้านทันที
		}

		// เคสที่ 3: กลุ่มเป้าหมายตรงกันเป๊ะ -> แสดงข้อความสำเร็จ
		return c.JSON(fiber.Map{"message": foundMessage.Value})
	})

	log.Fatal(app.Listen(":8080"))
}
