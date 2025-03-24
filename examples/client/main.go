package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Aliciahan/go-routine-queue/pkg/queue"
)

func main() {
	// 连接数据库
	dbConnStr := os.Getenv("DB_CONNECTION_STRING")
	db, err := queue.NewDBConnector(dbConnStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// 创建任务负载
	type TaskPayload struct {
		ID      string    `json:"id"`
		Name    string    `json:"name"`
		Data    string    `json:"data"`
		Created time.Time `json:"created"`
	}

	// 创建10个示例任务
	for i := 0; i < 100; i++ {
		payload := TaskPayload{
			ID:      fmt.Sprintf("task-%d", i),
			Name:    fmt.Sprintf("Example Task %d", i),
			Data:    fmt.Sprintf("This is task data for task %d", i),
			Created: time.Now(),
		}

		// 序列化任务负载
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			log.Printf("Failed to marshal task payload: %v", err)
			continue
		}

		// 创建队列管理器
		//qm := queue.NewQueueManager(db, fmt.Sprintf("client-%d", time.Now().UnixNano()))

		// 将任务加入队列
		taskID, err := db.EnqueueTask("default", payloadBytes)
		if err != nil {
			log.Printf("Failed to enqueue task: %v", err)
			continue
		}

		log.Printf("Task %s enqueued with ID: %d", payload.ID, taskID)
	}

	// 查询队列状态
	log.Println("Checking queue status...")
	time.Sleep(2 * time.Second)

	// 创建HTTP客户端查询队列状态
	resp, err := http.Get(os.Getenv("SERVER_URI") + "/api/queues/default")
	if err != nil {
		log.Fatalf("Failed to get queue status: %v", err)
	}
	defer resp.Body.Close()

	var status queue.QueueStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		log.Fatalf("Failed to decode response: %v", err)
	}

	log.Printf("Queue Status: %+v", status)

	// 等待所有任务处理完成
	for status.PendingTasks > 0 {
		time.Sleep(1 * time.Second)

		resp, err := http.Get(os.Getenv("SERVER_URI") + "/api/queues/default")
		if err != nil {
			log.Fatalf("Failed to get queue status: %v", err)
		}

		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			resp.Body.Close()
			log.Fatalf("Failed to decode response: %v", err)
		}
		resp.Body.Close()

		log.Printf("Pending tasks: %d, Active workers: %d", status.PendingTasks, status.ActiveWorkers)
	}

	log.Println("All tasks processed successfully!")
}
