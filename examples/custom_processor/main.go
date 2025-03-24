package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Aliciahan/go-routine-queue/pkg/queue"
)

// getEnv 获取环境变量，如果不存在则返回默认值
func getEnv(key, defaultValue string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultValue
	}
	return value
}

// 自定义任务处理器
type CustomTaskProcessor struct {
	Name string
}

// 自定义任务负载
type CustomTaskPayload struct {
	ID      string    `json:"id"`
	Name    string    `json:"name"`
	Data    string    `json:"data"`
	Created time.Time `json:"created"`
}

// Process 实现TaskProcessor接口
func (p *CustomTaskProcessor) Process(ctx context.Context, task *queue.Task) error {
	// 解析任务负载
	var payload CustomTaskPayload
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		return fmt.Errorf("failed to unmarshal task payload: %w", err)
	}

	// 打印任务信息
	fmt.Printf("[%s] Processing task: %s - %s\n", p.Name, payload.ID, payload.Name)

	// 模拟处理时间
	select {
	case <-time.After(500 * time.Millisecond):
		fmt.Printf("[%s] Task %s completed successfully\n", p.Name, payload.ID)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func main() {
	// 获取环境变量配置
	dbConnStr := getEnv("DB_CONNECTION_STRING", "postgres://postgres:postgres@localhost:5432/queue_db?sslmode=disable")

	// 连接数据库
	db, err := queue.NewDBConnector(dbConnStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// 初始化数据库表结构
	if err := db.InitSchema(); err != nil {
		log.Fatalf("Failed to initialize database schema: %v", err)
	}

	// 创建队列管理器
	qm := queue.NewQueueManager(db)

	// 注册自定义任务处理器
	qm.RegisterTaskProcessor("fast_queue", &CustomTaskProcessor{Name: "FastProcessor"})
	qm.RegisterTaskProcessor("slow_queue", &CustomTaskProcessor{Name: "SlowProcessor"})

	// 启动队列管理器
	if err := qm.Start(); err != nil {
		log.Fatalf("Failed to start queue manager: %v", err)
	}

	// 创建队列
	if err := qm.CreateQueue("fast_queue", 3); err != nil {
		// 如果队列已存在，忽略错误
		if fmt.Sprintf("%v", err) != "queue fast_queue already exists" {
			log.Fatalf("Failed to create fast_queue: %v", err)
		}
	}

	if err := qm.CreateQueue("slow_queue", 2); err != nil {
		// 如果队列已存在，忽略错误
		if fmt.Sprintf("%v", err) != "queue slow_queue already exists" {
			log.Fatalf("Failed to create slow_queue: %v", err)
		}
	}

	// 创建监控接口
	monitor := queue.NewMonitor(qm, db)

	// 启动监控HTTP服务
	go func() {
		log.Printf("Starting monitor server on :8080")
		if err := monitor.Start(":8080"); err != nil {
			log.Fatalf("Failed to start monitor server: %v", err)
		}
	}()

	// 创建并提交一些任务
	go func() {
		// 等待服务启动
		time.Sleep(1 * time.Second)

		// 创建任务
		for i := 0; i < 5; i++ {
			// 创建快队列任务
			fastPayload := CustomTaskPayload{
				ID:      fmt.Sprintf("fast-task-%d", i),
				Name:    fmt.Sprintf("Fast Task %d", i),
				Data:    fmt.Sprintf("This is fast task data for task %d", i),
				Created: time.Now(),
			}

			// 序列化任务负载
			fastPayloadBytes, err := json.Marshal(fastPayload)
			if err != nil {
				log.Printf("Failed to marshal fast task payload: %v", err)
				continue
			}

			// 将任务加入快队列
			fastTaskID, err := db.EnqueueTask("fast_queue", fastPayloadBytes)
			if err != nil {
				log.Printf("Failed to enqueue fast task: %v", err)
				continue
			}

			log.Printf("Fast Task %s enqueued with ID: %d", fastPayload.ID, fastTaskID)

			// 创建慢队列任务
			slowPayload := CustomTaskPayload{
				ID:      fmt.Sprintf("slow-task-%d", i),
				Name:    fmt.Sprintf("Slow Task %d", i),
				Data:    fmt.Sprintf("This is slow task data for task %d", i),
				Created: time.Now(),
			}

			// 序列化任务负载
			slowPayloadBytes, err := json.Marshal(slowPayload)
			if err != nil {
				log.Printf("Failed to marshal slow task payload: %v", err)
				continue
			}

			// 将任务加入慢队列
			slowTaskID, err := db.EnqueueTask("slow_queue", slowPayloadBytes)
			if err != nil {
				log.Printf("Failed to enqueue slow task: %v", err)
				continue
			}

			log.Printf("Slow Task %s enqueued with ID: %d", slowPayload.ID, slowTaskID)

			// 间隔一段时间
			time.Sleep(500 * time.Millisecond)
		}
	}()

	// 等待中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 等待信号
	sig := <-sigCh
	log.Printf("Received signal: %v, shutting down...", sig)

	// 停止队列管理器
	qm.Stop()

	// 等待一段时间确保所有任务都能完成
	log.Println("Waiting for all tasks to complete...")
	time.Sleep(2 * time.Second)

	log.Println("Shutdown complete")
}
