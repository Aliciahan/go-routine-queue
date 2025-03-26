package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Aliciahan/go-routine-queue/pkg/queue"
)

func main() {
	// 获取环境变量配置
	dbConnStr := getEnv("DB_CONNECTION_STRING", "postgres://postgres:postgres@localhost:5432/queue_db?sslmode=disable")
	defaultWorkerCount, _ := strconv.Atoi(getEnv("DEFAULT_WORKER_COUNT", "5"))
	cleanupIntervalSec, _ := strconv.Atoi(getEnv("CLEANUP_INTERVAL", "3600"))
	monitorAddr := getEnv("MONITOR_ADDR", ":8080")
	instanceID := getEnv("INSTANCE_ID", generateInstanceID())

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
	qm := queue.NewQueueManager(db, instanceID)
	qm.SetCleanupInterval(time.Duration(cleanupIntervalSec) * time.Second)

	// 启动队列管理器
	if err := qm.Start(); err != nil {
		log.Fatalf("Failed to start queue manager: %v", err)
	}

	// 创建默认队列（如果需要）
	if err := qm.CreateQueue("default", defaultWorkerCount); err != nil {
		// 如果队列已存在，忽略错误
		if fmt.Sprintf("%v", err) != "queue default already exists" {
			log.Fatalf("Failed to create default queue: %v", err)
		}
	}

	// 创建监控接口
	monitor := queue.NewMonitor(qm, db)

	// 启动监控HTTP服务
	go func() {
		log.Printf("Starting monitor server on %s", monitorAddr)
		if err := monitor.Start(monitorAddr); err != nil {
			log.Fatalf("Failed to start monitor server: %v", err)
		}
	}()

	// 等待中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// 优雅关闭
	log.Println("Shutting down...")
	monitor.Stop()
	qm.Stop()
	log.Println("Shutdown complete")
}

// getEnv 获取环境变量，如果不存在则返回默认值
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// generateInstanceID 生成唯一的实例ID
func generateInstanceID() string {
	// 使用主机名和时间戳生成唯一ID
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	// 格式：hostname-timestamp
	return fmt.Sprintf("%s-%d", hostname, time.Now().UnixNano())
}
