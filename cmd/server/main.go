package main

import (
	"fmt"
	"log"
	"net"
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
	instanceRole := getEnv("INSTANCE_ROLE", "both") // server, worker, or both

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

	// 创建worker协调器
	hostname, _ := os.Hostname()
	ip := getOutboundIP().String()
	coordinator := queue.NewWorkerCoordinator(db, instanceID, hostname, ip)

	// 启动worker协调器
	if err := coordinator.Start(); err != nil {
		log.Fatalf("Failed to start worker coordinator: %v", err)
	}

	// 根据实例角色启动相应服务
	var monitor *queue.Monitor
	if instanceRole == "server" || instanceRole == "both" {
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

		// 设置默认队列的全局worker数量
		if err := coordinator.SetGlobalWorkerCount("default", defaultWorkerCount); err != nil {
			log.Printf("Warning: Failed to set global worker count: %v", err)
		}

		// 创建监控接口
		monitor = queue.NewMonitor(qm, db, coordinator)

		// 启动监控HTTP服务
		go func() {
			log.Printf("Starting monitor server on %s", monitorAddr)
			if err := monitor.Start(monitorAddr); err != nil {
				log.Fatalf("Failed to start monitor server: %v", err)
			}
		}()
	}

	// 等待中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// 优雅关闭
	log.Println("Shutting down...")
	if instanceRole == "server" || instanceRole == "both" {
		monitor.Stop()
		qm.Stop()
	}
	coordinator.Stop()
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
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	return fmt.Sprintf("%s-%d", hostname, time.Now().UnixNano())
}

// getOutboundIP 获取本机对外IP地址
func getOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return net.ParseIP("127.0.0.1")
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP
}
