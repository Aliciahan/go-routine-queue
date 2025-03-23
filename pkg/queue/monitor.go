package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Monitor 提供系统状态查询功能
type Monitor struct {
	queueManager *QueueManager
	db           *DBConnector
	server       *http.Server
}

// NewMonitor 创建一个新的监控接口
func NewMonitor(qm *QueueManager, db *DBConnector) *Monitor {
	return &Monitor{
		queueManager: qm,
		db:           db,
	}
}

// Start 启动监控HTTP服务
func (m *Monitor) Start(addr string) error {
	mux := http.NewServeMux()

	// 注册API路由
	mux.HandleFunc("/api/queues", m.handleGetAllQueues)
	mux.HandleFunc("/api/queues/", m.handleQueueOperations)
	mux.HandleFunc("/api/stats", m.handleGetStats)
	mux.HandleFunc("/health", m.handleHealth)

	// 创建HTTP服务器
	m.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// 启动HTTP服务器
	fmt.Printf("Starting monitor server on %s\n", addr)
	return m.server.ListenAndServe()
}

// Stop 停止监控HTTP服务
func (m *Monitor) Stop() error {
	if m.server != nil {
		// 创建一个5秒超时的上下文
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 优雅地关闭服务器
		return m.server.Shutdown(ctx)
	}
	return nil
}

// handleGetAllQueues 处理获取所有队列的请求
func (m *Monitor) handleGetAllQueues(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 获取所有队列状态
	statuses, err := m.queueManager.GetAllQueueStatus()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get queue statuses: %v", err), http.StatusInternalServerError)
		return
	}

	// 返回JSON响应
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(statuses)
}

// handleQueueOperations 处理单个队列的操作
func (m *Monitor) handleQueueOperations(w http.ResponseWriter, r *http.Request) {
	// 解析队列名称
	queueName := r.URL.Path[len("/api/queues/"):]
	if queueName == "" {
		http.Error(w, "Queue name is required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		// 获取单个队列状态
		status, err := m.queueManager.GetQueueStatus(queueName)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to get queue status: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)

	case http.MethodPost:
		// 创建新队列
		var req struct {
			WorkerCount int `json:"worker_count"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		if req.WorkerCount <= 0 {
			http.Error(w, "Worker count must be positive", http.StatusBadRequest)
			return
		}

		if err := m.queueManager.CreateQueue(queueName, req.WorkerCount); err != nil {
			http.Error(w, fmt.Sprintf("Failed to create queue: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)

	case http.MethodPut:
		// 更新队列worker数量
		var req struct {
			WorkerCount int `json:"worker_count"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		if req.WorkerCount <= 0 {
			http.Error(w, "Worker count must be positive", http.StatusBadRequest)
			return
		}

		if err := m.queueManager.UpdateWorkerCount(queueName, req.WorkerCount); err != nil {
			http.Error(w, fmt.Sprintf("Failed to update worker count: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)

	case http.MethodDelete:
		// 删除队列
		if err := m.queueManager.DeleteQueue(queueName); err != nil {
			http.Error(w, fmt.Sprintf("Failed to delete queue: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleGetStats 处理获取系统统计信息的请求
func (m *Monitor) handleGetStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 获取所有队列状态
	statuses, err := m.queueManager.GetAllQueueStatus()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get queue statuses: %v", err), http.StatusInternalServerError)
		return
	}

	// 计算统计信息
	totalQueues := len(statuses)
	totalWorkers := 0
	totalActiveWorkers := 0
	totalPendingTasks := 0

	for _, status := range statuses {
		totalWorkers += status.WorkerCount
		totalActiveWorkers += status.ActiveWorkers
		totalPendingTasks += status.PendingTasks
	}

	// 构建响应
	stats := struct {
		TotalQueues        int       `json:"total_queues"`
		TotalWorkers       int       `json:"total_workers"`
		TotalActiveWorkers int       `json:"total_active_workers"`
		TotalPendingTasks  int       `json:"total_pending_tasks"`
		Timestamp          time.Time `json:"timestamp"`
	}{
		TotalQueues:        totalQueues,
		TotalWorkers:       totalWorkers,
		TotalActiveWorkers: totalActiveWorkers,
		TotalPendingTasks:  totalPendingTasks,
		Timestamp:          time.Now(),
	}

	// 返回JSON响应
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// handleHealth 处理健康检查请求
func (m *Monitor) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 检查数据库连接
	if err := m.db.db.Ping(); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"status": "unhealthy", "reason": "database connection failed"})
		return
	}

	// 返回健康状态
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}
