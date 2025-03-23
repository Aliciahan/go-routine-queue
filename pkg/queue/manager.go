package queue

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// QueueManager 负责队列的创建、删除和worker数量的动态调整
type QueueManager struct {
	db                *DBConnector
	workerPools       map[string]*WorkerPool
	mu                sync.RWMutex
	cleanupTicker     *time.Ticker
	ctx               context.Context
	cancel            context.CancelFunc
	processorRegistry *TaskProcessorRegistry
}

// NewQueueManager 创建一个新的队列管理器
func NewQueueManager(db *DBConnector) *QueueManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &QueueManager{
		db:                db,
		workerPools:       make(map[string]*WorkerPool),
		mu:                sync.RWMutex{},
		cleanupTicker:     time.NewTicker(time.Hour), // 默认每小时清理一次
		ctx:               ctx,
		cancel:            cancel,
		processorRegistry: NewTaskProcessorRegistry(),
	}
}

// Start 启动队列管理器
func (qm *QueueManager) Start() error {
	// 从数据库加载已存在的队列配置
	queues, err := qm.db.GetAllQueues()
	if err != nil {
		return fmt.Errorf("failed to load queues: %w", err)
	}

	// 为每个队列创建工作器池
	for _, q := range queues {
		qm.CreateQueue(q.Name, q.WorkerCount)
	}

	// 启动清理任务
	go qm.startCleanupTask()

	return nil
}

// Stop 停止队列管理器
func (qm *QueueManager) Stop() {
	qm.cancel()
	qm.cleanupTicker.Stop()

	// 停止所有工作器池
	qm.mu.Lock()
	defer qm.mu.Unlock()

	for _, pool := range qm.workerPools {
		pool.Stop()
	}
}

// CreateQueue 创建一个新队列并启动指定数量的worker
func (qm *QueueManager) CreateQueue(queueName string, workerCount int) error {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	// 检查队列是否已存在
	if _, exists := qm.workerPools[queueName]; exists {
		return fmt.Errorf("queue %s already exists", queueName)
	}

	// 在数据库中创建队列
	if err := qm.db.CreateQueue(queueName, workerCount); err != nil {
		return fmt.Errorf("failed to create queue in database: %w", err)
	}

	// 创建工作器池
	pool := NewWorkerPool(qm.db, queueName, workerCount)

	// 设置任务处理器
	processor := qm.processorRegistry.GetProcessor(queueName)
	pool.SetProcessor(processor)

	qm.workerPools[queueName] = pool

	// 启动工作器池
	pool.Start()

	return nil
}

// UpdateWorkerCount 更新指定队列的worker数量
func (qm *QueueManager) UpdateWorkerCount(queueName string, newWorkerCount int) error {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	// 检查队列是否存在
	pool, exists := qm.workerPools[queueName]
	if !exists {
		return fmt.Errorf("queue %s does not exist", queueName)
	}

	// 更新数据库中的worker数量
	if err := qm.db.UpdateQueueWorkerCount(queueName, newWorkerCount); err != nil {
		return fmt.Errorf("failed to update worker count in database: %w", err)
	}

	// 更新工作器池中的worker数量
	pool.UpdateWorkerCount(newWorkerCount)

	return nil
}

// DeleteQueue 删除指定队列
func (qm *QueueManager) DeleteQueue(queueName string) error {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	// 检查队列是否存在
	pool, exists := qm.workerPools[queueName]
	if !exists {
		return fmt.Errorf("queue %s does not exist", queueName)
	}

	// 停止工作器池
	pool.Stop()

	// 从数据库中删除队列
	if err := qm.db.DeleteQueue(queueName); err != nil {
		return fmt.Errorf("failed to delete queue from database: %w", err)
	}

	// 从内存中删除队列
	delete(qm.workerPools, queueName)

	return nil
}

// GetQueueStatus 获取指定队列的状态
func (qm *QueueManager) GetQueueStatus(queueName string) (*QueueStatus, error) {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	// 检查队列是否存在
	pool, exists := qm.workerPools[queueName]
	if !exists {
		return nil, fmt.Errorf("queue %s does not exist", queueName)
	}

	// 获取队列状态
	pendingTasks, err := qm.db.GetPendingTaskCount(queueName)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending task count: %w", err)
	}

	return &QueueStatus{
		QueueName:     queueName,
		WorkerCount:   pool.GetWorkerCount(),
		PendingTasks:  pendingTasks,
		ActiveWorkers: pool.GetActiveWorkerCount(),
	}, nil
}

// GetAllQueueStatus 获取所有队列的状态
func (qm *QueueManager) GetAllQueueStatus() ([]*QueueStatus, error) {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	statuses := make([]*QueueStatus, 0, len(qm.workerPools))

	for queueName, pool := range qm.workerPools {
		pendingTasks, err := qm.db.GetPendingTaskCount(queueName)
		if err != nil {
			return nil, fmt.Errorf("failed to get pending task count for queue %s: %w", queueName, err)
		}

		statuses = append(statuses, &QueueStatus{
			QueueName:     queueName,
			WorkerCount:   pool.GetWorkerCount(),
			PendingTasks:  pendingTasks,
			ActiveWorkers: pool.GetActiveWorkerCount(),
		})
	}

	return statuses, nil
}

// startCleanupTask 启动清理任务
func (qm *QueueManager) startCleanupTask() {
	for {
		select {
		case <-qm.cleanupTicker.C:
			qm.cleanupCompletedTasks()
		case <-qm.ctx.Done():
			return
		}
	}
}

// cleanupCompletedTasks 清理已完成的任务
func (qm *QueueManager) cleanupCompletedTasks() {
	// 设置清理的时间阈值，例如清理7天前的已完成任务
	threshold := time.Now().AddDate(0, 0, -7)

	// 执行清理操作
	deleted, err := qm.db.CleanupCompletedTasks(threshold)
	if err != nil {
		fmt.Printf("Failed to cleanup completed tasks: %v\n", err)
		return
	}

	fmt.Printf("Cleaned up %d completed tasks\n", deleted)
}

// SetCleanupInterval 设置清理间隔
func (qm *QueueManager) SetCleanupInterval(interval time.Duration) {
	qm.cleanupTicker.Stop()
	qm.cleanupTicker = time.NewTicker(interval)
}

// RegisterTaskProcessor 注册队列的任务处理器
func (qm *QueueManager) RegisterTaskProcessor(queueName string, processor TaskProcessor) {
	qm.processorRegistry.Register(queueName, processor)

	// 如果队列已存在，更新其处理器
	qm.mu.RLock()
	pool, exists := qm.workerPools[queueName]
	qm.mu.RUnlock()

	if exists {
		pool.SetProcessor(processor)
	}
}

// SetDefaultTaskProcessor 设置默认任务处理器
func (qm *QueueManager) SetDefaultTaskProcessor(processor TaskProcessor) {
	qm.processorRegistry.SetDefaultProcessor(processor)
}

// QueueStatus 表示队列的状态信息
type QueueStatus struct {
	QueueName     string `json:"queue_name"`
	WorkerCount   int    `json:"worker_count"`
	PendingTasks  int    `json:"pending_tasks"`
	ActiveWorkers int    `json:"active_workers"`
}
