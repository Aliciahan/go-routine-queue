package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerPool 管理goroutine工作器的生命周期
type WorkerPool struct {
	db             *DBConnector
	queueName      string
	workerCount    int32
	activeWorkers  int32
	workers        map[string]context.CancelFunc
	mu             sync.RWMutex
	processTimeout time.Duration
	pollInterval   time.Duration
	processor      TaskProcessor
	instanceID     string // 实例ID，用于标识worker所属的实例
}

// NewWorkerPool 创建一个新的工作器池
func NewWorkerPool(db *DBConnector, queueName string, workerCount int, instanceID string) *WorkerPool {
	return &WorkerPool{
		db:             db,
		queueName:      queueName,
		workerCount:    int32(workerCount),
		activeWorkers:  0,
		workers:        make(map[string]context.CancelFunc),
		mu:             sync.RWMutex{},
		processTimeout: 5 * time.Minute,         // 默认任务处理超时时间
		pollInterval:   1 * time.Second,         // 默认轮询间隔
		processor:      &DefaultTaskProcessor{}, // 默认使用默认处理器
		instanceID:     instanceID,              // 设置实例ID
	}
}

// Start 启动工作器池
func (wp *WorkerPool) Start() {
	// 启动指定数量的工作器
	for i := 0; i < int(wp.workerCount); i++ {
		wp.startWorker()
	}
}

// Stop 停止工作器池
func (wp *WorkerPool) Stop() {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	// 停止所有工作器
	for _, cancel := range wp.workers {
		cancel()
	}

	// 清空工作器映射
	wp.workers = make(map[string]context.CancelFunc)
	atomic.StoreInt32(&wp.activeWorkers, 0)
}

// UpdateWorkerCount 更新工作器数量
func (wp *WorkerPool) UpdateWorkerCount(newCount int) {
	oldCount := int(atomic.LoadInt32(&wp.workerCount))
	atomic.StoreInt32(&wp.workerCount, int32(newCount))

	// 如果新数量大于旧数量，启动额外的工作器
	if newCount > oldCount {
		for i := 0; i < newCount-oldCount; i++ {
			wp.startWorker()
		}
	} else if newCount < oldCount {
		// 如果新数量小于旧数量，停止多余的工作器
		wp.stopExcessWorkers(oldCount - newCount)
	}
}

// GetWorkerCount 获取工作器数量
func (wp *WorkerPool) GetWorkerCount() int {
	return int(atomic.LoadInt32(&wp.workerCount))
}

// GetActiveWorkerCount 获取活跃工作器数量
func (wp *WorkerPool) GetActiveWorkerCount() int {
	return int(atomic.LoadInt32(&wp.activeWorkers))
}

// startWorker 启动一个新的工作器
func (wp *WorkerPool) startWorker() {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	// 创建工作器上下文
	ctx, cancel := context.WithCancel(context.Background())
	workerID := fmt.Sprintf("%s-%s-%d", wp.instanceID, wp.queueName, time.Now().UnixNano())

	// 保存取消函数
	wp.workers[workerID] = cancel

	// 更新worker_counts
	wp.updateWorkerCount(1)

	// 启动工作器goroutine
	go func() {
		defer func() {
			wp.mu.Lock()
			delete(wp.workers, workerID)
			atomic.AddInt32(&wp.activeWorkers, -1)
			wp.mu.Unlock()

			// 恢复可能的panic
			if r := recover(); r != nil {
				fmt.Printf("Worker %s panic: %v\n", workerID, r)
			}
		}()

		atomic.AddInt32(&wp.activeWorkers, 1)
		wp.workerLoop(ctx, workerID)
	}()
}

// stopExcessWorkers 停止多余的工作器
func (wp *WorkerPool) stopExcessWorkers(count int) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	// 获取所有工作器ID
	workerIDs := make([]string, 0, len(wp.workers))
	for id := range wp.workers {
		workerIDs = append(workerIDs, id)
	}

	// 停止指定数量的工作器
	for i := 0; i < count && i < len(workerIDs); i++ {
		if cancel, exists := wp.workers[workerIDs[i]]; exists {
			cancel()
			delete(wp.workers, workerIDs[i])
			// 更新worker_counts
			wp.updateWorkerCount(-1)
		}
	}
}

// workerLoop 工作器主循环
func (wp *WorkerPool) workerLoop(ctx context.Context, workerID string) {
	for {
		select {
		case <-ctx.Done():
			// 工作器被取消
			return
		default:
			// 尝试获取并处理任务
			wp.processTask(ctx, workerID)

			// 等待一段时间再次尝试
			time.Sleep(wp.pollInterval)
		}
	}
}

// processTask 处理任务
func (wp *WorkerPool) processTask(ctx context.Context, workerID string) {
	// 创建一个带超时的上下文
	taskCtx, cancel := context.WithTimeout(ctx, wp.processTimeout)
	defer cancel()

	// 从队列中获取任务
	task, err := wp.db.DequeueTask(taskCtx, wp.queueName, workerID)
	if err != nil {
		fmt.Printf("Worker %s failed to dequeue task: %v\n", workerID, err)
		return
	}

	// 如果没有任务，直接返回
	if task == nil {
		return
	}

	// 处理任务
	err = wp.executeTask(taskCtx, task)
	if err != nil {
		// 任务执行失败
		fmt.Printf("Worker %s failed to execute task %d: %v\n", workerID, task.ID, err)
		wp.db.FailTask(task.ID, err.Error())
	} else {
		// 任务执行成功
		wp.db.CompleteTask(task.ID)
	}
}

// executeTask 执行具体的任务逻辑
func (wp *WorkerPool) executeTask(ctx context.Context, task *Task) error {
	// 使用注册的任务处理器处理任务
	if wp.processor != nil {
		return wp.processor.Process(ctx, task)
	}

	// 如果没有设置处理器，使用默认的简单处理逻辑
	select {
	case <-time.After(100 * time.Millisecond):
		// 任务处理成功
		return nil
	case <-ctx.Done():
		// 任务处理被取消或超时
		return ctx.Err()
	}
}

// SetProcessTimeout 设置任务处理超时时间
func (wp *WorkerPool) SetProcessTimeout(timeout time.Duration) {
	wp.processTimeout = timeout
}

// SetPollInterval 设置轮询间隔
func (wp *WorkerPool) SetPollInterval(interval time.Duration) {
	wp.pollInterval = interval
}

// SetProcessor 设置任务处理器
func (wp *WorkerPool) SetProcessor(processor TaskProcessor) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if processor != nil {
		wp.processor = processor
	} else {
		// 如果传入nil，使用默认处理器
		wp.processor = &DefaultTaskProcessor{}
	}
}

// updateWorkerCount 更新worker_counts字段
func (wp *WorkerPool) updateWorkerCount(delta int) {
	// 获取当前worker_counts
	rows, err := wp.db.db.Query(
		"SELECT worker_counts FROM worker_instances WHERE instance_id = $1",
		wp.instanceID,
	)
	if err != nil {
		fmt.Printf("Failed to get worker counts: %v\n", err)
		return
	}
	defer rows.Close()

	var workerCountsJSON string
	workerCounts := make(map[string]int)

	if rows.Next() {
		if err := rows.Scan(&workerCountsJSON); err != nil {
			fmt.Printf("Failed to scan worker counts: %v\n", err)
			return
		}
		if err := json.Unmarshal([]byte(workerCountsJSON), &workerCounts); err != nil {
			fmt.Printf("Failed to unmarshal worker counts: %v\n", err)
			return
		}
	}

	// 更新worker数量
	workerCounts[wp.queueName] += delta
	if workerCounts[wp.queueName] < 0 {
		workerCounts[wp.queueName] = 0
	}

	// 将更新后的worker_counts保存回数据库
	newWorkerCountsJSON, err := json.Marshal(workerCounts)
	if err != nil {
		fmt.Printf("Failed to marshal worker counts: %v\n", err)
		return
	}

	_, err = wp.db.db.Exec(
		"UPDATE worker_instances SET worker_counts = $1 WHERE instance_id = $2",
		string(newWorkerCountsJSON), wp.instanceID,
	)
	if err != nil {
		fmt.Printf("Failed to update worker counts: %v\n", err)
		return
	}
}
