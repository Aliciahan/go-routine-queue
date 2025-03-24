package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// WorkerInstance 表示一个worker实例的信息
type WorkerInstance struct {
	InstanceID    string         `json:"instance_id"`    // 实例唯一标识符
	Hostname      string         `json:"hostname"`       // 主机名
	IP            string         `json:"ip"`             // IP地址
	StartedAt     time.Time      `json:"started_at"`     // 启动时间
	LastHeartbeat time.Time      `json:"last_heartbeat"` // 最后一次心跳时间
	WorkerCounts  map[string]int `json:"worker_counts"`  // 每个队列的worker数量
}

// WorkerCoordinator 负责跨实例协调worker的部署和监控
type WorkerCoordinator struct {
	db              *DBConnector
	instanceID      string
	hostname        string
	ip              string
	heartbeatTicker *time.Ticker
	ctx             context.Context
	cancel          context.CancelFunc
	mu              sync.RWMutex
	workerPools     map[string]*WorkerPool
	maxWorkers      map[string]int // 每个队列的全局最大worker数量
}

// NewWorkerCoordinator 创建一个新的worker协调器
func NewWorkerCoordinator(db *DBConnector, instanceID, hostname, ip string) *WorkerCoordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerCoordinator{
		db:              db,
		instanceID:      instanceID,
		hostname:        hostname,
		ip:              ip,
		heartbeatTicker: time.NewTicker(10 * time.Second), // 默认每10秒发送一次心跳
		ctx:             ctx,
		cancel:          cancel,
		mu:              sync.RWMutex{},
		workerPools:     make(map[string]*WorkerPool),
		maxWorkers:      make(map[string]int),
	}
}

// Start 启动协调器
func (wc *WorkerCoordinator) Start() error {
	// 初始化worker实例表
	if err := wc.initSchema(); err != nil {
		return err
	}

	// 注册实例
	if err := wc.registerInstance(); err != nil {
		return err
	}

	// 启动心跳检测
	go wc.startHeartbeat()

	// 启动worker数量调整
	go wc.startWorkerBalancing()

	return nil
}

// Stop 停止协调器
func (wc *WorkerCoordinator) Stop() {
	wc.cancel()
	wc.heartbeatTicker.Stop()

	// 注销实例
	wc.deregisterInstance()

	// 停止所有工作器池
	wc.mu.Lock()
	defer wc.mu.Unlock()

	for _, pool := range wc.workerPools {
		pool.Stop()
	}
}

// initSchema 初始化数据库表结构
func (wc *WorkerCoordinator) initSchema() error {
	// 创建worker实例表
	_, err := wc.db.db.Exec(`
		CREATE TABLE IF NOT EXISTS worker_instances (
			instance_id VARCHAR(255) PRIMARY KEY,
			hostname VARCHAR(255) NOT NULL,
			ip VARCHAR(255) NOT NULL,
			started_at TIMESTAMP NOT NULL DEFAULT NOW(),
			last_heartbeat TIMESTAMP NOT NULL DEFAULT NOW(),
			worker_counts JSONB NOT NULL DEFAULT '{}'::jsonb
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create worker_instances table: %w", err)
	}

	// 创建全局worker配置表
	_, err = wc.db.db.Exec(`
		CREATE TABLE IF NOT EXISTS global_worker_config (
			queue_name VARCHAR(255) PRIMARY KEY REFERENCES queues(name),
			max_workers INT NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create global_worker_config table: %w", err)
	}

	return nil
}

// registerInstance 注册worker实例
func (wc *WorkerCoordinator) registerInstance() error {
	workerCountsJSON, err := json.Marshal(map[string]int{})
	if err != nil {
		return fmt.Errorf("failed to marshal worker counts: %w", err)
	}

	_, err = wc.db.db.Exec(
		"INSERT INTO worker_instances (instance_id, hostname, ip, worker_counts) VALUES ($1, $2, $3, $4) "+
			"ON CONFLICT (instance_id) DO UPDATE SET hostname = $2, ip = $3, last_heartbeat = NOW()",
		wc.instanceID, wc.hostname, wc.ip, string(workerCountsJSON),
	)
	if err != nil {
		return fmt.Errorf("failed to register worker instance: %w", err)
	}

	return nil
}

// deregisterInstance 注销worker实例
func (wc *WorkerCoordinator) deregisterInstance() error {
	_, err := wc.db.db.Exec("DELETE FROM worker_instances WHERE instance_id = $1", wc.instanceID)
	if err != nil {
		fmt.Printf("Failed to deregister worker instance: %v\n", err)
		return err
	}
	return nil
}

// startHeartbeat 启动心跳检测
func (wc *WorkerCoordinator) startHeartbeat() {
	for {
		select {
		case <-wc.heartbeatTicker.C:
			wc.sendHeartbeat()
		case <-wc.ctx.Done():
			return
		}
	}
}

// sendHeartbeat 发送心跳并更新worker数量
func (wc *WorkerCoordinator) sendHeartbeat() {
	wc.mu.RLock()
	workerCounts := make(map[string]int)
	for queueName, pool := range wc.workerPools {
		workerCounts[queueName] = pool.GetWorkerCount()
	}
	wc.mu.RUnlock()

	workerCountsJSON, err := json.Marshal(workerCounts)
	if err != nil {
		fmt.Printf("Failed to marshal worker counts: %v\n", err)
		return
	}

	_, err = wc.db.db.Exec(
		"UPDATE worker_instances SET last_heartbeat = NOW(), worker_counts = $1 WHERE instance_id = $2",
		string(workerCountsJSON), wc.instanceID,
	)
	if err != nil {
		fmt.Printf("Failed to update heartbeat: %v\n", err)
	}
}

// startWorkerBalancing 启动worker数量平衡调整
func (wc *WorkerCoordinator) startWorkerBalancing() {
	balanceTicker := time.NewTicker(30 * time.Second) // 每30秒检查一次
	defer balanceTicker.Stop()

	for {
		select {
		case <-balanceTicker.C:
			wc.balanceWorkers()
		case <-wc.ctx.Done():
			return
		}
	}
}

// balanceWorkers 平衡各实例上的worker数量
func (wc *WorkerCoordinator) balanceWorkers() {
	// 获取所有活跃实例
	instances, err := wc.getActiveInstances()
	if err != nil {
		fmt.Printf("Failed to get active instances: %v\n", err)
		return
	}

	// 如果没有活跃实例，直接返回
	if len(instances) == 0 {
		return
	}

	// 获取全局worker配置
	globalConfig, err := wc.getGlobalWorkerConfig()
	if err != nil {
		fmt.Printf("Failed to get global worker config: %v\n", err)
		return
	}

	// 计算当前实例应该运行的worker数量
	wc.mu.Lock()
	defer wc.mu.Unlock()

	for queueName, maxWorkers := range globalConfig {
		// 计算每个实例应分配的worker数量
		workerPerInstance := maxWorkers / len(instances)
		remainder := maxWorkers % len(instances)

		// 为当前实例分配worker
		var targetWorkers int
		for i, instance := range instances {
			if instance.InstanceID == wc.instanceID {
				targetWorkers = workerPerInstance
				if i < remainder {
					targetWorkers++ // 分配余数
				}
				break
			}
		}

		// 更新当前实例的worker数量
		pool, exists := wc.workerPools[queueName]
		if exists {
			currentWorkers := pool.GetWorkerCount()
			if currentWorkers != targetWorkers {
				fmt.Printf("Adjusting workers for queue %s: %d -> %d\n", queueName, currentWorkers, targetWorkers)
				pool.UpdateWorkerCount(targetWorkers)
			}
		} else if targetWorkers > 0 {
			// 如果队列不存在但需要worker，创建新的worker池
			fmt.Printf("Creating new worker pool for queue %s with %d workers\n", queueName, targetWorkers)
			pool = NewWorkerPool(wc.db, queueName, targetWorkers, wc.instanceID)
			wc.workerPools[queueName] = pool
			pool.Start()
		}
	}
}

// getActiveInstances 获取所有活跃的worker实例
func (wc *WorkerCoordinator) getActiveInstances() ([]WorkerInstance, error) {
	// 设置心跳超时时间，例如30秒
	heartbeatThreshold := time.Now().Add(-30 * time.Second)

	rows, err := wc.db.db.Query(
		"SELECT instance_id, hostname, ip, started_at, last_heartbeat, worker_counts FROM worker_instances WHERE last_heartbeat > $1",
		heartbeatThreshold,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var instances []WorkerInstance
	for rows.Next() {
		var instance WorkerInstance
		var workerCountsJSON string
		if err := rows.Scan(
			&instance.InstanceID,
			&instance.Hostname,
			&instance.IP,
			&instance.StartedAt,
			&instance.LastHeartbeat,
			&workerCountsJSON,
		); err != nil {
			return nil, err
		}

		// 解析worker数量JSON
		if err := json.Unmarshal([]byte(workerCountsJSON), &instance.WorkerCounts); err != nil {
			return nil, err
		}

		instances = append(instances, instance)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return instances, nil
}

// getGlobalWorkerConfig 获取全局worker配置
func (wc *WorkerCoordinator) getGlobalWorkerConfig() (map[string]int, error) {
	rows, err := wc.db.db.Query("SELECT queue_name, max_workers FROM global_worker_config")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	config := make(map[string]int)
	for rows.Next() {
		var queueName string
		var maxWorkers int
		if err := rows.Scan(&queueName, &maxWorkers); err != nil {
			return nil, err
		}
		config[queueName] = maxWorkers
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return config, nil
}

// SetGlobalWorkerCount 设置队列的全局worker数量
func (wc *WorkerCoordinator) SetGlobalWorkerCount(queueName string, maxWorkers int) error {
	_, err := wc.db.db.Exec(
		"INSERT INTO global_worker_config (queue_name, max_workers) VALUES ($1, $2) "+
			"ON CONFLICT (queue_name) DO UPDATE SET max_workers = $2, updated_at = NOW()",
		queueName, maxWorkers,
	)
	if err != nil {
		return fmt.Errorf("failed to set global worker count: %w", err)
	}

	// 更新内存中的配置
	wc.mu.Lock()
	wc.maxWorkers[queueName] = maxWorkers
	wc.mu.Unlock()

	// 立即触发worker平衡
	go wc.balanceWorkers()

	return nil
}

// GetWorkerInstanceStatus 获取所有worker实例的状态
func (wc *WorkerCoordinator) GetWorkerInstanceStatus() ([]WorkerInstance, error) {
	return wc.getActiveInstances()
}

// GetTotalWorkerCount 获取指定队列的全局worker总数
func (wc *WorkerCoordinator) GetTotalWorkerCount(queueName string) (int, error) {
	instances, err := wc.getActiveInstances()
	if err != nil {
		return 0, err
	}

	totalCount := 0
	for _, instance := range instances {
		if count, exists := instance.WorkerCounts[queueName]; exists {
			totalCount += count
		}
	}

	return totalCount, nil
}
