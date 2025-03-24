package queue

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq" // PostgreSQL驱动
)

// DBConnector 处理与PostgreSQL的交互
type DBConnector struct {
	db *sql.DB
}

// Queue 表示队列配置
type Queue struct {
	Name        string
	WorkerCount int
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// Task 表示队列中的任务
type Task struct {
	ID         int64
	QueueName  string
	Payload    []byte
	Status     string // pending, processing, completed, failed
	CreatedAt  time.Time
	UpdatedAt  time.Time
	StartedAt  *time.Time
	EndedAt    *time.Time
	Error      string
	WorkerID   string
	InstanceID string
}

// NewDBConnector 创建一个新的数据库连接器
func NewDBConnector(connectionString string) (*DBConnector, error) {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// 测试连接
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &DBConnector{db: db}, nil
}

// Close 关闭数据库连接
func (d *DBConnector) Close() error {
	return d.db.Close()
}

// InitSchema 初始化数据库表结构
func (d *DBConnector) InitSchema() error {
	// 创建队列表
	_, err := d.db.Exec(`
		CREATE TABLE IF NOT EXISTS queues (
			name VARCHAR(255) PRIMARY KEY,
			worker_count INT NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create queues table: %w", err)
	}

	// 创建任务表
	_, err = d.db.Exec(`
		CREATE TABLE IF NOT EXISTS tasks (
			id SERIAL PRIMARY KEY,
			queue_name VARCHAR(255) NOT NULL REFERENCES queues(name),
			payload BYTEA NOT NULL,
			status VARCHAR(50) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
			started_at TIMESTAMP,
			ended_at TIMESTAMP,
			error TEXT,
			worker_id VARCHAR(255),
			instance_id VARCHAR(255)
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create tasks table: %w", err)
	}

	// 创建索引
	_, err = d.db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_queue_status ON tasks (queue_name, status)
	`)
	if err != nil {
		return fmt.Errorf("failed to create index on tasks table: %w", err)
	}

	return nil
}

// CreateQueue 在数据库中创建队列
func (d *DBConnector) CreateQueue(name string, workerCount int) error {
	_, err := d.db.Exec(
		"INSERT INTO queues (name, worker_count) VALUES ($1, $2)",
		name, workerCount,
	)
	if err != nil {
		// 检查是否是唯一约束冲突错误
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" { // 23505是PostgreSQL的唯一约束冲突错误码
			fmt.Println("Queue already exists: ", name)
			return nil
		}
	}
	return err
}

// UpdateQueueWorkerCount 更新队列的worker数量
func (d *DBConnector) UpdateQueueWorkerCount(name string, workerCount int) error {
	_, err := d.db.Exec(
		"UPDATE queues SET worker_count = $1, updated_at = NOW() WHERE name = $2",
		workerCount, name,
	)
	return err
}

// DeleteQueue 删除队列
func (d *DBConnector) DeleteQueue(name string) error {
	// 开始事务
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 删除队列中的所有任务
	_, err = tx.Exec("DELETE FROM tasks WHERE queue_name = $1", name)
	if err != nil {
		return err
	}

	// 删除队列
	_, err = tx.Exec("DELETE FROM queues WHERE name = $1", name)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// GetQueue 获取队列信息
func (d *DBConnector) GetQueue(name string) (*Queue, error) {
	var q Queue
	err := d.db.QueryRow(
		"SELECT name, worker_count, created_at, updated_at FROM queues WHERE name = $1",
		name,
	).Scan(&q.Name, &q.WorkerCount, &q.CreatedAt, &q.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("queue %s not found", name)
	} else if err != nil {
		return nil, err
	}

	return &q, nil
}

// GetAllQueues 获取所有队列
func (d *DBConnector) GetAllQueues() ([]*Queue, error) {
	rows, err := d.db.Query("SELECT name, worker_count, created_at, updated_at FROM queues")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var queues []*Queue
	for rows.Next() {
		var q Queue
		if err := rows.Scan(&q.Name, &q.WorkerCount, &q.CreatedAt, &q.UpdatedAt); err != nil {
			return nil, err
		}
		queues = append(queues, &q)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return queues, nil
}

// EnqueueTask 将任务加入队列
func (d *DBConnector) EnqueueTask(queueName string, payload []byte) (int64, error) {
	var id int64
	err := d.db.QueryRow(
		"INSERT INTO tasks (queue_name, payload, status) VALUES ($1, $2, 'pending') RETURNING id",
		queueName, payload,
	).Scan(&id)

	return id, err
}

// DequeueTask 从队列中获取一个待处理的任务
func (d *DBConnector) DequeueTask(ctx context.Context, queueName, workerID string) (*Task, error) {
	// 从workerID中提取实例ID（格式：instanceID-queueName-timestamp）
	parts := strings.Split(workerID, "-")
	instanceID := ""
	if len(parts) >= 2 {
		instanceID = parts[0]
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// 使用FOR UPDATE SKIP LOCKED获取一个未处理的任务并锁定
	var task Task
	now := time.Now()
	err = tx.QueryRowContext(
		ctx,
		`SELECT id, queue_name, payload, status, created_at, updated_at 
		FROM tasks 
		WHERE queue_name = $1 AND status = 'pending' 
		ORDER BY created_at ASC 
		FOR UPDATE SKIP LOCKED 
		LIMIT 1`,
		queueName,
	).Scan(&task.ID, &task.QueueName, &task.Payload, &task.Status, &task.CreatedAt, &task.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, nil // 没有待处理的任务
	} else if err != nil {
		return nil, err
	}

	// 更新任务状态为处理中
	_, err = tx.ExecContext(
		ctx,
		"UPDATE tasks SET status = 'processing', worker_id = $1, instance_id = $2, started_at = $3, updated_at = $3 WHERE id = $4",
		workerID, instanceID, now, task.ID,
	)
	if err != nil {
		return nil, err
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	task.Status = "processing"
	task.WorkerID = workerID
	task.InstanceID = instanceID
	task.StartedAt = &now
	task.UpdatedAt = now

	return &task, nil
}

// CompleteTask 标记任务为已完成
func (d *DBConnector) CompleteTask(taskID int64) error {
	now := time.Now()
	_, err := d.db.Exec(
		"UPDATE tasks SET status = 'completed', ended_at = $1, updated_at = $1 WHERE id = $2",
		now, taskID,
	)
	return err
}

// FailTask 标记任务为失败
func (d *DBConnector) FailTask(taskID int64, errMsg string) error {
	now := time.Now()
	_, err := d.db.Exec(
		"UPDATE tasks SET status = 'failed', error = $1, ended_at = $2, updated_at = $2 WHERE id = $3",
		errMsg, now, taskID,
	)
	return err
}

// GetPendingTaskCount 获取队列中待处理任务的数量
func (d *DBConnector) GetPendingTaskCount(queueName string) (int, error) {
	var count int
	err := d.db.QueryRow(
		"SELECT COUNT(*) FROM tasks WHERE queue_name = $1 AND status = 'pending'",
		queueName,
	).Scan(&count)

	return count, err
}

// CleanupCompletedTasks 清理已完成的任务
func (d *DBConnector) CleanupCompletedTasks(threshold time.Time) (int64, error) {
	result, err := d.db.Exec(
		"DELETE FROM tasks WHERE (status = 'completed' OR status = 'failed') AND ended_at < $1",
		threshold,
	)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

// ResetStaleTasks 重置卡住的任务
func (d *DBConnector) ResetStaleTasks(threshold time.Time) (int64, error) {
	result, err := d.db.Exec(
		"UPDATE tasks SET status = 'pending', worker_id = NULL, started_at = NULL, updated_at = NOW() WHERE status = 'processing' AND updated_at < $1",
		threshold,
	)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}
