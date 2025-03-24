# Go Routine Queue

一个基于Golang和PostgreSQL的高性能分布式队列系统，每个worker是一个独立的goroutine，支持动态扩缩容和任务处理。

## 特性

- 基于PostgreSQL的分布式协调，确保任务不会重复处理
- 根据队列名称动态调整worker数量，实现资源的高效利用
- 自动清理已完成任务，避免数据库膨胀
- 提供完整的RESTful监控API接口，方便系统状态查询
- 支持自定义任务处理器，灵活适应不同业务场景
- 优雅的启动和关闭机制，确保任务不会丢失
- 高效的任务分发和处理，最大化系统吞吐量

## 架构

系统由以下核心组件组成：

1. **队列管理器(QueueManager)**: 负责队列的创建、删除和worker数量的动态调整，同时管理任务处理器的注册
2. **工作器池(WorkerPool)**: 管理goroutine工作器的生命周期，包括启动、停止和任务处理
3. **数据库连接层(DBConnector)**: 处理与PostgreSQL的交互，包括任务的入队、出队和状态更新
4. **任务处理器(TaskProcessor)**: 执行具体的任务逻辑，支持自定义实现
5. **监控接口(Monitor)**: 提供RESTful API，用于查询系统状态和管理队列

## 安装

### 前置条件

- Go 1.18+
- PostgreSQL 10+

### 安装步骤

1. 克隆仓库

```bash
git clone https://github.com/Aliciahan/go-routine-queue.git
cd go-routine-queue
```

2. 安装依赖

```bash
go mod download
```

3. 编译

```bash
go build -o queue-server ./cmd/server
```

## 使用方法

### 启动服务器

```bash
# 设置环境变量
export DB_CONNECTION_STRING="postgres://postgres:postgres@localhost:5432/queue_db?sslmode=disable"
export DEFAULT_WORKER_COUNT=5
export CLEANUP_INTERVAL=3600
export MONITOR_ADDR=":8080"

# 启动服务器
./queue-server
```

### 客户端示例

```go
package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/Aliciahan/go-routine-queue/pkg/queue"
)

func main() {
	// 连接数据库
	dbConnStr := "postgres://postgres:postgres@localhost:5432/queue_db?sslmode=disable"
	db, err := queue.NewDBConnector(dbConnStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// 创建任务负载
	payload := map[string]interface{}{
		"id":   "task-1",
		"name": "Example Task",
		"data": "This is task data",
	}

	// 序列化任务负载
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Fatalf("Failed to marshal task payload: %v", err)
	}

	// 将任务加入队列
	taskID, err := db.EnqueueTask("default", payloadBytes)
	if err != nil {
		log.Fatalf("Failed to enqueue task: %v", err)
	}

	fmt.Printf("Task enqueued with ID: %d\n", taskID)
}
```

### 自定义任务处理器

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/Aliciahan/go-routine-queue/pkg/queue"
)

// 自定义任务处理器
type CustomTaskProcessor struct{}

// Process 实现TaskProcessor接口
func (p *CustomTaskProcessor) Process(ctx context.Context, task *queue.Task) error {
	// 解析任务负载
	var payload map[string]interface{}
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		return err
	}

	// 处理任务
	fmt.Printf("Processing task: %v\n", payload)

	// 返回nil表示处理成功
	return nil
}

func main() {
	// 连接数据库
	dbConnStr := "postgres://postgres:postgres@localhost:5432/queue_db?sslmode=disable"
	db, err := queue.NewDBConnector(dbConnStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// 创建队列管理器
	qm := queue.NewQueueManager(db)

	// 注册自定义任务处理器
	qm.RegisterTaskProcessor("default", &CustomTaskProcessor{})

	// 启动队列管理器
	if err := qm.Start(); err != nil {
		log.Fatalf("Failed to start queue manager: %v", err)
	}
	defer qm.Stop()

	// 创建队列
	if err := qm.CreateQueue("default", 5); err != nil {
		log.Printf("Queue already exists: %v", err)
	}

	// 应用程序逻辑...
}
```

## API文档

### 监控API

#### 队列管理

| 端点 | 方法 | 描述 |
|------|------|------|
| `/api/queues` | GET | 获取所有队列状态 |
| `/api/queues/{queue_name}` | GET | 获取指定队列状态 |
| `/api/queues/{queue_name}` | POST | 创建新队列 |
| `/api/queues/{queue_name}` | PUT | 更新队列配置 |
| `/api/queues/{queue_name}` | DELETE | 删除队列 |

#### 实例管理

| 端点 | 方法 | 描述 |
|------|------|------|
| `/api/instances` | GET | 获取所有活跃实例信息，包括当前实例ID |

#### Worker分配

| 端点 | 方法 | 描述 |
|------|------|------|
| `/api/worker-allocations` | GET | 获取所有队列的worker分配情况 |
| `/api/worker-allocations?queue={queue_name}` | GET | 获取指定队列的worker分配情况 |

#### 系统信息

| 端点 | 方法 | 描述 |
|------|------|------|
| `/api/stats` | GET | 获取系统统计信息 |
| `/health` | GET | 健康检查 |

## 环境变量

| 变量名 | 描述 | 默认值 |
|--------|------|--------|
| `DB_CONNECTION_STRING` | PostgreSQL连接字符串 | `postgres://postgres:postgres@localhost:5432/queue_db?sslmode=disable` |
| `DEFAULT_WORKER_COUNT` | 默认worker数量 | `5` |
| `CLEANUP_INTERVAL` | 清理间隔时间(秒) | `3600` |
| `MONITOR_ADDR` | 监控服务器地址 | `:8080` |

## 多实例部署

系统支持多实例部署，每个实例拥有唯一的实例ID，可以通过环境变量`INSTANCE_ID`指定，如果不指定则自动生成。多实例部署时，系统会自动在各实例间分配worker，确保资源的高效利用。

### 实例ID

```bash
# 设置实例ID
export INSTANCE_ID="instance-1"
```

如果不设置，系统会基于主机名和时间戳自动生成唯一的实例ID：

```go
// 生成唯一的实例ID
func generateInstanceID() string {
    hostname, err := os.Hostname()
    if err != nil {
        hostname = "unknown"
    }
    return fmt.Sprintf("%s-%d", hostname, time.Now().UnixNano())
}
```

### Worker分配机制

系统会根据活跃实例数量和队列配置的总worker数量，自动计算每个实例应分配的worker数量，并在实例启动、关闭或心跳超时时自动重新平衡worker分配。

可以通过API监控不同实例上的worker分配情况：

```bash
# 获取所有队列的worker分配情况
curl http://localhost:8080/api/worker-allocations

# 获取指定队列的worker分配情况
curl http://localhost:8080/api/worker-allocations?queue=default
```

响应示例：

```json
{
  "queue": "default",
  "allocations": {
    "instance-1": 3,
    "instance-2": 2
  }
}
```

## 性能优化

- 使用连接池管理数据库连接
- 批量处理任务以减少数据库操作
- 定期清理已完成任务以优化数据库性能
- 使用原子操作和互斥锁确保并发安全

## 贡献

欢迎提交问题和拉取请求！

## 许可证

Apache License 2.0
