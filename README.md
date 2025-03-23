# Go Routine Queue

一个基于Golang和PostgreSQL的分布式队列系统，每个worker是一个goroutine。

## 特性

- 基于PostgreSQL的分布式协调
- 根据queue_name动态调整worker数量
- 自动清理冗余数据
- 提供监控API接口
- 高效的任务分发和处理

## 架构

系统由以下核心组件组成：

1. **队列管理器(QueueManager)**: 负责队列的创建、删除和worker数量的动态调整
2. **工作器池(WorkerPool)**: 管理goroutine工作器的生命周期
3. **数据库连接层(DBConnector)**: 处理与PostgreSQL的交互
4. **任务处理器(TaskProcessor)**: 执行具体的任务逻辑
5. **监控接口(Monitor)**: 提供系统状态查询功能

## 使用方法

```go
// 示例代码将在实现后提供
```

## 配置

系统配置通过环境变量或配置文件进行设置：

- `DB_CONNECTION_STRING`: PostgreSQL连接字符串
- `DEFAULT_WORKER_COUNT`: 默认worker数量
- `CLEANUP_INTERVAL`: 清理间隔时间(秒)

## 许可证

Apache License 2.0
