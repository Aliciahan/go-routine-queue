package queue

import (
	"context"
	"sync"
)

// TaskProcessor 定义任务处理器接口
type TaskProcessor interface {
	// Process 处理任务，返回错误表示处理失败
	Process(ctx context.Context, task *Task) error
}

// DefaultTaskProcessor 默认任务处理器实现
type DefaultTaskProcessor struct{}

// Process 实现默认的任务处理逻辑
func (p *DefaultTaskProcessor) Process(ctx context.Context, task *Task) error {
	// 默认实现只是简单返回成功
	// 实际应用中，用户应该实现自己的处理器
	return nil
}

// TaskProcessorRegistry 任务处理器注册表
type TaskProcessorRegistry struct {
	processors       map[string]TaskProcessor
	defaultProcessor TaskProcessor
	mu               sync.RWMutex
}

// NewTaskProcessorRegistry 创建一个新的任务处理器注册表
func NewTaskProcessorRegistry() *TaskProcessorRegistry {
	return &TaskProcessorRegistry{
		processors:       make(map[string]TaskProcessor),
		defaultProcessor: &DefaultTaskProcessor{},
		mu:               sync.RWMutex{},
	}
}

// Register 注册队列的任务处理器
func (r *TaskProcessorRegistry) Register(queueName string, processor TaskProcessor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.processors[queueName] = processor
}

// GetProcessor 获取队列的任务处理器
func (r *TaskProcessorRegistry) GetProcessor(queueName string) TaskProcessor {
	r.mu.RLock()
	defer r.mu.RUnlock()

	processor, exists := r.processors[queueName]
	if !exists {
		return r.defaultProcessor
	}
	return processor
}

// SetDefaultProcessor 设置默认任务处理器
func (r *TaskProcessorRegistry) SetDefaultProcessor(processor TaskProcessor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.defaultProcessor = processor
}
