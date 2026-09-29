package main

import (
	"context"
	"sync"
	"time"
)

// Task представляет собой задачу, которая может быть выполнена воркером
type Task interface {
	// Execute выполняет задачу и возвращает результат или ошибку
	Execute() (any, error)
}

// Result представляет результат выполнения задачи
type Result struct {
	Value  any
	Error  error
	TaskID int
}

type queuedTask struct {
	task   Task
	id     int
	result chan Result
}

type workerPool struct {
	workerCount int
	queueSize   int

	taskQueue chan queuedTask

	mu         sync.Mutex
	started    bool
	stopped    bool
	nextTaskID int

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup
}

// WorkerPool представляет пул воркеров для обработки задач
type WorkerPool interface {
	// Start запускает воркеров, начинает обработку задач
	Start(ctx context.Context) error

	// Submit отправляет задачу в пул для выполнения
	// Возвращает канал с результатом и ID задачи
	Submit(task Task) (<-chan Result, int, error)

	// Stop останавливает пул воркеров с таймаутом ожидания
	// stopTimeout - время ожидания завершения текущих задач
	Stop(stopTimeout time.Duration) error

	// ActiveWorkers возвращает количество работающих воркеров
	ActiveWorkers() int

	// PendingTasks возвращает количество задач в очереди
	PendingTasks() int
}
