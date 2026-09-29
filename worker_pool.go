package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func NewWorkerPool(workerCount int, queueSize int) workerPool {
	return workerPool{
		workerCount: workerCount,
		queueSize:   queueSize,
		taskQueue:   make(chan queuedTask, queueSize),
	}
}

func (wp *workerPool) Start(ctx context.Context) error {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if wp.started {
		return nil
	}

	wp.ctx, wp.cancel = context.WithCancel(ctx)
	wp.started = true

	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go func(workerID int) {
			defer wp.wg.Done()

			for {
				select {
				case <-wp.ctx.Done():
					return
				case j, ok := <-wp.taskQueue:
					if !ok {
						return
					}

					func() {
						defer func() {
							if r := recover(); r != nil {
								j.result <- Result{
									Error:  fmt.Errorf("panic: %v", r),
									TaskID: j.id,
								}
								close(j.result)
							}
						}()

						v, err := j.task.Execute()
						j.result <- Result{
							Value:  v,
							Error:  err,
							TaskID: j.id,
						}
						close(j.result)
					}()
				}
			}
		}(i)
	}

	return nil
}

func (wp *workerPool) Submit(task Task) (<-chan Result, int, error) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	resultChan := make(chan Result, 1)

	if !wp.started || wp.stopped {
		close(resultChan)
		return resultChan, -1, errors.New("worker pool is not started or already stopped")
	}

	if wp.ctx != nil && wp.ctx.Err() != nil {
		close(resultChan)
		return resultChan, -1, wp.ctx.Err()
	}

	id := wp.nextTaskID
	wp.nextTaskID++

	j := queuedTask{
		task:   task,
		id:     id,
		result: resultChan,
	}

	wp.taskQueue <- j

	return resultChan, id, nil
}

func (wp *workerPool) Stop(stopTimeout time.Duration) error {
	wp.mu.Lock()
	if wp.stopped {
		wp.mu.Unlock()
		return nil
	}
	wp.stopped = true
	close(wp.taskQueue)

	done := make(chan struct{})
	go func() {
		wp.wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(stopTimeout)
	defer timer.Stop()

	select {
	case <-done:
		if wp.cancel != nil {
			wp.cancel()
		}
		return nil
	case <-timer.C:
		if wp.cancel != nil {
			wp.cancel()
		}
		return context.DeadlineExceeded
	}
}

func (wp *workerPool) ActiveWorkers() int {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if wp.started && !wp.stopped && wp.ctx != nil && wp.ctx.Err() == nil {
		return wp.workerCount
	}
	return 0
}

func (wp *workerPool) PendingTasks() int {
	return len(wp.taskQueue)
}
