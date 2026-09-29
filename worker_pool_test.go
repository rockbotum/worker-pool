package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

type okTask struct {
	v any
}

func (t okTask) Execute() (any, error) {
	return t.v, nil
}

type errTask struct {
	err error
}

func (t errTask) Execute() (any, error) {
	return nil, t.err
}

type panicTask struct{}

func (t panicTask) Execute() (any, error) {
	panic("boom")
}

type slowTask struct {
	d time.Duration
	v any
}

func (t slowTask) Execute() (any, error) {
	time.Sleep(t.d)
	return t.v, nil
}

func TestWorkerPool_StartAndExecuteTask(t *testing.T) {
	pool := NewWorkerPool(2, 10)

	if err := pool.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	ch, id, err := pool.Submit(okTask{v: 42})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if id < 0 {
		t.Fatalf("expected valid task id, got %d", id)
	}

	res := <-ch
	if res.Error != nil {
		t.Fatalf("unexpected error: %v", res.Error)
	}
	if res.Value != 42 {
		t.Fatalf("expected 42, got %v", res.Value)
	}
	if res.TaskID != id {
		t.Fatalf("expected TaskID %d, got %d", id, res.TaskID)
	}

	if err := pool.Stop(2 * time.Second); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestWorkerPool_TaskReturnsError(t *testing.T) {
	pool := NewWorkerPool(1, 10)

	if err := pool.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	wantErr := errors.New("task failed")
	ch, _, err := pool.Submit(errTask{err: wantErr})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	res := <-ch
	if res.Error == nil {
		t.Fatal("expected error, got nil")
	}
	if res.Error.Error() != wantErr.Error() {
		t.Fatalf("expected %q, got %q", wantErr.Error(), res.Error.Error())
	}

	_ = pool.Stop(2 * time.Second)
}

func TestWorkerPool_PanicInTask(t *testing.T) {
	pool := NewWorkerPool(2, 10)

	if err := pool.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	ch1, _, err := pool.Submit(panicTask{})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	ch2, _, err := pool.Submit(okTask{v: "still alive"})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	res1 := <-ch1
	if res1.Error == nil {
		t.Fatal("expected error from panic task, got nil")
	}

	res2 := <-ch2
	if res2.Error != nil {
		t.Fatalf("unexpected error from second task: %v", res2.Error)
	}
	if res2.Value != "still alive" {
		t.Fatalf("expected %q, got %v", "still alive", res2.Value)
	}

	_ = pool.Stop(2 * time.Second)
}

func TestWorkerPool_MultipleTasks(t *testing.T) {
	pool := NewWorkerPool(4, 20)

	if err := pool.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	const n = 10
	results := make([]<-chan Result, 0, n)

	for i := 0; i < n; i++ {
		ch, _, err := pool.Submit(okTask{v: i})
		if err != nil {
			t.Fatalf("Submit() error = %v", err)
		}
		results = append(results, ch)
	}

	for i, ch := range results {
		res := <-ch
		if res.Error != nil {
			t.Fatalf("task %d returned error: %v", i, res.Error)
		}
		if res.Value != i {
			t.Fatalf("task %d expected %d, got %v", i, i, res.Value)
		}
	}

	if err := pool.Stop(2 * time.Second); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestWorkerPool_StopTimeout(t *testing.T) {
	pool := NewWorkerPool(1, 10)

	if err := pool.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	_, _, err := pool.Submit(slowTask{
		d: 2 * time.Second,
		v: "done",
	})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	err = pool.Stop(100 * time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestWorkerPool_ContextCancelStopsAccepting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	pool := NewWorkerPool(2, 10)
	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	cancel()
	time.Sleep(50 * time.Millisecond)

	_, id, err := pool.Submit(okTask{v: 1})
	if err == nil {
		t.Fatal("expected Submit to fail after context cancel")
	}
	if id != -1 {
		t.Fatalf("expected invalid task id after cancel, got %d", id)
	}

	_ = pool.Stop(2 * time.Second)
}
