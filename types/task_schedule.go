package types

import (
	"context"
	"sync"
	"time"

	"github.com/golang/glog"
)

type Task struct {
	Name string

	Next *Task

	// output
	Result interface{}
	Error  error

	f    func(context.Context) (interface{}, error)
	done chan struct{}
}

func NewTask(name string, f func(context.Context) (interface{}, error)) *Task {
	return &Task{
		Name: name,
		f:    f,
		done: make(chan struct{}),
	}
}

func (t *Task) Run(ctx context.Context) error {
	defer close(t.done)

	t.Result, t.Error = t.f(ctx)
	return t.Error
}

func (t *Task) Done() <-chan struct{} {
	return t.done
}

type Scheduler interface {
	Schedule(t *Task)
}

type ListScheduler struct {
	tasks        chan *Task
	workerNumber int

	closed bool
	sync.RWMutex
}

func NewListScheduler(maxBufferedTask, workerNumber int) *ListScheduler {
	b := &ListScheduler{
		tasks:        make(chan *Task, maxBufferedTask),
		workerNumber: workerNumber,
	}
	b.start()
	return b
}

func (s *ListScheduler) Schedule(t *Task) {
	s.RLock()
	defer s.RUnlock()

	if s.closed {
		return
	}
	s.tasks <- t
}

func (s *ListScheduler) Close() {
	s.Lock()
	defer s.Unlock()

	close(s.tasks)
	s.closed = true
}

func (s *ListScheduler) start() {
	for i := 0; i < s.workerNumber; i++ {
		go func() {
			for {
				task, ok := <-s.tasks
				if !ok {
					return
				}

				ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
				if err := task.Run(ctx); err != nil {
					glog.Errorf("task %s failed: %v", task.Name, err)
				}
				if task.Next != nil {
					s.Schedule(task.Next)
				}
				cancel()
			}
		}()
	}
}
