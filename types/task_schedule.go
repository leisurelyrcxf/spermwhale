package types

import (
	"context"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/leisurelyrcxf/spermwhale/errors"
)

type Task struct {
	Name string

	next *Task

	// output
	result interface{}
	err    error

	f    func(context.Context) (interface{}, error)
	done chan struct{}

	sync.Mutex
}

func NewTask(name string, f func(context.Context) (interface{}, error)) *Task {
	return &Task{
		Name: name,
		f:    f,
		done: make(chan struct{}),
	}
}

func (t *Task) GetNext() *Task {
	t.Lock()
	defer t.Unlock()

	return t.next
}

func (t *Task) SetNext(next *Task) {
	t.Lock()
	defer t.Unlock()

	t.next = next
}

func (t *Task) WaitFinish(ctx context.Context) (interface{}, error) {
	select {
	case <-ctx.Done():
		return nil, errors.Annotatef(errors.ErrFailedToWaitTask, "detail: %v", ctx.Err().Error())
	case <-t.done:
		return t.result, t.err
	}
}

func (t *Task) cancelNexts(reason error) {
	t.Lock()
	defer t.Unlock()

	for ele := t.next; ele != nil; ele = ele.next {
		ele.err = errors.Annotatef(reason, "detail: '%v'", errors.ErrCancelledDueToParentFailed.Msg)
		close(ele.done)
	}
}

func (t *Task) run(ctx context.Context) error {
	if t.finished() {
		return t.err
	}
	defer close(t.done)

	t.result, t.err = t.f(ctx)
	return t.err
}

func (t *Task) finished() bool {
	select {
	case <-t.done:
		return true
	default:
		return false
	}
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

func (s *ListScheduler) Schedule(t *Task) error {
	s.RLock()
	defer s.RUnlock()

	if s.closed {
		return errors.ErrSchedulerClosed
	}
	s.tasks <- t
	return nil
}

func (s *ListScheduler) Close() {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return
	}
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
				if err := task.run(ctx); err != nil {
					if !errors.IsRetryableTransactionErr(err) {
						glog.Errorf("task %s failed: %v", task.Name, err)
					}
					cancel()
					task.cancelNexts(err)
					continue
				}
				if next := task.GetNext(); next != nil {
					if err := s.Schedule(next); err != nil {
						next.err = err
						close(next.done)
					}
				}
				cancel()
			}
		}()
	}
}
