package types

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/leisurelyrcxf/spermwhale/errors"
)

const defaultRunTimeout = time.Second * 30

var DummyResult = &struct{}{}

type Task struct {
	ID   string
	Name string

	ctx        context.Context
	cancel     context.CancelFunc
	runTimeout time.Duration

	// output
	result interface{}
	err    error

	f    func(context.Context) (interface{}, error)
	done chan struct{}
}

func NewTaskNoResult(id string, name string, runTimeout time.Duration, g func(context.Context) error) *Task {
	return NewTask(id, name, runTimeout, func(ctx context.Context) (i interface{}, err error) {
		if err := g(ctx); err != nil {
			return nil, err
		}
		return DummyResult, nil
	})
}

func NewTask(id string, name string, runTimeout time.Duration, f func(context.Context) (interface{}, error)) *Task {
	t := newTask(id, name, runTimeout, f)
	return (&t).genContext()
}

func newTask(id string, name string, runTimeout time.Duration, f func(context.Context) (interface{}, error)) Task {
	if runTimeout <= 0 {
		runTimeout = defaultRunTimeout
	}
	return Task{
		ID:         id,
		Name:       name,
		runTimeout: runTimeout,
		f:          f,
		done:       make(chan struct{}),
	}
}

func (t *Task) genContext() *Task {
	t.ctx, t.cancel = context.WithCancel(context.Background())
	return t
}

func (t *Task) WaitFinish(ctx context.Context) (interface{}, error) {
	select {
	case <-ctx.Done():
		return nil, errors.Annotatef(errors.ErrFailedToWaitTask, ctx.Err().Error())
	case <-t.done:
		return t.result, t.err
	}
}

func (t *Task) Cancel() {
	t.cancel()
}

func (t *Task) Run() error {
	defer close(t.done)

	ctx, cancel := context.WithTimeout(t.ctx, t.runTimeout)
	defer cancel()

	t.result, t.err = t.f(ctx)
	return t.err
}

func (t *Task) Finished() bool {
	select {
	case <-t.done:
		return true
	default:
		return false
	}
}

type ListTask struct {
	Task

	prev, next atomic.Value
}

func NewListTaskNoResult(id string, name string, runTimeout time.Duration,
	g func(ctx context.Context, prevResult interface{}) error) *ListTask {
	return NewListTaskWithResult(id, name, runTimeout, func(ctx context.Context, prevResult interface{}) (i interface{}, err error) {
		if err := g(ctx, prevResult); err != nil {
			return nil, err
		}
		return DummyResult, nil
	})
}

func NewListTaskWithResult(id string, name string, runTimeout time.Duration,
	g func(ctx context.Context, prevResult interface{}) (interface{}, error)) *ListTask {
	t := &ListTask{Task: newTask(id, name, runTimeout, nil)}
	t.Task.f = func(ctx context.Context) (interface{}, error) {
		prev := t.Prev()
		if prev == nil {
			return g(ctx, nil)
		}
		if prev.err != nil {
			t.err = errors.Annotatef(prev.err, "detail: error propagated from previous task %s(%s)", prev.ID, prev.Name)
			return nil, t.err
		}
		if prev.result == nil {
			panic("previous task func returned (nil, nil), which is not allowed")
		}
		return g(ctx, prev.result)
	}
	t.genContext()
	return t
}

func (t *ListTask) Prev() *ListTask {
	obj := t.prev.Load()
	if obj == nil {
		return nil
	}
	return obj.(*ListTask)
}

func (t *ListTask) Next() *ListTask {
	obj := t.next.Load()
	if obj == nil {
		return nil
	}
	return obj.(*ListTask)
}

func (t *ListTask) SetNext(next *ListTask) {
	t.next.Store(next)
}

func (t *ListTask) SetPrev(prev *ListTask) {
	t.prev.Store(prev)
}
