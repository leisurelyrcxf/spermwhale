package types

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
)

const defaultRunTimeout = time.Second * 30

var (
	InvalidTaskResult = &struct{}{}
	DummyTaskResult   = &struct{}{}
)

type Task struct {
	ID   string
	Name string

	ctx        context.Context
	cancel     context.CancelFunc
	runTimeout time.Duration

	// output
	result interface{}
	err    error

	f          func(context.Context) (interface{}, error)
	done       chan struct{}
	onFinished func()
}

func NewTaskNoResult(id string, name string, runTimeout time.Duration, g func(context.Context) error) *Task {
	return NewTask(id, name, runTimeout, func(ctx context.Context) (i interface{}, err error) {
		if err := g(ctx); err != nil {
			return nil, err
		}
		return DummyTaskResult, nil
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
		result:     InvalidTaskResult,
		err:        errors.ErrDontUseThisBeforeTaskFinished,
		runTimeout: runTimeout,
		f:          f,
		done:       make(chan struct{}),
	}
}

func (t *Task) genContext() *Task {
	t.ctx, t.cancel = context.WithCancel(context.Background())
	return t
}

func (t *Task) Result() interface{} {
	assert.Must(t.result != InvalidTaskResult)
	return t.result
}

func (t *Task) Err() error {
	assert.Must(t.err != errors.ErrDontUseThisBeforeTaskFinished)
	return t.err
}

func (t *Task) ErrUnsafe() error {
	return t.err
}

func (t *Task) WaitFinishWithContext(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-t.done:
		return true
	}
}

func (t *Task) MustWaitFinishWithContext(ctx context.Context) {
	if t.WaitFinishWithContext(ctx) {
		return
	}
	t.cancel()
	<-t.done
}

func (t *Task) WaitFinish() {
	<-t.done
}

func (t *Task) Cancel() {
	t.cancel()
}

func (t *Task) Run() error {
	defer func() {
		close(t.done)

		if t.onFinished != nil {
			t.onFinished()
		}
	}()

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
		return DummyTaskResult, nil
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
		assert.Must(prev.Finished())
		if prev.err != nil {
			return nil, errors.Annotatef(prev.err, "detail: error propagated from previous task %s(%s)", prev.ID, prev.Name)
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

type TreeTask struct {
	Task

	children     []*TreeTask
	childrenDone int64

	parent *TreeTask
}

func NewTreeTaskNoResult(
	id string, name string, runTimeout time.Duration,
	parent *TreeTask,
	g func(ctx context.Context, childrenResult []interface{}) error) *TreeTask {
	return NewTreeTaskWithResult(
		id, name, runTimeout,
		parent,
		func(ctx context.Context, childrenResult []interface{}) (i interface{}, err error) {
			return nil, g(ctx, childrenResult)
		})
}

func NewTreeTaskWithResult(
	id string, name string, runTimeout time.Duration,
	parent *TreeTask,
	g func(ctx context.Context, childrenResult []interface{}) (interface{}, error)) *TreeTask {
	t := &TreeTask{Task: newTask(id, name, runTimeout, nil), parent: parent}
	if t.parent != nil {
		t.parent.children = append(t.parent.children, t)
	}
	t.Task.f = func(ctx context.Context) (interface{}, error) {
		var childrenResult []interface{}
		for _, child := range t.Children() {
			assert.Must(child.Finished())
			if child.err != nil {
				return nil, errors.Annotatef(child.err, "error propagated from child task %s(%s)", child.ID, child.Name)
			}
			childrenResult = append(childrenResult, child.result)
		}
		return g(ctx, childrenResult)
	}
	t.genContext()
	t.onFinished = func() {
		t.parent.childDone()
	}
	return t
}

func (t *TreeTask) childDone() {
	if t == nil {
		return
	}

	if newVal := atomic.AddInt64(&t.childrenDone, 1); newVal == int64(len(t.children)) {
		if err := t.Run(); err != nil {
			glog.Errorf("parent task %s(%s) failed: %v", t.ID, t.Name, err)
		}
	}
}

func (t *TreeTask) Children() []*TreeTask {
	return t.children
}

func (t *TreeTask) AllChildrenSuccess() bool {
	return t.ChildrenSuccess(t.children)
}

func (t *TreeTask) ChildrenSuccess(children []*TreeTask) bool {
	for _, child := range children {
		if !child.Finished() {
			return false
		}
		if child.Err() != nil {
			return false
		}
	}
	return true
}
