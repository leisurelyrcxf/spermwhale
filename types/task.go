package types

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types/basic"
)

type ListTask struct {
	basic.Task

	prev, next  atomic.Value
	initialized bool
}

func NewListTaskNoResult(id string, name string, runTimeout time.Duration,
	g func(ctx context.Context, prevResult interface{}) error) *ListTask {
	return NewListTaskWithResult(id, name, runTimeout, func(ctx context.Context, prevResult interface{}) (i interface{}, err error) {
		if err := g(ctx, prevResult); err != nil {
			return nil, err
		}
		return basic.DummyTaskResult, nil
	})
}

func NewListTaskWithResult(id string, name string, runTimeout time.Duration,
	g func(ctx context.Context, prevResult interface{}) (interface{}, error)) *ListTask {
	return (&ListTask{}).Initialize(id, name, runTimeout, g)
}

func (t *ListTask) Initialize(id string, name string, runTimeout time.Duration,
	g func(ctx context.Context, prevResult interface{}) (interface{}, error)) *ListTask {
	assert.Must(!t.initialized)

	t.prev.Store((*ListTask)(nil))
	t.next.Store((*ListTask)(nil))
	t.Task.Initialize(id, name, runTimeout, func(ctx context.Context) (interface{}, error) {
		prev := t.Prev()
		if prev == nil {
			return g(ctx, nil)
		}
		if err := prev.Err(); err != nil {
			return nil, errors.Annotatef(err, "detail: error propagated from previous task %s(%s)", prev.ID, prev.Name)
		}
		res := prev.Result()
		if res == nil {
			panic("previous task func returned (nil, nil), which is not allowed")
		}
		return g(ctx, res)
	})
	t.initialized = true
	return t
}

func (t *ListTask) Prev() *ListTask {
	return t.prev.Load().(*ListTask)
}

func (t *ListTask) Next() *ListTask {
	return t.next.Load().(*ListTask)
}

func (t *ListTask) SetNext(next *ListTask) {
	t.next.Store(next)
}

func (t *ListTask) SetPrev(prev *ListTask) {
	t.prev.Store(prev)
}

type TreeTask struct {
	basic.Task

	children     []*TreeTask
	childrenDone int64

	parent      *TreeTask
	initialized bool
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
	return (&TreeTask{}).Initialize(id, name, runTimeout, parent, g)
}

func (t *TreeTask) Initialize(id string, name string, runTimeout time.Duration,
	parent *TreeTask,
	g func(ctx context.Context, childrenResult []interface{}) (interface{}, error)) *TreeTask {
	assert.Must(!t.initialized)

	if t.parent = parent; t.parent != nil {
		t.parent.children = append(t.parent.children, t)
	}
	t.Task.Initialize(id, name, runTimeout, func(ctx context.Context) (interface{}, error) {
		var childrenResult []interface{}
		for _, child := range t.Children() {
			if err := child.Err(); err != nil {
				return nil, errors.Annotatef(err, "error propagated from child task %s(%s)", child.ID, child.Name)
			}
			childrenResult = append(childrenResult, child.Result())
		}
		return g(ctx, childrenResult)
	}).OnFinished(func() {
		t.parent.childDone()
	})
	t.initialized = true
	return t
}

func (t *TreeTask) childDone() {
	if t == nil {
		return
	}

	if atomic.AddInt64(&t.childrenDone, 1) == int64(len(t.children)) {
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
