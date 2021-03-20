package basic

import (
	"context"
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
)

const defaultRunTimeout = time.Second * 30

var (
	InvalidTaskResult = &struct{}{}
	DummyTaskResult   = struct{}{}
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

	Data interface{}
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
	return (&Task{}).Initialize(id, name, runTimeout, f)
}

func (t *Task) Initialize(id string, name string, runTimeout time.Duration, f func(context.Context) (interface{}, error)) *Task {
	assert.Must(t.done == nil)

	t.ID = id
	t.Name = name

	t.ctx, t.cancel = context.WithCancel(context.Background())
	if runTimeout <= 0 {
		runTimeout = defaultRunTimeout
	}
	t.runTimeout = runTimeout

	t.result = InvalidTaskResult
	t.err = errors.ErrDontUseThisBeforeTaskFinished

	t.f = f
	t.done = make(chan struct{})
	return t
}

func (t *Task) OnFinished(g func()) {
	t.onFinished = g
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
		t.err = errors.CASError(t.err, consts.ErrCodeDontUseThisBeforeTaskFinished, errors.ErrGoRoutineExited)
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
