package types

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/integration_test/utils"
	"github.com/leisurelyrcxf/spermwhale/types"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestListScheduler(t *testing.T) {
	for i := 0; i < 100; i++ {
		if !testListScheduler(t, i) {
			return
		}
	}
}

func testListScheduler(t *testing.T, round int) (b bool) {
	assert := testifyassert.New(t)

	s := NewConcurrentListScheduler(16, 1024, 1)
	defer s.Close()

	const (
		taskNumberPerKey = 100000

		key1InitialValue = 1000
		key1Delta        = 1

		key2InitialValue = 2000
		key2Delta        = 2

		key3InitialValue = 3000
		key3Delta        = 3
	)
	var (
		key1Tasks = make([]*types.ListTask, taskNumberPerKey)
		key2Tasks = make([]*types.ListTask, taskNumberPerKey)
		key3Tasks = make([]*types.ListTask, taskNumberPerKey)
	)
	init := func(tasks []*types.ListTask, key string, initialValue, delta int) {
		for i := 0; i < len(tasks); i++ {
			tasks[i] = types.NewListTaskWithResult(key, key, 0, func(ctx context.Context, prevResult interface{}) (i interface{}, err error) {
				if prevResult == nil {
					return initialValue, nil
				}
				return prevResult.(int) + delta, nil
			})
		}
	}
	init(key1Tasks, "k1", key1InitialValue, key1Delta)
	init(key2Tasks, "k22", key2InitialValue, key2Delta)
	init(key3Tasks, "key3", key3InitialValue, key3Delta)

	schedule := func(tasks []*types.ListTask) {
		const (
			schedulerNumberPerKey = 10
			part                  = taskNumberPerKey / schedulerNumberPerKey
		)
		for i := 0; i < schedulerNumberPerKey; i++ {
			go func(goRoutineIndex int) {
				for _, t := range tasks[goRoutineIndex*part : utils.MinInt((goRoutineIndex+1)*part, len(tasks))] {
					if !assert.NoError(s.Schedule(t)) {
						return
					}
				}
			}(i)
		}
	}
	schedule(key1Tasks)
	schedule(key2Tasks)
	schedule(key3Tasks)

	check := func(tasks []*types.ListTask, key string, initialValue, delta int) (b bool) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		maxVal := 0
		for _, task := range tasks {
			if !assert.Truef(task.WaitFinishWithContext(ctx), "key: %s", key) ||
				!assert.NoErrorf(task.Err(), "key: %s", key) {
				return
			}
			i := task.Result().(int)
			if !assert.Greater(i, 0) {
				return
			}
			if i > maxVal {
				maxVal = i
			}
		}
		return assert.Equalf(initialValue+delta*(len(tasks)-1), maxVal, "key: %s", key)
	}
	if !check(key1Tasks, "key1", key1InitialValue, key1Delta) {
		return
	}
	if !check(key2Tasks, "key2", key2InitialValue, key2Delta) {
		return
	}
	if !check(key3Tasks, "key3", key3InitialValue, key3Delta) {
		return
	}
	return true
}

func TestListSchedulerPropagateErr(t *testing.T) {
	assert := testifyassert.New(t)

	s := NewListScheduler(1024, 10)
	const (
		taskNumberPerKey = 100000

		key1InitialValue = 1000
		key1Delta        = 1
	)
	var (
		key1Tasks = make([]*types.ListTask, taskNumberPerKey)
	)
	init := func(tasks []*types.ListTask, key string, initialValue, delta int) {
		for i := 0; i < len(tasks); i++ {
			tasks[i] = types.NewListTaskWithResult(key, key, 0, func(ctx context.Context, prevResult interface{}) (i interface{}, err error) {
				if prevResult == nil {
					return initialValue, nil
				}
				return prevResult.(int) + delta, nil
			})
		}
	}
	init(key1Tasks, "key1", key1InitialValue, key1Delta)

	if !assert.NoError(s.Schedule(types.NewListTaskNoResult("key1", "key1",
		0, func(ctx context.Context, prevResult interface{}) error {
			return errors.ErrInject
		}))) {
		return
	}
	schedule := func(tasks []*types.ListTask) {
		const schedulerNumberPerKey = 10
		var part = len(tasks) / schedulerNumberPerKey
		for i := 0; i < schedulerNumberPerKey; i++ {
			go func(goRoutineIndex int) {
				for _, t := range tasks[goRoutineIndex*part : utils.MinInt((goRoutineIndex+1)*part, len(tasks))] {
					if !assert.NoError(s.Schedule(t)) {
						return
					}
				}
			}(i)
		}
	}
	schedule(key1Tasks)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	for _, task := range key1Tasks {
		finished := task.WaitFinishWithContext(ctx)
		if !assert.True(finished) || !assert.Error(task.Err()) || !assert.Equal(errors.ErrInject.Code, task.Err().(*errors.Error).Code) {
			return
		}
	}
}

func TestListSchedulerCancelWait(t *testing.T) {
	for i := 0; i < 1; i++ {
		if !testListSchedulerCancelWait(t) {
			return
		}
	}
}

func testListSchedulerCancelWait(t *testing.T) (b bool) {
	assert := testifyassert.New(t)

	s := NewListScheduler(1024, 10)
	defer s.Close()
	const (
		TaskNumber = 1000
	)
	tasks := make([]*types.ListTask, TaskNumber)
	func(tasks []*types.ListTask) {
		for i := 0; i < len(tasks); i++ {
			tasks[i] = types.NewListTaskNoResult(fmt.Sprintf("key_%d", i), fmt.Sprintf("key_%d", i), 0, func(ctx context.Context, prevResult interface{}) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Minute):
					return nil
				}
			})
		}
	}(tasks)

	func(tasks []*types.ListTask) {
		const schedulerNumberPerKey = 10
		var part = len(tasks) / schedulerNumberPerKey
		for i := 0; i < schedulerNumberPerKey; i++ {
			go func(goRoutineIndex int) {
				for _, t := range tasks[goRoutineIndex*part : utils.MinInt((goRoutineIndex+1)*part, len(tasks))] {
					if !assert.NoError(s.Schedule(t)) {
						return
					}
				}
			}(i)
		}
	}(tasks)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	for _, task := range tasks {
		task.Cancel()
	}
	for _, task := range tasks {
		if finished := task.WaitFinishWithContext(ctx); !assert.True(finished) ||
			!assert.Error(task.Err()) ||
			!assert.Contains(task.Err().Error(), context.Canceled.Error()) {
			return
		}
	}
	return true
}
