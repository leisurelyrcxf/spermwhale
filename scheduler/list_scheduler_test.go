package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/basic"
	"github.com/leisurelyrcxf/spermwhale/utils"

	testifyassert "github.com/stretchr/testify/assert"
)

const rounds = 10000

func TestListScheduler(t *testing.T) {
	for i := 0; i <= rounds; i++ {
		if !testListScheduler(t, i) {
			return
		}
		if i%1000 == 0 {
			t.Logf("round %d finished", i)
		}
	}
}

func testListScheduler(t *testing.T, _ int) (b bool) {
	assert := testifyassert.New(t)

	s := NewConcurrentDynamicListScheduler(16, 1024, 1)
	defer s.Close()

	const (
		taskNumberPerKey = 100

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
	init := func(tasks []*types.ListTask, id basic.TaskId, initialValue, delta int) {
		for i := 0; i < len(tasks); i++ {
			tasks[i] = types.NewListTaskWithResult(id, id.Key, 0, func(ctx context.Context, prevResult interface{}) (i interface{}, err error) {
				if prevResult == nil {
					return initialValue, nil
				}
				return prevResult.(int) + delta, nil
			})
		}
	}
	init(key1Tasks, basic.NewTaskId(1024, "k1"), key1InitialValue, key1Delta)
	init(key2Tasks, basic.NewTaskId(1024, "k22"), key2InitialValue, key2Delta)
	init(key3Tasks, basic.NewTaskId(1, "key3"), key3InitialValue, key3Delta)

	schedule := func(tasks []*types.ListTask) {
		const (
			schedulerNumberPerKey = 10
			part                  = taskNumberPerKey / schedulerNumberPerKey
		)
		for i := 0; i < schedulerNumberPerKey; i++ {
			go func(goRoutineIndex int) {
				for _, t := range tasks[goRoutineIndex*part : utils.MinInt((goRoutineIndex+1)*part, len(tasks))] {
					if !assert.NoError(s.ScheduleListTask(t)) {
						return
					}
				}
			}(i)
		}
	}
	schedule(key1Tasks)
	schedule(key2Tasks)
	schedule(key3Tasks)

	var tasks = make([]*basic.Task, taskNumberPerKey)
	for i := 0; i < len(tasks); i++ {
		var (
			i = i
		)
		tasks[i] = basic.NewTask(basic.NewTaskId(uint64(i), ""), fmt.Sprintf("task-%d", i), 0, func(ctx context.Context) (interface{}, error) {
			return i, nil
		})
		if !assert.NoError(s.Schedule(tasks[i])) {
			return
		}
	}

	check := func(tasks []*types.ListTask, initialValue, delta int) (b bool) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		maxVal := 0
		for _, task := range tasks {
			if !assert.Truef(task.WaitFinishWithContext(ctx), "id: %s", task.ID) ||
				!assert.NoErrorf(task.Err(), "id: %s", task.ID) {
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
		return assert.Equalf(initialValue+delta*(len(tasks)-1), maxVal, "id: %s", tasks[0].ID)
	}
	if !check(key1Tasks, key1InitialValue, key1Delta) {
		return
	}
	if !check(key2Tasks, key2InitialValue, key2Delta) {
		return
	}
	if !check(key3Tasks, key3InitialValue, key3Delta) {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	for i, task := range tasks {
		if !assert.Truef(task.WaitFinishWithContext(ctx), "task_id: %s", task.ID) ||
			!assert.NoErrorf(task.Err(), "task_id: %s", task.ID) {
			return
		}
		if !assert.Equal(i, task.Result().(int)) {
			return
		}
	}
	return true
}

func TestListSchedulerPropagateErr(t *testing.T) {
	assert := testifyassert.New(t)

	s := NewDynamicListScheduler(1024, 10)
	const (
		taskNumberPerKey = 100000

		key1InitialValue = 1000
		key1Delta        = 1
	)
	var (
		key1Tasks = make([]*types.ListTask, taskNumberPerKey)
		id        = basic.NewTaskId(1000, "key1")
	)
	func(tasks []*types.ListTask, id basic.TaskId, initialValue, delta int) {
		for i := 0; i < len(tasks); i++ {
			tasks[i] = types.NewListTaskWithResult(id, id.Key, 0, func(ctx context.Context, prevResult interface{}) (i interface{}, err error) {
				if prevResult == nil {
					return initialValue, nil
				}
				return prevResult.(int) + delta, nil
			})
		}
	}(key1Tasks, id, key1InitialValue, key1Delta)

	if !assert.NoError(s.ScheduleListTask(types.NewListTaskNoResult(id, id.Key, 0, func(ctx context.Context, prevResult interface{}) error {
		return errors.ErrInject
	}))) {
		return
	}
	func(tasks []*types.ListTask) {
		const schedulerNumberPerKey = 10
		var part = len(tasks) / schedulerNumberPerKey
		for i := 0; i < schedulerNumberPerKey; i++ {
			go func(goRoutineIndex int) {
				for _, t := range tasks[goRoutineIndex*part : utils.MinInt((goRoutineIndex+1)*part, len(tasks))] {
					if !assert.NoError(s.ScheduleListTask(t)) {
						return
					}
				}
			}(i)
		}
	}(key1Tasks)

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

	s := NewDynamicListScheduler(1024, 10)
	defer s.Close()
	const (
		TaskNumber = 1000
	)
	tasks := make([]*types.ListTask, TaskNumber)
	func(tasks []*types.ListTask) {
		for i := 0; i < len(tasks); i++ {
			var key = fmt.Sprintf("key_%d", i)
			tasks[i] = types.NewListTaskNoResult(basic.NewTaskId(1000, key), key, 0, func(ctx context.Context, prevResult interface{}) error {
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
					if !assert.NoError(s.ScheduleListTask(t)) {
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
