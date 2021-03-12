package scheduler

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestTreeScheduler(t *testing.T) {
	for i := 0; i <= 10000; i++ {
		if !testTreeScheduler(t, i) {
			return
		}
		if i%1000 == 0 {
			t.Logf("round %d finished", i)
		}
	}
}

func testTreeScheduler(t *testing.T, round int) (b bool) {
	assert := testifyassert.New(t)

	s := NewConcurrentStaticTreeScheduler(32, 1024, 1)
	defer s.Close()

	const (
		taskNumber  = 100
		umbrellaNum = 10
	)

	var (
		root = types.NewTreeTaskWithResult(
			"root", "root", 0,
			nil, func(ctx context.Context, prevResults []interface{}) (i interface{}, err error) {
				var sum int
				for _, prev := range prevResults {
					sum += prev.(int)
				}
				return sum, nil
			},
		)

		expSuperResult    int
		expInternalResult int
	)
	for u := 0; u < umbrellaNum; u++ {
		var internal = types.NewTreeTaskWithResult(
			"internal", "internal", 0,
			root, func(ctx context.Context, prevResults []interface{}) (i interface{}, err error) {
				var sum int
				for _, prev := range prevResults {
					sum += prev.(int)
				}
				return sum, nil
			},
		)

		expInternalResult = 0
		for i := 0; i < taskNumber; i++ {
			expSuperResult += i
			expInternalResult += i
			key := strconv.Itoa(i)
			k := i
			_ = types.NewTreeTaskWithResult(
				key, key, 0,
				internal, func(ctx context.Context, prevResult []interface{}) (interface{}, error) {
					return k, nil
				},
			)
		}
	}

	if !assert.NoError(s.ScheduleTree(root)) {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	if !assert.True(root.WaitFinishWithContext(ctx)) {
		return
	}
	for _, internal := range root.Children() {
		if !assert.Equal(expInternalResult, internal.Result()) {
			return
		}
	}
	return assert.Equal(expSuperResult, root.Result())
}

func TestTreeSchedulerPropagateErr(t *testing.T) {
	for i := 0; i <= 10000; i++ {
		if !testTreeSchedulerPropagateErr(t, i) {
			t.Errorf("testTreeSchedulerPropagateErr round %d failed", i)
			return
		}
		if i%1000 == 0 {
			t.Logf("testTreeSchedulerPropagateErr round %d finished", i)
		}
	}
}

func testTreeSchedulerPropagateErr(t *testing.T, round int) (b bool) {
	assert := testifyassert.New(t)

	s := NewConcurrentStaticTreeScheduler(32, 1024, 1)
	defer s.Close()

	const (
		taskNumber  = 100
		umbrellaNum = 10
	)

	var root = types.NewTreeTaskWithResult(
		"root", "root", 0,
		nil, func(ctx context.Context, prevResults []interface{}) (i interface{}, err error) {
			var sum int
			for _, prev := range prevResults {
				sum += prev.(int)
			}
			return sum, nil
		},
	)
	for u := 0; u < umbrellaNum; u++ {
		var internal = types.NewTreeTaskWithResult(
			"internal", "internal", 0,
			root, func(ctx context.Context, prevResults []interface{}) (i interface{}, err error) {
				var sum int
				for _, prev := range prevResults {
					sum += prev.(int)
				}
				return sum, nil
			},
		)

		_ = types.NewTreeTaskWithResult(
			"0", "0", 0,
			internal, func(ctx context.Context, prevResult []interface{}) (i interface{}, err error) {
				return 0, errors.ErrInject
			},
		)
		for i := 1; i < taskNumber; i++ {
			j := i
			key := strconv.Itoa(j)
			_ = types.NewTreeTaskWithResult(
				key, key, 0,
				internal, func(ctx context.Context, prevResult []interface{}) (i interface{}, err error) {
					return j, nil
				},
			)
		}
	}

	if !assert.NoError(s.ScheduleTree(root)) {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	if !assert.True(root.WaitFinishWithContext(ctx)) {
		return
	}
	for _, internal := range root.Children() {
		if !assert.Nil(internal.Result()) || !assert.Equal(errors.ErrInject.Code, errors.GetErrorCode(internal.Err())) {
			return
		}
	}

	return assert.Nil(root.Result()) &&
		assert.Equal(errors.ErrInject.Code, errors.GetErrorCode(root.Err()))
}
