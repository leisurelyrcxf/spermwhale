package types

import (
	"context"
	"testing"
	"time"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types/basic"
)

func TestTask_Result(t *testing.T) {
	for i := 0; i < 100000; i++ {
		if !testTaskResult(t, i) {
			t.Errorf("test failed @round %d", i)
			return
		}
		if i%1000 == 0 {
			t.Logf("test succeeded @round %d", i)
		}
	}
}

func testTaskResult(t *testing.T, round int) (b bool) {
	assert := testifyassert.New(t)

	task := NewTreeTaskNoResult(basic.NewTaskId(0, "id"), "name", 0, nil, func(ctx context.Context, childrenResult []interface{}) error {
		return errors.ErrInject
	})

	go func() {
		time.Sleep(time.Millisecond)
		_ = task.Run()
	}()

	for {
		if task.Finished() {
			return assert.Equal(errors.ErrInject, task.Err())
		}
	}
}
