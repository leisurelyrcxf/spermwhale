package timer

import (
	"sync"
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

type AggrTimerTask struct {
	ScheduleTime time.Time
	Val          interface{}
}

func (t AggrTimerTask) Until() time.Duration {
	return time.Until(t.ScheduleTime)
}

func (t AggrTimerTask) OK() bool {
	return t.Until() <= 0
}

func (t AggrTimerTask) IsEmpty() bool {
	return t.ScheduleTime.IsZero()
}

func NewAggrTimerTask(when time.Time, val interface{}) AggrTimerTask {
	return AggrTimerTask{
		ScheduleTime: when,
		Val:          val,
	}
}

type AggrFunc func([]interface{})

type AggrTimer struct {
	input chan AggrTimerTask

	batchSize int
	aggrFunc  AggrFunc

	quit chan struct{}
	wg   *sync.WaitGroup
}

func NewAggrTimer(chSize int, batchSize int, aggrFunc AggrFunc, quit chan struct{}, wg *sync.WaitGroup) *AggrTimer {
	return &AggrTimer{
		input:     make(chan AggrTimerTask, chSize),
		batchSize: batchSize,
		aggrFunc:  aggrFunc,

		quit: quit,
		wg:   wg,
	}
}

func (timer *AggrTimer) Schedule(t AggrTimerTask) {
	timer.input <- t
}

func (timer *AggrTimer) Start() {
	timer.wg.Add(1)

	go func() {
		defer timer.wg.Done()

		var (
			waitingForTask      AggrTimerTask
			waitingForTaskUntil time.Duration

			t              *time.Timer
			bufferedValues []interface{}
		)

		select {
		case waitingForTask = <-timer.input:
			waitingForTaskUntil = waitingForTask.Until()
			t = time.NewTimer(utils.MaxDuration(waitingForTaskUntil, MININT))
			bufferedValues = make([]interface{}, 0, timer.batchSize+10)
			break
		case <-timer.quit:
			return
		}

		for {
			select {
			case <-t.C:
				break
			case <-timer.quit:
				return
			}

			if !waitingForTask.IsEmpty() {
				assert.Must(waitingForTask.OK())
				bufferedValues = append(bufferedValues, waitingForTask.Val)
				waitingForTask, waitingForTaskUntil = AggrTimerTask{}, 0
			}
			assert.Must(waitingForTaskUntil == 0)

			for j := len(timer.input) - 1; j >= 0; j-- {
				task := <-timer.input
				if until := task.Until(); until >= 0 {
					waitingForTask, waitingForTaskUntil = task, until
					break
				}
				if bufferedValues = append(bufferedValues, task.Val); len(bufferedValues) >= timer.batchSize {
					timer.aggrFunc(bufferedValues)
					bufferedValues = bufferedValues[:0]
				}
				if j == 0 {
					j = len(timer.input)
				}
			}
			if len(bufferedValues) > 0 {
				timer.aggrFunc(bufferedValues)
				bufferedValues = bufferedValues[:0]
			}
			t.Reset(utils.MaxDuration(waitingForTaskUntil, MININT))
		}
	}()
}
