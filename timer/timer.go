package timer

import (
	"container/heap"
	"sync"
	"time"

	"github.com/leisurelyrcxf/spermwhale/utils"
)

// MININT is the minimal interval for delete to run. In most cases, it is better to be set as 0
const MININT = time.Second

type Task struct {
	ScheduleTime time.Time
	f            func()
}

func NewTask(scheduleTime time.Time, f func()) *Task {
	return &Task{
		ScheduleTime: scheduleTime,
		f:            f,
	}
}

type BasicTimer struct {
	input chan *Task

	quit chan struct{}
	wg   *sync.WaitGroup
}

func NewBasicTimer(chSize int, quit chan struct{}, wg *sync.WaitGroup) *BasicTimer {
	return &BasicTimer{
		input: make(chan *Task, chSize),
		quit:  quit,
		wg:    wg,
	}
}

func (timer *BasicTimer) Schedule(t *Task) {
	timer.input <- t
}

func (timer *BasicTimer) Start() {
	timer.wg.Add(1)
	go func() {
		defer timer.wg.Done()

		for {
			select {
			case task := <-timer.input:
				if duration := time.Until(task.ScheduleTime); duration > 0 {
					select {
					case <-time.After(duration):
						break
					case <-timer.quit:
						return
					}
				}
				task.f()
			case <-timer.quit:
				return
			}
		}
	}()
}

type ConcurrentBasicTimer struct {
	timers []*BasicTimer

	quit     chan struct{}
	quitOnce sync.Once
	wg       sync.WaitGroup
}

func NewConcurrentBasicTimer(partitionNumber int, chSizePerPartition int) *ConcurrentBasicTimer {
	c := &ConcurrentBasicTimer{quit: make(chan struct{})}
	c.Initialize(partitionNumber, chSizePerPartition)
	return c
}

func (c *ConcurrentBasicTimer) Initialize(partitionNumber int, chSizePerPartition int) {
	c.timers = make([]*BasicTimer, partitionNumber)
	for i := range c.timers {
		c.timers[i] = NewBasicTimer(chSizePerPartition, c.quit, &c.wg)
	}
}

func (c *ConcurrentBasicTimer) Schedule(t *Task) {
	c.timers[t.ScheduleTime.UnixNano()%int64(len(c.timers))].Schedule(t)
}

func (c *ConcurrentBasicTimer) Close() {
	c.quitOnce.Do(func() {
		close(c.quit)
	})
	c.wg.Wait()
}

func (c *ConcurrentBasicTimer) Start() {
	for _, t := range c.timers {
		t.Start()
	}
}

type TaskQueue []*Task

func (h TaskQueue) Len() int {
	return len(h)
}

func (h TaskQueue) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h TaskQueue) Less(i, j int) bool {
	return h[i].ScheduleTime.Before(h[j].ScheduleTime)
}

func (h *TaskQueue) Push(x interface{}) {
	item := x.(*Task)
	*h = append(*h, item)
}

func (h *TaskQueue) Pop() interface{} {
	old, n := *h, len(*h)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

type Timer struct {
	input    chan *Task
	quit     chan struct{}
	quitOnce sync.Once
}

func NewTimer() *Timer {
	return &Timer{
		input: make(chan *Task),
		quit:  make(chan struct{}),
	}
}

func (timer *Timer) Schedule(t *Task) {
	timer.input <- t
}

func (timer *Timer) Close() {
	timer.quitOnce.Do(func() {
		close(timer.quit)
	})
}

func (timer *Timer) Start() {
	go func() {
		h := make(TaskQueue, 0)

		item := <-timer.input
		heap.Push(&h, &item)
		t := time.NewTimer(time.Until(h[0].ScheduleTime))

		for {
			//Check unfinished incoming first
			for i, incoming := 0, true; i < 10 && incoming; i++ {
				select {
				case item := <-timer.input:
					heap.Push(&h, &item)
				default:
					incoming = false
				}
			}

			t.Reset(utils.MaxDuration(time.Until(h[0].ScheduleTime), MININT))

			select {
			case <-timer.quit:
				return
			//New Item incoming, break the timer
			case item := <-timer.input:
				heap.Push(&h, &item)
				if item.ScheduleTime.After(h[0].ScheduleTime) {
					continue
				}
				t.Reset(utils.MaxDuration(time.Until(h[0].ScheduleTime), MININT))
				//Wait until next item to be deleted
			case <-t.C:
				for !h[0].ScheduleTime.After(time.Now()) {
					heap.Pop(&h).(*Task).f()
				}
				t.Reset(utils.MaxDuration(time.Until(h[0].ScheduleTime), MININT))
			}
		}
	}()
}
