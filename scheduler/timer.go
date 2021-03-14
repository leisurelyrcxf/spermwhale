package scheduler

import (
	"container/heap"
	"sync"
	"time"

	"github.com/leisurelyrcxf/spermwhale/utils"
)

type TimerTask struct {
	ScheduleTime time.Time
	f            func()
}

func NewTimerTask(scheduleTime time.Time, f func()) *TimerTask {
	return &TimerTask{
		ScheduleTime: scheduleTime,
		f:            f,
	}
}

type BasicTimer struct {
	input chan *TimerTask

	quit     chan struct{}
	quitOnce sync.Once
	wg       *sync.WaitGroup
}

func NewBasicTimer(chSize int, wg *sync.WaitGroup) *BasicTimer {
	return &BasicTimer{
		input: make(chan *TimerTask, chSize),
		quit:  make(chan struct{}),
		wg:    wg,
	}
}

func (timer *BasicTimer) Schedule(t *TimerTask) {
	timer.input <- t
}

func (timer *BasicTimer) Close() {
	timer.quitOnce.Do(func() {
		close(timer.quit)
	})
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
	wg     sync.WaitGroup
}

func NewConcurrentBasicTimer(partitionNumber int, chSizePerPartition int) *ConcurrentBasicTimer {
	c := &ConcurrentBasicTimer{}
	c.Initialize(partitionNumber, chSizePerPartition)
	return c
}

func (c *ConcurrentBasicTimer) Initialize(partitionNumber int, chSizePerPartition int) {
	c.timers = make([]*BasicTimer, partitionNumber)
	for i := range c.timers {
		c.timers[i] = NewBasicTimer(chSizePerPartition, &c.wg)
	}
}

func (c *ConcurrentBasicTimer) Schedule(t *TimerTask) {
	c.timers[t.ScheduleTime.UnixNano()%int64(len(c.timers))].Schedule(t)
}

func (c *ConcurrentBasicTimer) Close() {
	for _, t := range c.timers {
		t.Close()
	}
	c.wg.Wait()
}

func (c *ConcurrentBasicTimer) Start() {
	for _, t := range c.timers {
		t.Start()
	}
}

type TimerTaskQueue []*TimerTask

func (h TimerTaskQueue) Len() int {
	return len(h)
}

func (h TimerTaskQueue) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h TimerTaskQueue) Less(i, j int) bool {
	return h[i].ScheduleTime.Before(h[j].ScheduleTime)
}

func (h *TimerTaskQueue) Push(x interface{}) {
	item := x.(*TimerTask)
	*h = append(*h, item)
}

func (h *TimerTaskQueue) Pop() interface{} {
	old, n := *h, len(*h)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// MININT is the minimal interval for delete to run. In most cases, it is better to be set as 0
const MININT = 0

type Timer struct {
	input    chan *TimerTask
	quit     chan struct{}
	quitOnce sync.Once
}

func NewTimer() *Timer {
	return &Timer{
		input: make(chan *TimerTask),
		quit:  make(chan struct{}),
	}
}

func (timer *Timer) Schedule(t *TimerTask) {
	timer.input <- t
}

func (timer *Timer) Close() {
	timer.quitOnce.Do(func() {
		close(timer.quit)
	})
}

func (timer *Timer) Start() {
	go func() {
		h := make(TimerTaskQueue, 0)

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
					heap.Pop(&h).(*TimerTask).f()
				}
				t.Reset(utils.MaxDuration(time.Until(h[0].ScheduleTime), MININT))
			}
		}
	}()
}
