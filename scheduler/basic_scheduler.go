package types

import (
	"sync"

	"github.com/leisurelyrcxf/spermwhale/types"

	"github.com/golang/glog"
	"github.com/leisurelyrcxf/spermwhale/errors"
)

type BasicScheduler struct {
	tasks        chan *types.Task
	workerNumber int

	closed bool
	sync.RWMutex
}

func NewBasicScheduler(maxBufferedTask, workerNumber int) *BasicScheduler {
	b := &BasicScheduler{
		tasks:        make(chan *types.Task, maxBufferedTask),
		workerNumber: workerNumber,
	}
	b.start()
	return b
}

func (s *BasicScheduler) Schedule(t *types.Task) error {
	s.RLock()
	defer s.RUnlock()

	if s.closed {
		return errors.ErrSchedulerClosed
	}
	s.tasks <- t
	return nil
}

func (s *BasicScheduler) Close() {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return
	}
	close(s.tasks)
	s.closed = true
}

func (s *BasicScheduler) start() {
	for i := 0; i < s.workerNumber; i++ {
		go func() {
			for {
				task, ok := <-s.tasks
				if !ok {
					return
				}

				if err := task.Run(); err != nil {
					glog.Errorf("task %s failed: %v", task.Name, err)
				}
			}
		}()
	}
}
