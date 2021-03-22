package scheduler

import (
	"hash/crc32"
	"sync"

	"github.com/leisurelyrcxf/spermwhale/types/basic"

	"github.com/golang/glog"
	"github.com/leisurelyrcxf/spermwhale/assert"

	"github.com/leisurelyrcxf/spermwhale/errors"
)

type BasicScheduler struct {
	tasks        chan *basic.Task
	workerNumber int

	closed bool
	sync.RWMutex
	wg sync.WaitGroup
}

func NewBasicScheduler(maxBufferedTask, workerNumber int) *BasicScheduler {
	b := &BasicScheduler{
		tasks:        make(chan *basic.Task, maxBufferedTask),
		workerNumber: workerNumber,
	}
	b.start()
	return b
}

func (s *BasicScheduler) Schedule(t *basic.Task) error {
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
	if s.closed {
		s.Unlock()
		return
	}
	close(s.tasks)
	s.closed = true
	s.Unlock()

	s.wg.Wait()
	assert.Must(len(s.tasks) == 0)
}

func (s *BasicScheduler) start() {
	for i := 0; i < s.workerNumber; i++ {
		s.wg.Add(1)

		go func() {
			defer s.wg.Done()

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

type ConcurrentBasicScheduler struct {
	partitions []*BasicScheduler
}

func NewConcurrentBasicScheduler(partitionNum int, maxBufferedPerPartition int, workerNumberPerPartition int) *ConcurrentBasicScheduler {
	s := &ConcurrentBasicScheduler{partitions: make([]*BasicScheduler, partitionNum)}
	for i := range s.partitions {
		s.partitions[i] = NewBasicScheduler(maxBufferedPerPartition, workerNumberPerPartition)
	}
	return s
}

func (s *ConcurrentBasicScheduler) Schedule(task *basic.Task) error {
	return s.partition(task.ID).Schedule(task)
}

func (s *ConcurrentBasicScheduler) Close() {
	for _, partition := range s.partitions {
		partition.Close()
	}
}

func (s *ConcurrentBasicScheduler) partition(taskID string) *BasicScheduler {
	return s.partitions[int(crc32.ChecksumIEEE([]byte(taskID)))%len(s.partitions)]
}
