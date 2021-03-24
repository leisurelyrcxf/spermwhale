package scheduler

import (
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/basic"
)

type StaticTreeScheduler struct {
	BasicScheduler
}

func NewStaticTreeScheduler(maxBufferedTask, workerNumber int) *StaticTreeScheduler {
	s := &StaticTreeScheduler{BasicScheduler: BasicScheduler{
		tasks:        make(chan *basic.Task, maxBufferedTask),
		workerNumber: workerNumber,
	}}
	s.start()
	return s
}

func (s *StaticTreeScheduler) ScheduleTree(root *types.TreeTask) error {
	children := root.Children()
	if len(children) == 0 {
		return s.BasicScheduler.Schedule(&root.Task)
	}

	for _, child := range children {
		if err := s.ScheduleTree(child); err != nil {
			return err
		}
	}
	return nil
}

type ConcurrentStaticTreeScheduler struct {
	partitions []*BasicScheduler
}

func NewConcurrentStaticTreeScheduler(partitionNum int, maxBufferedPerPartition int, workerNumberPerPartition int) *ConcurrentStaticTreeScheduler {
	s := &ConcurrentStaticTreeScheduler{partitions: make([]*BasicScheduler, partitionNum)}
	for i := range s.partitions {
		s.partitions[i] = NewBasicScheduler(maxBufferedPerPartition, workerNumberPerPartition)
	}
	return s
}

func (s *ConcurrentStaticTreeScheduler) ScheduleTree(root *types.TreeTask) error {
	children := root.Children()
	if len(children) == 0 {
		return s.partition(root.ID).Schedule(&root.Task)
	}

	for _, child := range children {
		if err := s.ScheduleTree(child); err != nil {
			return err
		}
	}
	return nil
}

func (s *ConcurrentStaticTreeScheduler) Close() {
	for _, partition := range s.partitions {
		partition.Close()
	}
}

func (s *ConcurrentStaticTreeScheduler) partition(taskId basic.TaskId) *BasicScheduler {
	return s.partitions[taskId.Hash()%uint64(len(s.partitions))] // TODO change to &
}
