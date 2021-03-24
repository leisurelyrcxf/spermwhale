package scheduler

import (
	"context"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/basic"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
)

type list struct {
	head          *types.ListTask // head is const
	tail          *types.ListTask
	listProtector sync.Mutex

	// Additional info, this is used by TaskSchedule
	lastFinishedTask  *types.ListTask
	queuedOrExecuting bool
}

func newList(head *types.ListTask) *list {
	return &list{
		head: head,
		tail: head,
	}
}

func (l *list) append(t *types.ListTask) {
	l.listProtector.Lock()
	defer l.listProtector.Unlock()

	assert.Must(l.tail != nil)
	assert.Must(t.Next() == nil)
	l.tail.SetNext(t)
	t.SetPrev(l.tail)
	l.tail = t
}

type TaskMap struct {
	sync.Mutex
	m map[basic.TaskId]*list
}

func (m *TaskMap) Initialize() {
	m.m = make(map[basic.TaskId]*list)
}

func (m *TaskMap) Get(id basic.TaskId) *list {
	m.Lock()
	defer m.Unlock()

	return m.m[id]
}

func (m *TaskMap) Set(id basic.TaskId, l *list) {
	m.Lock()
	m.m[id] = l
	m.Unlock()
}

func (m *TaskMap) Del(id basic.TaskId) {
	m.Lock()
	delete(m.m, id)
	m.Unlock()
}

func (m *TaskMap) ForEach(f func(id basic.TaskId, l *list)) {
	m.Lock()
	defer m.Unlock()

	for k, v := range m.m {
		f(k, v)
	}
}

type DynamicListScheduler struct {
	tasks              chan interface{}
	taskListsProtector sync.RWMutex
	closed             bool

	taskListMap  TaskMap
	workerNumber int

	lm concurrency.TaskLockManager
	wg sync.WaitGroup
}

func NewDynamicListScheduler(maxBufferedTask, workerNumber int) *DynamicListScheduler {
	b := &DynamicListScheduler{
		tasks:        make(chan interface{}, maxBufferedTask),
		workerNumber: workerNumber,
	}
	b.taskListMap.Initialize()
	b.lm.Initialize()
	b.start()
	return b
}

func (s *DynamicListScheduler) Schedule(t *basic.Task) error {
	return s.schedule(t)
}

func (s *DynamicListScheduler) ScheduleListTask(t *types.ListTask) error {
	s.lm.Lock(t.ID)
	l := s.taskListMap.Get(t.ID)
	if l != nil {
		l.append(t)
	} else {
		l = newList(t)
		s.taskListMap.Set(t.ID, l)
	}
	if l.queuedOrExecuting {
		s.lm.Unlock(t.ID)
		return nil
	}
	l.queuedOrExecuting = true
	s.lm.Unlock(t.ID)

	return s.schedule(l)
}

func (s *DynamicListScheduler) schedule(t interface{}) error {
	s.taskListsProtector.RLock()
	defer s.taskListsProtector.RUnlock()
	if s.closed {
		return errors.ErrSchedulerClosed
	}

	s.tasks <- t
	return nil
}

func (s *DynamicListScheduler) GC(tasks []*types.ListTask) {
	for _, task := range tasks {
		s.taskListMap.Del(task.ID)
	}
}

func (s *DynamicListScheduler) Close() {
	s.taskListsProtector.Lock()
	if s.closed {
		s.taskListsProtector.Unlock()
		return
	}

	close(s.tasks)
	s.closed = true
	s.taskListsProtector.Unlock()

	s.wg.Wait()
	assert.Must(len(s.tasks) == 0)
	s.taskListMap.ForEach(func(_ basic.TaskId, l *list) {
		assert.Must(l.lastFinishedTask == l.tail) // TODO remove this in product
	})
	s.taskListMap.m = nil
}

func (s *DynamicListScheduler) start() {
	for i := 0; i < s.workerNumber; i++ {
		s.wg.Add(1)

		go func() {
			defer s.wg.Done()

			for {
				taskObj, ok := <-s.tasks
				if !ok {
					return
				}
				if task, ok := taskObj.(*basic.Task); ok {
					if err := task.Run(); err != nil {
						if !errors.IsRetryableTransactionErr(err) && err != context.Canceled && status.Code(err) != codes.Canceled {
							if glog.V(1) {
								glog.Errorf("task %s(%s) failed: %v", task.ID, task.Name, err)
							}
						}
					}
					continue
				}
				tl := taskObj.(*list)
				assert.Must(tl.queuedOrExecuting)

				var firstTask *types.ListTask

				s.lm.Lock(tl.head.ID)
				if tl.lastFinishedTask == nil {
					firstTask = tl.head
				} else {
					firstTask = tl.lastFinishedTask.Next()
				}
				s.lm.Unlock(tl.head.ID)

				assert.Must(firstTask != nil)
				for task := firstTask; ; {
					if err := task.Run(); err != nil {
						if !errors.IsRetryableTransactionErr(err) && err != context.Canceled && status.Code(err) != codes.Canceled {
							glog.Errorf("task %s(%s) failed: %v", task.ID, task.Name, err)
						}
					}

					s.lm.Lock(task.ID)
					if next := task.Next(); next != nil {
						task = next
						s.lm.Unlock(task.ID)
						continue
					}
					tl.lastFinishedTask = task
					assert.Must(tl.queuedOrExecuting)
					tl.queuedOrExecuting = false
					s.lm.Unlock(task.ID)
					break
				}
			}
		}()
	}
}

type ConcurrentDynamicListScheduler struct {
	partitions []*DynamicListScheduler
}

func NewConcurrentDynamicListScheduler(partitionNum int, maxBufferedPerPartition int, workerNumberPerPartition int) *ConcurrentDynamicListScheduler {
	s := &ConcurrentDynamicListScheduler{partitions: make([]*DynamicListScheduler, partitionNum)}
	for i := range s.partitions {
		s.partitions[i] = NewDynamicListScheduler(maxBufferedPerPartition, workerNumberPerPartition)
	}
	return s
}

func (s *ConcurrentDynamicListScheduler) Schedule(t *basic.Task) error {
	return s.partition(t.ID).Schedule(t)
}

func (s *ConcurrentDynamicListScheduler) ScheduleListTask(t *types.ListTask) error {
	return s.partition(t.ID).ScheduleListTask(t)
}

func (s *ConcurrentDynamicListScheduler) GC(tasks []*types.ListTask) {
	for _, t := range tasks {
		s.partition(t.ID).taskListMap.Del(t.ID)
	}
}

func (s *ConcurrentDynamicListScheduler) Close() {
	for _, partition := range s.partitions {
		partition.Close()
	}
}

func (s *ConcurrentDynamicListScheduler) partition(taskId basic.TaskId) *DynamicListScheduler {
	return s.partitions[taskId.Hash()%uint64(len(s.partitions))]
}
