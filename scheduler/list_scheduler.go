package types

import (
	"sync"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
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

type ListScheduler struct {
	taskLists          chan *list
	taskListsProtector sync.RWMutex
	closed             bool

	taskListMap  concurrency.ConcurrentMap
	workerNumber int

	lm *concurrency.LockManager
}

func NewListScheduler(maxBufferedTask, workerNumber int) *ListScheduler {
	b := &ListScheduler{
		taskLists:    make(chan *list, maxBufferedTask),
		taskListMap:  concurrency.NewConcurrentMap(16),
		workerNumber: workerNumber,
		lm:           concurrency.NewLockManager(),
	}
	b.start()
	return b
}

func (s *ListScheduler) Schedule(t *types.ListTask) error {
	s.lm.Lock(t.ID)
	var l *list
	listObj, ok := s.taskListMap.Get(t.ID)
	if ok {
		l = listObj.(*list)
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

	s.taskListsProtector.RLock()
	defer s.taskListsProtector.RUnlock()
	if s.closed {
		return errors.ErrSchedulerClosed
	}

	s.taskLists <- l
	return nil
}

func (s *ListScheduler) GC(tasks []*types.ListTask) {
	taskKeys := make(map[string]struct{})
	for _, t := range tasks {
		taskKeys[t.ID] = struct{}{}
	}
	for key := range taskKeys {
		s.taskListMap.Del(key)
	}
}

func (s *ListScheduler) Close() {
	s.taskListsProtector.Lock()
	defer s.taskListsProtector.Unlock()
	if s.closed {
		return
	}

	close(s.taskLists)
	s.closed = true
}

func (s *ListScheduler) start() {
	for i := 0; i < s.workerNumber; i++ {
		go func() {
			for {
				tl, ok := <-s.taskLists
				if !ok {
					return
				}
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
						if !errors.IsRetryableTransactionErr(err) {
							glog.Errorf("task %s failed: %v", task.Name, err)
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
