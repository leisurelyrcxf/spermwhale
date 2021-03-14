package transaction

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
)

//type Type int
//
//const (
//	Invalid Type = iota
//	WriteIntentCleared
//	VersionRemoved
//	VersionWritten
//)

type readForWriteCond struct {
	waitress  chan struct{}
	timeouted concurrency.AtomicBool
}

func newReadForWriteCond() *readForWriteCond {
	return &readForWriteCond{waitress: make(chan struct{})}
}

func (t *readForWriteCond) Wait(ctx context.Context, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case <-waitCtx.Done():
		t.timeouted.Set(true)
		return waitCtx.Err()
	case <-t.waitress:
		return nil
	}
}

func (t *readForWriteCond) notify() {
	close(t.waitress)
}

type readForWriteConds map[string]*readForWriteCond

func (conds *readForWriteConds) add(key string, cond *readForWriteCond) {
	if *conds == nil {
		*conds = make(map[string]*readForWriteCond)
	}
	(*conds)[key] = cond
}

func (conds readForWriteConds) mustGet(key string) *readForWriteCond {
	cond := conds[key]
	assert.Must(cond != nil)
	return cond
}

type priorityQueue struct {
	sync.RWMutex

	key            string
	pendingReaders []*transaction
}

func newPriorityQueue(key string) *priorityQueue {
	return &priorityQueue{key: key}
}

// Heap Interface
func (pq *priorityQueue) Swap(i, j int) {
	pq.pendingReaders[i], pq.pendingReaders[j] = pq.pendingReaders[j], pq.pendingReaders[i]
}
func (pq *priorityQueue) Less(i, j int) bool {
	return pq.pendingReaders[i].id < pq.pendingReaders[j].id
}
func (pq *priorityQueue) Len() int {
	return len(pq.pendingReaders)
}
func (pq *priorityQueue) Push(x interface{}) {
	pq.pendingReaders = append(pq.pendingReaders, x.(*transaction))
}
func (pq *priorityQueue) Pop() interface{} {
	last := pq.pendingReaders[len(pq.pendingReaders)-1]
	pq.pendingReaders = pq.pendingReaders[0 : len(pq.pendingReaders)-1]
	return last
}

func (pq *priorityQueue) pushReader(reader *transaction) (*readForWriteCond, error) {
	pq.Lock()
	defer pq.Unlock()

	if len(pq.pendingReaders) > 0 {
		if minReader := pq.pendingReaders[0]; reader.id.Version() <= minReader.id.Version() {
			assert.Must(reader.id.Version() < minReader.id.Version())
			return nil, errors.Annotatef(errors.ErrWriteReadConflict, "priorityQueue::pushReaderOnKey: readerVersion <= minReader.version")
		}
	}

	if pq.Len()+1 > consts.MaxReadForWriteQueueCapacityPerKey {
		return nil, errors.ErrReadForWriteQueueFull
	}

	cond := newReadForWriteCond()
	reader.readForWriteConds.add(pq.key, cond)
	heap.Push(pq, reader)
	if pq.Len() == 1 {
		cond.notify()
		return nil, nil
	}
	return cond, nil
}

func (pq *priorityQueue) notifyKeyDone(writerVersion uint64) {
	pq.RLock()
	if len(pq.pendingReaders) == 0 {
		pq.RUnlock()
		return
	}
	if writerVersion != pq.pendingReaders[0].id.Version() {
		pq.RUnlock()
		return
	}
	pq.RUnlock()

	pq.Lock()
	defer pq.Unlock()

	if len(pq.pendingReaders) == 0 {
		return
	}
	if writerVersion != pq.pendingReaders[0].id.Version() {
		return
	}
	heap.Pop(pq)
	for len(pq.pendingReaders) > 0 {
		cond := pq.pendingReaders[0].readForWriteConds.mustGet(pq.key)
		cond.notify()
		if !cond.timeouted.Get() {
			break
		}
		heap.Pop(pq)
	}
}

type readForWriteQueues struct {
	m concurrency.ConcurrentMap
}

func (wm *readForWriteQueues) Initialize(partitionNum int) {
	wm.m.Initialize(partitionNum)
}

func (wm *readForWriteQueues) pushReaderOnKey(key string, reader *transaction) (*readForWriteCond, error) {
	return wm.m.GetLazy(key, func() interface{} {
		return newPriorityQueue(key)
	}).(*priorityQueue).pushReader(reader)
}

func (wm *readForWriteQueues) notifyKeyDone(key string, writeVersion uint64) {
	pq, ok := wm.m.Get(key)
	if !ok {
		return
	}
	pq.(*priorityQueue).notifyKeyDone(writeVersion)
}

func (wm *readForWriteQueues) Close() {
	wm.m.Clear()
}
