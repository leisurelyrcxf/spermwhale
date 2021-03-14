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
	waitress chan struct{}
}

func newReadForWriteCond() *readForWriteCond {
	return &readForWriteCond{waitress: make(chan struct{})}
}

func (t *readForWriteCond) Wait(ctx context.Context, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case <-waitCtx.Done():
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
	sync.Mutex

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

func (pq *priorityQueue) minReader() *transaction {
	if len(pq.pendingReaders) == 0 {
		return nil
	}
	return pq.pendingReaders[0]
}

func (pq *priorityQueue) pushReader(reader *transaction) (*readForWriteCond, error) {
	pq.Lock()
	defer pq.Unlock()

	if minReader := pq.minReader(); minReader != nil && reader.id.Version() <= minReader.id.Version() {
		assert.Must(reader.id.Version() < minReader.id.Version())
		return nil, errors.Annotatef(errors.ErrWriteReadConflict, "priorityQueue::pushReaderOnKey: readerVersion <= minReader.version")
	}

	if pq.Len()+1 > consts.MaxReadForWriteQueueCapacityPerKey {
		return nil, errors.ErrReadForWriteQueueFull
	}

	cond := newReadForWriteCond()
	reader.readForWriteConds.add(pq.key, cond)
	heap.Push(pq, reader)
	if pq.Len() == 1 {
		return nil, nil
	}
	return cond, nil
}

func (pq *priorityQueue) notifyKeyDone(writerVersion uint64) {
	pq.Lock()
	defer pq.Unlock()

	minReader := pq.minReader()
	if minReader == nil {
		return
	}
	if writerVersion != minReader.id.Version() {
		return
	}
	heap.Pop(pq)
	if newMinReader := pq.minReader(); newMinReader != nil {
		newMinReader.readForWriteConds.mustGet(pq.key).notify()
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
	//wm.m.GetLazy(key, func() interface{} {
	//	return newPriorityQueue()
	//}).(*priorityQueue).notifyKeyDone(writeVersion)
}

func (wm *readForWriteQueues) Close() {
	wm.m.Clear()
}
