package transaction

import (
	"container/heap"
	"sync"

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

type priorityQueue struct {
	sync.Mutex

	pendingReaders []*transaction
}

func newPriorityQueue() *priorityQueue {
	return &priorityQueue{}
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

	reader.readForWriteCond = newReadForWriteCond()
	heap.Push(pq, reader)
	if pq.Len() == 1 {
		return nil, nil
	}
	return reader.readForWriteCond, nil
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
		newMinReader.readForWriteCond.notify()
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
		return newPriorityQueue()
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
