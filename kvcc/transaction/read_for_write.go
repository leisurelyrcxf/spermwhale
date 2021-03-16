package transaction

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
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

type reader struct {
	id types.TxnId
	readForWriteCond
}

func newReader(id types.TxnId) *reader {
	return &reader{
		id: id,
		readForWriteCond: readForWriteCond{
			waitress: make(chan struct{}),
		},
	}
}

type priorityQueue struct {
	sync.RWMutex

	key            string
	pendingReaders []*reader
	maxMinReader   types.TxnId
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
	pq.pendingReaders = append(pq.pendingReaders, x.(*reader))
}
func (pq *priorityQueue) Pop() interface{} {
	last := pq.pendingReaders[len(pq.pendingReaders)-1]
	pq.pendingReaders = pq.pendingReaders[0 : len(pq.pendingReaders)-1]
	return last
}

func (pq *priorityQueue) pushReader(readerTxnId types.TxnId) (*readForWriteCond, error) {
	pq.RLock()
	if readerTxnId <= pq.maxMinReader {
		assert.Must(readerTxnId < pq.maxMinReader)
		pq.RUnlock()
		return nil, errors.Annotatef(errors.ErrWriteReadConflict, "priorityQueue::pushReaderOnKey: readerVersion <= pq.maxMinReader")
	}

	if pq.Len()+1 > consts.MaxReadForWriteQueueCapacityPerKey {
		pq.RUnlock()
		return nil, errors.ErrReadForWriteQueueFull
	}
	pq.RUnlock()

	pq.Lock()
	defer pq.Unlock()

	if readerTxnId <= pq.maxMinReader {
		assert.Must(readerTxnId < pq.maxMinReader)
		return nil, errors.Annotatef(errors.ErrWriteReadConflict, "priorityQueue::pushReaderOnKey: readerVersion <= pq.maxMinReader")
	}

	if pq.Len()+1 > consts.MaxReadForWriteQueueCapacityPerKey {
		return nil, errors.ErrReadForWriteQueueFull
	}

	reader := newReader(readerTxnId)
	heap.Push(pq, reader)
	if pq.Len() == 1 {
		pq.maxMinReader = pq.maxMinReader.Max(readerTxnId)
		reader.notify()
		return nil, nil
	}
	return &reader.readForWriteCond, nil
}

func (pq *priorityQueue) notifyKeyDone(readForWriteTxnId types.TxnId) {
	pq.RLock()
	if len(pq.pendingReaders) == 0 {
		pq.RUnlock()
		return
	}
	if readForWriteTxnId != pq.pendingReaders[0].id {
		pq.RUnlock()
		return
	}
	pq.RUnlock()

	pq.Lock()
	defer pq.Unlock()

	if len(pq.pendingReaders) == 0 {
		return
	}
	if readForWriteTxnId != pq.pendingReaders[0].id {
		return
	}
	x := heap.Pop(pq)
	glog.V(50).Infof("popped %d, len: %d", x.(*reader).id, pq.Len())
	for len(pq.pendingReaders) > 0 {
		newMinReader := pq.pendingReaders[0]
		pq.maxMinReader = pq.maxMinReader.Max(newMinReader.id)
		newMinReader.notify()
		if !newMinReader.timeouted.Get() {
			break
		}
		pq.pendingReaders = nil
		glog.V(50).Infof("%d timeouted, cleared heap", newMinReader.id)
		//x := heap.Pop(pq)
		//glog.V(10).Infof("popped %d, len: %d", x.(*reader).id, pq.Len())
	}
}

type readForWriteQueues struct {
	m concurrency.ConcurrentMap
}

func (wm *readForWriteQueues) Initialize(partitionNum int) {
	wm.m.Initialize(partitionNum)
}

func (wm *readForWriteQueues) pushReaderOnKey(key string, readerTxnId types.TxnId) (*readForWriteCond, error) {
	return wm.m.GetLazy(key, func() interface{} {
		return newPriorityQueue(key)
	}).(*priorityQueue).pushReader(readerTxnId)
}

func (wm *readForWriteQueues) notifyKeyDone(key string, readForWriteTxnId types.TxnId) {
	pq, ok := wm.m.Get(key)
	if !ok {
		return
	}
	pq.(*priorityQueue).notifyKeyDone(readForWriteTxnId)
}

func (wm *readForWriteQueues) Close() {
	wm.m.Clear()
}
