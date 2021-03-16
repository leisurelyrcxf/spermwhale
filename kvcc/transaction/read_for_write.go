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

type readers []*reader

// Heap Interface
//
func (rs readers) Len() int {
	return len(rs)
}
func (rs readers) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}
func (rs readers) Less(i, j int) bool {
	return rs[i].id < rs[j].id
}
func (rs *readers) Push(x interface{}) {
	*rs = append(*rs, x.(*reader))
}
func (rs *readers) Pop() interface{} {
	old, n := *rs, len(*rs)
	item := old[n-1]
	*rs = old[:n-1]
	return item
}

func (rs readers) SecondMin() *reader {
	switch len(rs) {
	case 0, 1:
		return nil
	case 2:
		return rs[1]
	default:
		if rs.Less(1, 2) {
			return rs[1]
		}
		return rs[2]
	}
}

type readForWriteQueue struct {
	sync.RWMutex

	key            string
	pendingReaders readers
	maxMinReader   types.AtomicTxnId
}

func newReadForWriteQueue(key string) *readForWriteQueue {
	return &readForWriteQueue{key: key}
}

func (pq *readForWriteQueue) pushReader(readerTxnId types.TxnId) (*readForWriteCond, error) {
	if maxMinReaderId := pq.maxMinReader.Get(); readerTxnId <= maxMinReaderId {
		assert.Must(readerTxnId < maxMinReaderId)
		return nil, errors.Annotatef(errors.ErrWriteReadConflict, "readForWriteQueue::pushReaderOnKey: readerVersion(%d) <= pq.maxMinReader(%d)", readerTxnId, maxMinReaderId)
	}

	pq.Lock()
	defer pq.Unlock()

	maxMinReaderId := pq.maxMinReader.Get()
	if readerTxnId <= maxMinReaderId {
		assert.Must(readerTxnId < maxMinReaderId)
		return nil, errors.Annotatef(errors.ErrWriteReadConflict, "readForWriteQueue::pushReaderOnKey: readerVersion(%d) <= pq.maxMinReader(%d)", readerTxnId, maxMinReaderId)
	}

	if pq.pendingReaders.Len()+1 > consts.MaxReadForWriteQueueCapacityPerKey {
		return nil, errors.ErrReadForWriteQueueFull
	}

	if len(pq.pendingReaders) >= 2 {
		if quasiSecondMinReader := pq.pendingReaders[1]; quasiSecondMinReader.timeouted.Get() {
			pq.pendingReaders = nil
			glog.V(8).Infof("quasi-second-min reader %d timeouted, cleared heap", quasiSecondMinReader.id)
		}
	}
	reader := newReader(readerTxnId)
	heap.Push(&pq.pendingReaders, reader)
	if pq.pendingReaders.Len() == 1 {
		//reader.notify()
		if reader.id > maxMinReaderId {
			pq.maxMinReader.Set(reader.id)
		}
		return nil, nil
	}
	return &reader.readForWriteCond, nil
}

func (pq *readForWriteQueue) notifyKeyDone(readForWriteTxnId types.TxnId) {
	pq.Lock()
	defer pq.Unlock()

	if len(pq.pendingReaders) == 0 || pq.pendingReaders[0].id != readForWriteTxnId {
		return
	}
	x := heap.Pop(&pq.pendingReaders)
	glog.V(50).Infof("popped %d, len: %d", x.(*reader).id, pq.pendingReaders.Len())
	if len(pq.pendingReaders) == 0 {
		return
	}

	minReader := pq.pendingReaders[0]
	if minReader.notify(); !minReader.timeouted.Get() {
		pq.maxMinReader.SetIfBiggerUnsafe(minReader.id)
		return
	}
	pq.pendingReaders = nil
	glog.V(15).Infof("%d timeouted, cleared heap", minReader.id)
}
