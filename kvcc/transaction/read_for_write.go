package transaction

import (
	"container/heap"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

type readForWriteCond struct {
	waitress   chan struct{}
	timeouted  concurrency.AtomicBool
	notifyTime int64
}

func (cond *readForWriteCond) Wait(ctx context.Context, timeout time.Duration) error {
	if cond.notifyTime > 0 {
		return nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case <-waitCtx.Done():
		cond.timeouted.Set(true)
		return waitCtx.Err()
	case <-cond.waitress:
		return nil
	}
}

func (cond *readForWriteCond) notify() {
	cond.notifyTime = time.Now().UnixNano()
	close(cond.waitress)
}

type reader struct {
	id types.TxnId
	readForWriteCond

	addTime uint64
}

func newReader(id types.TxnId) *reader {
	return &reader{
		id: id,
		readForWriteCond: readForWriteCond{
			waitress: make(chan struct{}),
		},
		addTime: uint64(time.Now().UnixNano()),
	}
}

type readers []*reader

// Sort Interface
func (rs readers) Len() int {
	return len(rs)
}
func (rs readers) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}
func (rs readers) Less(i, j int) bool {
	return rs[i].id < rs[j].id
}

// Heap interface
func (rs *readers) Push(x interface{}) {
	*rs = append(*rs, x.(*reader))
}
func (rs *readers) Pop() interface{} {
	old, n := *rs, len(*rs)
	item := old[n-1]
	*rs = old[:n-1]
	return item
}

// Customized functions
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
func (rs readers) PrintString() string {
	strs := make([]string, len(rs))
	for i := range rs {
		strs[i] = fmt.Sprintf("%d", rs[i].id)
	}
	return strings.Join(strs, ",")
}

type readForWriteQueue struct {
	sync.RWMutex

	key          string
	maxQueuedAge time.Duration

	pendingReaders readers
	maxMinReader   types.AtomicTxnId
}

func newReadForWriteQueue(key string, maxQueuedAge time.Duration) *readForWriteQueue {
	return &readForWriteQueue{
		key:          key,
		maxQueuedAge: maxQueuedAge,
	}
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

	if len(pq.pendingReaders) > 0 {
		if err := utils.CheckOldMan(pq.pendingReaders[0].addTime, pq.maxQueuedAge); err != nil {
			pq.pendingReaders = nil
			glog.V(1).Infof("min reader too stale (%v), cleared heap", err)
		}
	}

	reader := newReader(readerTxnId)
	heap.Push(&pq.pendingReaders, reader)
	if pq.pendingReaders.Len() == 1 {
		reader.notify()
		if reader.id > maxMinReaderId {
			pq.maxMinReader.Set(reader.id)
		}
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
