package transaction

import (
	"container/heap"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/algo"
	"github.com/leisurelyrcxf/spermwhale/assert"
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
func (rs readers) Len() int           { return len(rs) }
func (rs readers) Swap(i, j int)      { rs[i], rs[j] = rs[j], rs[i] }
func (rs readers) Less(i, j int) bool { return rs[i].id < rs[j].id }

// Heap interface
func (rs *readers) Push(x interface{}) { *rs = append(*rs, x.(*reader)) }
func (rs *readers) Pop() interface{} {
	old, n := *rs, len(*rs)
	item := old[n-1]
	*rs = old[:n-1]
	return item
}

// Algo interface
func (rs readers) Greater(i, j int) bool     { return rs[i].id > rs[j].id }
func (rs readers) Equal(i, j int) bool       { return rs[i].id == rs[j].id }
func (rs readers) Slice(i, j int) algo.Slice { return rs[i:j] }
func (rs readers) At(i int) interface{}      { return rs[i] }

// Customized functions
func (rs readers) head() *reader { return rs[0] }
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

	key             string
	maxQueuedAge    time.Duration
	capacity        int
	maxReadersCount int

	readers
	kMaxReaders readers

	maxReaderId, lastMaxReaderId types.TxnId
	maxHeadId                    types.AtomicTxnId
	notified                     int64
}

func newReadForWriteQueue(key string, capacity int, maxQueuedAge time.Duration, maxReadersRatio float64) *readForWriteQueue {
	return &readForWriteQueue{
		key:             key,
		maxQueuedAge:    maxQueuedAge,
		capacity:        capacity,
		maxReadersCount: int(float64(capacity) * maxReadersRatio),
	}
}

func (pq *readForWriteQueue) pushReader(readerTxnId types.TxnId) (*readForWriteCond, error) {
	if maxHeadId := pq.maxHeadId.Get(); readerTxnId <= maxHeadId {
		assert.Must(readerTxnId < maxHeadId)
		return nil, errors.Annotatef(errors.ErrWriteReadConflict, "readForWriteQueue::pushReaderOnKey: readerVersion(%d) <= pq.maxHeadId(%d)", readerTxnId, maxHeadId)
	}

	pq.Lock()
	defer pq.Unlock()

	maxHeadId := pq.maxHeadId.Get()
	if pq.Len() > 0 {
		if success, headChanged := pq.check("check-heap-head-age", 3, func(head *reader) error {
			return utils.CheckOldMan(head.addTime, pq.maxQueuedAge)
		}); success && headChanged {
			pq.notify()
			maxHeadId = pq.updateMaxHeadId(maxHeadId)
		}
	}

	if readerTxnId <= maxHeadId {
		assert.Must(readerTxnId < maxHeadId)
		return nil, errors.Annotatef(errors.ErrWriteReadConflict, "readForWriteQueue::pushReaderOnKey: readerVersion(%d) <= pq.maxHeadId(%d)", readerTxnId, maxHeadId)
	}

	if pq.Len()+1 > pq.capacity {
		return nil, errors.ErrReadForWriteQueueFull
	}

	reader := newReader(readerTxnId)
	if pq.push(reader); pq.Len() == 1 {
		pq.notify()
		pq.updateMaxHeadId(maxHeadId)
	}
	return &reader.readForWriteCond, nil
}

func (pq *readForWriteQueue) notifyKeyDone(readForWriteTxnId types.TxnId) {
	pq.Lock()
	defer pq.Unlock()

	if pq.Len() == 0 || pq.head().id != readForWriteTxnId {
		return
	}

	if pq.pop(); pq.Len() == 0 {
		return
	}

	pq.notify()
	if success, headChanged := pq.check("check-heap-head-cond-timeout", 15, func(head *reader) error {
		if head.timeouted.Get() {
			return errors.ErrReadForWriteReaderTimeouted
		}
		return nil
	}); success {
		pq.updateMaxHeadId(pq.maxHeadId.Get())
		if headChanged {
			pq.notify()
		}
	}
}

func (pq *readForWriteQueue) push(r *reader) {
	defer pq.verifyInvariant() // TODO remove this

	heap.Push(&pq.readers, r)

	if r.id > pq.maxReaderId {
		pq.maxReaderId = r.id
	}

	{
		// Update k max readers
		if len(pq.kMaxReaders) < pq.maxReadersCount {
			if r.id > pq.lastMaxReaderId {
				heap.Push(&pq.kMaxReaders, r)
			}
			return
		}
		if pq.kMaxReaders.head().id >= r.id {
			return
		}
		pq.kMaxReaders[0] = r
		heap.Fix(&pq.kMaxReaders, 0)
	}
}

func (pq *readForWriteQueue) pop() {
	defer pq.verifyInvariant() // TODO remove this

	var r *reader
	if r = heap.Pop(&pq.readers).(*reader); pq.Len() == 0 {
		assert.Must(r.id == pq.maxReaderId) // max reader id must be the last popped
		pq.maxReaderId = 0
	}
	if len(pq.kMaxReaders) > 0 && r.id >= pq.kMaxReaders.head().id {
		assert.Must(pq.Len() <= len(pq.kMaxReaders))
		pq.kMaxReaders = nil // otherwise will violate kMaxReaders are top k
		pq.lastMaxReaderId = pq.maxReaderId
	}
}

func (pq *readForWriteQueue) check(desc string, v glog.Level, checker func(head *reader) error) (success bool, headChanged bool) {
	defer pq.verifyInvariant() // TODO remove this

	err := checker(pq.head())
	if err == nil {
		return true, false
	}
	if len(pq.kMaxReaders) == 0 || checker(pq.kMaxReaders.head()) != nil {
		pq.readers = nil
		pq.maxReaderId = 0
		pq.kMaxReaders = nil
		pq.lastMaxReaderId = 0
		glog.V(v).Infof("[%s] both heap failed with '%v', cleared both heap", desc, err)
		return false, true
	}
	pq.readers = pq.kMaxReaders
	pq.kMaxReaders = nil
	pq.lastMaxReaderId = pq.maxReaderId
	glog.V(v).Infof("[%s] pending readers heap head check failed with error '%v' but max readers heap head is ok, pending readers heap replaced to max readers heap", desc, err)
	return true, true
}

func (pq *readForWriteQueue) updateMaxHeadId(maxHeadId types.TxnId) (newMaxHead types.TxnId) {
	if head := pq.head(); head.id > maxHeadId {
		maxHeadId = head.id
		pq.maxHeadId.Set(maxHeadId)
	}
	return maxHeadId
}

func (pq *readForWriteQueue) notify() {
	pq.head().notify()
	pq.notified++
	glog.V(50).Infof("notified %d, total count: %d, queued: %d", pq.head().id, pq.notified, pq.Len())
}

func (pq *readForWriteQueue) verifyInvariant() {
	//return // TODO change this
	assert.Must(len(pq.kMaxReaders) <= pq.maxReadersCount)
	if len(pq.kMaxReaders) == 0 {
		return
	}
	kthMax := algo.KthMaxInPlace(append(make(readers, 0, pq.Len()), pq.readers...), len(pq.kMaxReaders)).(*reader)
	assert.Must(pq.kMaxReaders.head().id == kthMax.id)
}
