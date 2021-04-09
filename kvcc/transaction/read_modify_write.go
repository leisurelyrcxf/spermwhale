package transaction

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/leisurelyrcxf/spermwhale/types/basic"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/algo"
	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

type readModifyWriteCond struct {
	waitress   chan struct{}
	timeouted  basic.AtomicBool
	NotifyTime int64
}

func (cond *readModifyWriteCond) Wait(ctx context.Context, timeout time.Duration) error {
	if cond.NotifyTime > 0 {
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

func (cond *readModifyWriteCond) notify() {
	cond.NotifyTime = time.Now().UnixNano()
	close(cond.waitress)
}

type reader struct {
	ReaderVersion     uint64
	WaitWhenReadDirty bool

	readModifyWriteCond

	addTime uint64
}

func newReader(opt types.KVCCReadOption) *reader {
	return &reader{
		ReaderVersion:     opt.ReaderVersion,
		WaitWhenReadDirty: opt.WaitWhenReadDirty,
		readModifyWriteCond: readModifyWriteCond{
			waitress: make(chan struct{}),
		},
		addTime: uint64(time.Now().UnixNano()),
	}
}

type readers []*reader

// Sort Interface
func (rs readers) Len() int           { return len(rs) }
func (rs readers) Swap(i, j int)      { rs[i], rs[j] = rs[j], rs[i] }
func (rs readers) Less(i, j int) bool { return rs[i].ReaderVersion < rs[j].ReaderVersion }

// Heap interface
func (rs *readers) Push(x interface{}) { *rs = append(*rs, x.(*reader)) }
func (rs *readers) Pop() interface{} {
	old, n := *rs, len(*rs)
	item := old[n-1]
	*rs = old[:n-1]
	return item
}

// Algo interface
func (rs readers) Greater(i, j int) bool     { return rs[i].ReaderVersion > rs[j].ReaderVersion }
func (rs readers) Equal(i, j int) bool       { return rs[i].ReaderVersion == rs[j].ReaderVersion }
func (rs readers) Slice(i, j int) algo.Slice { return rs[i:j] }
func (rs readers) At(i int) interface{}      { return rs[i] }

// Customized functions
func (rs readers) head() *reader { return rs[0] }
func (rs readers) second() *reader {
	switch len(rs) {
	case 1:
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

type readModifyWriteQueue struct {
	sync.RWMutex

	key             string
	maxQueuedAge    time.Duration
	capacity        int
	maxReadersCount int

	readers
	kMaxReaders readers

	maxReaderVersion, lastMaxReaderVersion uint64
	maxHeadVersion                         basic.AtomicUint64
	notified                               int64
}

func newReadModifyWriteQueue(key string, cfg types.ReadModifyWriteQueueCfg) *readModifyWriteQueue {
	return &readModifyWriteQueue{
		key:             key,
		maxQueuedAge:    cfg.MaxQueuedAge,
		capacity:        cfg.CapacityPerKey,
		maxReadersCount: int(float64(cfg.CapacityPerKey) * cfg.MaxReadersRatio),
	}
}

func (pq *readModifyWriteQueue) pushReader(readOpt types.KVCCReadOption) (*readModifyWriteCond, error) {
	readerVersion := readOpt.ReaderVersion
	if maxHeadVersion := pq.maxHeadVersion.Get(); readerVersion <= maxHeadVersion {
		assert.Must(readerVersion < maxHeadVersion)
		return nil, errors.Annotatef(errors.ErrWriteReadConflict,
			"readModifyWriteQueue::pushReaderOnKey: readerVersion(%d) <= pq.maxHeadVersion(%d)", readerVersion, maxHeadVersion)
	}

	pq.Lock()
	defer pq.Unlock()

	var maxHeadVersion = pq.maxHeadVersion.Get()
	if pq.Len() > 0 {
		if success, headChanged := pq.check("check-heap-head-age", 8, func(head *reader) error {
			return utils.CheckOldMan(head.addTime, pq.maxQueuedAge)
		}); success && headChanged {
			pq.notify()
			maxHeadVersion = pq.updateMaxHeadVersion(maxHeadVersion)
		}
	}

	if readerVersion <= maxHeadVersion {
		assert.Must(readerVersion < maxHeadVersion)
		return nil, errors.Annotatef(errors.ErrWriteReadConflict, "readModifyWriteQueue::pushReaderOnKey: readerVersion(%d) <= pq.maxHeadVersion(%d)", readerVersion, maxHeadVersion)
	}

	if pq.Len()+1 > pq.capacity {
		return nil, errors.ErrReadModifyWriteQueueFull
	}

	reader := newReader(readOpt)
	if pq.push(reader); pq.Len() == 1 {
		pq.notify()
		pq.updateMaxHeadVersion(maxHeadVersion)
	}
	return &reader.readModifyWriteCond, nil
}

func (pq *readModifyWriteQueue) notifyKeyEvent(readModifyWriteTxnId types.TxnId, eventType ReadModifyWriteKeyEventType) {
	pq.Lock()
	defer pq.Unlock()

	if pq.Len() == 0 || pq.head().ReaderVersion != readModifyWriteTxnId.Version() {
		return
	}

	if eventType == ReadModifyWriteKeyEventTypeKeyWritten {
		if second := pq.second(); second == nil || !second.WaitWhenReadDirty {
			return
		}
	}

	if pq.pop(); pq.Len() == 0 {
		return
	}

	pq.notify()
	if success, headChanged := pq.check("check-heap-head-cond-timeout", 15, func(head *reader) error {
		if head.timeouted.Get() {
			return errors.ErrReadModifyWriteReaderTimeouted
		}
		return nil
	}); success {
		pq.updateMaxHeadVersion(pq.maxHeadVersion.Get())
		if headChanged {
			pq.notify()
		}
	}
}

func (pq *readModifyWriteQueue) push(r *reader) {
	defer pq.chkInvariant() // TODO remove this

	heap.Push(&pq.readers, r)

	if r.ReaderVersion > pq.maxReaderVersion {
		pq.maxReaderVersion = r.ReaderVersion
	}

	// Update k max readers
	if pq.maxReadersCount == 0 {
		return
	}
	if len(pq.kMaxReaders) < pq.maxReadersCount {
		if r.ReaderVersion > pq.lastMaxReaderVersion {
			heap.Push(&pq.kMaxReaders, r)
		}
		return
	}
	assert.Must(pq.kMaxReaders.Len() == pq.maxReadersCount)
	if r.ReaderVersion <= pq.kMaxReaders.head().ReaderVersion {
		return
	}
	pq.kMaxReaders[0] = r
	heap.Fix(&pq.kMaxReaders, 0)
}

func (pq *readModifyWriteQueue) pop() {
	defer pq.chkInvariant() // TODO remove this

	var r *reader
	if r = heap.Pop(&pq.readers).(*reader); pq.Len() == 0 {
		assert.Must(r.ReaderVersion == pq.maxReaderVersion) // max reader id must be the last popped
		pq.maxReaderVersion = 0
	}
	if pq.kMaxReaders.Len() > 0 && r.ReaderVersion >= pq.kMaxReaders.head().ReaderVersion {
		assert.Must(r.ReaderVersion == pq.kMaxReaders.head().ReaderVersion)
		assert.Must(pq.Len() == pq.kMaxReaders.Len()-1)
		pq.kMaxReaders = nil // otherwise will violate kMaxReaders are top k
		pq.lastMaxReaderVersion = pq.maxReaderVersion
	}
}

func (pq *readModifyWriteQueue) check(desc string, v glog.Level, checker func(head *reader) error) (success bool, headChanged bool) {
	defer pq.chkInvariant() // TODO remove this

	err := checker(pq.head())
	if err == nil {
		return true, false
	}
	if len(pq.kMaxReaders) == 0 || checker(pq.kMaxReaders.head()) != nil {
		pq.readers = nil
		pq.maxReaderVersion = 0
		pq.kMaxReaders = nil
		pq.lastMaxReaderVersion = 0
		glog.V(v).Infof("[%s] both heap failed with '%v', cleared both heap", desc, err)
		return false, true
	}
	pq.readers = pq.kMaxReaders
	pq.kMaxReaders = nil
	pq.lastMaxReaderVersion = pq.maxReaderVersion
	glog.V(v).Infof("[%s] pending readers heap head check failed with error '%v' but max readers heap head is ok, pending readers heap replaced to max readers heap", desc, err)
	return true, true
}

func (pq *readModifyWriteQueue) updateMaxHeadVersion(maxHeadVersion uint64) (newMaxHeadVersion uint64) {
	if head := pq.head(); head.ReaderVersion > maxHeadVersion {
		maxHeadVersion = head.ReaderVersion
		pq.maxHeadVersion.Set(maxHeadVersion)
	}
	return maxHeadVersion
}

func (pq *readModifyWriteQueue) notify() {
	pq.head().notify()
	pq.notified++
	glog.V(60).Infof("notified %d, total count: %d, queued: %d", pq.head().ReaderVersion, pq.notified, pq.Len())
}

func (pq *readModifyWriteQueue) chkInvariant() {
	return // TODO change this
	//assert.Must(len(pq.kMaxReaders) <= pq.maxReadersCount)
	//if len(pq.kMaxReaders) == 0 {
	//	return
	//}
	//kthMax := algo.KthMaxInPlace(append(make(readers, 0, pq.Len()), pq.readers...), len(pq.kMaxReaders)).(*reader)
	//assert.Must(pq.kMaxReaders.head().ReaderVersion == kthMax.ReaderVersion)
}
