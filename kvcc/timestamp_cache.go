package kvcc

import (
	"sync"
	"sync/atomic"

	"github.com/leisurelyrcxf/spermwhale/kvcc/transaction"

	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/golang/glog"
	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
)

const TimestampCacheVerboseLevel = 260

type Writer struct {
	*transaction.Writer
	*KeyInfo
}

func (l Writer) Done() {
	l.Writer.Unlock()
	l.KeyInfo.done(l.ID.Version())
}

type KeyInfo struct {
	mu sync.RWMutex

	key              string
	maxReaderVersion uint64
	//maxWriterId types.TxnId
	maxBuffered, maxBufferedLower int

	writers                 redblacktree.Tree
	maxRemovedWriterVersion uint64
	writingWriters          redblacktree.Tree
}

func NewKeyInfo(key string) *KeyInfo {
	return &KeyInfo{
		key:              key,
		maxBuffered:      consts.DefaultTimestampCacheMaxBufferedWriters,
		maxBufferedLower: consts.GetTimestampCacheMaxBufferedWritersLower(consts.DefaultTimestampCacheMaxBufferedWriters, consts.DefaultTimestampCacheMaxBufferedWritersLowerRatio),
		writers: *redblacktree.NewWith(func(a, b interface{}) int {
			av, bv := a.(uint64), b.(uint64)
			if av >= bv {
				return int(av - bv)
			}
			return -1
		}),
		writingWriters: *redblacktree.NewWith(func(a, b interface{}) int {
			av, bv := a.(uint64), b.(uint64)
			if av >= bv {
				return int(av - bv)
			}
			return -1
		}),
	}
}

func (i *KeyInfo) GetMaxReaderVersion() uint64 {
	return atomic.LoadUint64(&i.maxReaderVersion)
}

func (i *KeyInfo) AddWriter(txn *transaction.Transaction) (writer Writer, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	writerVersion := txn.ID.Version()
	if writerVersion < i.GetMaxReaderVersion() { // TODO no need atomic no need to atomic load here
		return Writer{}, errors.ErrWriteReadConflict
	}

	if i.writers.Size()+1 > i.maxBuffered {
		iterator := i.writers.Iterator()
		var (
			toRemoveKeys []interface{}
			writersSize  = i.writers.Size()
		)
		for iterator.Next() && writersSize > i.maxBufferedLower {
			if k, w := iterator.Key(), iterator.Value().(*transaction.Writer); !w.IsWriting() {
				toRemoveKeys = append(toRemoveKeys, k)
				i.maxRemovedWriterVersion = w.ID.Version()
				writersSize--
			} else {
				break
			}
		}
		for _, k := range toRemoveKeys {
			i.writers.Remove(k)
		}
		if i.writers.Size()+1 > i.maxBuffered {
			return Writer{}, errors.ErrTimestampCacheWriteQueueFull
		}
	}

	if writerVersion <= i.maxRemovedWriterVersion {
		return Writer{}, errors.ErrStaleWriteWriterVersionSmallerThanMaxRemovedWriterVersion
	}

	//if prevWriterObj, found := i.writers.Get(writerVersion); found {
	//	prevWriter := prevWriterObj.(*transaction.Writer)
	//	if prevWriter.IsWriting() {
	//		assert.MustNoError(errors.ErrPrevWriterNotFinishedYet) // TODO remove this in product
	//		return Writer{}, errors.ErrPrevWriterNotFinishedYet
	//	}
	//	if dbMeta.InternalVersion <= prevWriter.Meta.InternalVersion {
	//		assert.MustNoError(errors.ErrInternalVersionSmallerThanPrevWriter) // TODO remove this in product
	//		return Writer{}, errors.ErrInternalVersionSmallerThanPrevWriter
	//	}
	//}

	w := transaction.NewWriter(txn)
	i.writers.Put(writerVersion, w)
	i.writingWriters.Put(writerVersion, w)
	if glog.V(TimestampCacheVerboseLevel) {
		glog.Infof("[TimestampCache][KeyInfo '%s'][addWriterUnsafe] add writer-%d, max reader version: %d", i.key, writerVersion, i.maxReaderVersion)
	}
	w.Lock()
	return Writer{Writer: w, KeyInfo: i}, nil
}

func (i *KeyInfo) findWriters(opt *types.KVCCReadOption, atomicMaxReadVersion *uint64, minSnapshotVersionViolated *bool) (_ *KeyInfo, w *transaction.Writer, writingWritersBefore transaction.WritingWriters, err error) {
	assert.Must(opt.UpdateTimestampCache || (opt.ReadExactVersion && !opt.IsSnapshotRead))

	i.mu.RLock()
	defer func() {
		if err != nil {
			assert.Must(!opt.GetMaxReadVersion)
			i.mu.RUnlock()
			return
		}

		if opt.UpdateTimestampCache {
			*atomicMaxReadVersion = i.updateMaxReaderVersion(opt.ReaderVersion)
		} else {
			*atomicMaxReadVersion = i.GetMaxReaderVersion()
		}

		if w == nil || opt.ReadExactVersion {
			i.mu.RUnlock()
			return
		}

		if curNode, found := i.writingWriters.Floor(w.ID.Version() - 1); found {
			for j := 0; j < consts.DefaultTimestampCacheMaxSeekedWritingWriters && curNode != nil; j, curNode = j+1, i.prev(curNode) {
				writingWritersBefore = append(writingWritersBefore, curNode.Value.(*transaction.Writer))
			}
			if curNode != nil {
				writingWritersBefore = append(writingWritersBefore, transaction.HasMoreWritingWriters)
			}
		}
		i.mu.RUnlock()
	}()

	if opt.ReadExactVersion {
		exactNode, found := i.writers.Get(opt.ExactVersion)
		w, _ := exactNode.(*transaction.Writer)
		assert.Must(!found || w != nil)
		return i, w, nil, nil
	}

	maxBelowOrEqualWriterNode, found := i.writers.Floor(opt.ReaderVersion) // max <=
	if !found {                                                            //  all writer id > opt.ReaderVersion or empty
		return i, nil, nil, nil
	}

	if !opt.IsSnapshotRead {
		return i, maxBelowOrEqualWriterNode.Value.(*transaction.Writer), nil, nil
	}

	assert.Must(maxBelowOrEqualWriterNode != nil)
	var node = maxBelowOrEqualWriterNode
	for {
		assert.Must(opt.ReaderVersion >= opt.MinAllowedSnapshotVersion)
		w := node.Value.(*transaction.Writer)
		if w.IsCommitted() {
			glog.V(TimestampCacheVerboseLevel).Infof("[TimestampCache][KeyInfo '%s'][FindWriters] found clean writer-%d, max reader version: %d", i.key, node.Value.(*transaction.Writer).ID.Version(), i.maxReaderVersion)
			return i, w, nil, nil
		}
		newReaderVersion := w.ID.Version() - 1
		if newReaderVersion < opt.MinAllowedSnapshotVersion {
			*minSnapshotVersionViolated = true

			if !opt.WaitWhenReadDirty {
				return i, w, nil, errors.ErrMinAllowedSnapshotVersionViolated
			}
			return i, w, nil, nil
		}
		opt.ReaderVersion, node = newReaderVersion, i.prev(node) // hold invariant (node.Version <= opt.ReaderVersion && node is the largest one)
		if node == nil {
			return i, nil, nil, nil
		}
	}
}

func (i *KeyInfo) RemoveVersion(version uint64) {
	i.mu.Lock()
	i.writers.Remove(version)
	i.mu.Unlock()
}

// done marks writer done
func (i *KeyInfo) done(writerVersion uint64) {
	i.mu.Lock()
	i.writingWriters.Remove(writerVersion)
	i.mu.Unlock()
}

func (i *KeyInfo) prev(node *redblacktree.Node) *redblacktree.Node {
	if node.Left != nil {
		node = node.Left
		for node.Right != nil {
			node = node.Right
		}
		return node
	}
	for node.Parent != nil && node != node.Parent.Right {
		node = node.Parent
	}
	return node.Parent
}

func (i *KeyInfo) next(node *redblacktree.Node) *redblacktree.Node {
	if node.Right != nil {
		node = node.Right
		for node.Left != nil {
			node = node.Left
		}
		return node
	}
	for node.Parent != nil && node != node.Parent.Left {
		node = node.Parent
	}
	return node.Parent
}

func (i *KeyInfo) updateMaxReaderVersion(readerVersion uint64) (currentMaxReaderVersion uint64) {
	for currentMaxReaderVersion = i.GetMaxReaderVersion(); readerVersion > currentMaxReaderVersion; currentMaxReaderVersion = i.GetMaxReaderVersion() {
		if atomic.CompareAndSwapUint64(&i.maxReaderVersion, currentMaxReaderVersion, readerVersion) {
			return readerVersion
		}
	}
	return currentMaxReaderVersion
}

type TimestampCache struct {
	m concurrency.ConcurrentMap
}

func NewTimestampCache() *TimestampCache {
	tc := &TimestampCache{}
	tc.m.Initialize(64)
	return tc
}

func (cache *TimestampCache) GetMaxReaderVersion(key string) uint64 {
	keyInfo, ok := cache.get(key)
	if !ok {
		return 0
	}
	return keyInfo.GetMaxReaderVersion()
}

func (cache *TimestampCache) AddWriter(key string, txn *transaction.Transaction) (writer Writer, err error) {
	return cache.getLazy(key).AddWriter(txn)
}

func (cache *TimestampCache) FindWriters(key string, opt *types.KVCCReadOption, atomicMaxReadVersion *uint64, minSnapshotVersionViolated *bool) (i *KeyInfo, w *transaction.Writer, writingWritersBefore transaction.WritingWriters, err error) {
	return cache.getLazy(key).findWriters(opt, atomicMaxReadVersion, minSnapshotVersionViolated)
}

func (cache *TimestampCache) RemoveVersion(key string, writerVersion uint64) {
	cache.getLazy(key).RemoveVersion(writerVersion)
}

func (cache *TimestampCache) getLazy(key string) *KeyInfo {
	return cache.m.GetLazy(key, func() interface{} {
		return NewKeyInfo(key)
	}).(*KeyInfo)
}

func (cache *TimestampCache) get(key string) (*KeyInfo, bool) {
	obj, ok := cache.m.Get(key)
	if !ok {
		return nil, false
	}
	return obj.(*KeyInfo), true
}
