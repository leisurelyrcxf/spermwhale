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

func (i *KeyInfo) TryLock(dbMeta types.DBMeta, txn *transaction.Transaction) (writer *transaction.Writer, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	writerVersion := txn.ID.Version()
	if writerVersion < i.GetMaxReaderVersion() { // TODO no need atomic no need to atomic load here
		return nil, errors.ErrWriteReadConflict
	}

	if i.writers.Size()+1 > i.maxBuffered {
		return nil, errors.ErrTimestampCacheWriteQueueFull // TODO changed to wait.
	}

	if writerVersion <= i.maxRemovedWriterVersion {
		return nil, errors.ErrStaleWriteWriterVersionSmallerThanMaxRemovedWriterVersion
	}

	if prevWriterObj, found := i.writers.Get(writerVersion); found {
		prevWriter := prevWriterObj.(*transaction.Writer)
		if prevWriter.IsWriting() {
			assert.MustNoError(errors.ErrPrevWriterNotFinishedYet) // TODO remove this in product
			return nil, errors.ErrPrevWriterNotFinishedYet
		}
		if dbMeta.InternalVersion <= prevWriter.Meta.InternalVersion {
			assert.MustNoError(errors.ErrInternalVersionSmallerThanPrevWriter) // TODO remove this in product
			return nil, errors.ErrInternalVersionSmallerThanPrevWriter
		}
	}

	w := transaction.NewWriter(dbMeta, txn)
	w.OnUnlocked = func() {
		i.mu.Lock()
		defer i.mu.Unlock()

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
		i.writingWriters.Remove(writerVersion)
	}
	i.writers.Put(writerVersion, w)
	glog.V(TimestampCacheVerboseLevel).Infof("[TimestampCache][KeyInfo '%s'][TryLock] add writer-%d, max reader version: %d", i.key, writerVersion, i.maxReaderVersion)
	i.writingWriters.Put(writerVersion, w)
	w.Lock()
	return w, nil
}

func (i *KeyInfo) findWriters(opt *types.KVCCReadOption) (w *transaction.Writer, writingWritersBefore transaction.WritingWriters, maxReadVersion uint64, err error) {
	assert.Must(opt.IsUpdateTimestampCache() || (opt.IsReadExactVersion() && !opt.IsSnapshotRead()))

	i.mu.RLock()
	defer func() {
		if err != nil && !errors.IsNotExistsErr(err) {
			i.mu.RUnlock()
			return
		}

		if opt.IsUpdateTimestampCache() {
			maxReadVersion = i.updateMaxReaderVersion(opt.ReaderVersion)
		}

		if w == nil || opt.IsReadExactVersion() {
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

	if opt.IsReadExactVersion() {
		exactNode, found := i.writers.Get(opt.ExactVersion)
		if !found {
			if opt.ExactVersion > i.maxRemovedWriterVersion {
				return nil, nil, 0, errors.ErrKeyOrVersionNotExist
			}
			return nil, nil, 0, nil
		}
		assert.Must(!opt.IsSnapshotRead())
		return exactNode.(*transaction.Writer), nil, 0, nil
	}

	maxBelowOrEqualWriterNode, found := i.writers.Floor(opt.ReaderVersion) // max <=
	if !found {                                                            //  all writer id > opt.ReaderVersion or empty
		return nil, nil, 0, nil
	}

	if !opt.IsSnapshotRead() {
		return maxBelowOrEqualWriterNode.Value.(*transaction.Writer), nil, 0, nil
	}

	assert.Must(maxBelowOrEqualWriterNode != nil)
	var node = maxBelowOrEqualWriterNode
	for {
		assert.Must(opt.ReaderVersion >= opt.MinAllowedSnapshotVersion)
		if node.Value.(*transaction.Writer).IsCommitted() {
			glog.V(TimestampCacheVerboseLevel).Infof("[TimestampCache][KeyInfo '%s'][FindWriters] found clean writer-%d, max reader version: %d", i.key, node.Value.(*transaction.Writer).ID.Version(), i.maxReaderVersion)
			return node.Value.(*transaction.Writer), nil, 0, nil
		}
		newReaderVersion := node.Value.(*transaction.Writer).ID.Version() - 1
		if newReaderVersion < opt.MinAllowedSnapshotVersion {
			if !opt.IsWaitWhenReadDirty() {
				return node.Value.(*transaction.Writer), nil, 0, errors.ErrMinAllowedSnapshotVersionViolated
			}
			return node.Value.(*transaction.Writer), nil, 0, nil
		}
		opt.ReaderVersion, node = newReaderVersion, i.prev(node) // hold invariant (node.Version <= opt.ReaderVersion && node is the largest one)
		if node == nil {
			return nil, nil, 0, nil
		}
	}
}

func (i *KeyInfo) RemoveVersion(version uint64) {
	i.mu.Lock()
	i.writers.Remove(version)
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

func (cache *TimestampCache) TryLock(key string, meta types.DBMeta, txn *transaction.Transaction) (writer *transaction.Writer, err error) {
	return cache.getLazy(key).TryLock(meta, txn)
}

func (cache *TimestampCache) FindWriters(key string, opt *types.KVCCReadOption) (w *transaction.Writer, writingWritersBefore transaction.WritingWriters, maxReadVersion uint64, err error) {
	return cache.getLazy(key).findWriters(opt)
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
