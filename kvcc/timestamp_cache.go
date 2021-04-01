package kvcc

import (
	"sync"
	"sync/atomic"

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
		maxBufferedLower: int(float64(consts.DefaultTimestampCacheMaxBufferedWriters) * consts.DefaultTimestampCacheMaxBufferedWritersLowerRatio),
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

func (i *KeyInfo) TryLock(writerVersion uint64) (writer *types.Writer, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if writerVersion < i.GetMaxReaderVersion() { // TODO no need atomic no need to atomic load here
		return nil, errors.ErrWriteReadConflict
	}

	if i.writers.Size()+1 > i.maxBuffered {
		return nil, errors.ErrTimestampCacheWriteQueueFull // TODO changed to wait.
	}

	if writerVersion <= i.maxRemovedWriterVersion {
		return nil, errors.Annotatef(errors.ErrStaleWrite, "writerVersion <= i.maxRemovedWriterVersion")
	}

	w := types.NewWriter(writerVersion)
	w.OnUnlocked = func() {
		i.mu.Lock()
		defer i.mu.Unlock()

		iterator := i.writers.Iterator()
		var toRemoveKeys []interface{}
		for iterator.Next() && i.writers.Size() > i.maxBufferedLower {
			if k, w := iterator.Key(), iterator.Value().(*types.Writer); !w.IsWriting() {
				toRemoveKeys = append(toRemoveKeys, k)
				i.maxRemovedWriterVersion = w.Version
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

func (i *KeyInfo) FindWriter(opt *types.KVCCReadOption) (w *types.Writer, maxReadVersion uint64, err error) {
	i.mu.RLock()
	defer func() {
		assert.Must(!opt.IsNotUpdateTimestampCache() || (opt.IsGetExactVersion() && !opt.IsSnapshotRead()))
		if !opt.IsNotUpdateTimestampCache() && err == nil {
			maxReadVersion = i.updateMaxReaderVersion(opt.ReaderVersion)
		} else {
			maxReadVersion = i.GetMaxReaderVersion()
		}
		if w != nil {
			if maxBelowNode, found := i.writingWriters.Floor(w.Version - 1); found {
				w.Next = maxBelowNode.Value.(*types.Writer)
			}
		}
		i.mu.RUnlock()
	}()

	if opt.IsGetExactVersion() {
		exactNode, found := i.writers.Get(opt.ExactVersion) // max <=
		if !found {                                         //  all writer id > opt.ReaderVersion or empty
			return nil, 0, nil
		}
		assert.Must(!opt.IsSnapshotRead())
		return exactNode.(*types.Writer), 0, nil
	}

	maxBelowOrEqualWriterNode, found := i.writers.Floor(opt.ReaderVersion) // max <=
	if !found {                                                            //  all writer id > opt.ReaderVersion or empty
		return nil, 0, nil
	}

	if !opt.IsSnapshotRead() {
		return maxBelowOrEqualWriterNode.Value.(*types.Writer), 0, nil
	}

	assert.Must(maxBelowOrEqualWriterNode != nil)
	var node = maxBelowOrEqualWriterNode
	for {
		assert.Must(opt.ReaderVersion >= opt.MinAllowedSnapshotVersion)
		if node.Value.(*types.Writer).IsClean() {
			glog.V(TimestampCacheVerboseLevel).Infof("[TimestampCache][KeyInfo '%s'][FindWriter] found clean writer-%d, max reader version: %d", i.key, node.Value.(*types.Writer).Version, i.maxReaderVersion)
			return node.Value.(*types.Writer), 0, nil
		}
		if node.Value.(*types.Writer).Version-1 < opt.MinAllowedSnapshotVersion {
			if !opt.IsWaitWhenReadDirty() {
				return node.Value.(*types.Writer), 0, errors.ErrMinAllowedSnapshotVersionViolated
			}
			return node.Value.(*types.Writer), 0, nil
		}
		opt.ReaderVersion = node.Value.(*types.Writer).Version - 1
		if node = i.prev(node); node == nil {
			return nil, 0, nil
		}
		// hold invariant (node.Version <= opt.ReaderVersion && node is the largest one)
	}
}

func (i *KeyInfo) MarkCommitted(writerVersion uint64) {
	i.mu.RLock()
	val, ok := i.writers.Get(writerVersion)
	if ok {
		val.(*types.Writer).MarkClean()
	}
	i.mu.RUnlock()
}

func (i *KeyInfo) MarkRollbacked(writerVersion uint64) {
	i.mu.RLock()
	val, ok := i.writers.Get(writerVersion)
	if ok {
		val.(*types.Writer).MarkRollbacked()
	}
	i.mu.RUnlock()
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
	t concurrency.ConcurrentTxnMap
}

func NewTimestampCache() *TimestampCache {
	tc := &TimestampCache{}
	tc.m.Initialize(64)
	tc.t.Initialize(64)
	return tc
}

func (cache *TimestampCache) GetMaxReaderVersion(key string) uint64 {
	keyInfo, ok := cache.get(key)
	if !ok {
		return 0
	}
	return keyInfo.GetMaxReaderVersion()
}

func (cache *TimestampCache) TryLock(key string, writerVersion uint64) (writer *types.Writer, err error) {
	return cache.getLazy(key).TryLock(writerVersion)
}

func (cache *TimestampCache) FindWriter(key string, opt *types.KVCCReadOption) (w *types.Writer, maxReadVersion uint64, err error) {
	return cache.getLazy(key).FindWriter(opt)
}

func (cache *TimestampCache) MarkCommitted(key string, writerVersion uint64) {
	cache.getLazy(key).MarkCommitted(writerVersion)
}

func (cache *TimestampCache) MarkRollbacked(key string, writerVersion uint64) {
	cache.getLazy(key).MarkRollbacked(writerVersion)
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
