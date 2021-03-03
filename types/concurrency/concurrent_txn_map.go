package concurrency

import (
	"sync"

	"github.com/leisurelyrcxf/spermwhale/types"
)

type concurrentTxnMapPartition struct {
	mutex sync.RWMutex
	m     map[types.TxnId]interface{}
}

func (cmp *concurrentTxnMapPartition) get(key types.TxnId) (interface{}, bool) {
	cmp.mutex.RLock()
	defer cmp.mutex.RUnlock()
	val, ok := cmp.m[key]
	return val, ok
}

func (cmp *concurrentTxnMapPartition) getLazy(key types.TxnId, constructor func() interface{}) interface{} {
	cmp.mutex.RLock()
	val, ok := cmp.m[key]
	if ok {
		cmp.mutex.RUnlock()
		return val
	}
	cmp.mutex.RUnlock()

	cmp.mutex.Lock()
	defer cmp.mutex.Unlock()

	val, ok = cmp.m[key]
	if ok {
		return val
	}

	val = constructor()
	cmp.m[key] = val

	return val
}

func (cmp *concurrentTxnMapPartition) set(key types.TxnId, val interface{}) {
	cmp.mutex.Lock()
	defer cmp.mutex.Unlock()
	cmp.m[key] = val
}

func (cmp *concurrentTxnMapPartition) setIf(key types.TxnId, val interface{}, pred func(prev interface{}, exist bool) bool) (bool, interface{}) {
	cmp.mutex.Lock()
	defer cmp.mutex.Unlock()
	prev, ok := cmp.m[key]
	if pred(prev, ok) {
		cmp.m[key] = val
		return true, prev
	}
	return false, prev
}

func (cmp *concurrentTxnMapPartition) del(key types.TxnId) {
	cmp.mutex.Lock()
	defer cmp.mutex.Unlock()
	delete(cmp.m, key)
}

func (cmp *concurrentTxnMapPartition) forEachLocked(cb func(types.TxnId, interface{})) {
	for key, val := range cmp.m {
		cb(key, val)
	}
}

type ConcurrentTxnMap struct {
	partitions []*concurrentTxnMapPartition
}

func (cmp *ConcurrentTxnMap) Initialize(partitionNum int) {
	cmp.partitions = make([]*concurrentTxnMapPartition, partitionNum)
	for i := range cmp.partitions {
		cmp.partitions[i] = &concurrentTxnMapPartition{m: make(map[types.TxnId]interface{})}
	}
}

func (cmp *ConcurrentTxnMap) hash(s types.TxnId) uint64 {
	return uint64(s) % uint64(len(cmp.partitions))
}

func (cmp *ConcurrentTxnMap) RLock() {
	for i := 0; i < len(cmp.partitions); i++ {
		cmp.partitions[i].mutex.RLock()
	}
}

func (cmp *ConcurrentTxnMap) RUnlock() {
	for i := len(cmp.partitions) - 1; i >= 0; i-- {
		cmp.partitions[i].mutex.RUnlock()
	}
}

func (cmp *ConcurrentTxnMap) Lock() {
	for i := 0; i < len(cmp.partitions); i++ {
		cmp.partitions[i].mutex.Lock()
	}
}

func (cmp *ConcurrentTxnMap) Unlock() {
	for i := len(cmp.partitions) - 1; i >= 0; i-- {
		cmp.partitions[i].mutex.Unlock()
	}
}

func (cmp *ConcurrentTxnMap) MustGet(key types.TxnId) interface{} {
	val, ok := cmp.Get(key)
	if !ok {
		panic("key not exists")
	}
	return val
}

func (cmp *ConcurrentTxnMap) Get(key types.TxnId) (interface{}, bool) {
	return cmp.partitions[cmp.hash(key)].get(key)
}

func (cmp *ConcurrentTxnMap) GetLazy(key types.TxnId, constructor func() interface{}) interface{} {
	return cmp.partitions[cmp.hash(key)].getLazy(key, constructor)
}

func (cmp *ConcurrentTxnMap) Set(key types.TxnId, val interface{}) {
	cmp.partitions[cmp.hash(key)].set(key, val)
}

func (cmp *ConcurrentTxnMap) SetIf(key types.TxnId, val interface{}, pred func(prev interface{}, exist bool) bool) (bool, interface{}) {
	return cmp.partitions[cmp.hash(key)].setIf(key, val, pred)
}

func (cmp *ConcurrentTxnMap) Del(key types.TxnId) {
	cmp.partitions[cmp.hash(key)].del(key)
}

func (cmp *ConcurrentTxnMap) ForEachLoosed(cb func(types.TxnId, interface{})) {
	for _, partition := range cmp.partitions {
		partition.mutex.RLock()
		partition.forEachLocked(cb)
		partition.mutex.RUnlock()
	}
}

func (cmp *ConcurrentTxnMap) ForEachStrict(cb func(types.TxnId, interface{})) {
	cmp.RLock()
	for _, partition := range cmp.partitions {
		partition.forEachLocked(cb)
	}
	cmp.RUnlock()
}

func (cmp *ConcurrentTxnMap) Clear() {
	cmp.Lock()
	for _, partition := range cmp.partitions {
		partition.m = nil
	}
	cmp.Unlock()
}

func (cmp *ConcurrentTxnMap) Size() (sz int) {
	cmp.RLock()
	for _, partition := range cmp.partitions {
		sz += len(partition.m)
	}
	cmp.RUnlock()
	return
}