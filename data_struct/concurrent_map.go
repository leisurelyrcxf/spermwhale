package data_struct

import (
	"hash/crc32"
	"sync"
)

type concurrentMapPartition struct {
	mutex sync.RWMutex
	m     map[string]interface{}
}

func (cmp *concurrentMapPartition) get(key string) (interface{}, bool) {
	cmp.mutex.RLock()
	defer cmp.mutex.RUnlock()
	val, ok := cmp.m[key]
	return val, ok
}

func (cmp *concurrentMapPartition) getLazy(key string, constructor func() interface{}) interface{} {
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

func (cmp *concurrentMapPartition) set(key string, val interface{}) {
	cmp.mutex.Lock()
	defer cmp.mutex.Unlock()
	cmp.m[key] = val
}

func (cmp *concurrentMapPartition) setIf(key string, val interface{}, pred func(prev interface{}, exist bool) bool) (bool, interface{}) {
	cmp.mutex.Lock()
	defer cmp.mutex.Unlock()
	prev, ok := cmp.m[key]
	if pred(prev, ok) {
		cmp.m[key] = val
		return true, nil
	}
	return false, prev
}

func (cmp *concurrentMapPartition) del(key string) {
	cmp.mutex.Lock()
	defer cmp.mutex.Unlock()
	delete(cmp.m, key)
}

func (cmp *concurrentMapPartition) forEachLocked(cb func(string, interface{})) {
	for key, val := range cmp.m {
		cb(key, val)
	}
}

type ConcurrentMap struct {
	partitions []*concurrentMapPartition
}

func NewConcurrentMap(partitionNum int) ConcurrentMap {
	cm := ConcurrentMap{partitions: make([]*concurrentMapPartition, partitionNum)}
	for i := 0; i < partitionNum; i++ {
		cm.partitions[i] = &concurrentMapPartition{m:make(map[string]interface{})}
	}
	return cm
}

func (cmp *ConcurrentMap) hash(s string) int {
	return int(crc32.ChecksumIEEE([]byte(s))) % len(cmp.partitions)
}

func (cmp *ConcurrentMap) RLock() {
	for i := 0; i < len(cmp.partitions); i++ {
		cmp.partitions[i].mutex.RLock()
	}
}

func (cmp *ConcurrentMap) RUnlock() {
	for i := len(cmp.partitions) - 1; i >= 0; i-- {
		cmp.partitions[i].mutex.RUnlock()
	}
}

func (cmp *ConcurrentMap) MustGet(key string) interface{} {
	val, ok := cmp.Get(key)
	if !ok {
		panic("key not exists")
	}
	return val
}

func (cmp *ConcurrentMap) Get(key string) (interface{}, bool) {
	return cmp.partitions[cmp.hash(key)].get(key)
}

func (cmp *ConcurrentMap) GetLazy(key string, constructor func() interface{}) interface{} {
	return cmp.partitions[cmp.hash(key)].getLazy(key, constructor)
}

func (cmp *ConcurrentMap) Set(key string, val interface{}) {
	cmp.partitions[cmp.hash(key)].set(key, val)
}

func (cmp *ConcurrentMap) SetIf(key string, val interface{}, pred func(prev interface{}, exist bool) bool) (bool, interface{}) {
	return cmp.partitions[cmp.hash(key)].setIf(key, val, pred)
}

func (cmp *ConcurrentMap) Del(key string) {
	cmp.partitions[cmp.hash(key)].del(key)
}

func (cmp *ConcurrentMap) ForEachLoosed(cb func(string, interface{})) {
	for _, partition := range cmp.partitions {
		partition.mutex.RLock()
		partition.forEachLocked(cb)
		partition.mutex.RUnlock()
	}
}

func (cmp *ConcurrentMap) ForEachStrict(cb func(string, interface{})) {
	cmp.RLock()
	for _, partition := range cmp.partitions {
		partition.forEachLocked(cb)
	}
	cmp.RUnlock()
}

func (cmp *ConcurrentMap) Size() (sz int) {
	cmp.RLock()
	for _, partition := range cmp.partitions {
		sz += len(partition.m)
	}
	cmp.RUnlock()
	return
}
