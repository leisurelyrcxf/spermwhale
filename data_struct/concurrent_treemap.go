package data_struct

import (
	"sync"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
)

type ConcurrentTreeMap struct {
	mutex sync.RWMutex
	tm    *treemap.Map
}

func NewConcurrentTreeMap(comparator utils.Comparator) *ConcurrentTreeMap {
	return &ConcurrentTreeMap{
		tm: treemap.NewWith(comparator),
	}
}

func (ctm *ConcurrentTreeMap) Get(key interface{}) (interface{}, bool) {
	ctm.mutex.Lock()
	defer ctm.mutex.Unlock()
	return ctm.tm.Get(key)
}

func (ctm *ConcurrentTreeMap) Put(key interface{}, val interface{}) {
	ctm.mutex.Lock()
	ctm.tm.Put(key, val)
	ctm.mutex.Unlock()
}

func (ctm *ConcurrentTreeMap) Remove(key interface{}) {
	ctm.mutex.Lock()
	ctm.tm.Remove(key)
	ctm.mutex.Unlock()
}

func (ctm *ConcurrentTreeMap) Clear() {
	ctm.mutex.Lock()
	ctm.tm.Clear()
	ctm.mutex.Unlock()
}

func (ctm *ConcurrentTreeMap) Max() (key interface{}, value interface{}) {
	ctm.mutex.RLock()
	defer ctm.mutex.RUnlock()
	return ctm.tm.Max()
}

func (ctm *ConcurrentTreeMap) Min() (key interface{}, value interface{}) {
	ctm.mutex.RLock()
	defer ctm.mutex.RUnlock()
	return ctm.tm.Min()
}

func (ctm *ConcurrentTreeMap) MaxIf(pred func(key interface{}) bool) (key interface{}, value interface{}) {
	ctm.mutex.RLock()
	defer ctm.mutex.RUnlock()
	for {
		key, value = ctm.tm.Max()
		if key == nil {
			return key, value
		}
		if pred(key) {
			return key, value
		}
		ctm.tm.Remove(key)
	}
}

func (ctm *ConcurrentTreeMap) Find(f func(key interface{}, value interface{}) bool) (interface{}, interface{}) {
	ctm.mutex.RLock()
	defer ctm.mutex.RUnlock()
	return ctm.tm.Find(f)
}
