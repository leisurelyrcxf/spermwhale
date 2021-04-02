package concurrency

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

func (ctm *ConcurrentTreeMap) Update(key interface{}, modifier func(old interface{}) (new interface{}, modified bool)) (success bool) {
	ctm.mutex.Lock()
	defer ctm.mutex.Unlock()

	old, ok := ctm.tm.Get(key)
	if !ok {
		return false
	}
	if nv, modified := modifier(old); modified {
		ctm.tm.Put(key, nv)
	}
	return true
}

func (ctm *ConcurrentTreeMap) Put(key interface{}, val interface{}) {
	ctm.mutex.Lock()
	defer ctm.mutex.Unlock()

	ctm.tm.Put(key, val)
}

func (ctm *ConcurrentTreeMap) Insert(key interface{}, val interface{}) (success bool) {
	ctm.mutex.Lock()
	defer ctm.mutex.Unlock()

	if _, ok := ctm.tm.Get(key); ok {
		return false
	}
	ctm.tm.Put(key, val)
	return true
}

func (ctm *ConcurrentTreeMap) Remove(key interface{}) {
	ctm.mutex.Lock()
	defer ctm.mutex.Unlock()

	ctm.tm.Remove(key)
}

func (ctm *ConcurrentTreeMap) RemoveIf(key interface{}, pred func(prev interface{}) bool) {
	ctm.mutex.Lock()
	defer ctm.mutex.Unlock()

	old, ok := ctm.tm.Get(key)
	if !ok {
		return // no need to remove
	}

	if pred(old) {
		ctm.tm.Remove(key)
	}
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

func (ctm *ConcurrentTreeMap) Find(f func(key interface{}, value interface{}) bool) (foundKey interface{}, foundValue interface{}) {
	ctm.mutex.RLock()
	defer ctm.mutex.RUnlock()
	return ctm.tm.Find(f)
}

func (ctm *ConcurrentTreeMap) Floor(key interface{}) (foundKey interface{}, foundValue interface{}) {
	ctm.mutex.RLock()
	defer ctm.mutex.RUnlock()
	return ctm.tm.Floor(key)
}

func (ctm *ConcurrentTreeMap) Ceiling(key interface{}) (foundKey interface{}, foundValue interface{}) {
	ctm.mutex.RLock()
	defer ctm.mutex.RUnlock()
	return ctm.tm.Ceiling(key)
}
