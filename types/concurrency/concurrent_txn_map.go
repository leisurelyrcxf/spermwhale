package concurrency

import (
	"sync"
	"time"

	"github.com/leisurelyrcxf/spermwhale/types/basic"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/timer"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

type concurrentTxnMapPartition struct {
	mutex sync.RWMutex
	m     map[types.TxnId]interface{}

	gcThread *timer.AggrTimer
}

func (cmp *concurrentTxnMapPartition) startGCThread(channelSize int, minInterrupt time.Duration, quit chan struct{}, wg *sync.WaitGroup, currentTransactionCount *basic.AtomicInt64) {
	cmp.gcThread = timer.NewAggrTimer(channelSize, 64, minInterrupt, func(objs []interface{}) {
		cmp.mutex.Lock()
		for _, obj := range objs {
			delete(cmp.m, obj.(types.TxnId))
		}
		cmp.mutex.Unlock()
		currentTransactionCount.Add(-int64(len(objs)))
	}, quit, wg)
	cmp.gcThread.Start()
}

func (cmp *concurrentTxnMapPartition) removeWhen(txn types.TxnId, scheduleTime time.Time) {
	cmp.gcThread.Schedule(timer.NewAggrTimerTask(scheduleTime, txn))
}

func (cmp *concurrentTxnMapPartition) contains(key types.TxnId) bool {
	cmp.mutex.RLock()
	defer cmp.mutex.RUnlock()

	_, ok := cmp.m[key]
	return ok
}

func (cmp *concurrentTxnMapPartition) get(key types.TxnId) (interface{}, bool) {
	cmp.mutex.RLock()
	defer cmp.mutex.RUnlock()
	val, ok := cmp.m[key]
	return val, ok
}

func (cmp *concurrentTxnMapPartition) getLazy(key types.TxnId, constructor func() interface{}) interface{} {
	_, val := cmp.insertIfNotExists(key, constructor)
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
		return true, val
	}
	return false, prev
}

func (cmp *concurrentTxnMapPartition) insertIfNotExists(key types.TxnId, constructor func() interface{}) (inserted bool, newVal interface{}) {
	cmp.mutex.RLock()
	old, ok := cmp.m[key]
	if ok {
		cmp.mutex.RUnlock()
		return false, old
	}
	cmp.mutex.RUnlock()

	cmp.mutex.Lock()
	defer cmp.mutex.Unlock()

	old, ok = cmp.m[key]
	if ok {
		return false, old
	}

	val := constructor()
	if val == nil {
		return false, nil
	}
	cmp.m[key] = val
	return true, val
}

func (cmp *concurrentTxnMapPartition) insert(key types.TxnId, val interface{}) error {
	cmp.mutex.RLock()
	if _, ok := cmp.m[key]; ok {
		cmp.mutex.RUnlock()
		return ErrPrevExists
	}
	cmp.mutex.RUnlock()

	cmp.mutex.Lock()
	defer cmp.mutex.Unlock()

	if _, ok := cmp.m[key]; ok {
		return ErrPrevExists
	}
	cmp.m[key] = val
	return nil
}

func (cmp *concurrentTxnMapPartition) del(key types.TxnId) {
	cmp.mutex.Lock()
	delete(cmp.m, key)
	cmp.mutex.Unlock()
}

func (cmp *concurrentTxnMapPartition) forEachLocked(cb func(types.TxnId, interface{})) {
	for key, val := range cmp.m {
		cb(key, val)
	}
}

type ConcurrentTxnMap struct {
	partitions []*concurrentTxnMapPartition

	quit chan struct{}
	wg   sync.WaitGroup

	TotalTransactionInserted basic.AtomicInt64
	CurrentTransactionCount  basic.AtomicInt64
}

func (cmp *ConcurrentTxnMap) Initialize(partitionNum int) {
	assert.Must(utils.IsPowerOf2(partitionNum))

	cmp.partitions = make([]*concurrentTxnMapPartition, partitionNum)
	for i := range cmp.partitions {
		cmp.partitions[i] = &concurrentTxnMapPartition{m: make(map[types.TxnId]interface{})}
	}
}

func (cmp *ConcurrentTxnMap) InitializeWithGCThreads(partitionNum int, chSizePerPartition int, minInterrupt time.Duration) {
	cmp.Initialize(partitionNum)

	cmp.quit = make(chan struct{})
	for _, p := range cmp.partitions {
		p.startGCThread(chSizePerPartition, minInterrupt, cmp.quit, &cmp.wg, &cmp.CurrentTransactionCount)
	}
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

func (cmp *ConcurrentTxnMap) Contains(key types.TxnId) bool {
	return cmp.partitions[cmp.hash(key)].contains(key)
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

func (cmp *ConcurrentTxnMap) SetIf(key types.TxnId, val interface{}, pred func(prev interface{}, exist bool) bool) (success bool, newValue interface{}) {
	return cmp.partitions[cmp.hash(key)].setIf(key, val, pred)
}

func (cmp *ConcurrentTxnMap) InsertIfNotExists(key types.TxnId, constructor func() interface{}) (inserted bool, newVal interface{}) {
	if inserted, newVal = cmp.partitions[cmp.hash(key)].insertIfNotExists(key, constructor); inserted {
		cmp.TotalTransactionInserted.Add(1)
		cmp.CurrentTransactionCount.Add(1)
	}
	return inserted, newVal
}

func (cmp *ConcurrentTxnMap) Insert(key types.TxnId, val interface{}) error {
	err := cmp.partitions[cmp.hash(key)].insert(key, val)
	if err == nil {
		cmp.TotalTransactionInserted.Add(1)
		cmp.CurrentTransactionCount.Add(1)
	}
	return err
}

func (cmp *ConcurrentTxnMap) Del(key types.TxnId) {
	cmp.partitions[cmp.hash(key)].del(key)
}

func (cmp *ConcurrentTxnMap) RemoveWhen(txn types.TxnId, scheduleTime time.Time) {
	cmp.partitions[cmp.hash(txn)].removeWhen(txn, scheduleTime)
}

func (cmp *ConcurrentTxnMap) hash(s types.TxnId) uint64 {
	return uint64(s) & uint64(len(cmp.partitions)-1)
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

func (cmp *ConcurrentTxnMap) Close() {
	cmp.Lock()
	for _, partition := range cmp.partitions {
		partition.m = nil
	}

	if cmp.quit != nil {
		close(cmp.quit)
		cmp.quit = nil
	}
	cmp.Unlock()

	cmp.wg.Wait()
}

func (cmp *ConcurrentTxnMap) Size() (sz int) {
	cmp.RLock()
	for _, partition := range cmp.partitions {
		sz += len(partition.m)
	}
	cmp.RUnlock()
	return
}
