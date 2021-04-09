package concurrency

import (
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

type rwCount struct {
	readerCount int32
	writerCount bool
}

func (rw rwCount) WithIncrReader() rwCount {
	rw.readerCount++
	return rw
}

func (rw rwCount) WithDecrReader() rwCount {
	rw.readerCount--
	return rw
}

func (rw rwCount) IsEmpty() bool {
	return !rw.writerCount && rw.readerCount == 0
}

func (rw rwCount) String() string {
	return fmt.Sprintf("reader_count: %d, writer_count: %d", rw.readerCount, utils.Bool2Int(rw.writerCount))
}

type advancedTxnLockManagerPartition struct {
	m map[types.TxnId]rwCount

	mu     sync.RWMutex
	rCount sync.Cond
	wCount sync.Cond
}

func (p *advancedTxnLockManagerPartition) Initialize() {
	p.m = make(map[types.TxnId]rwCount)
	p.rCount.L = &p.mu
	p.wCount.L = &p.mu
}

func (p *advancedTxnLockManagerPartition) Lock(id types.TxnId) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for !p.cas(id) {
		p.wCount.Wait()
	}
	for p.m[id].readerCount > 0 {
		p.rCount.Wait()
	}
}

func (p *advancedTxnLockManagerPartition) cas(id types.TxnId) (swapped bool) {
	if val := p.m[id]; !val.writerCount {
		val.writerCount = true
		p.m[id] = val
		return true
	}
	return false
}

func (p *advancedTxnLockManagerPartition) Unlock(id types.TxnId) {
	p.mu.Lock()
	delete(p.m, id)
	p.mu.Unlock()

	p.wCount.Broadcast()
}

func (p *advancedTxnLockManagerPartition) RLock(id types.TxnId) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for !p.tryRLock(id) {
		p.wCount.Wait()
	}
}

func (p *advancedTxnLockManagerPartition) tryRLock(id types.TxnId) (grabbed bool) {
	if val := p.m[id]; !val.writerCount {
		p.m[id] = val.WithIncrReader()
		return true
	}
	return false
}

func (p *advancedTxnLockManagerPartition) RUnlock(id types.TxnId) {
	p.mu.Lock()
	defer func() {
		p.mu.Unlock()

		p.rCount.Broadcast()
	}()

	newRW := p.m[id].WithDecrReader()
	if newRW.IsEmpty() {
		delete(p.m, id)
		return
	}
	p.m[id] = newRW
}

type AdvancedTxnLockManager struct {
	partitions []advancedTxnLockManagerPartition
}

func NewAdvancedTxnLockManager(partitionNum int) *AdvancedTxnLockManager {
	return (&AdvancedTxnLockManager{}).Initialize(partitionNum)
}

func (lm *AdvancedTxnLockManager) Initialize(partitionNum int) *AdvancedTxnLockManager {
	assert.Must(utils.IsPowerOf2(partitionNum))

	lm.partitions = make([]advancedTxnLockManagerPartition, partitionNum)
	for i := range lm.partitions {
		lm.partitions[i].Initialize()
	}
	return lm
}

func (lm *AdvancedTxnLockManager) Lock(txnId types.TxnId) {
	lm.partitions[txnId.Version()&(uint64(len(lm.partitions)-1))].Lock(txnId)
}

func (lm *AdvancedTxnLockManager) Unlock(txnId types.TxnId) {
	lm.partitions[txnId.Version()&(uint64(len(lm.partitions)-1))].Unlock(txnId)
}

func (lm *AdvancedTxnLockManager) RLock(txnId types.TxnId) {
	lm.partitions[txnId.Version()&(uint64(len(lm.partitions)-1))].RLock(txnId)
}

func (lm *AdvancedTxnLockManager) RUnlock(txnId types.TxnId) {
	lm.partitions[txnId.Version()&(uint64(len(lm.partitions)-1))].RUnlock(txnId)
}

type AdvancedLockManagerPartition struct {
	m map[string]rwCount

	mu     sync.RWMutex
	rCount sync.Cond
	wCount sync.Cond
}

func (p *AdvancedLockManagerPartition) Initialize() {
	p.m = make(map[string]rwCount)
	p.rCount.L = &p.mu
	p.wCount.L = &p.mu
}

func (p *AdvancedLockManagerPartition) Lock(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for !p.cas(key) {
		p.wCount.Wait()
	}
	for p.m[key].readerCount > 0 {
		p.rCount.Wait()
	}
}

func (p *AdvancedLockManagerPartition) cas(key string) (swapped bool) {
	if val := p.m[key]; !val.writerCount {
		val.writerCount = true
		p.m[key] = val
		return true
	}
	return false
}

func (p *AdvancedLockManagerPartition) Unlock(key string) {
	p.mu.Lock()
	delete(p.m, key)
	p.mu.Unlock()

	p.wCount.Broadcast()
}

func (p *AdvancedLockManagerPartition) RLock(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for !p.tryRLock(key) {
		p.wCount.Wait()
	}
}

func (p *AdvancedLockManagerPartition) tryRLock(key string) (grabbed bool) {
	if val := p.m[key]; !val.writerCount {
		p.m[key] = val.WithIncrReader()
		return true
	}
	return false
}

func (p *AdvancedLockManagerPartition) RUnlock(key string) {
	p.mu.Lock()
	defer func() {
		p.mu.Unlock()

		p.rCount.Broadcast()
	}()

	newRW := p.m[key].WithDecrReader()
	if newRW.IsEmpty() {
		delete(p.m, key)
		return
	}
	p.m[key] = newRW
}

type AdvancedLockManager struct {
	partitions []AdvancedLockManagerPartition
}

func NewAdvancedLockManager(partitionNum int) *AdvancedLockManager {
	return (&AdvancedLockManager{}).Initialize(partitionNum)
}

func (lm *AdvancedLockManager) Initialize(partitionNum int) *AdvancedLockManager {
	assert.Must(utils.IsPowerOf2(partitionNum))

	lm.partitions = make([]AdvancedLockManagerPartition, partitionNum)
	for i := range lm.partitions {
		lm.partitions[i].Initialize()
	}
	return lm
}

func (lm *AdvancedLockManager) Lock(key string) {
	lm.partitions[lm.hash(key)].Lock(key)
}

func (lm *AdvancedLockManager) Unlock(key string) {
	lm.partitions[lm.hash(key)].Unlock(key)
}

func (lm *AdvancedLockManager) RLock(key string) {
	lm.partitions[lm.hash(key)].RLock(key)
}

func (lm *AdvancedLockManager) RUnlock(key string) {
	lm.partitions[lm.hash(key)].RUnlock(key)
}

func (lm *AdvancedLockManager) hash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key)) & uint32(len(lm.partitions)-1)
}

type AdvancedTxnKeyUnionManagerPartition struct {
	m map[types.TxnKeyUnion]rwCount

	mu     sync.RWMutex
	rCount sync.Cond
	wCount sync.Cond
}

func (p *AdvancedTxnKeyUnionManagerPartition) Initialize() {
	p.m = make(map[types.TxnKeyUnion]rwCount)
	p.rCount.L = &p.mu
	p.wCount.L = &p.mu
}

func (p *AdvancedTxnKeyUnionManagerPartition) Lock(key types.TxnKeyUnion) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for !p.cas(key) {
		p.wCount.Wait()
	}
	for p.m[key].readerCount > 0 {
		p.rCount.Wait()
	}
}

func (p *AdvancedTxnKeyUnionManagerPartition) cas(key types.TxnKeyUnion) (swapped bool) {
	if val := p.m[key]; !val.writerCount {
		val.writerCount = true
		p.m[key] = val
		return true
	}
	return false
}

func (p *AdvancedTxnKeyUnionManagerPartition) Unlock(key types.TxnKeyUnion) {
	p.mu.Lock()
	delete(p.m, key)
	p.mu.Unlock()

	p.wCount.Broadcast()
}

func (p *AdvancedTxnKeyUnionManagerPartition) RLock(key types.TxnKeyUnion) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for !p.tryRLock(key) {
		p.wCount.Wait()
	}
}

func (p *AdvancedTxnKeyUnionManagerPartition) tryRLock(key types.TxnKeyUnion) (grabbed bool) {
	if val := p.m[key]; !val.writerCount {
		p.m[key] = val.WithIncrReader()
		return true
	}
	return false
}

func (p *AdvancedTxnKeyUnionManagerPartition) RUnlock(key types.TxnKeyUnion) {
	p.mu.Lock()
	defer func() {
		p.mu.Unlock()

		p.rCount.Broadcast()
	}()

	newRW := p.m[key].WithDecrReader()
	if newRW.IsEmpty() {
		delete(p.m, key)
		return
	}
	p.m[key] = newRW
}
