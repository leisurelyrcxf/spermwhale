package concurrency

import (
	"fmt"
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
