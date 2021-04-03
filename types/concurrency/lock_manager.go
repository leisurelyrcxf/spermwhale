package concurrency

import (
	"hash/crc32"
	"sync"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/basic"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

type LockManager interface {
	Lock(key string)
	Unlock(key string)
	RLock(key string)
	RUnlock(key string)
}

type TxnLockManager interface {
	Lock(id types.TxnId)
	Unlock(id types.TxnId)
	RLock(id types.TxnId)
	RUnlock(id types.TxnId)
}

// Simplest row lock manager
type BasicLockManager struct {
	rwMutexes []sync.RWMutex
}

const defaultSlotNum = 1024

func init() {
	assert.Must(utils.IsPowerOf2(defaultSlotNum))
}

func NewBasicLockManager() *BasicLockManager {
	return (&BasicLockManager{}).Initialize()
}

func (lm *BasicLockManager) Initialize() *BasicLockManager {
	lm.rwMutexes = make([]sync.RWMutex, defaultSlotNum)
	return lm
}

func (lm BasicLockManager) Lock(key string) {
	lm.rwMutexes[lm.hash(key)].Lock()
}

func (lm BasicLockManager) Unlock(key string) {
	lm.rwMutexes[lm.hash(key)].Unlock()
}

func (lm BasicLockManager) RLock(key string) {
	lm.rwMutexes[lm.hash(key)].RLock()
}

func (lm BasicLockManager) RUnlock(key string) {
	lm.rwMutexes[lm.hash(key)].RUnlock()
}

func (lm BasicLockManager) hash(key string) int {
	return int(crc32.ChecksumIEEE([]byte(key))) & (defaultSlotNum - 1)
}

type BasicTxnLockManager struct {
	rwMutexes []sync.RWMutex
}

func NewBasicTxnLockManager() *BasicTxnLockManager {
	return (&BasicTxnLockManager{}).Initialize()
}

func (lm *BasicTxnLockManager) Initialize() *BasicTxnLockManager {
	lm.rwMutexes = make([]sync.RWMutex, defaultSlotNum)
	return lm
}

func (lm BasicTxnLockManager) Lock(txnId types.TxnId) {
	lm.rwMutexes[txnId&(defaultSlotNum-1)].Lock()
}

func (lm BasicTxnLockManager) Unlock(txnId types.TxnId) {
	lm.rwMutexes[txnId&(defaultSlotNum-1)].Unlock()
}

func (lm BasicTxnLockManager) RLock(txnId types.TxnId) {
	lm.rwMutexes[txnId&(defaultSlotNum-1)].RLock()
}

func (lm BasicTxnLockManager) RUnlock(txnId types.TxnId) {
	lm.rwMutexes[txnId&(defaultSlotNum-1)].RUnlock()
}

type BasicTaskLockManager struct {
	rwMutexes []sync.RWMutex
}

func NewBasicTaskLockManager() *BasicTaskLockManager {
	return (&BasicTaskLockManager{}).Initialize()
}

func (lm *BasicTaskLockManager) Initialize() *BasicTaskLockManager {
	lm.rwMutexes = make([]sync.RWMutex, defaultSlotNum)
	return lm
}

func (lm BasicTaskLockManager) Lock(id basic.TaskId) {
	lm.rwMutexes[id.Hash()&(defaultSlotNum-1)].Lock()
}

func (lm BasicTaskLockManager) Unlock(id basic.TaskId) {
	lm.rwMutexes[id.Hash()&(defaultSlotNum-1)].Unlock()
}

func (lm BasicTaskLockManager) RLock(id basic.TaskId) {
	lm.rwMutexes[id.Hash()&(defaultSlotNum-1)].RLock()
}

func (lm BasicTaskLockManager) RUnlock(id basic.TaskId) {
	lm.rwMutexes[id.Hash()&(defaultSlotNum-1)].RUnlock()
}
