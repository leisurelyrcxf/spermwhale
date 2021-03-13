package concurrency

import (
	"hash/crc32"
	"sync"

	"github.com/leisurelyrcxf/spermwhale/types"
)

// Simplest row lock manager
type LockManager struct {
	rwMutexes []sync.RWMutex
}

const defaultSlotNum = 256

func NewLockManager() *LockManager {
	lm := &LockManager{
		rwMutexes: make([]sync.RWMutex, defaultSlotNum),
	}
	return lm
}

func (lm *LockManager) Lock(key string) {
	slotIdx := lm.hash(key)
	lm.rwMutexes[slotIdx].Lock()
}

func (lm *LockManager) Unlock(key string) {
	slotIdx := lm.hash(key)
	lm.rwMutexes[slotIdx].Unlock()
}

func (lm *LockManager) RLock(key string) {
	slotIdx := lm.hash(key)
	lm.rwMutexes[slotIdx].RLock()
}

func (lm *LockManager) RUnlock(key string) {
	slotIdx := lm.hash(key)
	lm.rwMutexes[slotIdx].RUnlock()
}

func (lm *LockManager) hash(key string) int {
	return int(crc32.ChecksumIEEE([]byte(key))) % len(lm.rwMutexes)
}

// Simplest row lock manager
type TxnLockManager struct {
	rwMutexes []sync.RWMutex
}

func NewTxnLockManager() *TxnLockManager {
	lm := &TxnLockManager{
		rwMutexes: make([]sync.RWMutex, defaultSlotNum),
	}
	return lm
}

func (lm *TxnLockManager) Lock(key types.TxnId) {
	slotIdx := lm.hash(key)
	lm.rwMutexes[slotIdx].Lock()
}

func (lm *TxnLockManager) Unlock(key types.TxnId) {
	slotIdx := lm.hash(key)
	lm.rwMutexes[slotIdx].Unlock()
}

func (lm *TxnLockManager) RLock(key types.TxnId) {
	slotIdx := lm.hash(key)
	lm.rwMutexes[slotIdx].RLock()
}

func (lm *TxnLockManager) RUnlock(key types.TxnId) {
	slotIdx := lm.hash(key)
	lm.rwMutexes[slotIdx].RUnlock()
}

func (lm *TxnLockManager) hash(key types.TxnId) int {
	return int(uint64(key) % uint64(len(lm.rwMutexes)))
}
