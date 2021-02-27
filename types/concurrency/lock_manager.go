package concurrency

import (
	"hash/crc32"
	"sync"
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

func (lm *LockManager) hash(key string) int {
	return int(crc32.ChecksumIEEE([]byte(key))) % len(lm.rwMutexes)
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
