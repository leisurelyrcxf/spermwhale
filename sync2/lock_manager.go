package sync2

import (
    "hash/crc32"
    "sync"
)

type LockManager struct {
    rwMutexes []sync.RWMutex
    mutexes []sync.Mutex
}

const defaultSlotNum = 1024

func NewLockManager() *LockManager {
    lm := &LockManager{
        rwMutexes: make([]sync.RWMutex, defaultSlotNum),
        mutexes:   make([]sync.Mutex, defaultSlotNum),
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

