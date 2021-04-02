package concurrency

import (
	"hash/crc32"
	"sync"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/basic"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

// Simplest row lock manager
type LockManager struct {
	rwMutexes []sync.RWMutex
}

const defaultSlotNum = 1024

func init() {
	assert.Must(utils.IsPowerOf2(defaultSlotNum))
}

func NewLockManager() *TxnLockManager {
	return (&TxnLockManager{}).Initialize()
}

func (lm *LockManager) Initialize() *LockManager {
	lm.rwMutexes = make([]sync.RWMutex, defaultSlotNum)
	return lm
}

func (lm LockManager) Lock(key string) {
	lm.rwMutexes[lm.hash(key)].Lock()
}

func (lm LockManager) Unlock(key string) {
	lm.rwMutexes[lm.hash(key)].Unlock()
}

func (lm LockManager) RLock(key string) {
	lm.rwMutexes[lm.hash(key)].RLock()
}

func (lm LockManager) RUnlock(key string) {
	lm.rwMutexes[lm.hash(key)].RUnlock()
}

func (lm LockManager) hash(key string) int {
	return int(crc32.ChecksumIEEE([]byte(key))) & (defaultSlotNum - 1)
}

type TxnLockManager struct {
	rwMutexes []sync.RWMutex
}

func NewTxnLockManager() *TxnLockManager {
	return (&TxnLockManager{}).Initialize()
}

func (lm *TxnLockManager) Initialize() *TxnLockManager {
	lm.rwMutexes = make([]sync.RWMutex, defaultSlotNum)
	return lm
}

func (lm TxnLockManager) Lock(key types.TxnKeyUnion) {
	lm.rwMutexes[key.Hash()&(defaultSlotNum-1)].Lock()
}

func (lm TxnLockManager) Unlock(key types.TxnKeyUnion) {
	lm.rwMutexes[key.Hash()&(defaultSlotNum-1)].Unlock()
}

func (lm TxnLockManager) RLock(key types.TxnKeyUnion) {
	lm.rwMutexes[key.Hash()&(defaultSlotNum-1)].RLock()
}

func (lm TxnLockManager) RUnlock(key types.TxnKeyUnion) {
	lm.rwMutexes[key.Hash()&(defaultSlotNum-1)].RUnlock()
}

type TxnIdLockManager struct {
	rwMutexes []sync.RWMutex
}

func NewTxnIdLockManager() *TxnIdLockManager {
	return (&TxnIdLockManager{}).Initialize()
}

func (lm *TxnIdLockManager) Initialize() *TxnIdLockManager {
	lm.rwMutexes = make([]sync.RWMutex, defaultSlotNum)
	return lm
}

func (lm TxnIdLockManager) Lock(txnId types.TxnId) {
	lm.rwMutexes[txnId&(defaultSlotNum-1)].Lock()
}

func (lm TxnIdLockManager) Unlock(txnId types.TxnId) {
	lm.rwMutexes[txnId&(defaultSlotNum-1)].Unlock()
}

func (lm TxnIdLockManager) RLock(txnId types.TxnId) {
	lm.rwMutexes[txnId&(defaultSlotNum-1)].RLock()
}

func (lm TxnIdLockManager) RUnlock(txnId types.TxnId) {
	lm.rwMutexes[txnId&(defaultSlotNum-1)].RUnlock()
}

type TaskLockManager struct {
	rwMutexes []sync.RWMutex
}

func NewTaskLockManager() *TaskLockManager {
	return (&TaskLockManager{}).Initialize()
}

func (lm *TaskLockManager) Initialize() *TaskLockManager {
	lm.rwMutexes = make([]sync.RWMutex, defaultSlotNum)
	return lm
}

func (lm TaskLockManager) Lock(id basic.TaskId) {
	lm.rwMutexes[id.Hash()&(defaultSlotNum-1)].Lock()
}

func (lm TaskLockManager) Unlock(id basic.TaskId) {
	lm.rwMutexes[id.Hash()&(defaultSlotNum-1)].Unlock()
}

func (lm TaskLockManager) RLock(id basic.TaskId) {
	lm.rwMutexes[id.Hash()&(defaultSlotNum-1)].RLock()
}

func (lm TaskLockManager) RUnlock(id basic.TaskId) {
	lm.rwMutexes[id.Hash()&(defaultSlotNum-1)].RUnlock()
}
