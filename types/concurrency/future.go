package concurrency

import (
	"sync"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/types"
)

type Future struct {
	sync.RWMutex

	keys           map[types.TxnKeyUnion]types.DBMeta
	flyingKeyCount int
	addedKeyCount  int

	txnTerminated, done bool
}

func NewFuture() *Future {
	f := &Future{}
	f.Initialize()
	return f
}

func (s *Future) Initialize() {
	s.keys = make(map[types.TxnKeyUnion]types.DBMeta, 10)
}

func (s *Future) GetAddedKeyCountUnsafe() int {
	s.RLock()
	defer s.RUnlock()
	return s.addedKeyCount
}

// IsDone return true if all keys are done
func (s *Future) IsDone() bool {
	s.RLock()
	defer s.RUnlock()
	return s.done
}

func (s *Future) MustAdd(key types.TxnKeyUnion, meta types.DBMeta) (insertedNewKey bool) {
	s.Lock()
	defer s.Unlock()

	assert.Must(!s.txnTerminated && !s.done)
	assert.Must(meta.VFlag&(consts.ValueMetaBitMaskCommitted|consts.ValueMetaBitMaskAborted|
		consts.ValueMetaBitMaskCleared|consts.ValueMetaBitMaskHasWriteIntent) == consts.ValueMetaBitMaskHasWriteIntent)
	if old, ok := s.keys[key]; ok {
		assert.Must(old.IsUncommitted() && meta.InternalVersion > old.InternalVersion)
		s.keys[key] = meta
		return false
	}

	s.keys[key] = meta
	s.flyingKeyCount++
	s.addedKeyCount++
	return true
}

func (s *Future) NotifyTerminated(state types.TxnState, onInvalidKeyState func(key types.TxnKeyUnion, meta *types.DBMeta)) {
	assert.Must(state.IsTerminated() && !s.txnTerminated && !s.done)
	s.Lock()
	defer s.Unlock()

	for key, old := range s.keys {
		if glog.V(210) {
			glog.Infof("[Future::NotifyTerminated] update key '%s' to state %s", key, state)
		}
		if old.IsKeyStateInvalid() {
			onInvalidKeyState(key, &old)
		}
		old.UpdateTxnState(state)
		assert.Must(old.IsValid())
		s.keys[key] = old
	}
	s.txnTerminated = true
}

func (s *Future) DoneKey(key types.TxnKeyUnion, state types.KeyState) (doneOnce bool) {
	assert.Must(s.txnTerminated)

	s.Lock()
	defer s.Unlock()

	oldDone := s.done
	newDone := s.doneKeyUnsafe(key, state)
	return !oldDone && newDone
}

func (s *Future) doneKeyUnsafe(key types.TxnKeyUnion, state types.KeyState) (futureDone bool) {
	assert.Must(!s.done || state.IsAborted() || s.keys[key].IsCommittedCleared())
	if old, ok := s.keys[key]; ok {
		if assert.Must(old.IsValid()); !old.IsCleared() {
			old.SetCleared()
			s.keys[key] = old
			s.flyingKeyCount-- // !done->done //else { already done }
		} // else { skip }
	} else {
		dbMeta := types.DBMeta{
			VFlag:           consts.ValueMetaBitMaskHasWriteIntent,
			InternalVersion: types.TxnInternalVersionMax,
		}
		assert.Must(state == types.KeyStateRollbackedCleared)
		dbMeta.UpdateKeyStateUnsafe(state)
		s.keys[key] = dbMeta // prevent future inserts
	}
	assert.Must(s.keys[key].IsTerminated())
	s.done = s.flyingKeyCount == 0
	return s.done
}

func (s *Future) MustGetDBMeta(key types.TxnKeyUnion) types.DBMeta {
	s.RLock()
	defer s.RUnlock()

	meta, ok := s.keys[key]
	assert.Must(ok)
	return meta
}

func (s *Future) GetDBMeta(key types.TxnKeyUnion) (types.DBMeta, bool) {
	s.RLock()
	defer s.RUnlock()

	meta, ok := s.keys[key]
	return meta, ok
}

func (s *Future) GetDBMetaUnsafe(key types.TxnKeyUnion) types.DBMeta {
	s.RLock()
	defer s.RUnlock()

	return s.keys[key]
}

func (s *Future) HasPositiveInternalVersion(key types.TxnKeyUnion, version types.TxnInternalVersion) bool {
	s.RLock()
	defer s.RUnlock()

	info := s.keys[key]
	assert.Must(info.IsValid() && version > 0)
	return info.InternalVersion == version
}

func (s *Future) AssertAllKeysOfState(state types.KeyState) {
	s.RLock()
	defer s.RUnlock()

	for key, info := range s.keys {
		_ = key
		assert.Must(info.GetKeyState() == state)
	}
}
