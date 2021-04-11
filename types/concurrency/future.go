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

	types.TxnState
	keys           map[types.TxnKeyUnion]types.DBMeta
	flyingKeyCount int
	addedKeyCount  int
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

func (s *Future) MustAdd(key types.TxnKeyUnion, meta types.DBMeta) (insertedNewKey bool) {
	s.Lock()
	defer s.Unlock()

	assert.Must(!s.IsTerminated())
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

func (s *Future) NotifyTerminated(state types.TxnState, chkAndClearInvalid func(key types.TxnKeyUnion, meta *types.DBMeta) (exists bool)) (doneOnce bool) {
	assert.Must(state.IsTerminated() && !state.IsCleared() && !s.IsTerminated())
	s.Lock()
	defer s.Unlock()

	s.TxnState = state
	for key, old := range s.keys {
		if glog.V(210) {
			glog.Infof("[Future::NotifyTerminated] update key '%s' to state %s", key, state)
		}
		if old.IsKeyStateInvalid() {
			if exists := chkAndClearInvalid(key, &old); !exists {
				old.UpdateTxnState(state)
				old.SetCleared()
				assert.Must(old.IsRollbackedCleared())
				s.keys[key] = old
				s.flyingKeyCount--
				continue
			}
		}
		old.UpdateTxnState(state)
		assert.Must(old.IsValid())
		s.keys[key] = old
	}
	if assert.Must(s.flyingKeyCount >= 0); s.flyingKeyCount == 0 {
		s.TxnState.SetCleared()
		return true
	}
	return false
}

func (s *Future) DoneKey(key types.TxnKeyUnion, state types.KeyState) (doneOnce bool) {
	assert.Must(s.IsTerminated())

	s.Lock()
	defer s.Unlock()

	oldDone := s.IsCleared()
	newDone := s.doneKeyUnsafe(key, state)
	return !oldDone && newDone
}

func (s *Future) doneKeyUnsafe(key types.TxnKeyUnion, state types.KeyState) (done bool) {
	assert.Must(!s.IsCleared() || state.IsAborted() || s.keys[key].IsCommittedCleared())
	if old, ok := s.keys[key]; ok {
		if assert.Must(old.IsValid()); !old.IsCleared() {
			old.SetCleared()
			assert.Must(old.GetKeyState() == state)
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
	if assert.Must(s.flyingKeyCount >= 0); s.flyingKeyCount == 0 {
		s.TxnState = types.TxnState(state)
		return true
	}
	return false
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

func (s *Future) AssertAllKeysCleared(state types.KeyState) {
	s.RLock()
	defer s.RUnlock()

	for key, info := range s.keys {
		_ = key
		assert.Must(info.GetKeyState() == state)
	}
	assert.Must(s.TxnState == types.TxnState(state))
}
