package concurrency

import (
	"sync"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/types"
)

type Future struct {
	sync.Mutex

	types.TxnState
	keys        map[types.TxnKeyUnion]types.DBMeta
	invalidKeys map[types.TxnKeyUnion]struct{}

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
	return s.addedKeyCount
}

func (s *Future) MustAddInvalid(key types.TxnKeyUnion, meta types.DBMeta) (insertedNewKey bool) {
	s.Lock()
	defer s.Unlock()

	if s.invalidKeys == nil {
		s.invalidKeys = map[types.TxnKeyUnion]struct{}{key: {}}
	} else {
		s.invalidKeys[key] = struct{}{}
	}
	return s.mustAddUnsafe(key, meta.WithInvalidKeyState())
}

func (s *Future) MustAdd(key types.TxnKeyUnion, meta types.DBMeta) (insertedNewKey bool) {
	s.Lock()
	defer s.Unlock()

	return s.mustAddUnsafe(key, meta)
}

func (s *Future) mustAddUnsafe(key types.TxnKeyUnion, meta types.DBMeta) (insertedNewKey bool) {
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

func (s *Future) NotifyTerminatedUnsafe(state types.TxnState, chkInvalid func(key types.TxnKeyUnion) (_ types.Value, exists bool)) (doneOnce bool) {
	assert.Must(state.IsTerminated() && !state.IsCleared() && !s.IsTerminated())
	//s.Lock()
	//defer s.Unlock()

	s.TxnState = state

	var flyingKeyDecreased bool
	for key := range s.invalidKeys {
		old := s.keys[key]
		assert.Must(old.IsKeyStateInvalid())
		if val, exists := chkInvalid(key); !exists {
			old.ClearInvalidKeyState()
			old.SetCleared()
			assert.Must(state == types.TxnStateRollbacking)
			s.flyingKeyCount--
			flyingKeyDecreased = true
		} else {
			old.UpdateByMeta(val.Meta)
			assert.Must(old.IsUncommitted())
		}
		s.keys[key] = old
	}
	s.invalidKeys = nil

	if assert.Must(s.flyingKeyCount >= 0); s.flyingKeyCount == 0 && flyingKeyDecreased {
		s.TxnState.SetCleared()
		return true
	}
	return false
}

func (s *Future) DoneKeyUnsafe(key types.TxnKeyUnion, state types.KeyState) (doneOnce bool) {
	assert.Must(s.IsTerminated())
	//s.Lock()
	//defer s.Unlock()

	oldDone := s.IsCleared()
	assert.Must(!oldDone || state.IsAborted() || s.keys[key].IsCommittedCleared())

	if old, ok := s.keys[key]; ok {
		if assert.Must(!old.IsKeyStateInvalid()); !old.IsClearedUnsafe() {
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
		dbMeta.UpdateAbortedKeyState(state)
		s.keys[key] = dbMeta // prevent future inserts
	}
	if assert.Must(s.flyingKeyCount >= 0); s.flyingKeyCount == 0 {
		s.TxnState = types.TxnState(state)
		return !oldDone
	}
	return false
}

func (s *Future) MustGetDBMetaUnsafe(key types.TxnKeyUnion) types.DBMeta {
	//s.RLock()
	//defer s.RUnlock()

	meta, ok := s.keys[key]
	assert.Must(ok)
	return meta
}

func (s *Future) GetDBMetaUnsafe(key types.TxnKeyUnion) (types.DBMeta, bool) {
	//s.RLock()
	//defer s.RUnlock()

	meta, ok := s.keys[key]
	return meta, ok
}

func (s *Future) AssertAllKeysClearedUnsafe() {
	//s.RLock()
	//defer s.RUnlock()

	for key, info := range s.keys {
		_ = key
		assert.Must(info.IsClearedUnsafe())
	}
}

func (s *Future) AssertAllNonTxnRecordKeysClearedUnsafe() {
	//s.RLock()
	//defer s.RUnlock()

	for key, info := range s.keys {
		assert.Must(key.IsTxnRecord() || info.IsClearedUnsafe())
	}
}
