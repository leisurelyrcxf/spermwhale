package concurrency

import (
	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/types"
)

type Composite struct {
	types.DBMeta
	Done bool
}

type Future struct {
	keys           map[string]Composite
	flyingKeyCount int
	addedKeyCount  int

	done bool
}

func NewFuture() *Future {
	f := &Future{}
	f.Initialize()
	return f
}

func (s *Future) Initialize() {
	s.keys = make(map[string]Composite, 10)
}

func (s *Future) GetKeyCountUnsafe() int {
	return len(s.keys)
}

func (s *Future) GetAddedKeyCountUnsafe() int {
	return s.addedKeyCount
}

func (s *Future) IsDoneUnsafe() bool {
	return s.done
}

func (s *Future) AddUnsafe(key string, meta types.DBMeta) (insertedNewKey bool, keyDone bool) {
	if keyDone, ok := s.keys[key]; ok {
		// Previous false -> already inserted
		// Previous true -> already done
		if !keyDone.Done && meta.InternalVersion > keyDone.InternalVersion {
			s.keys[key] = Composite{DBMeta: meta}
		}
		return false, keyDone.Done
	}
	s.keys[key] = Composite{DBMeta: meta}
	s.flyingKeyCount++
	s.addedKeyCount++
	return true, false
}

func (s *Future) MustAddUnsafe(key string, meta types.DBMeta) (insertedNewKey bool) {
	assert.Must(!s.done)
	if keyDone, ok := s.keys[key]; ok {
		// Previous false -> already inserted
		// Previous true -> already done
		assert.Must(!keyDone.Done && meta.InternalVersion > keyDone.InternalVersion)
		s.keys[key] = Composite{DBMeta: meta}
		return false
	}

	s.keys[key] = Composite{DBMeta: meta}
	s.flyingKeyCount++
	s.addedKeyCount++
	return true
}

func (s *Future) DoneOnceUnsafe(key string) (doneOnce bool) {
	if s.done {
		return false
	}
	return s.doneUnsafe(key)
}

func (s *Future) MustGetDBMetaUnsafe(key string) types.DBMeta {
	keyDone, ok := s.keys[key]
	assert.Must(ok)
	return keyDone.DBMeta
}

func (s *Future) GetDBMetaUnsafe(key string) (types.DBMeta, bool) {
	keyDone, ok := s.keys[key]
	return keyDone.DBMeta, ok
}

func (s *Future) HasPositiveInternalVersion(key string, version types.TxnInternalVersion) bool {
	return s.keys[key].InternalVersion == version
}

func (s *Future) doneUnsafe(key string) (futureDone bool) {
	if doneKey, ok := s.keys[key]; ok {
		if !doneKey.Done {
			doneKey.Done = true
			s.keys[key] = doneKey
			s.flyingKeyCount-- // false->true
		} //else { already done }
	} else {
		s.keys[key] = Composite{Done: true} // prevent future inserts
	}
	s.done = s.flyingKeyCount == 0
	return s.done
}

func (s *Future) doneUnsafeEx(key string) (doneOnce, done bool) {
	oldFlyingKeyCount := s.flyingKeyCount
	done = s.doneUnsafe(key)
	return s.flyingKeyCount < oldFlyingKeyCount, done
}

func (s *Future) contains(key string) bool {
	_, ok := s.keys[key]
	return ok
}

func (s *Future) length() int {
	return len(s.keys)
}

func (s *Future) IsKeyDoneUnsafe(key string) bool {
	return s.keys[key].Done
}
