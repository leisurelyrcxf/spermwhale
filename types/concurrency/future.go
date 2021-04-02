package concurrency

type Future struct {
	keys           map[string]bool
	flyingKeyCount int
	addedKeyCount  int

	done bool
}

func NewFuture() *Future {
	return &Future{}
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

func (s *Future) AddUnsafe(key string) (insertedNewKey bool, keyDone bool) {
	if keyDone, ok := s.keys[key]; ok {
		// Previous false -> already inserted
		// Previous true -> already done
		return false, keyDone
	}
	s.set(key, false)
	s.flyingKeyCount++
	s.addedKeyCount++
	return true, false
}

func (s *Future) DoneOnceUnsafe(key string) (doneOnce bool) {
	if s.done {
		return false
	}
	return s.doneUnsafe(key)
}

func (s *Future) doneUnsafe(key string) (futureDone bool) {
	if doneKey, ok := s.keys[key]; ok {
		if !doneKey {
			s.set(key, true)
			s.flyingKeyCount-- // false->true
		} //else { already done }
	} else {
		s.set(key, true) // prevent future inserts
	}
	s.done = s.flyingKeyCount == 0
	return s.done
}

func (s *Future) doneUnsafeEx(key string) (doneOnce, done bool) {
	oldFlyingKeyCount := s.flyingKeyCount
	done = s.doneUnsafe(key)
	return s.flyingKeyCount < oldFlyingKeyCount, done
}

func (s *Future) set(key string, b bool) {
	if s.keys == nil {
		s.keys = map[string]bool{key: b}
	} else {
		s.keys[key] = b
	}
}

func (s *Future) contains(key string) bool {
	_, ok := s.keys[key]
	return ok
}

func (s *Future) length() int {
	return len(s.keys)
}
