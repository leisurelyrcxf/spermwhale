package concurrency

import (
	"sync"
)

type ConcurrentSet struct {
	sync.RWMutex
	set map[string]struct{}
}

func NewConcurrentSet() *ConcurrentSet {
	return &ConcurrentSet{
		set: make(map[string]struct{}),
	}
}

func (s *ConcurrentSet) Initialize() {
	s.set = make(map[string]struct{})
}

func (s *ConcurrentSet) Insert(key string) (newLength int) {
	s.Lock()
	defer s.Unlock()

	s.set[key] = struct{}{}
	return len(s.set)
}

func (s *ConcurrentSet) Remove(key string) {
	s.Lock()
	defer s.Unlock()

	delete(s.set, key)
}

func (s *ConcurrentSet) Contains(key string) bool {
	s.RLock()
	defer s.RUnlock()

	_, ok := s.set[key]
	return ok
}

func (s *ConcurrentSet) Len() int {
	s.RLock()
	defer s.RUnlock()

	return len(s.set)
}
