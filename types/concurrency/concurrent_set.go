package concurrency

import "sync"

type ConcurrentSet struct {
	sync.Mutex
	set map[string]struct{}
}

func NewConcurrentSet() *ConcurrentSet {
	return &ConcurrentSet{
		set: make(map[string]struct{}),
	}
}

func (s *ConcurrentSet) Insert(key string) int {
	s.Lock()
	defer s.Unlock()

	s.set[key] = struct{}{}
	return len(s.set)
}
