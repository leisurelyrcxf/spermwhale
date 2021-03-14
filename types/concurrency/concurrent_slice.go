package concurrency

import "sync"

type ConcurrentStringSlice struct {
	sync.RWMutex
	slice []string
}

func NewConcurrentStringSlice() *ConcurrentStringSlice {
	return &ConcurrentStringSlice{}
}

func (s *ConcurrentStringSlice) Append(str string) {
	s.Lock()
	defer s.Unlock()

	s.slice = append(s.slice, str)
}

func (s *ConcurrentStringSlice) Len() int {
	s.RLock()
	defer s.RUnlock()

	return len(s.slice)
}

func (s *ConcurrentStringSlice) Strings() []string {
	s.RLock()
	defer s.RUnlock()

	return append(make([]string, 0, len(s.slice)), s.slice...)
}
