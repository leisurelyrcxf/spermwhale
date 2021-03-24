package basic

type Set map[string]struct{}

func (s Set) Contains(key string) bool {
	_, ok := s[key]
	return ok
}

func (s Set) MustFirst() string {
	for key := range s {
		return key
	}
	panic("set empty")
}

func (s Set) Insert(key string) {
	s[key] = struct{}{}
}

func (s *Set) Reset() {
	*s = make(map[string]struct{})
}
