package basic

type Set map[string]struct{}

func MakeSet(keys []string) Set {
	s := make(Set)
	for _, key := range keys {
		s[key] = struct{}{}
	}
	return s
}

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

func (s Set) InsertUnsafe(key string) {
	s[key] = struct{}{}
}

func (s *Set) Insert(key string) {
	if *s == nil {
		*s = Set{key: {}}
	} else {
		(*s)[key] = struct{}{}
	}
}

func (s *Set) Reset() {
	*s = make(map[string]struct{})
}
