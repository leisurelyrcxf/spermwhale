package basic

type Set map[string]struct{}

func (s Set) Contains(key string) bool {
	_, ok := s[key]
	return ok
}

func (s Set) MustFirstElement() string {
	for key := range s {
		return key
	}
	panic("set empty")
}
