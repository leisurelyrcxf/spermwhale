package utils

func Set2StringList(m map[string]bool) []string {
	l := make([]string, 0, len(m))
	for key := range m {
		l = append(l, key)
	}
	return l
}

func Contains(set map[string]struct{}, key string) bool {
	_, ok := set[key]
	return ok
}

func StringList2Set(keys []string) map[string]bool {
	m := map[string]bool{}
	for _, key := range keys {
		m[key] = false
	}
	return m
}
