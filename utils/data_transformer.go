package utils

func Set2StringList(m map[string]struct{}) []string {
	l := make([]string, 0, len(m))
	for key := range m {
		l = append(l, key)
	}
	return l
}

func StringList2Set(keys []string) map[string]struct{} {
	m := map[string]struct{}{}
	for _, key := range keys {
		m[key] = struct{}{}
	}
	return m
}

func Contains(set map[string]struct{}, key string) bool {
	_, ok := set[key]
	return ok
}
