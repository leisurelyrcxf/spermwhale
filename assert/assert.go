// Licensed under the MIT (MIT-LICENSE.txt) license.

package assert

import (
	"fmt"
	"sort"
)

func Must(b bool) {
	if b {
		return
	}
	panic("assertion failed")
}

func MustEqualStrings(s1, s2 []string) {
	if len(s1) != len(s2) {
		panic(fmt.Sprintf("len(s1)%v != len(s2)%v", s1, s2))
	}
	s1Copied, s2Copied := copyString(s1), copyString(s2)
	sort.Strings(s1Copied)
	sort.Strings(s2Copied)
	for i, s1i := range s1Copied {
		s2i := s2Copied[i]
		MustEqualString(s1i, s2i)
	}
}

func MustAllContain(m map[string]struct{}, strings []string) {
	for _, str := range strings {
		_, ok := m[str]
		Must(ok)
	}
}

func MustEqualString(s1, s2 string) {
	if s1 != s2 {
		panic(fmt.Sprintf("s1(%v) != s2(%v)", s1, s2))
	}
}

func MustNoError(err error) {
	if err == nil {
		return
	}
	panic(fmt.Sprintf("'%s', error happens, assertion failed", err.Error()))
}

func copyString(s1 []string) []string {
	return append(make([]string, 0, len(s1)), s1...)
}
