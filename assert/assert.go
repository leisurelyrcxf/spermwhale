// Licensed under the MIT (MIT-LICENSE.txt) license.

package assert

import (
	"fmt"
	"sort"

	"github.com/golang/glog"
)

func Must(b bool) {
	if b {
		return
	}
	panic("assertion failed")
}

func Mustf(b bool, format string, args ...interface{}) {
	if b {
		return
	}
	glog.Fatalf(format, args...)
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
