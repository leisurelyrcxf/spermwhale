package types

import (
	"runtime/debug"
	"strings"

	"github.com/golang/glog"

	testifyassert "github.com/stretchr/testify/assert"
)

type T interface {
	Errorf(format string, args ...interface{})
	Logf(format string, args ...interface{})
	Name() string
}

type myT struct {
	T
}

func (t myT) Errorf(format string, args ...interface{}) {
	if isMain() {
		t.T.Errorf(format, args...)
		return
	}
	glog.Fatalf(format, args...)
}

func isMain() bool {
	ss := string(debug.Stack())
	return strings.Contains(ss, "testing.(*T).Run")
}

func NewAssertion(t T) *testifyassert.Assertions {
	return testifyassert.New(myT{T: t})
}
