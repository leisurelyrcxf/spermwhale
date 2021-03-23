package types

import (
	"runtime/debug"
	"strings"

	"github.com/golang/glog"

	testifyassert "github.com/stretchr/testify/assert"
)

type T interface {
	Errorf(format string, args ...interface{})
}

type myT struct {
	t testifyassert.TestingT
}

func newT(t testifyassert.TestingT) myT {
	return myT{
		t: t,
	}
}

func (t myT) Errorf(format string, args ...interface{}) {
	if isMain() {
		t.t.Errorf(format, args...)
		return
	}
	glog.Fatalf(format, args...)
}

func isMain() bool {
	ss := string(debug.Stack())
	return strings.Contains(ss, "testing.(*T).Run")
}

func NewAssertion(t testifyassert.TestingT) *testifyassert.Assertions {
	return testifyassert.New(newT(t))
}
