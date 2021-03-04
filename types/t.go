package types

import (
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
)

type T interface {
	Errorf(format string, args ...interface{})
}

type MyT struct {
	t *testing.T
}

func NewT(t *testing.T) MyT {
	return MyT{
		t: t,
	}
}

func (t MyT) Errorf(format string, args ...interface{}) {
	if isMain() {
		t.t.Errorf(format, args...)
		return
	}
	print(fmt.Sprintf(format, args...))
	_ = os.Stderr.Sync()
}

func isMain() bool {
	ss := string(debug.Stack())
	return strings.Contains(ss, "testing.(*T).Run")
}

func NewAssertion(t *testing.T) *testifyassert.Assertions {
	return testifyassert.New(NewT(t))
}
