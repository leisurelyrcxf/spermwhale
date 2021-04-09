package types

import (
	"fmt"
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
	println(fmt.Sprintf(format, args...))
	glog.Fatalf(format, args...)
}

func isMain() bool {
	ss := string(debug.Stack())
	return strings.Contains(ss, "testing.(*T).Run")
}

type Assertions struct {
	testifyassert.Assertions
}

func (assert *Assertions) EqualIntValue(exp Value, actual Value) (b bool) {
	if !assert.Equalf(exp.Version, actual.Version, "versions not same") {
		return
	}
	if !assert.Equalf(exp.InternalVersion, actual.InternalVersion, "internal versions not same") {
		return
	}
	if !assert.Equal(exp.VFlag, actual.VFlag, "flags not same") {
		return
	}
	expInt, err := exp.Int()
	if !assert.NoError(err) {
		return
	}
	actualInt, err := actual.Int()
	if !assert.NoError(err) {
		return
	}
	if !assert.Equalf(expInt, actualInt, "int values not same") {
		return
	}
	return true
}

func (assert *Assertions) EqualValue(exp Value, actual Value) (b bool) {
	if !assert.Equalf(exp.Version, actual.Version, "versions not same") {
		return
	}
	if !assert.Equalf(exp.InternalVersion, actual.InternalVersion, "internal versions not same") {
		return
	}
	if !assert.Equal(exp.VFlag, actual.VFlag, "flags not same") {
		return
	}
	if !assert.Equalf(exp.V, actual.V, "V not the same") {
		return
	}
	return true
}

func NewAssertion(t T) *Assertions {
	var mt myT
	if vt, ok := t.(myT); !ok {
		mt = myT{T: t}
	} else {
		mt = vt
	}
	return &Assertions{Assertions: *testifyassert.New(mt)}
}
