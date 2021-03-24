package errors

import testifyassert "github.com/stretchr/testify/assert"

var (
	AssertIsErr = func(assert *testifyassert.Assertions, err error, exp *Error) bool {
		return assert.IsTypef(&Error{}, err, "expect type *errors.Error, but got %T(%v)", err, err) && assert.Equal(exp.Code, GetErrorCode(err))
	}
	AssertIsKeyOrVersionNotExistsErr = func(assert *testifyassert.Assertions, err error) bool {
		return AssertIsErr(assert, err, ErrKeyOrVersionNotExist)
	}
	AssertNilOrErr = func(assert *testifyassert.Assertions, err error, exp *Error) bool {
		if err == nil {
			return true
		}
		return AssertIsErr(assert, err, exp)
	}
)
