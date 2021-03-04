package errors

import testifyassert "github.com/stretchr/testify/assert"

var (
	AssertIsErr = func(assert *testifyassert.Assertions, err error, exp *Error) bool {
		return assert.IsType(&Error{}, err) && assert.Equal(exp.Code, GetErrorCode(err))
	}
	AssertIsVersionNotExistsErr = func(assert *testifyassert.Assertions, err error) bool {
		return AssertIsErr(assert, err, ErrVersionNotExists)
	}
	AssertNilOrErr = func(assert *testifyassert.Assertions, err error, exp *Error) bool {
		if err == nil {
			return true
		}
		return AssertIsErr(assert, err, exp)
	}
)
