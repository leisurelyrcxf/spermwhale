package utils

import (
	"math"
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestSafeIncr(t *testing.T) {
	assert := testifyassert.New(t)

	u := uint64(math.MaxUint64)
	assert.Equal(uint64(math.MaxUint64), u)
	SafeIncr(&u)
	assert.Equal(uint64(math.MaxUint64), u)
	u += 1
	assert.Equal(uint64(0), u)
}
