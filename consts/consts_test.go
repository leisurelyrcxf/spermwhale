package consts

import (
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestClearWriteIntent(t *testing.T) {
	assert := testifyassert.New(t)
	assert.Equal(0xfe, ValueMetaBitMaskWriteIntentClear)
}
