package types

import (
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestValue(t *testing.T) {
	assert := testifyassert.New(t)

	v := NewValue([]byte("123"), 3)
	assert.True(v.IsDirty())

	{
		v := v.WithNoWriteIntent()
		assert.False(v.IsDirty())
	}

	assert.True(v.IsDirty())
	v = v.WithNoWriteIntent()
	assert.False(v.IsDirty())
}
