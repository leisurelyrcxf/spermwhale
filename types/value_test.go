package types

import (
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestValue(t *testing.T) {
	assert := testifyassert.New(t)

	v := NewValue([]byte("123"), 3)
	assert.True(v.HasWriteIntent())

	{
		v := v.WithNoWriteIntent()
		assert.False(v.HasWriteIntent())
	}

	{
		assert.False(v.IsMaxReadVersionBiggerThanRequested())
		v := v.WithMaxReadVersionBiggerThanRequested()
		assert.True(v.IsMaxReadVersionBiggerThanRequested())
		v = v.WithNoWriteIntent()
		assert.False(v.HasWriteIntent())
		assert.True(v.IsMaxReadVersionBiggerThanRequested())
	}

	assert.True(v.HasWriteIntent())
	v.ClearWriteIntent()
	assert.False(v.HasWriteIntent())
}
