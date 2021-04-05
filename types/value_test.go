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
		v := v.WithCommitted()
		assert.True(!v.IsDirty() && v.IsCommitted())
	}

	{
		v := v
		v.SetAborted()
		assert.True(v.IsDirty() && v.IsAborted())
	}
}

func TestEmpty(t *testing.T) {
	assert := testifyassert.New(t)

	assert.True(EmptyValue.IsEmpty())
	assert.True(!EmptyValue.IsDirty())
	assert.True(!EmptyValue.IsCommitted())
	assert.True(!EmptyValue.IsAborted())

	assert.True(EmptyValueCC.IsEmpty())
	assert.True(!EmptyValueCC.IsDirty())
	assert.True(!EmptyValueCC.IsCommitted())
	assert.True(!EmptyValueCC.IsAborted())

	assert.True(EmptyTValue.IsEmpty())
	assert.True(!EmptyTValue.IsDirty())
	assert.True(!EmptyTValue.IsCommitted())
	assert.True(!EmptyTValue.IsAborted())

	assert.True(!EmptyDBValue.IsDirty())
	assert.True(!EmptyDBValue.IsCommitted())
}
