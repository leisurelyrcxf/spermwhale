package types

import (
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestKCCUpdateMetaOption(t *testing.T) {
	assert := testifyassert.New(t)

	opt := KVCCClearWriteIntent.CondReadModifyWrite(true)
	assert.True(opt.IsReadModifyWrite())
}
