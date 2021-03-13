package consts

import (
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestReadOptBitMask(t *testing.T) {
	assert := testifyassert.New(t)

	assert.Equal(uint8(0b11000000), commonReadOptBitMask)
	assert.Equal(0b10111111, RevertCommonReadOptBitMaskWaitNoWriteIntent)

	var flag1, flag2 uint8
	flag1 = ReadOptBitMaskNotUpdateTimestampCache
	flag2 = ReadOptBitMaskNotGetMaxReadVersion
	flag2 |= CommonReadOptBitMaskWaitNoWriteIntent
	assert.Equal(uint8(0b01000010), flag2)

	flag1 = InheritReadCommonFlag(flag1, flag2)
	assert.Equal(uint8(0b01000001), flag1)
	flag1 &= RevertCommonReadOptBitMaskWaitNoWriteIntent
	assert.Equal(uint8(ReadOptBitMaskNotUpdateTimestampCache), flag1)
}
