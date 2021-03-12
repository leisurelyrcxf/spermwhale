package consts

import (
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestReadOptBitMask(t *testing.T) {
	assert := testifyassert.New(t)

	assert.Equal(uint8(0b11111100), readOptBitMaskCommon)

	var flag1, flag2 uint8
	flag1 = ReadOptBitMaskNotUpdateTimestampCache
	flag2 = ReadOptBitMaskNotGetMaxReadVersion
	flag2 |= ReadOptBitMaskWaitNoWriteIntent
	assert.Equal(uint8(0b00000110), flag2)

	flag1 = InheritReadCommonFlag(flag1, flag2)
	assert.Equal(uint8(0b00000101), flag1)
	flag1 &= RevertReadOptBitMaskWaitNoWriteIntent
	assert.Equal(uint8(0b00000001), flag1)
}
