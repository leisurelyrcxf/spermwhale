package consts

import (
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestReadOptBitMask(t *testing.T) {
	assert := testifyassert.New(t)

	assert.Equal(uint8(0b11000000), txnKVCCCommonReadOptBitMask)
	assert.Equal(0b10111111, TxnKVCCCommonReadOptBitMaskClearWaitNoWriteIntent)

	const flag1Initial = uint8(KVCCReadOptBitMaskNotUpdateTimestampCache)
	var flag1, flag2 uint8
	flag1 = flag1Initial
	flag2 = KVCCReadOptBitMaskNotGetMaxReadVersion
	flag2 |= TxnKVCCCommonReadOptBitMaskWaitNoWriteIntent
	assert.Equal(uint8(0b01000100), flag2)

	flag1 = InheritReadCommonFlag(flag1, flag2)
	assert.Equal(uint8(0b01000010), flag1)
	flag1 &= TxnKVCCCommonReadOptBitMaskClearWaitNoWriteIntent
	assert.Equal(flag1Initial, flag1)
}
