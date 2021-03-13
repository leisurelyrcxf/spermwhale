package consts

import "time"

const (
	DefaultTooStaleWriteThreshold       = 5 * time.Second
	DefaultMaxClockDrift                = time.Second
	DefaultWoundUncommittedTxnThreshold = 5 * time.Second

	DefaultTxnManagerClearWorkerNumber = 20
	DefaultTxnManagerIOWorkerNumber    = 30
)

const (
	ReadOptBitMaskNotUpdateTimestampCache = 1
	ReadOptBitMaskNotGetMaxReadVersion    = 1 << 1
	ReadOptBitMaskReadForWriteFirstRead   = 1 << 2

	commonReadOptBitOffset                      = 6
	commonReadOptBitMask                        = uint8((0xffff << commonReadOptBitOffset) & 0xff)
	CommonReadOptBitMaskWaitNoWriteIntent       = 1 << commonReadOptBitOffset
	RevertCommonReadOptBitMaskWaitNoWriteIntent = ^CommonReadOptBitMaskWaitNoWriteIntent & 0xff

	WriteOptBitMaskClearWriteIntent = 1
	WriteOptBitMaskRemoveVersion    = 1 << 1
	WriteOptBitMaskReadForWrite     = 1 << 2
	WriteOptBitMaskTxnRecord        = 1 << 3
	WriteOptBitMaskFirstWrite       = 1 << 4

	ValueMetaBitMaskHasWriteIntent = 1
)

const (
	DefaultTabletServerPort = 20000
	DefaultOracleServerPort = 5555
	DefaultTxnServerPort    = 9999
	DefaultKVServerPort     = 10001
)

const (
	DefaultRetryWaitPeriod = time.Millisecond * 100

	LoosedOracleDiscardedBits = 10
	LoosedOraclePrecision     = 1 << LoosedOracleDiscardedBits
	LoosedOracleWaitPeriod    = LoosedOraclePrecision >> 2
)

const (
	MaxRetryTxnGet                    = 2
	MaxRetryResolveFoundedWriteIntent = 2
)

const (
	DefaultReadTimeout = time.Second * 10
)

func InheritReadCommonFlag(flag1, flag2 uint8) uint8 {
	return flag1 | (flag2 & commonReadOptBitMask)
}

const (
	MaxReadForWriteQueueCapacityPerKey  = 500
	MaxWriteIntentWaitersCapacityPerTxn = 100
)
