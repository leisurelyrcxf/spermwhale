package consts

import (
	"math"
	"time"
)

const (
	MinTxnInternalVersion             = 1
	MaxTxnInternalVersion             = math.MaxUint8 - 1
	PositiveInvalidTxnInternalVersion = math.MaxUint8
)

const (
	DefaultTooStaleWriteThreshold       = 5 * time.Second
	DefaultMaxClockDrift                = time.Second
	DefaultWoundUncommittedTxnThreshold = 5 * time.Second

	DefaultTxnManagerClearerNumber              = 20
	DefaultTxnManagerClearJobTimeout            = time.Second * 10
	DefaultTxnManagerWriterNumber               = 30
	DefaultTxnManagerReaderNumber               = 20
	DefaultTxnManagerMaxBufferedJobPerPartition = 10000
)

const (
	ReadOptBitMaskNotUpdateTimestampCache    = 1
	ReadOptBitMaskNotGetMaxReadVersion       = 1 << 1
	ReadOptBitMaskReadForWrite               = 1 << 2
	ReadOptBitMaskReadForWriteFirstReadOfKey = 1 << 3
	ReadOptBitMaskSnapshotRead               = 1 << 4

	commonReadOptBitOffset                      = 6
	commonReadOptBitMask                        = uint8((0xffff << commonReadOptBitOffset) & 0xff)
	CommonReadOptBitMaskWaitNoWriteIntent       = 1 << commonReadOptBitOffset
	RevertCommonReadOptBitMaskWaitNoWriteIntent = ^CommonReadOptBitMaskWaitNoWriteIntent & 0xff

	WriteOptBitMaskClearWriteIntent                   = 1
	WriteOptBitMaskRemoveVersion                      = 1 << 1
	WriteOptBitMaskRemoveVersionRollback              = 1 << 2
	WriteOptBitMaskReadForWrite                       = 1 << 3
	WriteOptBitMaskReadForWriteRollbackOrClearReadKey = 1 << 4
	WriteOptBitMaskTxnRecord                          = 1 << 5
	WriteOptBitMaskWriteByDifferentTxn                = 1 << 6

	ValueMetaBitMaskHasWriteIntent   = 1
	ValueMetaBitMaskClearWriteIntent = 0xfe
)

func IsWriteOptClearWriteIntent(flag uint8) bool {
	return flag&WriteOptBitMaskClearWriteIntent == WriteOptBitMaskClearWriteIntent
}

func IsWriteOptRemoveVersion(flag uint8) bool {
	return flag&WriteOptBitMaskRemoveVersion == WriteOptBitMaskRemoveVersion
}

func IsWriteOptRollbackVersion(flag uint8) bool {
	return flag&WriteOptBitMaskRemoveVersionRollback == WriteOptBitMaskRemoveVersionRollback
}

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
	MaxRetrySnapshotRead              = 5
	MaxRetryResolveFoundedWriteIntent = 2
)

const (
	DefaultReadTimeout = time.Second * 10
)

func InheritReadCommonFlag(flag1, flag2 uint8) uint8 {
	return flag1 | (flag2 & commonReadOptBitMask)
}

const (
	MaxReadForWriteQueueCapacityPerKey        = 500
	ReadForWriteQueueMaxReadersRatio          = 0.3333
	MaxWriteIntentWaitersCapacityPerTxnPerKey = 40
)
