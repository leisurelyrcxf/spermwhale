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
	KVCCReadOptBitMaskTxnRecord                  = 1
	KVCCReadOptBitMaskNotUpdateTimestampCache    = 1 << 1
	KVCCReadOptBitMaskNotGetMaxReadVersion       = 1 << 2
	KVCCReadOptBitMaskReadForWrite               = 1 << 3
	KVCCReadOptBitMaskReadForWriteFirstReadOfKey = 1 << 4
	KVCCReadOptBitMaskSnapshotRead               = 1 << 5 // NOTE run out of all bits...

	txnKVCCCommonReadOptBitOffset                     = 6
	txnKVCCCommonReadOptBitMask                       = uint8((0xffff << txnKVCCCommonReadOptBitOffset) & 0xff)
	TxnKVCCCommonReadOptBitMaskWaitNoWriteIntent      = 1 << txnKVCCCommonReadOptBitOffset
	TxnKVCCCommonReadOptBitMaskClearWaitNoWriteIntent = ^TxnKVCCCommonReadOptBitMaskWaitNoWriteIntent & 0xff

	KVReadOptBitMaskTxnRecord    = 1
	KVReadOptBitMaskExactVersion = 1 << 7
)

const (
	CommonWriteOptBitMaskTxnRecord        = 1
	CommonWriteOptBitMaskClearWriteIntent = 1 << 1
	CommonWriteOptBitMaskRemoveVersion    = 1 << 2

	KVCCWriteOptBitMaskWriteByDifferentTxn                = 1 << 3
	KVCCWriteOptBitMaskRemoveVersionRollback              = 1 << 4
	KVCCWriteOptBitMaskReadForWrite                       = 1 << 5
	KVCCWriteOptBitMaskReadForWriteRollbackOrClearReadKey = 1 << 6
)

const (
	ValueMetaBitMaskHasWriteIntent   = 1
	ValueMetaBitMaskClearWriteIntent = 0xfe
)

func IsWriteTxnRecord(flag uint8) bool {
	return flag&CommonWriteOptBitMaskTxnRecord == CommonWriteOptBitMaskTxnRecord
}

func IsWriteOptClearWriteIntent(flag uint8) bool {
	return flag&CommonWriteOptBitMaskClearWriteIntent == CommonWriteOptBitMaskClearWriteIntent
}

func IsWriteOptRemoveVersion(flag uint8) bool {
	return flag&CommonWriteOptBitMaskRemoveVersion == CommonWriteOptBitMaskRemoveVersion
}

func IsWriteOptRollbackVersion(flag uint8) bool {
	return flag&KVCCWriteOptBitMaskRemoveVersionRollback == KVCCWriteOptBitMaskRemoveVersionRollback
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

func InheritReadCommonFlag(flag1, from uint8) uint8 {
	return flag1 | (from & txnKVCCCommonReadOptBitMask)
}

const (
	MaxReadForWriteQueueCapacityPerKey        = 500
	ReadForWriteQueueMaxReadersRatio          = 0.3333
	MaxWriteIntentWaitersCapacityPerTxnPerKey = 40
)
