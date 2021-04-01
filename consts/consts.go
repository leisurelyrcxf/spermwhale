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

	DefaultTxnManagerClearerNumber              = 32
	DefaultTxnManagerClearJobTimeout            = time.Second * 10
	DefaultTxnManagerWriterNumber               = 32
	DefaultTxnManagerReaderNumber               = 32
	DefaultTxnManagerMaxBufferedJobPerPartition = 10000
	DefaultSnapshotBackwardPeriod               = time.Second
)

const (
	KVCCReadOptBitMaskTxnRecord                     = 1
	KVCCReadOptBitMaskNotUpdateTimestampCache       = 1 << 1
	KVCCReadOptBitMaskNotGetMaxReadVersion          = 1 << 2
	KVCCReadOptBitMaskReadModifyWrite               = 1 << 3
	KVCCReadOptBitMaskReadModifyWriteFirstReadOfKey = 1 << 4
	KVCCReadOptBitMaskSnapshotRead                  = 1 << 5
	KVCCReadOptBitMaskWaitWhenReadDirty             = 1 << 6

	KVReadOptBitMaskTxnRecord    = 1
	KVReadOptBitMaskExactVersion = 1 << 7
)

const (
	CommonWriteOptBitMaskTxnRecord        = 1
	CommonWriteOptBitMaskClearWriteIntent = 1 << 1
	CommonWriteOptBitMaskRemoveVersion    = 1 << 2

	KVCCWriteOptBitMaskWriteByDifferentTxn                   = 1 << 3
	KVCCWriteOptBitMaskRemoveVersionRollback                 = 1 << 4
	KVCCWriteOptBitMaskReadModifyWrite                       = 1 << 5
	KVCCWriteOptBitMaskReadModifyWriteRollbackOrClearReadKey = 1 << 6
)

const (
	TxnSnapshotReadOptionBitMaskExplicitSnapshotVersion           = 1
	TxnSnapshotReadOptionBitMaskRelativeSnapshotVersion           = 1 << 1
	TxnSnapshotReadOptionBitMaskRelativeMinAllowedSnapshotVersion = 1 << 2
	TxnSnapshotReadOptionBitMaskDontAllowVersionBack              = 1 << 3
)

const (
	ValueMetaBitMaskHasWriteIntent   = 1
	ValueMetaBitMaskClearWriteIntent = (^1) & 0xff
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

const (
	MaxReadModifyWriteQueueCapacityPerKey     = 500
	ReadModifyWriteQueueMaxReadersRatio       = 0.3333
	MaxWriteIntentWaitersCapacityPerTxnPerKey = 40
)

const (
	DefaultTimestampCacheMaxBufferedWriters           = 1024
	DefaultTimestampCacheMaxBufferedWritersLowerRatio = 0.5
)
