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
	DefaultStaleWriteThreshold          = 10 * time.Second
	DefaultMaxClockDrift                = time.Second
	DefaultWoundUncommittedTxnThreshold = 10 * time.Second

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
	KVCCReadOptBitMaskMetaOnly                      = 1 << 7

	KVReadOptBitMaskTxnRecord    = 1
	KVReadOptBitMaskExactVersion = 1 << 1
	KVReadOptBitMaskMetaOnly     = 1 << 7
)

const (
	KVKVCCWriteOptOptBitMaskTxnRecord = 1

	KVCCWriteOptBitMaskReadModifyWrite = 1 << 2

	CommonKVCCOpsOptBitMaskOperatedByDifferentTxn = 1
	CommonKVCCOpsOptBitMaskIsReadOnlyKey          = 1 << 1
	CommonKVCCOpsOptBitMaskReadModifyWrite        = 1 << 2

	KVCCUpdateMetaOptBitMaskStartBit           = 5
	KVCC2KVUpdateMetaOptExtractor              = (0xff << KVCCUpdateMetaOptBitMaskStartBit) & 0xff
	KVKVCCUpdateMetaOptBitMaskClearWriteIntent = 1 << KVCCUpdateMetaOptBitMaskStartBit

	KVCCRemoveTxnRecordOptBitMaskRollback = 1 << 6
)

const (
	TxnSnapshotReadOptionBitMaskExplicitSnapshotVersion           = 1
	TxnSnapshotReadOptionBitMaskRelativeSnapshotVersion           = 1 << 1
	TxnSnapshotReadOptionBitMaskRelativeMinAllowedSnapshotVersion = 1 << 2
	TxnSnapshotReadOptionBitMaskDontAllowVersionBack              = 1 << 3
)

const (
	TxnStateBitMaskUncommitted = 1
	TxnStateBitMaskStaging     = 1 << 1
	TxnStateBitMaskCommitted   = 1 << 2
	TxnStateBitMaskAborted     = 1 << 3
	TxnStateBitMaskDone        = 1 << 4

	TxnStateTerminatedMask = TxnStateBitMaskCommitted | TxnStateBitMaskAborted
)

const (
	ValueMetaBitMaskHasWriteIntent       = 1
	ValueMetaBitMaskCommitted            = TxnStateBitMaskCommitted // 1 << 2
	ValueMetaBitMaskAborted              = TxnStateBitMaskAborted   // 1 << 3
	ValueMetaBitMaskDone                 = TxnStateBitMaskDone      // 1 << 4
	ValueMetaBitMaskPreventedFutureWrite = 1 << 7

	ValueMetaBitMaskTerminated = ValueMetaBitMaskCommitted | ValueMetaBitMaskAborted

	clearWriteIntent = (^ValueMetaBitMaskHasWriteIntent) & 0xff
)

func IsDirty(flag uint8) bool {
	return flag&ValueMetaBitMaskHasWriteIntent == ValueMetaBitMaskHasWriteIntent
}

func WithCommitted(flag uint8) uint8 {
	flag &= clearWriteIntent
	flag |= ValueMetaBitMaskCommitted
	return flag
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
	MinTxnLifeSpan     = time.Second * 2
)

const (
	MaxReadModifyWriteQueueCapacityPerKey             = 500
	ReadModifyWriteQueueMaxReadersRatio               = 0.3333
	ReadModifyWriteTxnMinSupportedStaleWriteThreshold = 400 * time.Millisecond

	MaxWriteIntentWaitersCapacityPerTxnPerKey = 40
)

const (
	DefaultTimestampCacheMaxBufferedWriters           = 1024
	DefaultTimestampCacheMaxBufferedWritersLowerRatio = 0.8
	DefaultTimestampCacheMaxSeekedWritingWriters      = 6
)

func GetTimestampCacheMaxBufferedWritersLower(timestampCacheMaxBufferedWriters int, lowerRatio float64) int {
	return int(float64(timestampCacheMaxBufferedWriters) * lowerRatio)
}
