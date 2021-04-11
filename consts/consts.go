package consts

import (
	"math"
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"
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
	KVCCReadOptBitMaskCheckVersion                  = 1 << 8

	KVReadOptBitMaskTxnRecord    = 1
	KVReadOptBitMaskExactVersion = 1 << 1
	KVReadOptBitMaskMetaOnly     = 1 << 7
)

const (
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
	TxnStateBitMaskCleared     = 1 << 4
	TxnStateBitMaskInvalid     = 1 << 5

	TxnStateBitMaskClearInvalidKeyState = (^TxnStateBitMaskInvalid) & 0xff

	TxnStateBitMaskTerminated = TxnStateBitMaskCommitted | TxnStateBitMaskAborted

	txnStateCheckBitMaskCommitted   = TxnStateBitMaskCommitted | TxnStateBitMaskInvalid
	txnStateCheckBitMaskAborted     = TxnStateBitMaskAborted | TxnStateBitMaskInvalid
	txnStateCheckBitMaskCleared     = TxnStateBitMaskCleared | TxnStateBitMaskInvalid
	txnStateCheckBitMaskRollbacking = TxnStateBitMaskAborted | TxnStateBitMaskCleared | TxnStateBitMaskInvalid
)

func IsCommitted(flag uint8) bool {
	return flag&txnStateCheckBitMaskCommitted == TxnStateBitMaskCommitted
}

func IsAborted(flag uint8) bool {
	return flag&txnStateCheckBitMaskAborted == TxnStateBitMaskAborted
}

func IsRollbacking(flag uint8) bool {
	return flag&txnStateCheckBitMaskRollbacking == TxnStateBitMaskAborted
}

func IsCleared(flag uint8) bool {
	return flag&txnStateCheckBitMaskCleared == TxnStateBitMaskCleared
}

// IsTerminated indicate either IsCommitted() or IsAborted()
func IsTerminated(flag uint8) bool {
	terminatedFlags := flag & TxnStateBitMaskTerminated
	assert.Must(terminatedFlags != TxnStateBitMaskTerminated) // can't be both committed and aborted
	return terminatedFlags > 0 && flag&TxnStateBitMaskInvalid == 0
}

const (
	ValueMetaBitMaskHasWriteIntent       = 1
	ValueMetaBitMaskCommitted            = TxnStateBitMaskCommitted // 1 << 2
	ValueMetaBitMaskAborted              = TxnStateBitMaskAborted   // 1 << 3
	ValueMetaBitMaskCleared              = TxnStateBitMaskCleared   // 1 << 4
	ValueMetaBitMaskInvalidKeyState      = TxnStateBitMaskInvalid   // 1 << 5
	ValueMetaBitMaskTxnRecord            = 1 << 6
	ValueMetaBitMaskPreventedFutureWrite = 1 << 7

	valueMetaBitMaskCommittedCleared  = ValueMetaBitMaskCommitted | ValueMetaBitMaskCleared
	valueMetaBitMaskRollbackedCleared = ValueMetaBitMaskAborted | ValueMetaBitMaskCleared

	ValueMetaBitMaskClearWriteIntent     = (^ValueMetaBitMaskHasWriteIntent) & 0xff
	ValueMetaBitMaskClearInvalidKeyState = (^ValueMetaBitMaskInvalidKeyState) & 0xff

	valueMetaTxnBits = ValueMetaBitMaskCommitted | ValueMetaBitMaskAborted | ValueMetaBitMaskCleared | ValueMetaBitMaskInvalidKeyState

	valueKeyStateCheckCommittedCleared  = valueMetaBitMaskCommittedCleared | ValueMetaBitMaskInvalidKeyState
	valueKeyStateCheckRollbackedCleared = valueMetaBitMaskRollbackedCleared | ValueMetaBitMaskInvalidKeyState
	valueKeyStateCheckUncommitted       = valueMetaTxnBits | ValueMetaBitMaskHasWriteIntent
)

func IsCommittedClearedValue(flag uint8) bool {
	return flag&valueKeyStateCheckCommittedCleared == valueMetaBitMaskCommittedCleared
}

func IsRollbackedClearedValue(flag uint8) bool {
	return flag&valueKeyStateCheckRollbackedCleared == valueMetaBitMaskRollbackedCleared
}

func IsUncommittedValue(flag uint8) bool {
	return flag&valueKeyStateCheckUncommitted == ValueMetaBitMaskHasWriteIntent
}

func ExtractTxnBits(state uint8) uint8 {
	return state & valueMetaTxnBits
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
	MinTxnLifeSpan     = time.Second
)

const (
	DefaultMaxReadModifyWriteQueueCapacityPerKey      = 500
	ReadModifyWriteQueueMaxReadersRatio               = 0.7
	ReadModifyWriteQueueMaxQueuedAgeRatio             = 0.8 // divide by staleWriteThreshold
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

func init() {
	assert.Must(ValueMetaBitMaskCommitted > ValueMetaBitMaskHasWriteIntent)
	assert.Must(ValueMetaBitMaskInvalidKeyState < ValueMetaBitMaskTxnRecord)
	assert.Must(ValueMetaBitMaskCleared < ValueMetaBitMaskTxnRecord)
}
