package consts

import "time"

const (
	DefaultTooStaleWriteThreshold = 5 * time.Second
	DefaultMaxClockDrift          = time.Second

	DefaultTxnManagerClearWorkerNumber = 20
	DefaultTxnManagerIOWorkerNumber    = 30
)

const (
	ReadOptBitMaskExactVersion            = 1
	ReadOptBitMaskNotUpdateTimestampCache = 1 << 1
	ReadOptBitMaskNotGetMaxReadVersion    = 1 << 2

	WriteOptBitMaskClearWriteIntent = 1
	WriteOptBitMaskRemoveVersion    = 1 << 1

	ValueMetaBitMaskHasWriteIntent                    = 1
	ValueMetaBitMaskMaxReadVersionBiggerThanRequested = 1 << 1
)
