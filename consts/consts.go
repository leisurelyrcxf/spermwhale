package consts

import "time"

const (
	DefaultTooStaleWriteThreshold = 5 * time.Second
	DefaultMaxClockDrift          = time.Second

	DefaultTxnManagerClearWorkerNumber = 20
	DefaultTxnManagerIOWorkerNumber    = 30
)

const (
	ReadOptBitMaskNotUpdateTimestampCache = 1
	ReadOptBitMaskNotGetMaxReadVersion    = 1 << 1

	WriteOptBitMaskClearWriteIntent = 1
	WriteOptBitMaskRemoveVersion    = 1 << 1

	ValueMetaBitMaskHasWriteIntent = 1
)

const (
	DefaultTabletServerPort = 20000
	DefaultOracleServerPort = 5555
	DefaultTxnServerPort    = 9999
	DefaultKVServerPort     = 10001
)
