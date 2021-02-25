package consts

import "time"

const (
	DefaultTooStaleWriteThreshold = 5 * time.Second
	DefaultMaxClockDrift          = time.Second

	DefaultTxnManagerWorkerNumber = 12
)

func GetWaitTimestampCacheInvalidTimeout(staleWriteThr, maxClockDrift time.Duration) time.Duration {
	return staleWriteThr*2 + maxClockDrift*2
}
