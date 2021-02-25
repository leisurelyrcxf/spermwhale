package consts

import "time"

const (
	DefaultTooStaleWriteThreshold = 5 * time.Second
	DefaultMaxClockDrift          = time.Second

	DefaultTxnManagerWorkerNumber = 12
)
