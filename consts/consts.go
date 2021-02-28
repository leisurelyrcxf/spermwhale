package consts

import "time"

const (
	DefaultTooStaleWriteThreshold = 5 * time.Second
	DefaultMaxClockDrift          = time.Second

	DefaultTxnManagerClearWorkerNumber = 20
	DefaultTxnManagerIOWorkerNumber    = 30
)
