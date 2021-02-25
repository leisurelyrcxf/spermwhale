package consts

import "time"

const (
	MaxClockDrift                    = time.Second
	TooStaleWriteThreshold           = 5 * time.Second
	WaitTimestampCacheInvalidTimeout = TooStaleWriteThreshold*2 + MaxClockDrift*2
)
