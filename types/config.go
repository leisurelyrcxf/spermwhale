package types

import "time"

type TxnConfig struct {
	StaleWriteThreshold time.Duration
	MaxClockDrift       time.Duration
}

func (c TxnConfig) GetWaitTimestampCacheInvalidTimeout() time.Duration {
	return c.StaleWriteThreshold*2 + c.MaxClockDrift*2
}
