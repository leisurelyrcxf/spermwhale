package types

import "time"

type TxnConfig struct {
	StaleWriteThreshold time.Duration
	MaxClockDrift       time.Duration
}

func (c TxnConfig) GetWaitTimestampCacheInvalidTimeout() time.Duration {
	return c.StaleWriteThreshold + c.MaxClockDrift*10
}

func (c TxnConfig) SetStaleWriteThreshold(val time.Duration) TxnConfig {
	c.StaleWriteThreshold = val
	return c
}
