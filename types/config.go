package types

import (
	"time"

	"github.com/leisurelyrcxf/spermwhale/errors"

	"github.com/leisurelyrcxf/spermwhale/consts"
)

type TxnManagerConfig struct {
	WoundUncommittedTxnThreshold time.Duration
	ClearWorkerNum, IOWorkerNum  int
	MaxTaskBufferedPerPartition  int
}

func NewTxnManagerConfig(woundUncommittedTxnThreshold time.Duration) TxnManagerConfig {
	return TxnManagerConfig{
		WoundUncommittedTxnThreshold: woundUncommittedTxnThreshold,
		ClearWorkerNum:               consts.DefaultTxnManagerClearWorkerNumber,
		IOWorkerNum:                  consts.DefaultTxnManagerIOWorkerNumber,
		MaxTaskBufferedPerPartition:  consts.DefaultTxnManagerMaxIOTaskBufferedPerPartition,
	}
}

func (cfg TxnManagerConfig) WithClearWorkNum(clearWorkerNum int) TxnManagerConfig {
	cfg.ClearWorkerNum = clearWorkerNum
	return cfg
}

func (cfg TxnManagerConfig) WithIOWorkerNum(ioWorkerNum int) TxnManagerConfig {
	cfg.IOWorkerNum = ioWorkerNum
	return cfg
}

func (cfg TxnManagerConfig) WithMaxTaskBufferedPerPartition(maxTaskBufferedPerPartition int) TxnManagerConfig {
	cfg.MaxTaskBufferedPerPartition = maxTaskBufferedPerPartition
	return cfg
}

func (cfg TxnManagerConfig) Validate() error {
	if cfg.WoundUncommittedTxnThreshold <= time.Second {
		return errors.Annotatef(errors.ErrInvalidConfig, "cfg.WoundUncommittedTxnThreshold <= time.Second")
	}
	return nil
}

func (cfg TxnManagerConfig) WithWoundUncommittedTxnThreshold(woundUncommittedTxnThreshold time.Duration) TxnManagerConfig {
	cfg.WoundUncommittedTxnThreshold = woundUncommittedTxnThreshold
	return cfg
}

type TabletTxnConfig struct {
	StaleWriteThreshold time.Duration
	MaxClockDrift       time.Duration
}

func NewTabletTxnConfig(staleWriteThreshold time.Duration) TabletTxnConfig {
	return TabletTxnConfig{
		StaleWriteThreshold: staleWriteThreshold,
		MaxClockDrift:       consts.DefaultMaxClockDrift,
	}
}

func (cfg TabletTxnConfig) Validate() error {
	if cfg.StaleWriteThreshold <= time.Second {
		return errors.Annotatef(errors.ErrInvalidConfig, "cfg.StaleWriteThreshold <= time.Second")
	}
	if cfg.MaxClockDrift <= 0 {
		return errors.Annotatef(errors.ErrInvalidConfig, "cfg.MaxClockDrift <= 0")
	}
	return nil
}

func (cfg TabletTxnConfig) GetWaitTimestampCacheInvalidTimeout() time.Duration {
	return cfg.StaleWriteThreshold + cfg.MaxClockDrift*10
}

func (cfg TabletTxnConfig) WithStaleWriteThreshold(val time.Duration) TabletTxnConfig {
	cfg.StaleWriteThreshold = val
	return cfg
}

func (cfg TabletTxnConfig) WithMaxClockDrift(val time.Duration) TabletTxnConfig {
	cfg.MaxClockDrift = val
	return cfg
}
