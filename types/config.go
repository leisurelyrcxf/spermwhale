package types

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/leisurelyrcxf/spermwhale/errors"

	"github.com/leisurelyrcxf/spermwhale/consts"
)

type TxnConfig struct {
	WoundUncommittedTxnThreshold time.Duration
	ClearTimeout                 time.Duration
}

type TxnManagerConfig struct {
	TxnConfig
	ClearerNum, WriterNum, ReaderNum int
	MaxTaskBufferedPerPartition      int
}

func NewTxnManagerConfig(woundUncommittedTxnThreshold time.Duration) TxnManagerConfig {
	return TxnManagerConfig{
		TxnConfig: TxnConfig{
			WoundUncommittedTxnThreshold: woundUncommittedTxnThreshold,
			ClearTimeout:                 consts.DefaultTxnManagerClearJobTimeout,
		},
		ClearerNum:                  consts.DefaultTxnManagerClearerNumber,
		WriterNum:                   consts.DefaultTxnManagerWriterNumber,
		ReaderNum:                   consts.DefaultTxnManagerReaderNumber,
		MaxTaskBufferedPerPartition: consts.DefaultTxnManagerMaxBufferedJobPerPartition,
	}
}

func (cfg TxnManagerConfig) WithClearerNum(clearerNum int) TxnManagerConfig {
	cfg.ClearerNum = clearerNum
	return cfg
}

func (cfg TxnManagerConfig) WithWriterNum(writerNum int) TxnManagerConfig {
	cfg.WriterNum = writerNum
	return cfg
}

func (cfg TxnManagerConfig) WithReaderNum(readerNum int) TxnManagerConfig {
	cfg.ReaderNum = readerNum
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

type TabletTxnConfigMarshaller struct {
	StaleWriteThreshold string
	MaxClockDrift       string
}

type TabletTxnConfig struct {
	StaleWriteThreshold time.Duration
	MaxClockDrift       time.Duration
}

var DefaultTableTxnCfg = NewTabletTxnConfig(consts.DefaultStaleWriteThreshold)

func NewTabletTxnConfig(staleWriteThreshold time.Duration) TabletTxnConfig {
	return TabletTxnConfig{
		StaleWriteThreshold: staleWriteThreshold,
		MaxClockDrift:       consts.DefaultMaxClockDrift,
	}
}

func (cfg TabletTxnConfig) String() string {
	bytes, err := json.Marshal(TabletTxnConfigMarshaller{
		StaleWriteThreshold: cfg.StaleWriteThreshold.String(),
		MaxClockDrift:       cfg.MaxClockDrift.String(),
	})
	if err != nil {
		return fmt.Sprintf("TabletTxnConfig{'%s'}", err.Error())
	}
	return string(bytes)
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
	return cfg.StaleWriteThreshold + cfg.MaxClockDrift*10 + consts.MinTxnLifeSpan
}

func (cfg TabletTxnConfig) WithStaleWriteThreshold(val time.Duration) TabletTxnConfig {
	cfg.StaleWriteThreshold = val
	return cfg
}

func (cfg TabletTxnConfig) WithMaxClockDrift(val time.Duration) TabletTxnConfig {
	cfg.MaxClockDrift = val
	return cfg
}

func (cfg TabletTxnConfig) SupportReadModifyWriteTxn() bool {
	return cfg.StaleWriteThreshold >= consts.ReadModifyWriteTxnMinSupportedStaleWriteThreshold
}

var DefaultReadModifyWriteQueueCfg = NewReadModifyWriteQueueCfg(
	consts.MaxReadModifyWriteQueueCapacityPerKey,
	consts.ReadModifyWriteQueueMaxReadersRatio,
	consts.DefaultStaleWriteThreshold)

type ReadModifyWriteQueueCfg struct {
	CapacityPerKey  int
	MaxReadersRatio float64
	MaxQueuedAge    time.Duration
}

func NewReadModifyWriteQueueCfg(
	capacityPerKey int,
	maxReadersRatio float64,
	maxQueuedAge time.Duration) ReadModifyWriteQueueCfg {
	return ReadModifyWriteQueueCfg{
		CapacityPerKey:  capacityPerKey,
		MaxReadersRatio: maxReadersRatio,
		MaxQueuedAge:    maxQueuedAge,
	}
}

func (cfg ReadModifyWriteQueueCfg) WithMaxQueuedAge(maxQueuedAge time.Duration) ReadModifyWriteQueueCfg {
	cfg.MaxQueuedAge = maxQueuedAge
	return cfg
}

var DefaultTableTxnManagerCfg = NewTabletTxnManagerConfig(DefaultTableTxnCfg, DefaultReadModifyWriteQueueCfg)

type TabletTxnManagerConfig struct {
	TabletTxnConfig
	ReadModifyWriteQueueCfg

	// outputs
	TxnLifeSpan        time.Duration
	TxnInsertThreshold time.Duration
}

func NewTabletTxnManagerConfig(
	tabletCfg TabletTxnConfig,
	readModifyWriteQueueCfg ReadModifyWriteQueueCfg) TabletTxnManagerConfig {
	return TabletTxnManagerConfig{
		TabletTxnConfig:         tabletCfg,
		ReadModifyWriteQueueCfg: readModifyWriteQueueCfg,
	}.Sanitize()
}

func (c TabletTxnManagerConfig) Sanitize() TabletTxnManagerConfig {
	c.TxnLifeSpan = c.TabletTxnConfig.GetWaitTimestampCacheInvalidTimeout()
	c.TxnInsertThreshold = c.StaleWriteThreshold / 10 * 9
	return c
}
