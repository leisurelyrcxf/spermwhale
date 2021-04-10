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

func (cfg TabletTxnConfig) SupportReadModifyWriteTxn() bool {
	return cfg.StaleWriteThreshold >= consts.ReadModifyWriteTxnMinSupportedStaleWriteThreshold
}

var DefaultReadModifyWriteQueueCfg = NewReadModifyWriteQueueCfg(consts.DefaultMaxReadModifyWriteQueueCapacityPerKey)

type ReadModifyWriteQueueCfg struct {
	CapacityPerKey    int
	MaxReadersRatio   float64
	MaxQueuedAgeRatio float64 // StaleWriteThreshold
}

func NewReadModifyWriteQueueCfg(capacityPerKey int) ReadModifyWriteQueueCfg {
	return ReadModifyWriteQueueCfg{
		CapacityPerKey:    capacityPerKey,
		MaxReadersRatio:   consts.ReadModifyWriteQueueMaxReadersRatio,
		MaxQueuedAgeRatio: consts.ReadModifyWriteQueueMaxQueuedAgeRatio,
	}
}

func (c ReadModifyWriteQueueCfg) Validate() error {
	if c.CapacityPerKey <= 0 {
		return errors.Annotatef(errors.ErrInvalidConfig, "ReadModifyWriteQueueCfg::CapacityPerKey <= 0")
	}
	if c.MaxReadersRatio <= 0 {
		return errors.Annotatef(errors.ErrInvalidConfig, "ReadModifyWriteQueueCfg::c.MaxReadersRatio <= 0")
	}
	if c.MaxReadersRatio >= 0.9 {
		return errors.Annotatef(errors.ErrInvalidConfig, "ReadModifyWriteQueueCfg::c.MaxReadersRatio >= 0.9")
	}
	if c.MaxQueuedAgeRatio <= 0 {
		return errors.Annotatef(errors.ErrInvalidConfig, "ReadModifyWriteQueueCfg::c.MaxQueuedAgeRatio <= 0")
	}
	if c.MaxQueuedAgeRatio >= 0.9 {
		return errors.Annotatef(errors.ErrInvalidConfig, "ReadModifyWriteQueueCfg::c.MaxQueuedAgeRatio >= 0.9")
	}
	return nil
}

var (
	DefaultTableTxnManagerCfg = NewTabletTxnManagerConfig(DefaultTableTxnCfg, DefaultReadModifyWriteQueueCfg)
	TestTableTxnManagerCfg    = DefaultTableTxnManagerCfg.WithTest()
)

type TabletTxnManagerConfig struct {
	TabletTxnConfig
	ReadModifyWriteQueueCfg
	MinGCThreadMinInterrupt time.Duration
	Test                    bool

	// outputs
	TxnLifeSpan        time.Duration
	TxnInsertThreshold time.Duration
}

func NewTabletTxnManagerConfig(
	tabletCfg TabletTxnConfig,
	readModifyWriteQueueCfg ReadModifyWriteQueueCfg) TabletTxnManagerConfig {
	cfg := TabletTxnManagerConfig{
		TabletTxnConfig:         tabletCfg,
		ReadModifyWriteQueueCfg: readModifyWriteQueueCfg,
		MinGCThreadMinInterrupt: time.Second,
	}
	cfg.Sanitize()
	return cfg
}

func (c TabletTxnManagerConfig) Validate() error {
	if err := c.TabletTxnConfig.Validate(); err != nil {
		return err
	}
	return c.ReadModifyWriteQueueCfg.Validate()
}

func (c *TabletTxnManagerConfig) Sanitize() *TabletTxnManagerConfig {
	c.TxnLifeSpan = c.StaleWriteThreshold*5/2 + c.MaxClockDrift*10 + consts.MinTxnLifeSpan
	c.TxnInsertThreshold = c.StaleWriteThreshold / 10 * 9
	return c
}

func (c TabletTxnManagerConfig) WithStaleWriteThreshold(staleWriteThr time.Duration) TabletTxnManagerConfig {
	c.StaleWriteThreshold = staleWriteThr
	c.Sanitize()
	return c
}

func (c TabletTxnManagerConfig) WithMaxClockDrift(maxClockDrift time.Duration) TabletTxnManagerConfig {
	c.MaxClockDrift = maxClockDrift
	c.Sanitize()
	return c
}

func (c TabletTxnManagerConfig) WithTest() TabletTxnManagerConfig {
	c.Test = true
	return c
}

func (c TabletTxnManagerConfig) CondTest(b bool) TabletTxnManagerConfig {
	c.Test = b
	return c
}
