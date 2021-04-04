package types

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/assert"

	"github.com/leisurelyrcxf/spermwhale/consts"
	. "github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/proto/kvccpb"
)

type KVCCReadOption struct {
	ReaderVersion             uint64
	flag                      uint8
	ExactVersion              uint64
	MinAllowedSnapshotVersion uint64
}

func NewKVCCReadOption(readerVersion uint64) KVCCReadOption {
	return KVCCReadOption{
		ReaderVersion: readerVersion,
	}
}

func NewSnapshotKVCCReadOption(snapshotVersion uint64, minAllowedSnapshotVersion uint64) KVCCReadOption {
	opt := NewKVCCReadOption(snapshotVersion)
	opt.flag |= KVCCReadOptBitMaskSnapshotRead
	opt.flag |= KVCCReadOptBitMaskNotGetMaxReadVersion
	opt.MinAllowedSnapshotVersion = minAllowedSnapshotVersion
	return opt
}

func NewKVCCReadOptionFromPB(x *kvccpb.KVCCReadOption) KVCCReadOption {
	return KVCCReadOption{
		ReaderVersion:             x.ReaderVersion,
		flag:                      x.GetFlagSafe(),
		ExactVersion:              x.ExactVersion,
		MinAllowedSnapshotVersion: x.MinAllowedSnapshotVersion,
	}
}

func (opt KVCCReadOption) ToPB() *kvccpb.KVCCReadOption {
	return (&kvccpb.KVCCReadOption{
		ReaderVersion:             opt.ReaderVersion,
		ExactVersion:              opt.ExactVersion,
		MinAllowedSnapshotVersion: opt.MinAllowedSnapshotVersion,
	}).SetFlagSafe(opt.flag)
}

func (opt KVCCReadOption) WithNotUpdateTimestampCache() KVCCReadOption {
	opt.flag |= KVCCReadOptBitMaskNotUpdateTimestampCache
	return opt
}

func (opt KVCCReadOption) WithNotGetMaxReadVersion() KVCCReadOption {
	opt.flag |= KVCCReadOptBitMaskNotGetMaxReadVersion
	return opt
}

func (opt KVCCReadOption) CondReadModifyWrite(b bool) KVCCReadOption {
	if b {
		opt.flag |= KVCCReadOptBitMaskReadModifyWrite
	}
	return opt
}

func (opt KVCCReadOption) CondReadModifyWriteFirstReadOfKey(b bool) KVCCReadOption {
	if b {
		opt.flag |= KVCCReadOptBitMaskReadModifyWriteFirstReadOfKey
	}
	return opt
}

func (opt KVCCReadOption) WithTxnRecord() KVCCReadOption {
	opt.flag |= KVCCReadOptBitMaskTxnRecord
	return opt
}

func (opt KVCCReadOption) WithExactVersion(exactVersion uint64) KVCCReadOption {
	opt.ExactVersion = exactVersion
	return opt
}

func (opt KVCCReadOption) WithIncrReaderVersion() KVCCReadOption {
	opt.ReaderVersion += 1
	return opt
}

func (opt KVCCReadOption) CondWaitWhenReadDirty(b bool) KVCCReadOption {
	if b {
		opt.flag |= consts.KVCCReadOptBitMaskWaitWhenReadDirty
	}
	return opt
}

func (opt KVCCReadOption) WithMetaOnly() KVCCReadOption {
	opt.flag |= consts.KVCCReadOptBitMaskMetaOnly
	return opt
}

func (opt KVCCReadOption) IsReadExactVersion() bool {
	return opt.ExactVersion != 0
}

func (opt KVCCReadOption) IsTxnRecord() bool {
	return opt.flag&KVCCReadOptBitMaskTxnRecord == KVCCReadOptBitMaskTxnRecord
}

func (opt KVCCReadOption) IsUpdateTimestampCache() bool {
	return opt.flag&KVCCReadOptBitMaskNotUpdateTimestampCache == 0
}

func (opt KVCCReadOption) IsGetMaxReadVersion() bool {
	return opt.flag&KVCCReadOptBitMaskNotGetMaxReadVersion == 0
}

func (opt KVCCReadOption) IsReadModifyWrite() bool {
	return opt.flag&KVCCReadOptBitMaskReadModifyWrite == KVCCReadOptBitMaskReadModifyWrite
}

func (opt KVCCReadOption) IsReadModifyWriteFirstReadOfKey() bool {
	return opt.flag&KVCCReadOptBitMaskReadModifyWriteFirstReadOfKey == KVCCReadOptBitMaskReadModifyWriteFirstReadOfKey
}

func (opt KVCCReadOption) IsSnapshotRead() bool {
	return opt.flag&KVCCReadOptBitMaskSnapshotRead == KVCCReadOptBitMaskSnapshotRead
}

func (opt KVCCReadOption) IsWaitWhenReadDirty() bool {
	return opt.flag&KVCCReadOptBitMaskWaitWhenReadDirty == KVCCReadOptBitMaskWaitWhenReadDirty
}

func (opt KVCCReadOption) GetKVReadVersion() uint64 {
	if opt.IsReadExactVersion() {
		return opt.ExactVersion
	}
	return opt.ReaderVersion
}

func (opt KVCCReadOption) ToKVReadOption() (kvOpt KVReadOption) {
	if opt.IsReadExactVersion() {
		kvOpt = NewKVReadOption(opt.ExactVersion).WithExactVersion()
	} else {
		kvOpt = NewKVReadOption(opt.ReaderVersion)
	}
	if opt.IsTxnRecord() {
		kvOpt.Flag |= KVReadOptBitMaskTxnRecord
	}
	return kvOpt
}

func (opt KVCCReadOption) WithKVReadVersion(kvReadVersion uint64) (kvOpt KVReadOption) {
	assert.Must(kvReadVersion != 0)

	if kvOpt = opt.ToKVReadOption(); kvOpt.IsReadExactVersion() {
		assert.Must(kvReadVersion == kvOpt.Version)
	} else if kvReadVersion < kvOpt.Version {
		kvOpt.Version = kvReadVersion
	}
	return kvOpt
}

type KVCCWriteOption struct {
	flag uint8
}

func NewKVCCWriteOption() KVCCWriteOption {
	return KVCCWriteOption{}
}

func NewKVCCWriteOptionFromPB(x *kvccpb.KVCCWriteOption) KVCCWriteOption {
	return KVCCWriteOption{
		flag: x.GetFlagSafe(),
	}
}

func (opt *KVCCWriteOption) ToPB() *kvccpb.KVCCWriteOption {
	return (&kvccpb.KVCCWriteOption{}).SetFlagSafe(opt.flag)
}

func (opt KVCCWriteOption) WithClearWriteIntent() KVCCWriteOption {
	opt.flag |= CommonWriteOptBitMaskClearWriteIntent
	return opt
}

func (opt KVCCWriteOption) WithRollbackVersion() KVCCWriteOption {
	opt.flag |= CommonWriteOptBitMaskRemoveVersion
	opt.flag |= KVCCWriteOptBitMaskRemoveVersionRollback
	return opt
}

func (opt KVCCWriteOption) WithRemoveTxnRecordCondRollback(isRollback bool) KVCCWriteOption {
	opt.flag |= CommonWriteOptBitMaskRemoveVersion
	if isRollback {
		opt.flag |= KVCCWriteOptBitMaskRemoveVersionRollback
	}
	return opt.WithTxnRecord()
}

func (opt KVCCWriteOption) CondReadModifyWrite(b bool) KVCCWriteOption {
	if b {
		opt.flag |= KVCCWriteOptBitMaskReadModifyWrite
	}
	return opt
}

func (opt KVCCWriteOption) CondReadModifyWriteRollbackOrClearReadKey(b bool) KVCCWriteOption {
	if b {
		opt.flag |= KVCCWriteOptBitMaskReadModifyWriteRollbackOrClearReadKey
	}
	return opt
}

func (opt KVCCWriteOption) CondWriteByDifferentTransaction(b bool) KVCCWriteOption {
	if b {
		opt.flag |= KVCCWriteOptBitMaskWriteByDifferentTxn
	}
	return opt
}

func (opt KVCCWriteOption) WithTxnRecord() KVCCWriteOption {
	opt.flag |= CommonWriteOptBitMaskTxnRecord
	return opt
}

func (opt KVCCWriteOption) IsTxnRecord() bool {
	return consts.IsWriteTxnRecord(opt.flag)
}

func (opt KVCCWriteOption) IsClearWriteIntent() bool {
	return IsWriteOptClearWriteIntent(opt.flag)
}

func (opt KVCCWriteOption) IsRemoveVersion() bool {
	return IsWriteOptRemoveVersion(opt.flag)
}

func (opt KVCCWriteOption) IsRollbackVersion() bool {
	return IsWriteOptRollbackVersion(opt.flag)
}

func (opt KVCCWriteOption) IsReadModifyWrite() bool {
	return opt.flag&KVCCWriteOptBitMaskReadModifyWrite == KVCCWriteOptBitMaskReadModifyWrite
}

func (opt KVCCWriteOption) IsReadModifyWriteRollbackOrClearReadKey() bool {
	return opt.flag&KVCCWriteOptBitMaskReadModifyWriteRollbackOrClearReadKey == KVCCWriteOptBitMaskReadModifyWriteRollbackOrClearReadKey
}

func (opt KVCCWriteOption) IsWriteByDifferentTransaction() bool {
	return opt.flag&KVCCWriteOptBitMaskWriteByDifferentTxn == KVCCWriteOptBitMaskWriteByDifferentTxn
}

func (opt KVCCWriteOption) IsRollbackKey() bool {
	return !opt.IsTxnRecord() && opt.IsRollbackVersion()
}

func (opt KVCCWriteOption) ToKVWriteOption() KVWriteOption {
	return KVWriteOption{flag: opt.flag}
}

type KVCC interface {
	Get(ctx context.Context, key string, opt KVCCReadOption) (ValueCC, error)
	Set(ctx context.Context, key string, val Value, opt KVCCWriteOption) error
	Close() error
}
