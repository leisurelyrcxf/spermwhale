package types

import (
	"context"

	. "github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/proto/kvccpb"
)

type KVCCReadOption struct {
	ReaderVersion uint64
	flag          uint8
	ExactVersion  uint64
}

func NewKVCCReadOption(readerVersion uint64) KVCCReadOption {
	return KVCCReadOption{
		ReaderVersion: readerVersion,
	}
}

func NewKVCCReadOptionFromPB(x *kvccpb.KVCCReadOption) KVCCReadOption {
	return KVCCReadOption{
		ReaderVersion: x.ReaderVersion,
		flag:          x.GetFlagSafe(),
		ExactVersion:  x.ExactVersion,
	}
}

func (opt KVCCReadOption) ToPB() *kvccpb.KVCCReadOption {
	return (&kvccpb.KVCCReadOption{
		ReaderVersion: opt.ReaderVersion,
		ExactVersion:  opt.ExactVersion,
	}).SetFlagSafe(opt.flag)
}

func (opt KVCCReadOption) WithNotUpdateTimestampCache() KVCCReadOption {
	opt.flag |= ReadOptBitMaskNotUpdateTimestampCache
	return opt
}

func (opt KVCCReadOption) WithNotGetMaxReadVersion() KVCCReadOption {
	opt.flag |= ReadOptBitMaskNotGetMaxReadVersion
	return opt
}

func (opt KVCCReadOption) WithClearWaitNoWriteIntent() KVCCReadOption {
	opt.flag &= RevertCommonReadOptBitMaskWaitNoWriteIntent
	return opt
}

func (opt KVCCReadOption) CondReadForWrite(b bool) KVCCReadOption {
	if b {
		opt.flag |= ReadOptBitMaskReadForWrite
	}
	return opt
}

func (opt KVCCReadOption) CondReadForWriteFirstReadOfKey(b bool) KVCCReadOption {
	if b {
		opt.flag |= ReadOptBitMaskReadForWriteFirstReadOfKey
	}
	return opt
}

func (opt KVCCReadOption) WithExactVersion(exactVersion uint64) KVCCReadOption {
	opt.ExactVersion = exactVersion
	return opt
}

func (opt KVCCReadOption) WithSafeIncrReaderVersion() KVCCReadOption {
	SafeIncr(&opt.ReaderVersion)
	return opt
}

func (opt KVCCReadOption) WithIncrReaderVersion() KVCCReadOption {
	opt.ReaderVersion += 1
	return opt
}

func (opt KVCCReadOption) IsGetExactVersion() bool {
	return opt.ExactVersion != 0
}

func (opt KVCCReadOption) IsNotUpdateTimestampCache() bool {
	return opt.flag&ReadOptBitMaskNotUpdateTimestampCache == ReadOptBitMaskNotUpdateTimestampCache
}

func (opt KVCCReadOption) IsNotGetMaxReadVersion() bool {
	return opt.flag&ReadOptBitMaskNotGetMaxReadVersion == ReadOptBitMaskNotGetMaxReadVersion
}

func (opt KVCCReadOption) IsWaitNoWriteIntent() bool {
	return opt.flag&CommonReadOptBitMaskWaitNoWriteIntent == CommonReadOptBitMaskWaitNoWriteIntent
}

func (opt KVCCReadOption) IsReadForWrite() bool {
	return opt.flag&ReadOptBitMaskReadForWrite == ReadOptBitMaskReadForWrite
}

func (opt KVCCReadOption) IsReadForWriteFirstReadOfKey() bool {
	return opt.flag&ReadOptBitMaskReadForWriteFirstReadOfKey == ReadOptBitMaskReadForWriteFirstReadOfKey
}

func (opt KVCCReadOption) ToKVReadOption() KVReadOption {
	if opt.IsGetExactVersion() {
		return NewKVReadOption(opt.ExactVersion).WithExactVersion()
	}
	return NewKVReadOption(opt.ReaderVersion)
}

func (opt KVCCReadOption) InheritTxnReadOption(txnOpt TxnReadOption) KVCCReadOption {
	opt.flag = InheritReadCommonFlag(opt.flag, txnOpt.flag)
	return opt
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
	opt.flag |= WriteOptBitMaskClearWriteIntent
	return opt
}

func (opt KVCCWriteOption) WithRollbackVersion() KVCCWriteOption {
	opt.flag |= WriteOptBitMaskRemoveVersion
	opt.flag |= WriteOptBitMaskRemoveVersionRollback
	return opt
}

func (opt KVCCWriteOption) WithRemoveTxnRecordCondRollback(isRollback bool) KVCCWriteOption {
	opt.flag |= WriteOptBitMaskRemoveVersion
	if isRollback {
		opt.flag |= WriteOptBitMaskRemoveVersionRollback
	}
	return opt.WithTxnRecord()
}

func (opt KVCCWriteOption) CondReadForWrite(b bool) KVCCWriteOption {
	if b {
		opt.flag |= WriteOptBitMaskReadForWrite
	}
	return opt
}

func (opt KVCCWriteOption) CondReadForWriteRollbackOrClearReadKey(b bool) KVCCWriteOption {
	if b {
		opt.flag |= WriteOptBitMaskReadForWriteRollbackOrClearReadKey
	}
	return opt
}

func (opt KVCCWriteOption) CondWriteByDifferentTransaction(b bool) KVCCWriteOption {
	if b {
		opt.flag |= WriteOptBitMaskWriteByDifferentTxn
	}
	return opt
}

func (opt KVCCWriteOption) WithTxnRecord() KVCCWriteOption {
	opt.flag |= WriteOptBitMaskTxnRecord
	return opt
}

func (opt KVCCWriteOption) IsClearWriteIntent() bool {
	return IsWriteOptClearWriteIntent(opt.flag)
}

func (opt KVCCWriteOption) IsRemoveVersion() bool {
	return IsWriteOptRemoveVersion(opt.flag)
}

func (opt KVCCWriteOption) isRollbackVersion() bool {
	return IsWriteOptRollbackVersion(opt.flag)
}

func (opt KVCCWriteOption) IsReadForWrite() bool {
	return opt.flag&WriteOptBitMaskReadForWrite == WriteOptBitMaskReadForWrite
}

func (opt KVCCWriteOption) IsReadForWriteRollbackOrClearReadKey() bool {
	return opt.flag&WriteOptBitMaskReadForWriteRollbackOrClearReadKey == WriteOptBitMaskReadForWriteRollbackOrClearReadKey
}

func (opt KVCCWriteOption) IsTxnRecord() bool {
	return opt.flag&WriteOptBitMaskTxnRecord == WriteOptBitMaskTxnRecord
}

func (opt KVCCWriteOption) IsWriteByDifferentTransaction() bool {
	return opt.flag&WriteOptBitMaskWriteByDifferentTxn == WriteOptBitMaskWriteByDifferentTxn
}

func (opt KVCCWriteOption) IsRollbackKey() bool {
	return !opt.IsTxnRecord() && opt.isRollbackVersion()
}

func (opt KVCCWriteOption) ToKVWriteOption() KVWriteOption {
	return KVWriteOption{flag: opt.flag}
}

type KVCC interface {
	Get(ctx context.Context, key string, opt KVCCReadOption) (ValueCC, error)
	Set(ctx context.Context, key string, val Value, opt KVCCWriteOption) error
	Close() error
}
