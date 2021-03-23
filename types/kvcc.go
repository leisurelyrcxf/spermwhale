package types

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/consts"
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
	opt.flag |= KVCCReadOptBitMaskNotUpdateTimestampCache
	return opt
}

func (opt KVCCReadOption) WithNotGetMaxReadVersion() KVCCReadOption {
	opt.flag |= KVCCReadOptBitMaskNotGetMaxReadVersion
	return opt
}

func (opt KVCCReadOption) WithClearWaitNoWriteIntent() KVCCReadOption {
	opt.flag &= TxnKVCCCommonReadOptBitMaskClearWaitNoWriteIntent
	return opt
}

func (opt KVCCReadOption) CondReadForWrite(b bool) KVCCReadOption {
	if b {
		opt.flag |= KVCCReadOptBitMaskReadForWrite
	}
	return opt
}

func (opt KVCCReadOption) CondReadForWriteFirstReadOfKey(b bool) KVCCReadOption {
	if b {
		opt.flag |= KVCCReadOptBitMaskReadForWriteFirstReadOfKey
	}
	return opt
}

func (opt KVCCReadOption) WithSnapshotRead(snapshotVersion uint64) KVCCReadOption {
	opt.flag |= KVCCReadOptBitMaskSnapshotRead
	opt.flag |= KVCCReadOptBitMaskNotGetMaxReadVersion
	opt.ReaderVersion = snapshotVersion
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

func (opt KVCCReadOption) IsGetExactVersion() bool {
	return opt.ExactVersion != 0
}

func (opt KVCCReadOption) IsNotUpdateTimestampCache() bool {
	return opt.flag&KVCCReadOptBitMaskNotUpdateTimestampCache == KVCCReadOptBitMaskNotUpdateTimestampCache
}

func (opt KVCCReadOption) IsNotGetMaxReadVersion() bool {
	return opt.flag&KVCCReadOptBitMaskNotGetMaxReadVersion == KVCCReadOptBitMaskNotGetMaxReadVersion
}

func (opt KVCCReadOption) IsWaitNoWriteIntent() bool {
	return opt.flag&TxnKVCCCommonReadOptBitMaskWaitNoWriteIntent == TxnKVCCCommonReadOptBitMaskWaitNoWriteIntent
}

func (opt KVCCReadOption) IsReadForWrite() bool {
	return opt.flag&KVCCReadOptBitMaskReadForWrite == KVCCReadOptBitMaskReadForWrite
}

func (opt KVCCReadOption) IsReadForWriteFirstReadOfKey() bool {
	return opt.flag&KVCCReadOptBitMaskReadForWriteFirstReadOfKey == KVCCReadOptBitMaskReadForWriteFirstReadOfKey
}

func (opt KVCCReadOption) IsSnapshotRead() bool {
	return opt.flag&KVCCReadOptBitMaskSnapshotRead == KVCCReadOptBitMaskSnapshotRead
}

func (opt KVCCReadOption) IsTxnRecord() bool {
	return opt.flag&KVCCReadOptBitMaskTxnRecord == KVCCReadOptBitMaskTxnRecord
}

func (opt KVCCReadOption) ToKVReadOption() (kvOpt KVReadOption) {
	if opt.IsGetExactVersion() {
		kvOpt = NewKVReadOption(opt.ExactVersion).WithExactVersion()
	} else {
		kvOpt = NewKVReadOption(opt.ReaderVersion)
	}
	if opt.IsTxnRecord() {
		kvOpt.Flag |= KVReadOptBitMaskTxnRecord
	}
	return kvOpt
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

func (opt KVCCWriteOption) CondReadForWrite(b bool) KVCCWriteOption {
	if b {
		opt.flag |= KVCCWriteOptBitMaskReadForWrite
	}
	return opt
}

func (opt KVCCWriteOption) CondReadForWriteRollbackOrClearReadKey(b bool) KVCCWriteOption {
	if b {
		opt.flag |= KVCCWriteOptBitMaskReadForWriteRollbackOrClearReadKey
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

func (opt KVCCWriteOption) isRollbackVersion() bool {
	return IsWriteOptRollbackVersion(opt.flag)
}

func (opt KVCCWriteOption) IsReadForWrite() bool {
	return opt.flag&KVCCWriteOptBitMaskReadForWrite == KVCCWriteOptBitMaskReadForWrite
}

func (opt KVCCWriteOption) IsReadForWriteRollbackOrClearReadKey() bool {
	return opt.flag&KVCCWriteOptBitMaskReadForWriteRollbackOrClearReadKey == KVCCWriteOptBitMaskReadForWriteRollbackOrClearReadKey
}

func (opt KVCCWriteOption) IsWriteByDifferentTransaction() bool {
	return opt.flag&KVCCWriteOptBitMaskWriteByDifferentTxn == KVCCWriteOptBitMaskWriteByDifferentTxn
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
