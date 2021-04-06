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

func (opt KVCCReadOption) IsMetaOnly() bool {
	return opt.flag&KVCCReadOptBitMaskMetaOnly == KVCCReadOptBitMaskMetaOnly
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
	if opt.IsMetaOnly() {
		kvOpt.Flag |= KVReadOptBitMaskMetaOnly
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
func (opt KVCCWriteOption) ToKVWriteOption() KVWriteOption {
	return KVWriteOption{flag: opt.flag}
}

func (opt KVCCWriteOption) WithTxnRecord() KVCCWriteOption {
	opt.flag |= KVKVCCWriteOptOptBitMaskTxnRecord
	return opt
}
func (opt KVCCWriteOption) CondReadModifyWrite(b bool) KVCCWriteOption {
	if b {
		opt.flag |= KVCCWriteOptBitMaskReadModifyWrite
	}
	return opt
}

func (opt KVCCWriteOption) IsTxnRecord() bool {
	return opt.flag&KVKVCCWriteOptOptBitMaskTxnRecord == KVKVCCWriteOptOptBitMaskTxnRecord
}
func (opt KVCCWriteOption) IsReadModifyWrite() bool {
	return opt.flag&KVCCWriteOptBitMaskReadModifyWrite == KVCCWriteOptBitMaskReadModifyWrite
}

type KVCCOperationOption struct {
	Flag uint8
}

func (opt KVCCOperationOption) GetFlagAsUint32() uint32 {
	return uint32(opt.Flag) // TODO handle endian?
}

func (opt KVCCOperationOption) IsOperatedByDifferentTxn() bool {
	return opt.Flag&CommonKVCCOpsOptBitMaskOperatedByDifferentTxn == CommonKVCCOpsOptBitMaskOperatedByDifferentTxn
}
func (opt KVCCOperationOption) IsReadOnlyKey() bool {
	return opt.Flag&CommonKVCCOpsOptBitMaskIsReadOnlyKey == CommonKVCCOpsOptBitMaskIsReadOnlyKey
}
func (opt KVCCOperationOption) IsReadModifyWrite() bool {
	return opt.Flag&CommonKVCCOpsOptBitMaskReadModifyWrite == CommonKVCCOpsOptBitMaskReadModifyWrite
}

func (opt *KVCCOperationOption) CondSetOperatedByDifferentTxn(b bool) {
	if b {
		opt.Flag |= CommonKVCCOpsOptBitMaskOperatedByDifferentTxn
	}
}
func (opt *KVCCOperationOption) CondSetReadOnlyKey(b bool) {
	if b {
		opt.Flag |= CommonKVCCOpsOptBitMaskIsReadOnlyKey
	}
}
func (opt *KVCCOperationOption) CondSetReadModifyWrite(b bool) {
	if b {
		opt.Flag |= CommonKVCCOpsOptBitMaskReadModifyWrite
	}
}

type KVCCUpdateMetaOption struct {
	KVCCOperationOption
	TxnInternalVersion
}

var KVCCClearWriteIntent = KVCCUpdateMetaOption{KVCCOperationOption: KVCCOperationOption{Flag: KVKVCCUpdateMetaOptBitMaskClearWriteIntent}}

func NewKVCCCUpdateMetaOptionFromPB(opt *kvccpb.KVCCUpdateMetaOption) KVCCUpdateMetaOption {
	return KVCCUpdateMetaOption{
		KVCCOperationOption: KVCCOperationOption{Flag: uint8(opt.Flag)},
		TxnInternalVersion:  TxnInternalVersion(opt.TxnInternalVersion),
	}
}

func (opt KVCCUpdateMetaOption) ToPB() *kvccpb.KVCCUpdateMetaOption {
	return &kvccpb.KVCCUpdateMetaOption{
		Flag:               opt.GetFlagAsUint32(),
		TxnInternalVersion: opt.TxnInternalVersion.AsUint32(),
	}
}
func (opt KVCCUpdateMetaOption) ToKV() KVUpdateMetaOption {
	return KVUpdateMetaOption(opt.Flag & KVCC2KVUpdateMetaOptExtractor)
}

func (opt KVCCUpdateMetaOption) CondUpdateByDifferentTxn(b bool) KVCCUpdateMetaOption {
	opt.CondSetOperatedByDifferentTxn(b)
	return opt
}
func (opt KVCCUpdateMetaOption) CondReadOnlyKey(b bool) KVCCUpdateMetaOption {
	opt.CondSetReadOnlyKey(b)
	return opt
}
func (opt KVCCUpdateMetaOption) CondReadModifyWrite(b bool) KVCCUpdateMetaOption {
	opt.CondSetReadModifyWrite(b)
	return opt
}
func (opt KVCCUpdateMetaOption) WithInternalVersion(version TxnInternalVersion) KVCCUpdateMetaOption {
	opt.TxnInternalVersion = version
	return opt
}

func (opt KVCCUpdateMetaOption) IsClearWriteIntent() bool {
	return opt.Flag&KVKVCCUpdateMetaOptBitMaskClearWriteIntent == KVKVCCUpdateMetaOptBitMaskClearWriteIntent
}

type KVCCRollbackKeyOption struct {
	KVCCOperationOption
}

var EmptyKVCCRollbackKeyOption = KVCCRollbackKeyOption{}

func NewKVCCCRollbackKeyOptionFromPB(opt *kvccpb.KVCCRollbackKeyOption) KVCCRollbackKeyOption {
	return KVCCRollbackKeyOption{KVCCOperationOption: KVCCOperationOption{Flag: uint8(opt.Flag)}}
}

func (opt KVCCRollbackKeyOption) ToPB() *kvccpb.KVCCRollbackKeyOption {
	return &kvccpb.KVCCRollbackKeyOption{Flag: opt.GetFlagAsUint32()}
}

func (opt KVCCRollbackKeyOption) CondRollbackByDifferentTxn(b bool) KVCCRollbackKeyOption {
	opt.CondSetOperatedByDifferentTxn(b)
	return opt
}
func (opt KVCCRollbackKeyOption) CondReadOnlyKey(b bool) KVCCRollbackKeyOption {
	opt.CondSetReadOnlyKey(b)
	return opt
}
func (opt KVCCRollbackKeyOption) CondReadModifyWrite(b bool) KVCCRollbackKeyOption {
	opt.CondSetReadModifyWrite(b)
	return opt
}

type KVCCRemoveTxnRecordOption struct {
	KVCCOperationOption
}

var EmptyKVCCRemoveTxnRecordOption = KVCCRemoveTxnRecordOption{}

func NewKVCCCRemoveTxnRecordOptionFromPB(opt *kvccpb.KVCCRemoveTxnRecordOption) KVCCRemoveTxnRecordOption {
	return KVCCRemoveTxnRecordOption{KVCCOperationOption: KVCCOperationOption{Flag: uint8(opt.Flag)}}
}

func (opt KVCCRemoveTxnRecordOption) ToPB() *kvccpb.KVCCRemoveTxnRecordOption {
	return &kvccpb.KVCCRemoveTxnRecordOption{Flag: opt.GetFlagAsUint32()}
}

func (opt KVCCRemoveTxnRecordOption) CondRemoveByDifferentTransaction(b bool) KVCCRemoveTxnRecordOption {
	opt.CondSetOperatedByDifferentTxn(b)
	return opt
}
func (opt KVCCRemoveTxnRecordOption) CondRollback(b bool) KVCCRemoveTxnRecordOption {
	if b {
		opt.Flag |= KVCCRemoveTxnRecordOptBitMaskRollback
	}
	return opt
}

func (opt KVCCRemoveTxnRecordOption) IsRollback() bool {
	return opt.Flag&KVCCRemoveTxnRecordOptBitMaskRollback == KVCCRemoveTxnRecordOptBitMaskRollback
}

type KVCC interface {
	Get(ctx context.Context, key string, opt KVCCReadOption) (ValueCC, error)
	Set(ctx context.Context, key string, val Value, opt KVCCWriteOption) error
	UpdateMeta(ctx context.Context, key string, version uint64, opt KVCCUpdateMetaOption) error
	RollbackKey(ctx context.Context, key string, version uint64, opt KVCCRollbackKeyOption) error
	RemoveTxnRecord(ctx context.Context, version uint64, opt KVCCRemoveTxnRecordOption) error
	Close() error
}
