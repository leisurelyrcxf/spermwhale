package types

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	. "github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/proto/kvccpb"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

type KVCCReadOptionConst struct {
	MinAllowedSnapshotVersion uint64

	ExactVersion     uint64
	ReadExactVersion bool

	IsTxnRecord                     bool
	UpdateTimestampCache            bool // Default to true
	GetMaxReadVersion               bool // Default to true
	IsReadModifyWrite               bool
	IsReadModifyWriteFirstReadOfKey bool
	IsSnapshotRead                  bool
	WaitWhenReadDirty               bool
	IsMetaOnly                      bool
	IsCheckVersion                  bool
}

type KVCCReadOption struct {
	ReaderVersion uint64
	DBReadVersion uint64

	KVCCReadOptionConst
	flag uint16
}

func NewKVCCReadOption(readerVersion uint64) *KVCCReadOption {
	opt := &KVCCReadOption{ReaderVersion: readerVersion, DBReadVersion: MaxTxnVersion}
	opt.InitializeWithZeroFlag()
	return opt
}

func NewKVCCCheckKeyReadOption(readerVersion uint64, valueVersion uint64) *KVCCReadOption {
	opt := NewKVCCReadOption(readerVersion)
	assert.Must(valueVersion != 0)
	opt.ExactVersion, opt.ReadExactVersion = valueVersion, true
	opt.flag |= consts.KVCCReadOptBitMaskMetaOnly | consts.KVCCReadOptBitMaskCheckVersion
	opt.IsMetaOnly, opt.IsCheckVersion = true, true
	return opt
}

func NewKVCCCheckTxnRecordReadOption(txnId TxnId) *KVCCReadOption {
	assert.Must(txnId > 0)
	opt := NewKVCCReadOption(MaxTxnVersion)
	opt.ExactVersion, opt.ReadExactVersion = txnId.Version(), true
	opt.flag |= consts.KVCCReadOptBitMaskCheckVersion | consts.KVCCReadOptBitMaskTxnRecord
	opt.IsCheckVersion, opt.IsTxnRecord = true, true
	return opt
}

func NewKVCCSnapshotReadOption(snapshotVersion uint64, minAllowedSnapshotVersion uint64) *KVCCReadOption {
	opt := NewKVCCReadOption(snapshotVersion)
	opt.MinAllowedSnapshotVersion = minAllowedSnapshotVersion

	opt.flag |= KVCCReadOptBitMaskSnapshotRead
	opt.IsSnapshotRead = true

	opt.flag |= KVCCReadOptBitMaskNotGetMaxReadVersion
	opt.GetMaxReadVersion = false
	return opt
}

func NewKVCCReadOptionFromPB(x *kvccpb.KVCCReadOption) *KVCCReadOption {
	opt := &KVCCReadOption{
		ReaderVersion: x.ReaderVersion,
		DBReadVersion: x.DBReadVersion,
		KVCCReadOptionConst: KVCCReadOptionConst{
			MinAllowedSnapshotVersion: x.MinAllowedSnapshotVersion,
			ExactVersion:              x.ExactVersion,
		},
		flag: uint16(x.Flag),
	}
	opt.Initialize()
	return opt
}

func (opt *KVCCReadOption) ToPB() *kvccpb.KVCCReadOption {
	return &kvccpb.KVCCReadOption{
		ReaderVersion:             opt.ReaderVersion,
		Flag:                      uint32(opt.flag),
		ExactVersion:              opt.ExactVersion,
		MinAllowedSnapshotVersion: opt.MinAllowedSnapshotVersion,
		DBReadVersion:             opt.DBReadVersion,
	}
}

func (opt *KVCCReadOption) Initialize() {
	if opt.DBReadVersion == 0 {
		opt.DBReadVersion = MaxTxnVersion
	}
	opt.ReadExactVersion = opt.ExactVersion != 0
	opt.IsTxnRecord = opt.flag&KVCCReadOptBitMaskTxnRecord == KVCCReadOptBitMaskTxnRecord
	opt.UpdateTimestampCache = opt.flag&KVCCReadOptBitMaskNotUpdateTimestampCache == 0
	opt.GetMaxReadVersion = opt.flag&KVCCReadOptBitMaskNotGetMaxReadVersion == 0
	opt.IsReadModifyWrite = opt.flag&KVCCReadOptBitMaskReadModifyWrite == KVCCReadOptBitMaskReadModifyWrite
	opt.IsReadModifyWriteFirstReadOfKey = opt.flag&KVCCReadOptBitMaskReadModifyWriteFirstReadOfKey == KVCCReadOptBitMaskReadModifyWriteFirstReadOfKey
	opt.IsSnapshotRead = opt.flag&KVCCReadOptBitMaskSnapshotRead == KVCCReadOptBitMaskSnapshotRead
	opt.WaitWhenReadDirty = opt.flag&KVCCReadOptBitMaskWaitWhenReadDirty == KVCCReadOptBitMaskWaitWhenReadDirty
	opt.IsMetaOnly = opt.flag&KVCCReadOptBitMaskMetaOnly == KVCCReadOptBitMaskMetaOnly
	opt.IsCheckVersion = opt.flag&KVCCReadOptBitMaskCheckVersion == KVCCReadOptBitMaskCheckVersion
}

func (opt *KVCCReadOption) AssertFlags() {
	assert.Must(opt.ReaderVersion != 0)
	assert.Must(opt.DBReadVersion != 0)
	assert.Must(opt.ReadExactVersion == (opt.ExactVersion != 0))
	assert.Must(opt.IsTxnRecord == (opt.flag&KVCCReadOptBitMaskTxnRecord == KVCCReadOptBitMaskTxnRecord))
	assert.Must(opt.UpdateTimestampCache == (opt.flag&KVCCReadOptBitMaskNotUpdateTimestampCache == 0))
	assert.Must(opt.GetMaxReadVersion == (opt.flag&KVCCReadOptBitMaskNotGetMaxReadVersion == 0))
	assert.Must(opt.IsReadModifyWrite == (opt.flag&KVCCReadOptBitMaskReadModifyWrite == KVCCReadOptBitMaskReadModifyWrite))
	assert.Must(opt.IsReadModifyWriteFirstReadOfKey == (opt.flag&KVCCReadOptBitMaskReadModifyWriteFirstReadOfKey == KVCCReadOptBitMaskReadModifyWriteFirstReadOfKey))
	assert.Must(opt.IsSnapshotRead == (opt.flag&KVCCReadOptBitMaskSnapshotRead == KVCCReadOptBitMaskSnapshotRead))
	assert.Must(opt.WaitWhenReadDirty == (opt.flag&KVCCReadOptBitMaskWaitWhenReadDirty == KVCCReadOptBitMaskWaitWhenReadDirty))
	assert.Must(opt.IsMetaOnly == (opt.flag&KVCCReadOptBitMaskMetaOnly == KVCCReadOptBitMaskMetaOnly))
	assert.Must(opt.IsCheckVersion == (opt.flag&KVCCReadOptBitMaskCheckVersion == KVCCReadOptBitMaskCheckVersion))
}

func (opt *KVCCReadOption) InitializeWithZeroFlag() {
	opt.ReadExactVersion = opt.ExactVersion != 0
	opt.UpdateTimestampCache = true
	opt.GetMaxReadVersion = true
	opt.AssertFlags()
}

func (opt *KVCCReadOption) SetNotUpdateTimestampCache() *KVCCReadOption {
	opt.flag |= KVCCReadOptBitMaskNotUpdateTimestampCache
	opt.UpdateTimestampCache = false
	return opt
}

func (opt *KVCCReadOption) SetNotGetMaxReadVersion() *KVCCReadOption {
	opt.flag |= KVCCReadOptBitMaskNotGetMaxReadVersion
	opt.GetMaxReadVersion = false
	return opt
}

func (opt *KVCCReadOption) CondSetReadModifyWrite(b bool) *KVCCReadOption {
	if b {
		opt.flag |= KVCCReadOptBitMaskReadModifyWrite
		opt.IsReadModifyWrite = true
	}
	return opt
}

func (opt *KVCCReadOption) CondSetReadModifyWriteFirstReadOfKey(b bool) *KVCCReadOption {
	if b {
		opt.flag |= KVCCReadOptBitMaskReadModifyWriteFirstReadOfKey
		opt.IsReadModifyWriteFirstReadOfKey = true
	}
	return opt
}

func (opt *KVCCReadOption) SetExactVersion(exactVersion uint64) *KVCCReadOption {
	assert.Must(exactVersion != 0)
	opt.ExactVersion, opt.ReadExactVersion = exactVersion, true
	return opt
}

func (opt *KVCCReadOption) CondSetWaitWhenReadDirty(b bool) *KVCCReadOption {
	if b {
		opt.flag |= consts.KVCCReadOptBitMaskWaitWhenReadDirty
		opt.WaitWhenReadDirty = true
	}
	return opt
}

func (opt *KVCCReadOption) SetDBReadVersion(dbReadVersion uint64) *KVCCReadOption {
	assert.Must(dbReadVersion != 0)
	opt.DBReadVersion = dbReadVersion
	return opt
}

func (opt *KVCCReadOption) ToKV() (kvOpt KVReadOption) {
	if opt.ReadExactVersion {
		kvOpt = NewKVReadOptionWithExactVersion(opt.ExactVersion)
	} else {
		kvOpt = NewKVReadOption(utils.MinUint64(opt.ReaderVersion, opt.DBReadVersion))
	}
	assert.Must(kvOpt.Version != 0)
	if opt.IsTxnRecord {
		kvOpt.Flag |= KVReadOptBitMaskTxnRecord
	}
	if opt.IsMetaOnly {
		kvOpt.Flag |= KVReadOptBitMaskMetaOnly
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
func (opt KVCCWriteOption) ToKV() KVWriteOption {
	return KVWriteOption{flag: opt.flag}
}

func (opt KVCCWriteOption) CondReadModifyWrite(b bool) KVCCWriteOption {
	if b {
		opt.flag |= KVCCWriteOptBitMaskReadModifyWrite
	}
	return opt
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
	TxnInternalVersion // Used for verification
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
