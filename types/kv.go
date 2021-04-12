package types

import (
	"context"
	"encoding/json"

	"github.com/golang/glog"
	"github.com/leisurelyrcxf/spermwhale/assert"
	. "github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/proto/kvpb"
)

type DBType string

var AllDBTypes []DBType

var (
	addDBType = func(s string) DBType {
		t := DBType(s)
		AllDBTypes = append(AllDBTypes, t)
		return t
	}
	DBTypeMemory = addDBType("memory")
	DBTypeRedis  = addDBType("redis")
	DBTypeMongo  = addDBType("mongo")
)

type DBMeta struct {
	VFlag           `json:"F"`
	InternalVersion TxnInternalVersion `json:"I"`
}

var InvalidDBMeta = DBMeta{InternalVersion: TxnInternalVersionPositiveInvalid}

func (m DBMeta) WithVersion(version uint64) Meta {
	return Meta{
		Version:         version,
		InternalVersion: m.InternalVersion,
		VFlag:           m.VFlag,
	}
}
func (m DBMeta) WithInvalidKeyState() DBMeta {
	m.SetInvalidKeyState()
	return m
}

func (m DBMeta) IsValid() bool {
	return m.VFlag.IsValid() && m.InternalVersion.IsValid()
}

func (m DBMeta) AssertValid() {
	m.VFlag.AssertValid()
	assert.Must(m.InternalVersion.IsValid())
}

func (m *DBMeta) UpdateByMeta(meta Meta) {
	m.VFlag = meta.VFlag
	m.InternalVersion = meta.InternalVersion
}

type DBValue struct {
	DBMeta

	V []byte `json:"V"`
}

var EmptyDBValue = DBValue{}

func (v DBValue) WithCommittedCleared() DBValue {
	v.SetCommittedCleared()
	return v
}

func (v DBValue) Encode() []byte {
	b, err := json.Marshal(v)
	if err != nil {
		glog.Fatalf("encode to json failed: '%v'", err)
	}
	return b
}

func (v *DBValue) Decode(data []byte) error {
	return json.Unmarshal(data, v)
}

func (v DBValue) WithVersion(version uint64) Value {
	return Value{
		Meta: v.DBMeta.WithVersion(version),
		V:    v.V,
	}
}

type KVReadOption struct {
	Version uint64
	Flag    uint8
}

func NewKVReadOption(Version uint64) KVReadOption {
	return KVReadOption{
		Version: Version,
	}
}

func NewKVReadOptionWithExactVersion(exactVersion uint64) KVReadOption {
	return KVReadOption{
		Version: exactVersion,
		Flag:    KVReadOptBitMaskExactVersion,
	}
}

func NewKVReadCheckVersionOption(exactVersion uint64) KVReadOption {
	return KVReadOption{
		Version: exactVersion,
		Flag:    KVReadOptBitMaskExactVersion | KVReadOptBitMaskMetaOnly,
	}
}

func NewKVReadOptionFromPB(x *kvpb.KVReadOption) KVReadOption {
	return KVReadOption{
		Version: x.Version,
		Flag:    x.GetFlagSafe(),
	}
}

func (opt KVReadOption) ToPB() *kvpb.KVReadOption {
	return (&kvpb.KVReadOption{
		Version: opt.Version,
	}).SetFlagSafe(opt.Flag)
}

func (opt KVReadOption) CondTxnRecord(b bool) KVReadOption {
	if b {
		opt.Flag |= KVReadOptBitMaskTxnRecord
	}
	return opt
}

func (opt KVReadOption) WithTxnRecord() KVReadOption {
	opt.Flag |= KVReadOptBitMaskTxnRecord
	return opt
}

func (opt *KVReadOption) SetExactVersion(version uint64) {
	opt.Version = version
	opt.Flag |= KVReadOptBitMaskExactVersion
}

func (opt KVReadOption) IsTxnRecord() bool {
	return opt.Flag&KVReadOptBitMaskTxnRecord == KVReadOptBitMaskTxnRecord
}

func (opt KVReadOption) IsReadExactVersion() bool {
	return opt.Flag&KVReadOptBitMaskExactVersion == KVReadOptBitMaskExactVersion
}

func (opt KVReadOption) IsMetaOnly() bool {
	return opt.Flag&KVReadOptBitMaskMetaOnly == KVReadOptBitMaskMetaOnly
}

func (opt KVReadOption) ToKVCC() *KVCCReadOption {
	kvccOpt := NewKVCCReadOption(opt.Version).SetNotGetMaxReadVersion().SetNotUpdateTimestampCache()
	if opt.IsReadExactVersion() {
		kvccOpt.SetExactVersion(opt.Version)
	}
	if opt.IsMetaOnly() {
		kvccOpt.flag |= KVCCReadOptBitMaskMetaOnly
		kvccOpt.IsMetaOnly = true
	}
	if opt.IsTxnRecord() {
		kvccOpt.flag |= KVCCReadOptBitMaskTxnRecord
		kvccOpt.IsTxnRecord = true
	}
	return kvccOpt
}

type KVWriteOption struct {
	flag uint8
}

func NewKVWriteOption() KVWriteOption {
	return KVWriteOption{}
}

func NewKVWriteOptionFromPB(x *kvpb.KVWriteOption) KVWriteOption {
	return KVWriteOption{
		flag: x.GetFlagSafe(),
	}
}

func (opt *KVWriteOption) ToPB() *kvpb.KVWriteOption {
	return (&kvpb.KVWriteOption{}).SetFlagSafe(opt.flag)
}

type KVUpdateMetaOption uint8

func NewKVUpdateMetaOptionFromPB(opt *kvpb.KVUpdateMetaOption) KVUpdateMetaOption {
	return KVUpdateMetaOption(opt.Flag)
}

func (opt KVUpdateMetaOption) ToPB() *kvpb.KVUpdateMetaOption {
	return &kvpb.KVUpdateMetaOption{Flag: uint32(opt)}
}

func (opt KVUpdateMetaOption) IsClearWriteIntent() bool {
	return opt&KVKVCCUpdateMetaOptBitMaskClearWriteIntent == KVKVCCUpdateMetaOptBitMaskClearWriteIntent
}

type KV interface {
	Get(ctx context.Context, key string, opt KVReadOption) (Value, error)
	Set(ctx context.Context, key string, val Value, opt KVWriteOption) error
	KeyVersionCount(ctx context.Context, key string) (int64, error)
	UpdateMeta(ctx context.Context, key string, version uint64, opt KVUpdateMetaOption) error
	RollbackKey(ctx context.Context, key string, version uint64) error
	RemoveTxnRecord(ctx context.Context, version uint64) error
	Close() error
}
