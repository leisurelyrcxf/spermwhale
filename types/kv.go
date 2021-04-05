package types

import (
	"context"
	"encoding/json"

	"github.com/golang/glog"
	"github.com/leisurelyrcxf/spermwhale/consts"
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
	Flag            uint8              `json:"F"`
	InternalVersion TxnInternalVersion `json:"I"`
}

func (m DBMeta) IsDirty() bool {
	return consts.IsDirty(m.Flag)
}

func (m DBMeta) IsCommitted() bool {
	return m.Flag&consts.ValueMetaBitMaskCommitted == consts.ValueMetaBitMaskCommitted
}

func (m DBMeta) WithVersion(version uint64) Meta {
	return Meta{
		Version:         version,
		InternalVersion: m.InternalVersion,
		Flag:            m.Flag,
	}
}

type DBValue struct {
	DBMeta

	V []byte `json:"V"`
}

var EmptyDBValue = DBValue{}

func (v DBValue) WithCommitted() DBValue {
	v.Flag = consts.WithCommitted(v.Flag)
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

func (opt KVReadOption) WithTxnRecord() KVReadOption {
	opt.Flag |= KVReadOptBitMaskTxnRecord
	return opt
}

func (opt KVReadOption) WithExactVersion() KVReadOption {
	opt.Flag |= KVReadOptBitMaskExactVersion
	return opt
}

func (opt KVReadOption) IsTxnRecord() bool {
	return opt.Flag&KVReadOptBitMaskTxnRecord == KVReadOptBitMaskTxnRecord
}

func (opt KVReadOption) IsReadExactVersion() bool {
	return opt.Flag&KVReadOptBitMaskExactVersion == KVReadOptBitMaskExactVersion
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

func (opt KVWriteOption) WithTxnRecord() KVWriteOption {
	opt.flag |= KVKVCCWriteOptOptBitMaskTxnRecord
	return opt
}

func (opt KVWriteOption) IsTxnRecord() bool {
	return opt.flag&KVKVCCWriteOptOptBitMaskTxnRecord == KVKVCCWriteOptOptBitMaskTxnRecord
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
	UpdateMeta(ctx context.Context, key string, version uint64, opt KVUpdateMetaOption) error
	RollbackKey(ctx context.Context, key string, version uint64) error
	RemoveTxnRecord(ctx context.Context, version uint64) error
	Close() error
}
