package types

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/consts"
	. "github.com/leisurelyrcxf/spermwhale/consts"

	"github.com/leisurelyrcxf/spermwhale/proto/kvpb"
)

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

func (opt KVReadOption) IsGetExactVersion() bool {
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

func (opt KVWriteOption) WithClearWriteIntent() KVWriteOption {
	opt.flag |= CommonWriteOptBitMaskClearWriteIntent
	return opt
}

func (opt KVWriteOption) WithRemoveVersion() KVWriteOption {
	opt.flag |= CommonWriteOptBitMaskRemoveVersion
	return opt
}

func (opt KVWriteOption) IsTxnRecord() bool {
	return consts.IsWriteTxnRecord(opt.flag)
}

func (opt KVWriteOption) IsClearWriteIntent() bool {
	return IsWriteOptClearWriteIntent(opt.flag)
}

func (opt KVWriteOption) IsRemoveVersion() bool {
	return IsWriteOptRemoveVersion(opt.flag)
}

type KV interface {
	Get(ctx context.Context, key string, opt KVReadOption) (Value, error)
	Set(ctx context.Context, key string, val Value, opt KVWriteOption) error
	Close() error
}
