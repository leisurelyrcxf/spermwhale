package types

import (
	"context"

	. "github.com/leisurelyrcxf/spermwhale/consts"

	"github.com/leisurelyrcxf/spermwhale/proto/kvpb"
)

type KVReadOption struct {
	Version      uint64
	ExactVersion bool
}

func NewKVReadOption(Version uint64) KVReadOption {
	return KVReadOption{
		Version: Version,
	}
}

func NewKVReadOptionFromPB(x *kvpb.KVReadOption) KVReadOption {
	return KVReadOption{
		Version:      x.Version,
		ExactVersion: x.ExactVersion,
	}
}

func (opt KVReadOption) ToPB() *kvpb.KVReadOption {
	return &kvpb.KVReadOption{
		Version:      opt.Version,
		ExactVersion: opt.ExactVersion,
	}
}

func (opt KVReadOption) WithExactVersion() KVReadOption {
	opt.ExactVersion = true
	return opt
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
	opt.flag |= WriteOptBitMaskClearWriteIntent
	return opt
}

func (opt KVWriteOption) WithRemoveVersion() KVWriteOption {
	opt.flag |= WriteOptBitMaskRemoveVersion
	return opt
}

func (opt KVWriteOption) IsClearWriteIntent() bool {
	return opt.flag&WriteOptBitMaskClearWriteIntent == WriteOptBitMaskClearWriteIntent
}

func (opt KVWriteOption) IsRemoveVersion() bool {
	return opt.flag&WriteOptBitMaskRemoveVersion == WriteOptBitMaskRemoveVersion
}

type KV interface {
	Get(ctx context.Context, key string, opt KVReadOption) (Value, error)
	Set(ctx context.Context, key string, val Value, opt KVWriteOption) error
	Close() error
}
