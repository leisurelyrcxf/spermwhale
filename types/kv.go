package types

import (
	"context"

	. "github.com/leisurelyrcxf/spermwhale/consts"

	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"
)

type ReadOption struct {
	Version uint64
	flag    uint8
}

func NewReadOption(version uint64) ReadOption {
	return ReadOption{
		Version: version,
	}
}

func NewReadOptionFromPB(x *commonpb.ReadOption) ReadOption {
	return ReadOption{
		Version: x.Version,
		flag:    x.GetFlagSafe(),
	}
}

func (opt ReadOption) ToPB() *commonpb.ReadOption {
	return (&commonpb.ReadOption{
		Version: opt.Version,
	}).SetFlagSafe(opt.flag)
}

func (opt ReadOption) WithExactVersion() ReadOption {
	opt.flag |= ReadOptBitMaskExactVersion
	return opt
}

func (opt ReadOption) WithNotUpdateTimestampCache() ReadOption {
	opt.flag |= ReadOptBitMaskNotUpdateTimestampCache
	return opt
}

func (opt ReadOption) WithNotGetMaxReadVersion() ReadOption {
	opt.flag |= ReadOptBitMaskNotGetMaxReadVersion
	return opt
}

func (opt ReadOption) IsGetExactVersion() bool {
	return opt.flag&ReadOptBitMaskExactVersion > 0
}

func (opt ReadOption) IsNotUpdateTimestampCache() bool {
	return opt.flag&ReadOptBitMaskNotUpdateTimestampCache > 0
}

func (opt ReadOption) IsNotGetMaxReadVersion() bool {
	return opt.flag&ReadOptBitMaskNotGetMaxReadVersion > 0
}

type WriteOption struct {
	flag uint8
}

func NewWriteOption() WriteOption {
	return WriteOption{}
}

func NewWriteOptionFromPB(x *commonpb.WriteOption) WriteOption {
	return WriteOption{
		flag: x.GetFlagSafe(),
	}
}

func (opt *WriteOption) ToPB() *commonpb.WriteOption {
	return (&commonpb.WriteOption{}).SetFlagSafe(opt.flag)
}

func (opt WriteOption) WithClearWriteIntent() WriteOption {
	opt.flag |= WriteOptBitMaskClearWriteIntent
	return opt
}

func (opt WriteOption) WithRemoveVersion() WriteOption {
	opt.flag |= WriteOptBitMaskRemoveVersion
	return opt
}

func (opt WriteOption) IsClearWriteIntent() bool {
	return opt.flag&WriteOptBitMaskClearWriteIntent > 0
}

func (opt WriteOption) IsRemoveVersion() bool {
	return opt.flag&WriteOptBitMaskRemoveVersion > 0
}

type KV interface {
	Get(ctx context.Context, key string, opt ReadOption) (Value, error)
	Set(ctx context.Context, key string, val Value, opt WriteOption) error
	Close() error
}
