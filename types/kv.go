package types

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"
)

type ReadOption struct {
	Version                 uint64
	ExactVersion            bool
	NotUpdateTimestampCache bool
	GetMaxReadVersion       bool
}

func NewReadOption(version uint64) ReadOption {
	return ReadOption{
		Version: version,
	}
}

func NewReadOptionFromPB(x *commonpb.ReadOption) ReadOption {
	return ReadOption{
		Version:                 x.Version,
		ExactVersion:            x.ExactReadVersion,
		NotUpdateTimestampCache: x.NotUpdateTimestampCache,
		GetMaxReadVersion:       x.GetMaxReadVersion,
	}
}

func (opt ReadOption) ToPB() *commonpb.ReadOption {
	return &commonpb.ReadOption{
		Version:                 opt.Version,
		ExactReadVersion:        opt.ExactVersion,
		NotUpdateTimestampCache: opt.NotUpdateTimestampCache,
		GetMaxReadVersion:       opt.GetMaxReadVersion,
	}
}

func (opt ReadOption) WithExactVersion() ReadOption {
	opt.ExactVersion = true
	return opt
}

func (opt ReadOption) WithNotUpdateTimestampCache() ReadOption {
	opt.NotUpdateTimestampCache = true
	return opt
}

func (opt ReadOption) WithGetMaxReadVersion() ReadOption {
	opt.GetMaxReadVersion = true
	return opt
}

type WriteOption struct {
	ClearWriteIntent bool
	RemoveVersion    bool
}

func NewWriteOption() WriteOption {
	return WriteOption{}
}

func NewWriteOptionFromPB(x *commonpb.WriteOption) WriteOption {
	return WriteOption{
		ClearWriteIntent: x.ClearWriteIntent,
		RemoveVersion:    x.RemoveVersion,
	}
}

func (opt *WriteOption) ToPB() *commonpb.WriteOption {
	return &commonpb.WriteOption{
		ClearWriteIntent: opt.ClearWriteIntent,
		RemoveVersion:    opt.RemoveVersion,
	}
}

func (opt WriteOption) WithClearWriteIntent() WriteOption {
	opt.ClearWriteIntent = true
	return opt
}

func (opt WriteOption) WithRemoveVersion() WriteOption {
	opt.RemoveVersion = true
	return opt
}

type KV interface {
	Get(ctx context.Context, key string, opt ReadOption) (Value, error)
	Set(ctx context.Context, key string, val Value, opt WriteOption) error
	Close() error
}
