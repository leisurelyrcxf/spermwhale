package commonpb

import (
	"math"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
)

func ToPBError(e error) *Error {
	if e == nil {
		return nil
	}
	if e, ok := e.(*errors.Error); ok {
		return &Error{
			Code: int32(e.Code),
			Msg:  e.Msg,
		}
	}
	return &Error{
		Code: consts.ErrCodeUnknown,
		Msg:  e.Error(),
	}
}

func (x *Error) Error() error {
	if x == nil {
		return nil
	}
	return errors.NewError(int(x.Code), x.Msg)
}

func ToPBValue(v types.Value) *Value {
	return &Value{
		Meta: ToPBMeta(v.Meta),
		Val:  v.V,
	}
}

func (x *Value) Value() types.Value {
	return types.Value{
		V:    x.Val,
		Meta: x.Meta.Meta(),
	}
}

func ToPBMeta(v types.Meta) *ValueMeta {
	return &ValueMeta{
		WriteIntent: v.WriteIntent,
		Version:     v.Version,
	}
}

func (x *ValueMeta) Meta() types.Meta {
	return types.Meta{
		WriteIntent: x.WriteIntent,
		Version:     x.Version,
	}
}

func ToPBReadOption(o types.ReadOption) *ReadOption {
	return &ReadOption{
		Version:                 o.Version,
		ExactReadVersion:        o.ExactVersion,
		NotUpdateTimestampCache: o.NotUpdateTimestampCache,
	}
}

func (x *ReadOption) ReadOption() types.ReadOption {
	if x == nil {
		return types.ReadOption{
			Version:                 math.MaxUint64,
			ExactVersion:            false,
			NotUpdateTimestampCache: false,
		}
	}
	return types.ReadOption{
		Version:                 x.Version,
		ExactVersion:            x.ExactReadVersion,
		NotUpdateTimestampCache: x.NotUpdateTimestampCache,
	}
}

func ToPBWriteOption(o types.WriteOption) *WriteOption {
	return &WriteOption{
		ClearWriteIntent: o.ClearWriteIntent,
		RemoveVersion:    o.RemoveVersion,
	}
}

func (x *WriteOption) WriteOption() types.WriteOption {
	if x == nil {
		return types.WriteOption{
			ClearWriteIntent: false,
			RemoveVersion:    false,
		}
	}
	return types.WriteOption{
		ClearWriteIntent: x.ClearWriteIntent,
		RemoveVersion:    x.RemoveVersion,
	}
}

func (x *WriteOption) Validate() error {
	if x == nil {
		return nil
	}
	if x.ClearWriteIntent && x.RemoveVersion {
		return errors.Annotatef(errors.ErrInvalidRequest, "x.ClearWriteIntent && x.RemoveVersion")
	}
	return nil
}
