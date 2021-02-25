package commonpb

import (
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/consts"

	"github.com/leisurelyrcxf/spermwhale/types"
)

func ToPBError(e error) *Error {
	if e == nil {
		return nil
	}
	if e, ok := e.(*types.Error); ok {
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
	return fmt.Errorf("%v, error_code: %d", x.Msg, x.Code)
}

func (x *Value) ToValue() types.Value {
	return types.Value{
		V: x.Val,
		Meta: types.Meta{
			WriteIntent: x.Meta.WriteIntent,
			Version:     x.Meta.Version,
		},
	}
}

func (x *ValueMeta) Meta() types.Meta {
	return types.Meta{
		WriteIntent: x.WriteIntent,
		Version:     x.Version,
	}
}
