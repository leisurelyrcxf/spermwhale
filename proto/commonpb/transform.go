package commonpb

import (
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/types"
)

func ToPBError(e *types.Error) *Error {
	if e == nil {
		return nil
	}
	return &Error{
		Code: int32(e.Code),
		Msg:  e.Msg,
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
