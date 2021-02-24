package kvpb

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

func (x *VersionedValue) ToVersionedValue() types.VersionedValue {
	return types.VersionedValue{
		Value: types.Value{
			V: x.Value.Val,
			M: types.Meta{
				WriteIntent: x.Value.Meta.WriteIntent,
			},
		},
		Version: x.Version,
	}
}
