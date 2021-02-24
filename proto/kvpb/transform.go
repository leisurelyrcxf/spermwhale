package kvpb

import (
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/types"
)

func (x *Error) Error() error {
	if x == nil {
		return nil
	}
	return fmt.Errorf("%v, error_code: %d", x.Msg, x.Code)
}

func (x *VersionedValue) ToVersionedValue() types.VersionedValue {
	return types.VersionedValue{
		Value:   x.Value,
		Version: x.Version,
	}
}
