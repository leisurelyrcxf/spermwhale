package commonpb

import (
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/consts"
)

func (x *Error) Error() string {
	return fmt.Sprintf("%s, code: %d", x.Msg, x.Code)
}

func (x *WriteOption) Validate() error {
	if x == nil {
		return &Error{
			Code: consts.ErrCodeInvalidRequest,
			Msg:  "WriteOption == nil",
		}
	}
	if x.ClearWriteIntent && x.RemoveVersion {
		return &Error{
			Code: consts.ErrCodeInvalidRequest,
			Msg:  "x.ClearWriteIntent && x.RemoveVersion",
		}
	}
	return nil
}
