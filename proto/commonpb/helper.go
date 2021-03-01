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
	if x.IsClearWriteIntent() && x.IsRemoveVersion() {
		return &Error{
			Code: consts.ErrCodeInvalidRequest,
			Msg:  "x.ClearWriteIntent && x.RemoveVersion",
		}
	}
	return nil
}

func (x *WriteOption) IsClearWriteIntent() bool {
	return x.Flag&consts.WriteOptBitMaskClearWriteIntent > 0
}

func (x *WriteOption) IsRemoveVersion() bool {
	return x.Flag&consts.WriteOptBitMaskRemoveVersion > 0
}

func (x *WriteOption) GetFlagSafe() uint8 {
	return uint8(x.Flag & 0xff)
}

func (x *WriteOption) SetFlagSafe(opt uint8) *WriteOption {
	x.Flag = uint32(opt)
	return x
}

func (x *ReadOption) GetFlagSafe() uint8 {
	return uint8(x.Flag & 0xff)
}

func (x *ReadOption) SetFlagSafe(opt uint8) *ReadOption {
	x.Flag = uint32(opt)
	return x
}

func (x *ValueMeta) HasWriteIntent() bool {
	return x.Flag&consts.ValueMetaBitMaskHasWriteIntent > 0
}

func (x *ValueMeta) GetFlagSafe() uint8 {
	return uint8(x.Flag & 0xff)
}

func (x *ValueMeta) SetFlag(flag uint8) *ValueMeta {
	x.Flag = uint32(flag)
	return x
}
