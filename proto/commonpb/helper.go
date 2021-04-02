package commonpb

import (
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/consts"
)

func (x *Error) Error() string {
	return fmt.Sprintf("%s, code: %d", x.Msg, x.Code)
}

func (x *ValueMeta) IsDirty() bool {
	return x.Flag&consts.ValueMetaBitMaskHasWriteIntent == consts.ValueMetaBitMaskHasWriteIntent
}

func (x *ValueMeta) GetFlagSafe() uint8 {
	return uint8(x.Flag & 0xff)
}

func (x *ValueMeta) SetFlag(flag uint8) *ValueMeta {
	x.Flag = uint32(flag)
	return x
}

func (x *Value) IsEmpty() bool {
	return x == nil || x.Meta == nil || x.Meta.Version == 0
}

func (x *Value) Validate() error {
	if x == nil {
		return fmt.Errorf("[Value] x == nil")
	}
	if x.Meta == nil {
		return fmt.Errorf("[Value] x.Meta == nil")
	}
	return nil
}
