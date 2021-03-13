package commonpb

import (
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/consts"
)

func (x *Error) Error() string {
	return fmt.Sprintf("%s, code: %d", x.Msg, x.Code)
}

func (x *ValueMeta) HasWriteIntent() bool {
	return x.Flag&consts.ValueMetaBitMaskHasWriteIntent == consts.ValueMetaBitMaskHasWriteIntent
}

func (x *ValueMeta) GetFlagSafe() uint8 {
	return uint8(x.Flag & 0xff)
}

func (x *ValueMeta) SetFlag(flag uint8) *ValueMeta {
	x.Flag = uint32(flag)
	return x
}
