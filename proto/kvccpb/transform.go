package kvccpb

import (
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"
)

func (x *KVCCWriteOption) Validate() error {
	if x == nil {
		return &commonpb.Error{
			Code: consts.ErrCodeInvalidRequest,
			Msg:  "WriteOption == nil",
		}
	}
	return nil
}

func (x *KVCCWriteOption) GetFlagSafe() uint8 {
	return uint8(x.Flag & 0xff)
}

func (x *KVCCWriteOption) SetFlagSafe(opt uint8) *KVCCWriteOption {
	x.Flag = uint32(opt)
	return x
}

func (x *KVCCSetRequest) Validate() error {
	if err := x.Opt.Validate(); err != nil {
		return err
	}
	if x.Value.IsEmpty() {
		return errors.Annotatef(errors.ErrInvalidRequest, "x.Value.IsEmpty()")
	}
	return nil
}
