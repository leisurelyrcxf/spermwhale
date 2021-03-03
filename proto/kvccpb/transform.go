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
	if x.isClearWriteIntent() && x.isRemoveVersion() {
		return &commonpb.Error{
			Code: consts.ErrCodeInvalidRequest,
			Msg:  "x.isClearWriteIntent() && x.isRemoveVersion()",
		}
	}
	return nil
}

func (x *KVCCWriteOption) isClearWriteIntent() bool {
	return x.Flag&consts.WriteOptBitMaskClearWriteIntent > 0
}

func (x *KVCCWriteOption) isRemoveVersion() bool {
	return x.Flag&consts.WriteOptBitMaskRemoveVersion > 0
}

func (x *KVCCWriteOption) GetFlagSafe() uint8 {
	return uint8(x.Flag & 0xff)
}

func (x *KVCCWriteOption) SetFlagSafe(opt uint8) *KVCCWriteOption {
	x.Flag = uint32(opt)
	return x
}

func (x *KVCCReadOption) GetFlagSafe() uint8 {
	return uint8(x.Flag & 0xff)
}

func (x *KVCCReadOption) SetFlagSafe(opt uint8) *KVCCReadOption {
	x.Flag = uint32(opt)
	return x
}

func (x *KVCCSetRequest) Validate() error {
	if err := x.Opt.Validate(); err != nil {
		return err
	}
	if x.Opt.isClearWriteIntent() && x.Value.Meta.HasWriteIntent() {
		return errors.Annotatef(errors.ErrInvalidRequest, "x.Opt.isClearWriteIntent() && x.Value.Meta.HasWriteIntent()")
	}
	return nil
}
