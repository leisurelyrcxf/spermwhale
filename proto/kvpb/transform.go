package kvpb

import (
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"
)

func (x *KVWriteOption) Validate() error {
	if x == nil {
		return &commonpb.Error{
			Code: consts.ErrCodeInvalidRequest,
			Msg:  "WriteOption == nil",
		}
	}
	if x.IsClearWriteIntent() && x.IsRemoveVersion() {
		return &commonpb.Error{
			Code: consts.ErrCodeInvalidRequest,
			Msg:  "x.ClearWriteIntent && x.RemoveVersion",
		}
	}
	return nil
}

func (x *KVWriteOption) IsClearWriteIntent() bool {
	return x.Flag&consts.WriteOptBitMaskClearWriteIntent > 0
}

func (x *KVWriteOption) IsRemoveVersion() bool {
	return x.Flag&consts.WriteOptBitMaskRemoveVersion > 0
}

func (x *KVWriteOption) GetFlagSafe() uint8 {
	return uint8(x.Flag & 0xff)
}

func (x *KVWriteOption) SetFlagSafe(opt uint8) *KVWriteOption {
	x.Flag = uint32(opt)
	return x
}

func (x *KVSetRequest) Validate() error {
	if err := x.Opt.Validate(); err != nil {
		return err
	}
	if x.Opt.IsClearWriteIntent() && x.Value.Meta.HasWriteIntent() {
		return errors.Annotatef(errors.ErrInvalidRequest, "x.Opt.isClearWriteIntent() && x.Value.Meta.HasWriteIntent()")
	}
	return nil
}
