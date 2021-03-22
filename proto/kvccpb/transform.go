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
	if x.IsClearWriteIntent() && x.IsRemoveVersion() {
		return &commonpb.Error{
			Code: consts.ErrCodeInvalidRequest,
			Msg:  "x.isClearWriteIntent() && x.isRemoveVersion()",
		}
	}
	if x.IsRollbackVersion() && !x.IsRemoveVersion() {
		return &commonpb.Error{
			Code: consts.ErrCodeInvalidRequest,
			Msg:  "x.IsRollbackVersion() && !x.IsRemoveVersion()",
		}
	}
	return nil
}

func (x *KVCCWriteOption) IsClearWriteIntent() bool {
	return consts.IsWriteOptClearWriteIntent(uint8(x.Flag))
}

func (x *KVCCWriteOption) IsRemoveVersion() bool {
	return consts.IsWriteOptRemoveVersion(uint8(x.Flag))
}

func (x *KVCCWriteOption) IsRollbackVersion() bool {
	return consts.IsWriteOptRollbackVersion(uint8(x.Flag))
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
	if x.Value.IsEmpty() {
		return errors.Annotatef(errors.ErrInvalidRequest, "x.Value.IsEmpty()")
	}
	if x.Value.Meta.SnapshotVersion != 0 {
		return errors.Annotatef(errors.ErrInvalidRequest, "x.Value.Meta.SnapshotVersion != 0")
	}
	if x.Opt.IsClearWriteIntent() && x.Value.Meta.HasWriteIntent() {
		return errors.Annotatef(errors.ErrInvalidRequest, "x.Opt.IsClearWriteIntent() && x.Value.Meta.HasWriteIntent()")
	}
	if x.Opt.IsRemoveVersion() && x.Value.Meta.HasWriteIntent() {
		return errors.Annotatef(errors.ErrInvalidRequest, "x.Opt.IsRemoveVersion() && x.Value.Meta.HasWriteIntent()")
	}
	return nil
}
