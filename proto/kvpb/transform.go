package kvpb

import (
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"
)

func (x *KVWriteOption) Validate() error {
	if x == nil {
		return &commonpb.Error{
			Code: consts.ErrCodeInvalidRequest,
			Msg:  "WriteOption == nil",
		}
	}
	return nil
}

func (x *KVWriteOption) GetFlagSafe() uint8 {
	return uint8(x.Flag & 0xff) // TODO handle endian
}

func (x *KVWriteOption) SetFlagSafe(opt uint8) *KVWriteOption {
	x.Flag = uint32(opt)
	return x
}

func (x *KVSetRequest) Validate() error {
	if err := x.Opt.Validate(); err != nil {
		return err
	}
	return nil
}

func (x *KVReadOption) GetFlagSafe() uint8 {
	return uint8(x.Flag & 0xff)
}

func (x *KVReadOption) SetFlagSafe(opt uint8) *KVReadOption {
	x.Flag = uint32(opt)
	return x
}
