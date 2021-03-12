package txnpb

func (x *TxnReadOption) GetFlagSafe() uint8 {
	return uint8(x.Flag & 0xff)
}

func (x *TxnReadOption) SetFlagSafe(opt uint8) *TxnReadOption {
	x.Flag = uint32(opt)
	return x
}
