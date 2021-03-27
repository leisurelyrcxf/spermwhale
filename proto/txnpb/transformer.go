package txnpb

func (x *TxnOption) GetTxnType() uint8 {
	return uint8(x.Type)
}
