package txnpb

import "fmt"

func (x *TxnOption) GetTxnType() uint8 {
	return uint8(x.Type)
}

func (x *TxnSnapshotReadOption) GetFlagAsUint8() uint8 {
	return uint8(x.Flag)
}

func (x *TValue) Validate() error {
	if x == nil {
		return fmt.Errorf("[TValue] x == nil")
	}
	return x.Value.Validate()
}

type TValues []*TValue

func (x TValues) Validate(expectedLen int) error {
	if x == nil {
		return fmt.Errorf("[TValues] x == nil")
	}
	if len(x) != expectedLen {
		return fmt.Errorf("[TValues] expected length %d, got %d", expectedLen, len(x))
	}
	for _, v := range x {
		if err := v.Validate(); err != nil {
			return err
		}
	}
	return nil
}
