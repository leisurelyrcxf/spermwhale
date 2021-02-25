package txn

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/types"
)

type Store interface {
	GetTxn(ctx context.Context, id uint64) (*Txn, error)
}

type TransactionStore struct {
	kv types.KV
}

func (s *TransactionStore) GetTxn(ctx context.Context, id uint64, getterTxnVersion uint64) (*Txn, error) {
	val, err := s.kv.Get(ctx, txnKey(id), getterTxnVersion)
	if err != nil {
		return nil, err
	}
	return TxnDecode(val.V)
}
