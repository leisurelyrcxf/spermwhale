package txn

import (
	"context"
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/types"
)

func txnKey(id uint64) string {
	return fmt.Sprintf("txn_%d", id)
}

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
