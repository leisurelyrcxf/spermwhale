package txn

import (
	"context"
	"fmt"
	"math"

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

func (s *TransactionStore) GetTxn(ctx context.Context, id uint64) (*Txn, error) {
	val, err := s.kv.Get(ctx, txnKey(id), math.MaxUint64)
	if err != nil {
		return nil, errr
	}
	val.V
}
