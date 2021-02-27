package types

import "context"

type TxnId uint64

func (i TxnId) Version() uint64 {
	return uint64(i)
}

type TxnManager interface {
	BeginTransaction(ctx context.Context) (Txn, error)
	Close() error
}

type Txn interface {
	GetId() TxnId
	Get(ctx context.Context, key string) (Value, error)
	Set(ctx context.Context, key string, val []byte) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}
