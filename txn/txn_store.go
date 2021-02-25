package txn

import (
	"context"
	"math"
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"
	"github.com/leisurelyrcxf/spermwhale/types"
)

type Store interface {
	GetTxn(ctx context.Context, id uint64) (*Txn, error)
}

type TransactionStore struct {
	kv          types.KV
	oracle      *physical.Oracle
	stalePeriod time.Duration
}

func (s *TransactionStore) GetTxn(ctx context.Context, txnID uint64) (*Txn, error) {
	val, err := s.kv.Get(ctx, TransactionKey(txnID), types.NewReadOption(math.MaxUint64).SetNotUpdateTimestampCache())
	if err != nil && !errors.IsKeyNotExistsErr(err) {
		return nil, err
	}
	if err == nil {
		assert.Must(val.Meta.Version == txnID)
		return DecodeTxn(val.V)
	}

	if !s.oracle.IsTooStale(txnID, s.stalePeriod) {
		return nil, err
	}

	if val, err = s.kv.Get(ctx, TransactionKey(txnID), types.NewReadOption(math.MaxUint64)); err != nil &&
		!errors.IsKeyNotExistsErr(err) {
		return nil, err
	}

	if err == nil {
		assert.Must(val.Meta.Version == txnID)
		return DecodeTxn(val.V)
	}

	// errors.IsKeyNotExistsErr(err), since we updated timestamp cache of txn record,
	// thus txn commit will never succeed in the future, hence safe to rollback.
	txn := NewTxn(txnID, s.kv)
	return txn, txn.Rollback(ctx)
}
