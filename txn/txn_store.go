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

type TransactionStore struct {
	kv             types.KV
	oracle         *physical.Oracle
	staleThreshold time.Duration
	asyncJobs      chan<- Job
}

func (s *TransactionStore) GetTxn(ctx context.Context, txnID uint64, conflictedKey string) (*Txn, error) {
	readOpt := types.NewReadOption(math.MaxUint64)
	isTooStale := s.oracle.IsTooStale(txnID, s.staleThreshold)
	if !isTooStale {
		readOpt = readOpt.SetNotUpdateTimestampCache()
	}
	txnRecordData, err := s.kv.Get(ctx, TransactionKey(txnID), readOpt)
	if err != nil && !errors.IsNotExistsErr(err) {
		return nil, err
	}

	if err == nil {
		assert.Must(txnRecordData.Meta.Version == txnID)
		txn, err := DecodeTxn(txnRecordData.V)
		if err != nil {
			return nil, err
		}
		assert.Must(txn.ID == txnID)
		assert.Must(len(txn.WrittenKeys) > 0)
		assert.Must(txn.State == StateStaging)
		txn.kv = s.kv
		txn.staleThreshold = s.staleThreshold
		txn.oracle = s.oracle
		txn.store = s
		txn.asyncJobs = s.asyncJobs
		return txn, nil
	}

	assert.Must(errors.IsNotExistsErr(err))
	if !isTooStale {
		return nil, err
	}

	// since we've updated timestamp cache of txn record,
	// thus transaction commit won't succeed in the future (
	// because it needs to write transaction record with intent),
	// hence safe to rollback.
	txn := NewTxn(txnID, s.kv, s.staleThreshold, s.oracle, s, s.asyncJobs)
	txn.addWrittenKey(conflictedKey)
	_ = txn.Rollback(ctx)
	return txn, nil
}
