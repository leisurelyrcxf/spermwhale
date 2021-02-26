package txn

import (
	"context"
	"math"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"
	"github.com/leisurelyrcxf/spermwhale/types"
)

type TransactionStore struct {
	kv        types.KV
	oracle    *physical.Oracle
	cfg       types.TxnConfig
	asyncJobs chan<- Job
}

func (s *TransactionStore) LoadTransactionRecord(ctx context.Context, txnID uint64, conflictedKey string) (*Txn, error) {
	// TODO maybe get from txn manager first?
	readOpt := types.NewReadOption(math.MaxUint64)
	isTooStale := s.oracle.IsTooStale(txnID, s.cfg.StaleWriteThreshold)
	if !isTooStale {
		readOpt = readOpt.SetNotUpdateTimestampCache()
	}
	txnRecordData, err := s.kv.Get(ctx, TransactionKey(txnID), readOpt)
	if err != nil && !errors.IsNotExistsErr(err) {
		glog.Errorf("[LoadTransactionRecord] kv.Get txnRecordData returns unexpected error: %v", err)
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
		txn.cfg = s.cfg
		txn.oracle = s.oracle
		txn.store = s
		txn.asyncJobs = s.asyncJobs
		return txn, nil
	}

	assert.Must(errors.IsNotExistsErr(err))
	if !isTooStale {
		return nil, err
	}

	assert.Must(!readOpt.NotUpdateTimestampCache)
	// since we've updated timestamp cache of txn record,
	// thus will be 3 cases:
	// 1. transaction has rollbacked
	// 2. transaction has committed
	// 3. transaction not commit yet and guaranteed commit won't
	// succeed in the future (because it needs to write transaction
	// record with intent), hence safe to rollback.
	vv, err := s.kv.Get(ctx, conflictedKey, types.NewReadOption(txnID).SetExactVersion())
	if err != nil && !errors.IsNotExistsErr(err) {
		glog.Errorf("[CheckCommitState] kv.Get conflicted key %s returns unexpected error: %v", conflictedKey, err)
		return nil, err
	}
	txn := NewTxn(txnID, s.kv, s.cfg, s.oracle, s, s.asyncJobs)
	if errors.IsNotExistsErr(err) {
		// case 1
		txn.State = StateRollbacked
		return txn, nil
	}
	assert.Must(err == nil)
	if !vv.WriteIntent {
		// case 2
		txn.State = StateCommitted
		return txn, nil
	}
	// case 3
	txn.AddWrittenKey(conflictedKey)
	txn.State = StateRollbacking
	_ = txn.rollback(ctx, txn.ID, true, "stale transaction record not found") // help rollback if original txn coordinator was gone
	return txn, nil
}
