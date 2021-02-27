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
	kv     types.KV
	oracle *physical.Oracle
	cfg    types.TxnConfig
	s      *Scheduler
}

func (s *TransactionStore) LoadTransactionRecord(ctx context.Context, txnID types.TxnId, conflictedKey string) (*Txn, error) {
	// TODO maybe get from txn manager first?
	readOpt := types.NewReadOption(math.MaxUint64)
	isTooStale := s.oracle.IsTooStale(txnID.Version(), s.cfg.StaleWriteThreshold)
	if !isTooStale {
		readOpt = readOpt.SetNotUpdateTimestampCache()
	}
	txnRecordData, recordErr := s.kv.Get(ctx, TransactionKey(txnID), readOpt)
	if recordErr != nil && !errors.IsNotExistsErr(recordErr) {
		glog.Errorf("[LoadTransactionRecord] kv.Get txnRecordData returns unexpected error: %v", recordErr)
		return nil, recordErr
	}

	if recordErr == nil {
		assert.Must(txnRecordData.Meta.Version == txnID.Version())
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
		txn.s = s.s
		return txn, nil
	}

	assert.Must(errors.IsNotExistsErr(recordErr))
	// thus will be 3 cases:
	// 1. transaction has been rollbacked, conflictedKey must be gone
	// 2. transaction has been committed and cleared, conflictedKey must have been cleared (no write intent)
	// 3. transaction neither committed nor rollbacked
	vv, keyErr := s.kv.Get(ctx, conflictedKey, types.NewReadOption(txnID.Version()).SetExactVersion())
	if keyErr != nil && !errors.IsNotExistsErr(keyErr) {
		glog.Errorf("[CheckCommitState] kv.Get conflicted key %s returns unexpected error: %v", conflictedKey, keyErr)
		return nil, keyErr
	}
	txn := NewTxn(txnID, s.kv, s.cfg, s.oracle, s, s.s)
	if errors.IsNotExistsErr(keyErr) {
		// case 1
		txn.State = StateRollbacked
		return txn, nil
	}
	assert.Must(keyErr == nil)
	if !vv.WriteIntent {
		// case 2
		txn.State = StateCommitted
		return txn, nil
	}
	// case 3
	if !isTooStale {
		return nil, recordErr
	}
	assert.Must(!readOpt.NotUpdateTimestampCache)
	// since we've updated timestamp cache of txn record (in case isTooStale is true),
	// guaranteed commit won't succeed in the future (because it needs to write transaction
	// record with intent), hence safe to rollback.
	txn.WrittenKeys = append(txn.WrittenKeys, conflictedKey)
	txn.State = StateRollbacking
	_ = txn.rollback(ctx, math.MaxUint64, true, "stale transaction record not found") // help rollback if original txn coordinator was gone
	return txn, nil
}
