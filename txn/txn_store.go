package txn

import (
	"context"
	"math"
	"time"

	"github.com/leisurelyrcxf/spermwhale/utils"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
)

func getValueWrittenByTxn(ctx context.Context, kv types.KV, key string, txn types.TxnId, maxRetry int) (val types.Value, exists bool, err error) {
	for i := 0; i < maxRetry; i++ {
		ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		val, err = kv.Get(ctx, key, types.NewReadOption(txn.Version()).SetExactVersion())
		cancel()
		if err == nil {
			return val, true, nil
		}
		if errors.IsNotExistsErr(err) {
			return types.Value{}, false, nil
		}
		glog.Warningf("[getValueWrittenByTxn] kv.Get conflicted key %s returns unexpected error: %v", key, err)
		time.Sleep(time.Second)
	}
	assert.Must(err != nil)
	glog.Errorf("kv.Get returns unexpected error: %v", err)
	return types.Value{}, false, err
}

type TransactionStore struct {
	kv  types.KV
	cfg types.TxnConfig

	txnInitializer func(txn *Txn)
	txnConstructor func(txnId types.TxnId) *Txn
}

func (s *TransactionStore) loadTransactionRecordWithRetry(ctx context.Context, txnID types.TxnId, readOpt types.ReadOption, maxRetryTimes int) (txn *Txn, exists bool, err error) {
	assert.Must(maxRetryTimes > 0)
	for i := 0; i < maxRetryTimes; i++ {
		var isRetryableErr bool

		ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		txn, exists, err, isRetryableErr = s.loadTransactionRecord(ctx, txnID, readOpt)
		cancel()
		if err == nil {
			return txn, exists, nil
		}
		if !isRetryableErr {
			return nil, false, err
		}
		time.Sleep(time.Second)
	}
	assert.Must(txn == nil && err != nil)
	return nil, false, err
}

// NOTE: may return (nil, nil) which represents transaction record not exists
func (s *TransactionStore) loadTransactionRecord(ctx context.Context, txnID types.TxnId, readOpt types.ReadOption) (_ *Txn, exists bool, _ error, retryableErr bool) {
	txnRecordData, recordErr := s.kv.Get(ctx, TransactionKey(txnID), readOpt)
	if recordErr != nil && !errors.IsNotExistsErr(recordErr) {
		glog.Errorf("[loadTransactionRecord] kv.Get txnRecordData returns unexpected error: %v", recordErr)
		return nil, false, recordErr, true
	}

	if errors.IsNotExistsErr(recordErr) {
		return nil, false, nil, false
	}

	assert.Must(recordErr == nil)
	assert.Must(txnRecordData.Meta.Version == txnID.Version())
	txn, err := DecodeTxn(txnRecordData.V)
	if err != nil {
		return nil, false, err, false
	}
	assert.Must(txn.ID == txnID)
	assert.Must(len(txn.WrittenKeys) > 0)
	assert.Must(txn.State == types.TxnStateStaging || txn.State == types.TxnStateRollbacking)
	s.txnInitializer(txn)
	return txn, true, nil, false
}

func (s *TransactionStore) inferTransactionRecord(ctx context.Context, txnID types.TxnId, callerTxn types.TxnId, conflictedKey string) (*Txn, error) {
	// TODO maybe get from txn manager first?
	readOpt := types.NewReadOption(math.MaxUint64)
	isTooStale := utils.IsTooStale(txnID.Version(), s.cfg.StaleWriteThreshold)
	if !isTooStale {
		readOpt = readOpt.SetNotUpdateTimestampCache()
	}
	txn, recordExists, err := s.loadTransactionRecordWithRetry(ctx, txnID, readOpt, 2)
	if err != nil {
		return nil, err
	}
	if recordExists {
		assert.Must(txn != nil)
		return txn, nil
	}
	// Transaction record not exists
	// There will be 3 cases:
	// 1. transaction has been rollbacked, conflictedKey must be gone
	// 2. transaction has been committed and cleared, conflictedKey must have been cleared (no write intent)
	// 3. transaction neither committed nor rollbacked
	vv, keyExists, keyErr := getValueWrittenByTxn(ctx, s.kv, conflictedKey, txnID, 2)
	if keyErr != nil {
		glog.Errorf("[loadTransactionRecord] kv.Get conflicted key %s returns unexpected error: %v", conflictedKey, keyErr)
		return nil, keyErr
	}
	txn = s.txnConstructor(txnID)
	if !keyExists {
		// case 1
		txn.State = types.TxnStateRollbacked
		// nothing to rollback
		return txn, nil
	}
	if !vv.WriteIntent {
		// case 2
		txn.State = types.TxnStateCommitted
		return txn, nil
	}
	// case 3
	if !isTooStale {
		return nil, errors.Annotatef(errors.ErrKeyNotExist, "txn record of %d not exists", txnID)
	}
	assert.Must(!readOpt.NotUpdateTimestampCache)
	// since we've updated timestamp cache of txn record (in case isTooStale is true),
	// guaranteed commit won't succeed in the future (because it needs to write transaction
	// record with intent), hence safe to rollback.
	txn.WrittenKeys = append(txn.WrittenKeys, conflictedKey)
	txn.State = types.TxnStateRollbacking
	_ = txn.rollback(ctx, callerTxn, true, "stale transaction record not found") // help rollback if original txn coordinator was gone
	return txn, nil
}
