package txn

import (
	"context"
	"time"

	"github.com/leisurelyrcxf/spermwhale/utils"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
)

func getValueWrittenByTxnWithRetry(ctx context.Context, kv types.KVCC, key string, txnId types.TxnId, callerTxn *Txn, preventFutureWrite bool, maxRetry int) (val types.ValueCC, exists bool, err error) {
	readOpt := types.NewKVCCReadOption(txnId.Version()).WithExactVersion(txnId.Version())
	if !preventFutureWrite {
		readOpt = readOpt.WithNotUpdateTimestampCache()
	} else if callerTxn.ID == txnId {
		readOpt = readOpt.WithIncrReaderVersion() // hack to prevent future write so we will infer that max_reader_version > write_version hence future write with txnId can't succeed if key not exists
	}
	for i := 0; i < maxRetry && ctx.Err() == nil; i++ {
		if val, err = kv.Get(ctx, key, readOpt); err == nil || errors.IsNotExistsErr(err) {
			return val, err == nil, nil
		}
		glog.Warningf("[getValueWrittenByTxnWithRetry] kv.Get conflicted key %s returns unexpected error: %v", key, err)
		time.Sleep(time.Second)
	}
	if err == nil {
		ctxErr := ctx.Err()
		assert.Must(ctxErr != nil)
		glog.Errorf("kv.Get returns unexpected error: %v", ctxErr)
		return val, false, ctxErr
	}
	glog.Errorf("kv.Get returns unexpected error: %v", err)
	return val, false, err
}

type TransactionStore struct {
	kv  types.KVCC
	cfg types.TxnConfig

	txnInitializer func(txn *Txn)
	txnConstructor func(txnId types.TxnId) *Txn
}

func (s *TransactionStore) loadTransactionRecordWithRetry(ctx context.Context, txnID types.TxnId,
	preventFutureWrite bool, maxRetryTimes int) (txn *Txn, exists bool, err error) {
	readOpt := types.NewKVCCReadOption(types.MaxTxnVersion).WithNotGetMaxReadVersion()
	if !preventFutureWrite {
		readOpt = readOpt.WithNotUpdateTimestampCache()
	}
	for i := 0; i < maxRetryTimes && ctx.Err() == nil; i++ {
		var txnRecordData types.ValueCC
		txnRecordData, err = s.kv.Get(ctx, TransactionKey(txnID), readOpt)
		if err == nil {
			assert.Must(txnRecordData.Meta.Version == txnID.Version())
			txn, err := DecodeTxn(txnRecordData.V)
			if err != nil {
				return nil, true, err
			}
			assert.Must(txn.ID == txnID)
			assert.Must(len(txn.WrittenKeys) > 0)
			assert.Must(txn.State == types.TxnStateStaging || txn.State == types.TxnStateRollbacking)
			s.txnInitializer(txn)
			return txn, true, nil
		}
		if errors.IsNotExistsErr(err) {
			return nil, false, nil
		}
		time.Sleep(time.Second)
	}
	if err == nil {
		assert.Must(ctx.Err() != nil)
		return nil, false, ctx.Err()
	}
	return nil, false, err
}

func (s *TransactionStore) inferTransactionRecord(ctx context.Context, txnID types.TxnId, callerTxn *Txn, conflictedKey string) (*Txn, error) {
	assert.Must(conflictedKey != "")
	// TODO maybe get from txn manager first?
	preventFutureTxnRecordWrite := utils.IsTooStale(txnID.Version(), s.cfg.StaleWriteThreshold)
	txn, recordExists, err := s.loadTransactionRecordWithRetry(ctx, txnID, preventFutureTxnRecordWrite, 2)
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
	vv, keyExists, keyErr := getValueWrittenByTxnWithRetry(ctx, s.kv, conflictedKey, txnID, callerTxn, false /* no need to prevent future write because we can safe rollback if key not exists */, 2)
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
	if !vv.HasWriteIntent() {
		// case 2
		txn.State = types.TxnStateCommitted
		return txn, nil
	}
	if !preventFutureTxnRecordWrite {
		return nil, errors.Annotatef(errors.ErrKeyNotExist, "txn record of %d not exists", txnID)
	}
	// case 3
	// since we've updated timestamp cache of txn record (in case isTooStale is true),
	// guaranteed commit won't succeed in the future (because it needs to write transaction
	// record with intent), hence safe to rollback.
	txn.WrittenKeys = append(txn.WrittenKeys, conflictedKey)
	txn.State = types.TxnStateRollbacking
	_ = txn.rollback(ctx, callerTxn.ID, true, "stale transaction record not found") // help rollback if original txn coordinator was gone
	return txn, nil
}
