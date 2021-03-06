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

type TransactionStore struct {
	kv              types.KVCC
	cfg             types.TxnManagerConfig
	retryWaitPeriod time.Duration

	txnInitializer func(txn *Txn)
	txnConstructor func(txnId types.TxnId) *Txn
}

func (s *TransactionStore) getValueWrittenByTxnWithRetry(ctx context.Context, key string, txnId types.TxnId, callerTxn *Txn,
	preventFutureWrite bool, getMaxReadVersion bool, maxRetry int) (val types.ValueCC, exists bool, err error) {
	readOpt := types.NewKVCCReadOption(callerTxn.ID.Version()).WithExactVersion(txnId.Version())
	if !preventFutureWrite {
		readOpt = readOpt.WithNotUpdateTimestampCache()
	} else {
		if readOpt.ReaderVersion == txnId.Version() {
			readOpt = readOpt.WithIncrReaderVersion() // hack to prevent future write so we will infer that max_reader_version > write_version to prevent future write if key not exists
		}
		assert.Must(txnId.Version() < readOpt.ReaderVersion)
	}
	if !getMaxReadVersion {
		readOpt = readOpt.WithNotGetMaxReadVersion()
	}
	for i := 0; ; {
		if val, err = s.kv.Get(ctx, key, readOpt); err == nil || errors.IsNotExistsErr(err) {
			return val, err == nil, nil
		}
		glog.Warningf("[getValueWrittenByTxnWithRetry] kv.Get conflicted key %s returns unexpected error: %v", key, err)
		if i++; i >= maxRetry || ctx.Err() != nil {
			return val, false, err
		}
		time.Sleep(s.retryWaitPeriod)
	}
}

func (s *TransactionStore) getAnyValueWrittenByTxnWithRetry(ctx context.Context, keys []string, txnId types.TxnId, callTxn *Txn, maxRetry int) (key string, val types.ValueCC, exists bool, err error) {
	assert.Must(len(keys) > 0)
	for i := 0; ; {
		for _, key := range keys {
			val, err = s.kv.Get(ctx, key, types.NewKVCCReadOption(callTxn.ID.Version()).WithExactVersion(txnId.Version()).
				WithNotUpdateTimestampCache().WithNotGetMaxReadVersion())
			if err == nil || errors.IsNotExistsErr(err) {
				return key, val, err == nil, nil
			}
			glog.Warningf("[getAnyValueWrittenByTxn] kv.Get conflicted key %s returns unexpected error: %v", key, err)
			if i += 1; i >= maxRetry || ctx.Err() != nil {
				glog.Errorf("[getAnyValueWrittenByTxn] txn.kv.Get returns unexpected error: %v", err)
				return "", types.EmptyValueCC, false, err
			}
			time.Sleep(s.retryWaitPeriod)
		}
	}
}

func (s *TransactionStore) loadTransactionRecordWithRetry(ctx context.Context, txnID types.TxnId,
	preventFutureWrite bool, maxRetryTimes int) (txn *Txn, exists bool, err error) {
	readOpt := types.NewKVCCReadOption(types.MaxTxnVersion).WithNotGetMaxReadVersion()
	if !preventFutureWrite {
		readOpt = readOpt.WithNotUpdateTimestampCache()
	}
	for i := 0; ; {
		var txnRecordData types.ValueCC
		txnRecordData, err = s.kv.Get(ctx, TransactionKey(txnID), readOpt)
		if err == nil {
			assert.Must(txnRecordData.Meta.Version == txnID.Version())
			txn, err := DecodeTxn(txnRecordData.V)
			if err != nil {
				return nil, true, err
			}
			s.txnInitializer(txn)
			return txn, true, nil
		}
		if errors.IsNotExistsErr(err) {
			return nil, false, nil
		}
		if i++; i >= maxRetryTimes || ctx.Err() != nil {
			return nil, false, err
		}
		time.Sleep(s.retryWaitPeriod)
	}
}

func (s *TransactionStore) inferTransactionRecordWithRetry(
	ctx context.Context,
	txnId types.TxnId, callerTxn *Txn,
	keysWithWriteIntent map[string]struct{},
	allKeys []string,
	preventFutureTxnRecordWrite bool,
	maxRetry int) (txn *Txn, err error) {
	// TODO maybe get from txn manager first?
	var recordExists bool
	if txn, recordExists, err = s.loadTransactionRecordWithRetry(ctx, txnId, preventFutureTxnRecordWrite, maxRetry); err != nil {
		return nil, err
	}
	if recordExists {
		assert.Must(txn.ID == txnId)
		if len(keysWithWriteIntent) > 0 { // TODO remove this in product
			assert.MustAllContain(utils.StringList2Set(txn.WrittenKeys), utils.Set2StringList(keysWithWriteIntent))
		}
		assert.Must(txn.State == types.TxnStateStaging || txn.State == types.TxnStateRollbacking)
		return txn, nil
	}

	// Transaction record not exists, thus must be one among the 3 cases:
	// 1. transaction has been committed and cleared <=> all keys must have cleared write intent before the transaction record was removed, see the func Txn::onCommitted
	// 2. transaction has been rollbacked => all keys must have been removed before the transaction record was removed, see the func Txn::rollback
	// 3. transaction neither committed nor rollbacked
	//
	// Hence we get the keys. (we may get the same key a second time because the result of the key before seen transaction record not exists is not confident enough)
	// Transaction record not exists, get key to find out the truth.
	assert. /* do not remove this*/ Must(preventFutureTxnRecordWrite || len(keysWithWriteIntent) == len(allKeys))
	var (
		vv        types.ValueCC
		keyExists bool
		keyErr    error
	)
	if len(keysWithWriteIntent) == 1 {
		assert.Must(utils.Contains(keysWithWriteIntent, allKeys[0]))
		vv, keyExists, keyErr = s.getValueWrittenByTxnWithRetry(ctx, allKeys[0], txnId, callerTxn, false /*no need*/, false /*no need*/, maxRetry)
	} else {
		_, vv, keyExists, keyErr = s.getAnyValueWrittenByTxnWithRetry(ctx, allKeys, txnId, callerTxn, maxRetry)
	}
	if keyErr != nil {
		glog.Errorf("[loadTransactionRecord] s.getAnyValueWrittenByTxnWithRetry(txnId(%d), callerTxn(%d), keys(%s)) returns unexpected error: %v", txnId, callerTxn.ID, allKeys, keyErr)
		return nil, keyErr
	}
	if keyExists {
		if !vv.HasWriteIntent() {
			// case 1
			txn := s.txnConstructor(txnId)
			txn.WrittenKeys = allKeys
			txn.State = types.TxnStateCommitted
			if txnId == callerTxn.ID {
				txn.h.RemoveTxn(txn)
			}
			return txn, nil
		}
		// Must haven't committed.
		if !preventFutureTxnRecordWrite {
			return nil, errors.Annotatef(errors.ErrKeyNotExist, "txn record of %d not exists", txnId)
		}
	}
	// 1. key not exists
	//    1.1. preventFutureTxnRecordWrite, safe to rollback
	//    1.2. len(keysWithWriteIntent) == len(allKeys), one of the keys with write intent disappeared, safe to rollback
	// 2. key exists & vv.HasWriteIntent() && preventFutureTxnRecordWrite, since we've updated timestamp cache of txn record,
	//	  guaranteed commit won't succeed in the future (because it needs to write transaction record with intent),
	//	  hence safe to rollback., safe to rollback
	txn = s.txnConstructor(txnId)
	txn.WrittenKeys = allKeys
	if !keyExists && len(txn.WrittenKeys) == 1 {
		txn.State = types.TxnStateRollbacked // nothing to rollback
		if txnId == callerTxn.ID {
			txn.h.RemoveTxn(txn)
		}
		return txn, nil
	}
	txn.State = types.TxnStateRollbacking
	_ = txn.rollback(ctx, callerTxn.ID, true, "transaction record not found and prevented from being written") // help rollback if original txn coordinator was gone
	return txn, nil
}
