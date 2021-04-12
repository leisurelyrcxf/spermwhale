package txn

import (
	"context"
	"time"

	"github.com/leisurelyrcxf/spermwhale/txn/ttypes"

	"github.com/leisurelyrcxf/spermwhale/types/basic"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
)

type TransactionStore struct {
	kv              types.KVCC
	cfg             types.TxnManagerConfig
	retryWaitPeriod time.Duration

	txnInitializer        func(txn *Txn)
	partialTxnConstructor func(txnId types.TxnId, state types.TxnState, writtenKeys ttypes.KeyVersions) *Txn
}

func (s *TransactionStore) getValueWrittenByTxnWithRetry(ctx context.Context, key string, txnId types.TxnId, callerTxn *Txn,
	preventFutureWrite bool, getMaxReadVersion bool, maxRetry int) (val types.ValueCC, exists bool, notExistsErrSubCode int32, err error) {
	readOpt := types.NewKVCCCheckKeyReadOption(callerTxn.ID.Version(), txnId.Version())
	if !preventFutureWrite {
		readOpt.SetNotUpdateTimestampCache()
	} else {
		if readOpt.ReaderVersion == txnId.Version() {
			readOpt.ReaderVersion++ // hack to prevent future write so we will infer that max_reader_version > write_version to prevent future write if key not exists
		}
		assert.Must(readOpt.ReaderVersion > txnId.Version())
	}
	if !getMaxReadVersion {
		readOpt.SetNotGetMaxReadVersion()
	}
	for i := 0; ; {
		if val, err = s.kv.Get(ctx, key, *readOpt); err == nil || errors.IsNotExistsErrEx(err, &notExistsErrSubCode) {
			return val, err == nil, notExistsErrSubCode, nil
		}
		glog.Warningf("[getValueWrittenByTxnWithRetry] kv.Get conflicted key %s returns unexpected error: %v", key, err)
		if i++; i >= maxRetry || ctx.Err() != nil {
			return val, false, 0, err
		}
		time.Sleep(s.retryWaitPeriod)
	}
}

func (s *TransactionStore) getAnyValueWrittenByTxnWithRetry(ctx context.Context, keys ttypes.KeyVersions,
	txnId types.TxnId, callTxn *Txn, maxRetry int) (key string, val types.ValueCC, exists bool, notExistsErrSubCode int32, err error) {
	assert.Must(len(keys) > 0)
	for i := 0; ; {
		for key := range keys {
			val, err = s.kv.Get(ctx, key, *types.NewKVCCCheckKeyReadOption(callTxn.ID.Version(), txnId.Version()).
				SetNotUpdateTimestampCache().SetNotGetMaxReadVersion())
			if err == nil || errors.IsNotExistsErrEx(err, &notExistsErrSubCode) {
				return key, val, err == nil, notExistsErrSubCode, nil
			}
			glog.Warningf("[getAnyValueWrittenByTxn] kv.Get conflicted key %s returns unexpected error: %v", key, err)
			if i += 1; i >= maxRetry || ctx.Err() != nil {
				glog.Errorf("[getAnyValueWrittenByTxn] txn.kv.Get returns unexpected error: %v", err)
				return "", types.EmptyValueCC, false, 0, err
			}
			time.Sleep(s.retryWaitPeriod)
		}
	}
}

func (s *TransactionStore) loadTransactionRecordWithRetry(ctx context.Context, txnID types.TxnId, allWrittenKey2LastVersion ttypes.KeyVersions,
	preventFutureWrite bool, txnRecordFlag *types.VFlag, maxRetryTimes int) (txn *Txn, preventedFutureWrite bool, err error) {
	readOpt := types.NewKVCCCheckTxnRecordReadOption(txnID)
	if !preventFutureWrite {
		readOpt.SetNotUpdateTimestampCache().SetNotGetMaxReadVersion() // need we support getMaxReadVersion?
	}
	for i := 0; ; {
		var txnRecordData types.ValueCC
		txnRecordData, err = s.kv.Get(ctx, "", *readOpt)
		*txnRecordFlag = txnRecordData.VFlag
		if txnRecordData.IsAborted() {
			return s.partialTxnConstructor(txnID, types.TxnStateRollbacking, allWrittenKey2LastVersion).setErr(errors.ErrTransactionRecordNotFoundAndFoundAbortedValue), false, nil
		}
		if err == nil {
			assert.Must(txnRecordData.Meta.Version == txnID.Version())
			txn, err := DecodeTxn(txnID, txnRecordData.V)
			if err != nil {
				return nil, false, err
			}
			s.txnInitializer(txn)
			return txn, false, nil
		}
		if errors.IsNotExistsErr(err) {
			return nil, txnRecordData.MaxReadVersion > txnID.Version(), nil
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
	keysWithWriteIntent basic.Set, allWrittenKey2LastVersion ttypes.KeyVersions,
	preventFutureTxnRecordWrite bool,
	maxRetry int) (txn *Txn, committedKey string, err error) {
	// TODO maybe get from txn manager first?
	var (
		preventedFutureTxnRecordWrite bool
		txnRecordFlag                 types.VFlag
	)
	if txn, preventedFutureTxnRecordWrite, err = s.loadTransactionRecordWithRetry(ctx, txnId, allWrittenKey2LastVersion, preventFutureTxnRecordWrite, &txnRecordFlag, maxRetry); err != nil {
		return nil, "", err
	}
	if txn != nil {
		assert.Must(txn.ID == txnId)
		for key := range keysWithWriteIntent {
			assert.Must(txn.ContainsWrittenKey(key)) // TODO remove this in product
		}
		if txnRecordFlag.IsCommitted() && txn.AreWrittenKeysCompleted() { // TODO this is not necessary, only used for assertions, 'txn.AreWrittenKeysCompleted()' can be removed in product.
			assert.Must(txn.IsStaging())
			txn.MarkAllWrittenKeysCommitted()
			txn.TxnState = types.TxnStateCommitted
			return txn, "txn-record", nil
		}
		return txn, "", nil
	}
	// Txn record not exists
	assert.Must(!preventFutureTxnRecordWrite || preventedFutureTxnRecordWrite)

	// Transaction record not exists, thus must be one among the 3 cases:
	// 1. transaction has been committed and cleared <=> all keys must have cleared write intent before the transaction record was removed, see the func Txn::onCommitted
	// 2. transaction has been rollbacked => all keys must have been removed before the transaction record was removed, see the func Txn::rollback
	// 3. transaction neither committed nor rollbacked
	//
	// Hence we get the keys. (we may get the same key a second time because the result of the key before seen transaction record not exists is not confident enough)
	// Transaction record not exists, get key to find out the truth.
	assert. /* do not remove this*/ Must(preventedFutureTxnRecordWrite || (len(keysWithWriteIntent) == len(allWrittenKey2LastVersion) && len(keysWithWriteIntent) == 1))
	var (
		key                 string
		vv                  types.ValueCC
		keyExists           bool
		notExistsErrSubCode int32
		keyErr              error
	)
	if len(keysWithWriteIntent) == 1 {
		assert.Must(len(allWrittenKey2LastVersion) == 1)
		conflictedKey := allWrittenKey2LastVersion.MustFirstKey()
		assert.Must(keysWithWriteIntent.Contains(conflictedKey))
		vv, keyExists, notExistsErrSubCode, keyErr = s.getValueWrittenByTxnWithRetry(ctx, conflictedKey, txnId, callerTxn, false /*no need*/, false /*no need*/, maxRetry)
		key = conflictedKey
	} else {
		key, vv, keyExists, notExistsErrSubCode, keyErr = s.getAnyValueWrittenByTxnWithRetry(ctx, allWrittenKey2LastVersion, txnId, callerTxn, maxRetry)
	}
	assert.Must(!vv.IsAborted() || (!keyExists && keyErr == nil))
	if keyErr != nil {
		glog.Errorf("[loadTransactionRecord] s.getAnyValueWrittenByTxnWithRetry(txnId(%d), callerTxn(%d), keys(%v)) returns unexpected error: %v", txnId, callerTxn.ID, allWrittenKey2LastVersion, keyErr)
		return nil, "", keyErr
	}
	if keyExists {
		if vv.IsCommitted() {
			// case 1
			txn := s.partialTxnConstructor(txnId, types.TxnStateCommitted, allWrittenKey2LastVersion)
			txn.MarkWrittenKeyCommitted(key, vv.Value)
			//if preventedFutureTxnRecordWrite { // TODO test against this
			//	txn.MarkWrittenKeyCommitted(key, vv.Value)
			//}
			return txn, key, nil
		}
		// Must haven't committed.
		if !preventedFutureTxnRecordWrite {
			return nil, "", errors.Annotatef(errors.ErrKeyOrVersionNotExist, "txn record of %d not exists", txnId)
		}
	}
	// 1. key not exists
	//    1.1. preventedFutureTxnRecordWrite, safe to rollback
	//    1.2. len(keysWithWriteIntent) == len(allWrittenKey2LastVersion) == 1, key with write intent disappeared, safe to rollback
	// 2. preventedFutureTxnRecordWrite && key exists & vv.IsDirty(), since we've prevented write txn record,
	//	  guaranteed commit won't succeed in the future, hence safe to rollback.
	if txn = s.partialTxnConstructor(txnId, types.TxnStateRollbacking, allWrittenKey2LastVersion); !keyExists {
		txn.MarkWrittenKeyAborted(key, notExistsErrSubCode)
	}
	if vv.IsAborted() {
		txn.err = errors.ErrTransactionRecordNotFoundAndFoundAbortedValue
	} else if preventedFutureTxnRecordWrite {
		txn.err = errors.ErrTransactionRecordNotFoundAndWontBeWritten
	} else {
		assert.Must(!keyExists && len(allWrittenKey2LastVersion) == 1 && len(keysWithWriteIntent) == 1)
		// nothing to rollback
		//_ = txn.rollback(ctx, callerTxn.ID, true, fmt.Sprintf("write intent of key %s disappeared", key)) // help rollback if original txn coordinator was gone
	}
	return txn, "", nil
}
