package kvcc

import (
	"context"
	"math"
	"time"

	"github.com/leisurelyrcxf/spermwhale/types/concurrency"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/bench"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/kvcc/transaction"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

const OCCVerboseLevel = 110

// KV with concurrency control
type KVCC struct {
	types.TabletTxnConfig

	db types.KV

	txnManager *transaction.Manager
	lm         concurrency.AdvancedTxnLockManager
	tsCache    *TimestampCache
}

func NewKVCC(db types.KV, cfg types.TabletTxnManagerConfig) *KVCC {
	// Wait until uncertainty passed because timestamp cache is
	// invalid during starting (lost last stored values),
	// this is to prevent stale write violating stabilizability
	if !cfg.Test {
		time.Sleep(cfg.GetWaitTimestampCacheInvalidTimeout())
	}

	cc := &KVCC{
		TabletTxnConfig: cfg.TabletTxnConfig,
		db:              db,
		txnManager:      transaction.NewManager(cfg, db),
		tsCache:         NewTimestampCache(),
	}
	cc.lm.Initialize(64)
	return cc
}

func (kv *KVCC) Get(ctx context.Context, key string, opt types.KVCCReadOption) (x types.ValueCC, e error) {
	opt.AssertFlags() // TODO remove in product
	assert.Must((!opt.IsTxnRecord && key != "") || (opt.IsTxnRecord && key == "" && opt.ReadExactVersion && opt.GetMaxReadVersion && opt.IsCheckVersion))
	assert.Must(opt.ReadExactVersion || opt.UpdateTimestampCache)
	if !opt.UpdateTimestampCache && !opt.GetMaxReadVersion {
		assert.Must(opt.ReadExactVersion)
		val, err := kv.db.Get(ctx, key, opt.ToKV())
		return val.WithMaxReadVersion(0), err
	}

	//defer func() {
	//	if e == nil && !opt.IsMetaOnly {
	//		x.AssertValid()
	//	}
	//}()

	if opt.IsTxnRecord {
		if !utils.IsTooOld(opt.ExactVersion, kv.StaleWriteThreshold) {
			txn, err := kv.txnManager.GetTxn(types.TxnId(opt.ExactVersion))
			if err == nil {
				valCC, getErr := txn.GetTxnRecord(ctx, &opt)
				if getErr == nil && valCC.IsAborted() {
					valCC.V, getErr = nil, errors.GetNotExistsErrForAborted(valCC.IsClearedUint())
				}
				return valCC, getErr
			}
			glog.V(OCCVerboseLevel).Infof("[KVCC:Get] can't find txn-%d", opt.ExactVersion)
		}
		txnId := types.TxnId(opt.ExactVersion)
		kv.lm.RLock(txnId)
		assert.Must(utils.IsTooOld(opt.ExactVersion, kv.StaleWriteThreshold))
		kv.lm.RUnlock(txnId) // NOTE: this is enough, no need 'defer kv.lm.RUnlock(txnId)'

		val, err := kv.db.Get(ctx, "", opt.ToKV())
		return val.WithMaxReadVersion(types.MaxTxnVersion), err // NOTE: ignore even if not update timestamp cache
	}

	if opt.IsReadModifyWrite {
		assert.Must(opt.GetMaxReadVersion && /* Safe add max reader version condition */ !opt.IsCheckVersion /* && val.Version == 0 this is used to guarantee no more internal version will be written after seen */)
		if !kv.SupportReadModifyWriteTxn() {
			return types.EmptyValueCC, errors.Annotatef(errors.ErrInvalidConfig, "can't support read for write transaction with current config: %v", kv.TabletTxnConfig)
		}
		//assert.Must(!opt.IsReadExactVersion())
		if opt.ReaderVersion < kv.tsCache.GetMaxReaderVersion(key) {
			return types.EmptyValue.WithMaxReadVersion(kv.tsCache.GetMaxReaderVersion(key)), errors.Annotatef(errors.ErrWriteReadConflict, "read for write txn version < kv.tsCache.GetMaxReaderVersion(key: '%s')", key)
		}
		if opt.IsReadModifyWriteFirstReadOfKey {
			w, err := kv.txnManager.PushReadModifyWriteReaderOnKey(key, &opt)
			if err != nil {
				return types.EmptyValue.WithMaxReadVersion(kv.tsCache.GetMaxReaderVersion(key)), err
			}
			if waitErr := w.Wait(ctx, kv.StaleWriteThreshold/2); waitErr != nil {
				glog.V(8).Infof("KVCC:Get failed to wait read modify write queue event of key '%s', txn version: %d, err: %v", key, opt.ReaderVersion, waitErr)
				return types.EmptyValue.WithMaxReadVersion(kv.tsCache.GetMaxReaderVersion(key)), errors.Annotatef(errors.ErrReadModifyWriteWaitFailed, waitErr.Error())
			}
			if glog.V(OCCVerboseLevel) {
				glog.Infof("txn-%d key '%s' get enter, cost %s, time since notified: %s", opt.ReaderVersion, key, bench.Elapsed(), time.Duration(time.Now().UnixNano()-(w.NotifyTime)))
			}
		}
	}

	for try := 1; ; try++ {
		val, err, retry, retryDBReadVersion := kv.get(ctx, key, &opt, try)
		if !retry {
			return val, err
		}
		assert.Must(retryDBReadVersion != 0 && retryDBReadVersion != math.MaxUint64)
		opt.SetDBReadVersion(retryDBReadVersion)
	}
}

func (kv *KVCC) get(ctx context.Context, key string, opt *types.KVCCReadOption, try int) (valCC types.ValueCC, err error, retry bool, retryDBReadVersion uint64) {
	assert.Must(opt.ReaderVersion >= opt.MinAllowedSnapshotVersion)
	var (
		// outputs
		retriedTooManyTimes, minSnapshotVersionViolated bool
		atomicMaxReaderVersion                          uint64
		keyInfo                                         *KeyInfo
	)

	{
		var val types.Value
		if val, err, retry, retryDBReadVersion = kv.getValue(ctx, key, opt, &atomicMaxReaderVersion, &retriedTooManyTimes, &minSnapshotVersionViolated, &keyInfo); err != nil &&
			!errors.IsNotExistsErr(err) && err != errors.ErrMinAllowedSnapshotVersionViolated {
			glog.Errorf("txn-%d get key '%s' failed: '%v', minAllowedSnapshotVersion: %d, cost: %s", opt.ReaderVersion, key, err, opt.MinAllowedSnapshotVersion, bench.Elapsed())
		} else if glog.V(OCCVerboseLevel) {
			glog.Infof("txn-%d get key '%s' succeeded, version: %d, dirty: %v, exact_version: %d, err: %v, cost: %s",
				opt.ReaderVersion, key, val.Version, valCC.IsDirty(), opt.ExactVersion, err, bench.Elapsed())
		}
		if retry && try < consts.MaxRetryTxnGet {
			return types.EmptyValueCC, nil, true, retryDBReadVersion
		}

		assert.Must(err != nil || (opt.ReadExactVersion && val.Version == opt.ExactVersion) || (!opt.ReadExactVersion && val.Version <= opt.ReaderVersion))
		if opt.IsSnapshotRead {
			assert.Must(opt.ReaderVersion != 0)
			valCC = val.WithSnapshotVersion(opt.ReaderVersion)
			assert.Must(valCC.Version <= valCC.SnapshotVersion && opt.MinAllowedSnapshotVersion <= valCC.SnapshotVersion)
		} else {
			valCC = val.WithSnapshotVersion(0)
		}
		// Erase var val so won't bother to mix it with valueCC
	}

	if opt.GetMaxReadVersion {
		if opt.IsCheckVersion /* needs max read version to be atomic */ || atomicMaxReaderVersion > opt.ReaderVersion /* assez grand */ {
			valCC.MaxReadVersion = atomicMaxReaderVersion
		} else {
			valCC.MaxReadVersion = keyInfo.GetMaxReaderVersion()
		}
	} else {
		assert.Must(valCC.MaxReadVersion == 0) // TODO remove this in product
	}

	if err != nil {
		return valCC, err, false, 0
	}

	if opt.IsSnapshotRead && !valCC.IsCommitted() {
		if minSnapshotVersionViolated {
			assert.Must(valCC.Version-1 < opt.MinAllowedSnapshotVersion)
			valCC.V = nil
			return valCC, errors.ErrMinAllowedSnapshotVersionViolated, false, 0
		}
		if retriedTooManyTimes {
			assert.Must(valCC.Version-1 >= opt.MinAllowedSnapshotVersion)
			valCC.V = nil
			return valCC, errors.ErrSnapshotReadRetriedTooManyTimes, false, 0
		}
		assert.Must(false)
		return valCC, errors.UnreachableCode, false, 0
	}

	if valCC.IsAborted() {
		valCC.V = nil
		if opt.ReadExactVersion {
			return valCC, errors.GetNotExistsErrForAborted(valCC.IsClearedUint()), false, 0
		}
		assert.Must(try >= consts.MaxRetryTxnGet)
		return valCC, errors.GetReadUncommittedDataOfAbortedTxn(valCC.IsClearedUint()), false, 0 // TODO who should come first?
	}
	assert.Must(valCC.Version != 0)
	return valCC, nil, false, 0
}

func (kv *KVCC) getValue(ctx context.Context, key string, opt *types.KVCCReadOption,
	atomicMaxReadVersion *uint64, retriedTooManyTimes, minSnapshotVersionViolated *bool, keyInfo **KeyInfo) (val types.Value, err error, retry bool, retryVersion uint64) {
	var (
		w                    *transaction.Writer
		writingWritersBefore transaction.WritingWriters
	)

	*keyInfo, w, writingWritersBefore, err = kv.tsCache.FindWriters(key, opt, atomicMaxReadVersion, minSnapshotVersionViolated)
	if err != nil {
		assert.Must(opt.IsSnapshotRead && *atomicMaxReadVersion == 0 && err == errors.ErrMinAllowedSnapshotVersionViolated)
		return types.NewValue(nil, w.Transaction.ID.Version()), err, false, 0
	}
	w.WaitWritten()

	if opt.IsMetaOnly {
		if val, err, retry, retryVersion, ok := kv.getMetaOnly(key, w, opt); ok {
			return val, err, retry, retryVersion
		}
	}

	if !opt.IsSnapshotRead || w != nil {
		kvOpt, readCommitted := opt.ToKV(), false
		if w != nil && w.Transaction.IsCommitted() {
			kvOpt.SetExactVersion(w.Transaction.ID.Version())
			readCommitted = true
		}
		val, err = kv.db.Get(ctx, key, kvOpt)
		assert.Must(!readCommitted || (err == nil && val.Version == w.Transaction.ID.Version()))
		if err != nil && !errors.IsNotExistsErr(err) {
			return val, err, false, 0
		}
		assert.Must(!errors.IsNotExistsErr(err) || val.Version == 0) // if is not exists error then val.Version == 0
		if w == nil {
			assert.Must(!opt.IsSnapshotRead)
			return kv.checkGotValue(ctx, key, val, err, opt, *atomicMaxReadVersion)
		}

		if assert.Must(val.Version <= w.Transaction.ID.Version()); val.Version == w.Transaction.ID.Version() || opt.ReadExactVersion {
			assert.Must(err == nil || (errors.IsNotExistsErr(err) && opt.ReadExactVersion))
			if val.IsTerminated() {
				return val, err, false, 0
			}
			return kv.checkGotValueWithTxn(ctx, key, val, err, w.Transaction, opt, *atomicMaxReadVersion)
		}
		assert.Must(!w.Succeeded() || w.Transaction.IsAborted()) // Rollbacking was set before remove version in KV::RollbackKey() // TODO what if !w.Succeeded?
		if chkErr := writingWritersBefore.CheckRead(ctx, val.Version, consts.DefaultReadTimeout/10); chkErr != nil {
			return types.EmptyValue, chkErr, false, 0
		}
		return kv.checkGotValue(ctx, key, val, err, opt, *atomicMaxReadVersion)
	}

	for i, readerVersion := 0, uint64(0); ; {
		assert.Must(opt.ReaderVersion >= opt.MinAllowedSnapshotVersion)
		if val, err = kv.db.Get(ctx, key, opt.ToKV()); err != nil || val.IsCommitted() {
			return val, err, false, 0
		}
		if txn, getErr := kv.txnManager.GetTxn(types.TxnId(val.Version)); getErr == nil {
			txnState := txn.GetTxnState()
			if txnState.IsCommitted() {
				assert.Must(val.Version != 0 && val.InternalVersion != 0 &&
					*atomicMaxReadVersion > val.Version /*  No more write with val.Version would be possible after val.InternalVersion */)
				val.UpdateTxnState(txnState)
				return val, nil, false, 0
			}
			if txnState.IsAborted() {
				if i == consts.MaxRetrySnapshotRead-1 {
					*retriedTooManyTimes = true
					return kv.checkPreCheckedGotValue(ctx, key, val, nil, opt, *atomicMaxReadVersion)
				}
				opt.SetDBReadVersion(val.Version - 1) // TODO check retry times
				continue
			}
		}
		if readerVersion = val.Version - 1; readerVersion < opt.MinAllowedSnapshotVersion {
			*minSnapshotVersionViolated = true
			return kv.checkPreCheckedGotValue(ctx, key, val, nil, opt, *atomicMaxReadVersion)
		}
		if i == consts.MaxRetrySnapshotRead-1 {
			*retriedTooManyTimes = true
			return kv.checkPreCheckedGotValue(ctx, key, val, nil, opt, *atomicMaxReadVersion)
		}
		i, opt.ReaderVersion = i+1, readerVersion // NOTE: order can't change, guarantee the invariant: val.Version == floor(opt.ReaderVersion)
	}
}

func (kv *KVCC) getMetaOnly(key string, w *transaction.Writer, opt *types.KVCCReadOption) (val types.Value, err error, retry bool, retryDBVersion uint64, ok bool) {
	if opt.ReadExactVersion {
		var (
			txn       *transaction.Transaction
			getTxnErr error
		)
		if w != nil {
			txn = w.Transaction
		} else if txn, getTxnErr = kv.txnManager.GetTxn(types.TxnId(opt.ExactVersion)); getTxnErr != nil {
			return types.EmptyValue, nil, false, 0, false
		}
		state := txn.GetTxnState()
		if !state.IsTerminated() { // not safe
			return types.EmptyValue, nil, false, 0, false
		}
		meta, metaOK := txn.GetMetaUnsafe(key, state)
		assert.Must(metaOK || state.IsAborted())
		return types.Value{Meta: meta}, nil, false, 0, true
	}

	if w == nil {
		return types.EmptyValue, nil, false, 0, false
	}
	state := w.Transaction.GetTxnState()
	if !state.IsTerminated() {
		return types.EmptyValue, nil, false, 0, false
	}
	meta, metaOK := w.Transaction.GetMetaUnsafe(key, state)
	assert.Must(metaOK || state.IsAborted())
	return types.Value{Meta: meta}, nil, meta.IsAborted(), w.Transaction.ID.Version() - 1, true
}

func (kv *KVCC) checkGotValue(ctx context.Context, key string, val types.Value, err error, opt *types.KVCCReadOption, atomicMaxReadVersion uint64) (_ types.Value, _ error, retry bool, retryVersion uint64) {
	if val.IsTerminated() || (err != nil && (!opt.ReadExactVersion || !errors.IsNotExistsErr(err))) {
		return val, err, false, 0
	}
	assert.Must(err == nil || (errors.IsNotExistsErr(err) && opt.ReadExactVersion))
	return kv.checkPreCheckedGotValue(ctx, key, val, err, opt, atomicMaxReadVersion)
}

func (kv *KVCC) checkPreCheckedGotValue(ctx context.Context, key string, val types.Value, err error, opt *types.KVCCReadOption, atomicMaxReadVersion uint64) (_ types.Value, _ error, retry bool, retryVersion uint64) {
	var (
		txn    *transaction.Transaction
		getErr error
	)
	if opt.ReadExactVersion { // include err == nil or not exists
		if txn, getErr = kv.txnManager.GetTxn(types.TxnId(opt.ExactVersion)); getErr != nil {
			glog.V(1).Infof("[KVCC:checkGotValue][txn-%d][key-'%s'] failed to get txn for exact version %d", opt.ReaderVersion, key, opt.ExactVersion)
			return val, err, false, 0
		}
	} else {
		assert.Must(err == nil && val.Version != 0)
		if txn, getErr = kv.txnManager.GetTxn(types.TxnId(val.Version)); getErr != nil {
			glog.V(1).Infof("[KVCC:checkGotValue][txn-%d][key-'%s'] failed to get txn for read value %d", opt.ReaderVersion, key, val.Version)
			return val, err, false, 0
		}
	}
	return kv.checkGotValueWithTxn(ctx, key, val, err, txn, opt, atomicMaxReadVersion)
}

func (kv *KVCC) checkGotValueWithTxn(ctx context.Context, key string, val types.Value, err error,
	txn *transaction.Transaction, opt *types.KVCCReadOption, atomicMaxReadVersion uint64) (_ types.Value, _ error, retry bool, retryDBVersion uint64) {
	if opt.WaitWhenReadDirty {
		if waitErr := txn.WaitTerminateWithTimeout(ctx, consts.DefaultReadTimeout/10); waitErr != nil {
			glog.V(8).Infof("[KVCC:checkGotValueWithTxnAfterErrChecked][txn-%d][key-'%s'] failed to wait terminate for txn-%d", txn.ID, key, txn.ID.Version())
			return val, err, false, 0
		}
	}

	txnState := txn.GetTxnState()
	if txnState.IsCommitted() {
		assert.Must((errors.IsNotExistsErr(err) && atomicMaxReadVersion <= txn.ID.Version()) || (err == nil && val.Version != 0 && val.InternalVersion != 0))
		if atomicMaxReadVersion > txn.ID.Version() { // No more write with val.Version would be possible after val.InternalVersion
			val.UpdateTxnState(txnState)
			assert.Must(err == nil)
			return val, nil, false, 0
		}
		dbMeta, ok := txn.GetDBMetaWithoutTxnStateUnsafe(key)
		if assert.Must(ok && !dbMeta.IsKeyStateInvalid() &&
			val.InternalVersion <= dbMeta.InternalVersion); val.InternalVersion == dbMeta.InternalVersion {
			if errors.IsNotExistsErr(err) {
				if opt.IsMetaOnly {
					dbMeta.UpdateTxnState(txnState)
					return types.Value{Meta: dbMeta.WithVersion(txn.ID.Version())}, nil, false, 0
				}
				return val, err, true, opt.DBReadVersion // retry to get latest committed value
			}
			val.UpdateTxnState(txnState)
			return val, err, false, 0
		}
		if opt.IsMetaOnly {
			dbMeta.UpdateTxnState(txnState)
			return types.Value{Meta: dbMeta.WithVersion(txn.ID.Version())}, nil, false, 0
		}
		return val, err, true, opt.DBReadVersion // retry to get latest committed value
	}
	if txnState.IsAborted() {
		if txnState.IsCleared() || txn.IsKeyClearedUnsafe(key) { // TODO remove in product
			if vv, err := kv.db.Get(ctx, key, types.NewKVReadOptionWithExactVersion(txn.ID.Version())); !errors.IsNotExistsErr(err) {
				assert.Must(vv.Version == txn.ID.Version())
				glog.Fatalf("txn-%d value of key '%s' still exists after rollbacked", vv.Version, key)
			}
		}
		val.UpdateTxnState(txnState)
		if opt.ReadExactVersion {
			return val, err, false, 0
		}
		return val, err, true, utils.SafeDecr(val.Version)
	}
	assert.Must(!opt.WaitWhenReadDirty)
	return val, err, false, 0
}

func (kv *KVCC) UpdateMeta(ctx context.Context, key string, version uint64, opt types.KVCCUpdateMetaOption) (err error) {
	assert.Must(key != "")
	if !opt.IsClearWriteIntent() {
		return errors.ErrNotSupported
	}
	err = kv.txnManager.ClearWriteIntent(ctx, key, version, opt)
	if opt.IsReadModifyWrite() {
		kv.txnManager.SignalReadModifyWriteKeyEvent(types.TxnId(version), transaction.NewReadModifyWriteKeyEvent(key,
			transaction.GetReadModifyWriteKeyEventTypeClearWriteIntent(err == nil)))
	}
	return err
}

func (kv *KVCC) RollbackKey(ctx context.Context, key string, version uint64, opt types.KVCCRollbackKeyOption) (err error) {
	if err = kv.txnManager.RollbackKey(ctx, key, version, opt); err == nil {
		kv.tsCache.RemoveVersion(key, version)
	}
	if opt.IsReadModifyWrite() {
		kv.txnManager.SignalReadModifyWriteKeyEvent(types.TxnId(version), transaction.NewReadModifyWriteKeyEvent(key,
			transaction.GetReadModifyWriteKeyEventTypeRemoveVersion(err == nil)))
	}
	return err
}

func (kv *KVCC) RemoveTxnRecord(ctx context.Context, version uint64, opt types.KVCCRemoveTxnRecordOption) error {
	return kv.txnManager.RemoveTxnRecord(ctx, version, opt)
}

// Set must be non-blocking in current io framework.
func (kv *KVCC) Set(ctx context.Context, key string, val types.Value, opt types.KVCCWriteOption) (setErr error) {
	var (
		isTxnRecord = val.IsTxnRecord()
		txnId       = types.TxnId(val.Version)
	)
	assert.Must((!isTxnRecord && key != "") || (isTxnRecord && key == ""))
	assert.Must(val.IsDirty())

	// cache may lost after restarted, so ignore too stale write
	if err := utils.CheckOldMan(val.Version, kv.StaleWriteThreshold); err != nil {
		return err
	}

	var (
		txn *transaction.Transaction
	)
	defer func() {
		if txn != nil && errors.IsMustRollbackSetErr(setErr) {
			txn.SetAborted("txn-%d got must rollback error '%v' during setting key '%s' or txn-record", txn.ID, setErr, key) // don't rollback keys here because we wan't to notify upper layer as soon as possible
		}
	}()

	if isTxnRecord {
		kv.lm.Lock(txnId)
		defer kv.lm.Unlock(txnId)

		var inserted bool
		if inserted, txn, setErr = kv.txnManager.InsertTxnIfNotExists(txnId); setErr != nil {
			return setErr
		}
		if inserted {
			glog.V(OCCVerboseLevel).Infof("[KVCC::Set::setTxnRecord] created new txn-%d", txnId)
		}
		return txn.SetTxnRecord(ctx, val, opt)
	}

	if val.IsFirstWrite() {
		var (
			inserted bool
			err      error
		)
		inserted, txn, err = kv.txnManager.InsertTxnIfNotExists(txnId)
		if err != nil {
			return err
		}
		if inserted {
			glog.V(OCCVerboseLevel).Infof("[KVCC::Set::setKey] created new txn-%d", txnId)
		}
	} else {
		assert.Must(val.InternalVersion > types.TxnInternalVersionMin && val.InternalVersion <= types.TxnInternalVersionMax)
		var err error
		if txn, err = kv.txnManager.GetTxn(txnId); err != nil {
			return errors.Annotatef(err, "key: '%s'", key)
		}
	}

	txn.RLock()
	if txn.GetTxnState() != types.TxnStateUncommitted {
		if txn.IsCommitted() {
			glog.Fatalf("txn-%d write key '%s' after committed", txnId, key)
		}
		assert.Must(txn.IsAborted())
		glog.V(OCCVerboseLevel).Infof("[KVCC::setKey] want to insert key '%s' to txn-%d after rollbacked", key, txnId)
		err := errors.ErrWriteKeyAfterTabletTxnRollbacked
		txn.RUnlock()

		return err
	}

	w, err := kv.tsCache.AddWriter(key, txn)
	if err != nil {
		txn.RUnlock()

		return err
	}

	err = txn.SetRLocked(ctx, types.NewTxnKeyUnionKey(key), val, opt)
	w.SetResult(err)
	w.Done()
	txn.RUnlock()

	if err != nil {
		if glog.V(10) {
			glog.Errorf("txn-%d set key '%s' failed: '%v", txnId, key, err)
		}
		return err
	}

	if opt.IsReadModifyWrite() {
		kv.txnManager.SignalReadModifyWriteKeyEvent(txnId, transaction.NewReadModifyWriteKeyEvent(key, transaction.ReadModifyWriteKeyEventTypeKeyWritten))
	}
	if glog.V(OCCVerboseLevel) {
		glog.Infof("txn-%d set key '%s' (internal_version: %d) succeeded, cost %s", txnId, key, val.InternalVersion, bench.Elapsed())
	}
	return nil
}

func (kv *KVCC) GetTxnManager() *transaction.Manager {
	return kv.txnManager
}

func (kv *KVCC) Close() error {
	kv.tsCache.m.Clear()
	kv.txnManager.Close()
	return kv.db.Close()
}
