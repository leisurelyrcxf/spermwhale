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

const OCCVerboseLevel = 160

// KV with concurrency control
type KVCC struct {
	types.TabletTxnConfig

	db types.KV

	txnManager *transaction.Manager
	lm         concurrency.AdvancedTxnLockManager
	tsCache    *TimestampCache
}

func NewKVCC(db types.KV, cfg types.TabletTxnConfig) *KVCC {
	return newKVCC(db, cfg, false)
}

func NewKVCCForTesting(db types.KV, cfg types.TabletTxnConfig) *KVCC {
	return newKVCC(db, cfg, true)
}

func newKVCC(db types.KV, cfg types.TabletTxnConfig, testing bool) *KVCC {
	// Wait until uncertainty passed because timestamp cache is
	// invalid during starting (lost last stored values),
	// this is to prevent stale write violating stabilizability
	if !testing {
		time.Sleep(cfg.GetWaitTimestampCacheInvalidTimeout())
	}

	cc := &KVCC{
		TabletTxnConfig: cfg,
		db:              db,
		txnManager: transaction.NewManager(types.NewTabletTxnManagerConfig(
			cfg,
			types.DefaultReadModifyWriteQueueCfg.WithMaxQueuedAge(utils.MaxDuration(consts.MinTxnLifeSpan, cfg.StaleWriteThreshold)),
		), db),
		tsCache: NewTimestampCache(),
	}
	cc.lm.Initialize(128)
	return cc
}

func (kv *KVCC) Get(ctx context.Context, key string, opt types.KVCCReadOption) (types.ValueCC, error) {
	opt.AssertFlags() // TODO remove in product
	assert.Must((!opt.IsTxnRecord && key != "") || (opt.IsTxnRecord && key == "" && opt.ReadExactVersion && opt.GetMaxReadVersion))
	assert.Must(opt.ReadExactVersion || opt.UpdateTimestampCache)
	if !opt.UpdateTimestampCache && !opt.GetMaxReadVersion {
		assert.Must(opt.ReadExactVersion)
		val, err := kv.db.Get(ctx, key, opt.ToKV())
		return val.WithMaxReadVersion(0), err
	}

	if opt.IsTxnRecord {
		if !utils.IsTooOld(opt.ExactVersion, kv.StaleWriteThreshold) {
			txn, err := kv.txnManager.GetTxn(types.TxnId(opt.ExactVersion))
			if err == nil {
				return txn.GetTxnRecord(ctx, opt)
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
		assert.Must(key != "")
		if !kv.SupportReadModifyWriteTxn() {
			return types.EmptyValueCC, errors.Annotatef(errors.ErrInvalidConfig, "can't support read for write transaction with current config: %v", kv.TabletTxnConfig)
		}
		//assert.Must(!opt.IsReadExactVersion())
		if opt.ReaderVersion < kv.tsCache.GetMaxReaderVersion(key) {
			return kv.addMaxReadVersionForceFetchLatest(key, types.EmptyValue, opt.GetMaxReadVersion, opt.ReadExactVersion), errors.Annotatef(errors.ErrWriteReadConflict, "read for write txn version < kv.tsCache.GetMaxReaderVersion(key: '%s')", key)
		}
		if opt.IsReadModifyWriteFirstReadOfKey {
			w, err := kv.txnManager.PushReadModifyWriteReaderOnKey(key, opt)
			if err != nil {
				return kv.addMaxReadVersionForceFetchLatest(key, types.EmptyValue, opt.GetMaxReadVersion, opt.ReadExactVersion), err
			}
			if waitErr := w.Wait(ctx, utils.MaxDuration(time.Second/2, kv.StaleWriteThreshold/2)); waitErr != nil {
				glog.V(8).Infof("KVCC:Get failed to wait read modify write queue event of key '%s', txn version: %d, err: %v", key, opt.ReaderVersion, waitErr)
				return kv.addMaxReadVersionForceFetchLatest(key, types.EmptyValue, opt.GetMaxReadVersion, opt.ReadExactVersion), errors.Annotatef(errors.ErrReadModifyWriteWaitFailed, waitErr.Error())
			}
			if glog.V(60) {
				glog.Infof("txn-%d key '%s' get enter, cost %s, time since notified: %s", opt.ReaderVersion, key, bench.Elapsed(), time.Duration(time.Now().UnixNano()-(w.NotifyTime)))
			}
		}
	}
	if opt.DBReadVersion == 0 {
		opt.DBReadVersion = opt.GetKVReadVersion()
	}
	for try := 1; ; try++ {
		val, err, retry, retryDBReadVersion := kv.get(ctx, key, &opt, try)
		if !retry {
			return val, err
		}
		assert.Must(retryDBReadVersion != 0 && retryDBReadVersion != types.MaxTxnVersion && retryDBReadVersion != math.MaxUint64)
		opt.DBReadVersion = retryDBReadVersion
	}
}

func (kv *KVCC) get(ctx context.Context, key string, opt *types.KVCCReadOption, try int) (valCC types.ValueCC, err error, retry bool, retryDBReadVersion uint64) {
	assert.Must(opt.ReaderVersion >= opt.MinAllowedSnapshotVersion)
	var (
		// outputs
		retriedTooManyTimes, minSnapshotVersionViolated bool
		maxReadVersion                                  uint64
	)

	{
		var val types.Value
		if val, err, retry, retryDBReadVersion = kv.getValue(ctx, key, opt, &maxReadVersion, &retriedTooManyTimes, &minSnapshotVersionViolated); err != nil &&
			!errors.IsNotExistsErr(err) && err != errors.ErrMinAllowedSnapshotVersionViolated {
			glog.Errorf("txn-%d get key '%s' failed: '%v', minAllowedSnapshotVersion: %d, cost: %s", opt.ReaderVersion, key, err, opt.MinAllowedSnapshotVersion, bench.Elapsed())
		} else if glog.V(60) {
			var errDesc string
			if err != nil {
				errDesc = ", " + err.Error()
			}
			glog.Infof("txn-%d get key '%s' succeeded, dirty: %v%s, cost: %s", opt.ReaderVersion, key, valCC.IsDirty(), errDesc, bench.Elapsed())
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

	if opt.GetMaxReadVersion { // TODO this should be a bug
		if (opt.ReadExactVersion && maxReadVersion > opt.ExactVersion) || (!opt.ReadExactVersion && maxReadVersion > opt.ReaderVersion) {
			valCC.MaxReadVersion = maxReadVersion // assez grand
		} else {
			valCC.MaxReadVersion = kv.tsCache.GetMaxReaderVersion(key)
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
			return valCC, errors.ErrKeyOrVersionNotExist, false, 0
		}
		//assert.Must(err == nil && valCC.Version != 0 && opt.IsUpdateTimestampCache())
		assert.Must(try >= consts.MaxRetryTxnGet)
		return valCC, errors.ErrReadUncommittedDataPrevTxnToBeRollbacked, false, 0 // TODO who should come first?
	}
	assert.Must(valCC.Version != 0)
	return valCC, nil, false, 0
}

func (kv *KVCC) getValue(ctx context.Context, key string, opt *types.KVCCReadOption,
	maxReadVersion *uint64, retriedTooManyTimes, minSnapshotVersionViolated *bool) (val types.Value, err error, retry bool, retryVersion uint64) {
	var (
		w                    *transaction.Writer
		writingWritersBefore transaction.WritingWriters
	)

	w, writingWritersBefore, err = kv.tsCache.FindWriters(key, opt, maxReadVersion, minSnapshotVersionViolated)
	if err != nil {
		assert.Must(opt.IsSnapshotRead && *maxReadVersion == 0 && err == errors.ErrMinAllowedSnapshotVersionViolated)
		return types.NewValue(nil, w.ID.Version()), err, false, 0
	}

	w.WaitWritten()
	if opt.IsMetaOnly {
		if opt.ReadExactVersion {
			if w != nil {
				meta, err := w.Transaction.GetMeta(key)
				assert.Must(!w.Succeeded() || (err == nil && meta.InternalVersion >= 1))
				return types.Value{Meta: meta}, err, false, 0
			}
			if txn, err := kv.txnManager.GetTxn(types.TxnId(opt.ExactVersion)); err == nil {
				meta, err := txn.GetMeta(key)
				return types.Value{Meta: meta}, err, false, 0
			}
		} else if w != nil && w.Succeeded() {
			meta, isAborted := w.Transaction.MustGetMeta(key)
			return types.Value{Meta: meta}, nil, isAborted && !opt.ReadExactVersion, utils.SafeDecr(meta.Version)
		}
		defer func() { val.V = nil }() // TODO evaluate performance gain
	}

	if !opt.IsSnapshotRead || w != nil {
		kvOpt, readCommitted := opt.ToKV(), false
		if w != nil && w.IsCommitted() {
			kvOpt.SetExactVersion(w.ID.Version())
			readCommitted = true
		}
		val, err = kv.db.Get(ctx, key, kvOpt)
		assert.Must(!readCommitted || (err == nil && val.Version == w.ID.Version()))
		if err != nil && !errors.IsNotExistsErr(err) {
			return val, err, false, 0
		}
		assert.Must(!errors.IsNotExistsErr(err) || val.Version == 0) // if is not exists error then val.Version == 0
		if w == nil {
			assert.Must(!opt.IsSnapshotRead)
			return kv.checkGotValue(ctx, key, val, err, opt, *maxReadVersion)
		}

		if assert.Must(val.Version <= w.ID.Version()); val.Version == w.ID.Version() || opt.ReadExactVersion {
			assert.Must(err == nil || (errors.IsNotExistsErr(err) && opt.ReadExactVersion))
			if val.IsTerminated() {
				return val, err, false, 0
			}
			return kv.checkGotValueWithTxn(ctx, key, val, err, w.Transaction, opt, *maxReadVersion)
		}
		assert.Must(!w.Succeeded() || w.IsAborted()) // Rollbacking was set before remove version in KV::RollbackKey() // TODO what if !w.Succeeded?
		if chkErr := writingWritersBefore.CheckRead(ctx, val.Version, consts.DefaultReadTimeout/10); chkErr != nil {
			return types.EmptyValue, chkErr, false, 0
		}
		return kv.checkGotValue(ctx, key, val, err, opt, *maxReadVersion)
	}

	for i, readerVersion := 0, uint64(0); ; {
		assert.Must(opt.ReaderVersion >= opt.MinAllowedSnapshotVersion)
		if val, err = kv.db.Get(ctx, key, opt.ToKV()); err != nil || val.IsCommitted() {
			return kv.checkGotValue(ctx, key, val, err, opt, *maxReadVersion)
		}
		if readerVersion = val.Version - 1; readerVersion < opt.MinAllowedSnapshotVersion {
			*minSnapshotVersionViolated = true
			return kv.checkGotValue(ctx, key, val, err, opt, *maxReadVersion)
		}
		if i == consts.MaxRetrySnapshotRead-1 {
			*retriedTooManyTimes = true
			return kv.checkGotValue(ctx, key, val, err, opt, *maxReadVersion)
		}
		i, opt.ReaderVersion = i+1, readerVersion // NOTE: order can't change, guarantee the invariant: val.Version == floor(opt.ReaderVersion)
	}
}

func (kv *KVCC) checkGotValue(ctx context.Context, key string, val types.Value, err error, opt *types.KVCCReadOption, maxReadVersion uint64) (_ types.Value, _ error, retry bool, retryVersion uint64) {
	if val.IsTerminated() || (err != nil && (!opt.ReadExactVersion || !errors.IsNotExistsErr(err))) {
		return val, err, false, 0
	}
	assert.Must(err == nil || (errors.IsNotExistsErr(err) && opt.ReadExactVersion))

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
	return kv.checkGotValueWithTxn(ctx, key, val, err, txn, opt, maxReadVersion)
}

func (kv *KVCC) checkGotValueWithTxn(ctx context.Context, key string, val types.Value, err error,
	txn *transaction.Transaction, opt *types.KVCCReadOption, maxReadVersion uint64) (_ types.Value, _ error, retry bool, retryDBVersion uint64) {
	if opt.WaitWhenReadDirty {
		if waitErr := txn.WaitTerminateWithTimeout(ctx, consts.DefaultReadTimeout/10); waitErr != nil {
			glog.V(8).Infof("[KVCC:checkGotValueWithTxnAfterErrChecked][txn-%d][key-'%s'] failed to wait terminate for txn-%d", txn.ID, key, txn.ID.Version())
			return val, err, false, 0
		}
	}

	state := txn.GetTxnState()
	if state.IsCommitted() {
		assert.Must(err == nil && val.Version != 0 && val.InternalVersion > 0)
		if maxReadVersion > val.Version { // No more write with val.Version would be possible after val.InternalVersion
			assert.Must(txn.HasPositiveInternalVersion(key, val.InternalVersion)) // TODO remove in product
			val.SetCommitted()
			return val, err, false, 0
		}
		if txn.HasPositiveInternalVersion(key, val.InternalVersion) {
			val.SetCommitted()
			return val, err, false, 0
		}
		return val, err, true, opt.DBReadVersion // retry to get latest committed value
	}
	if state.IsAborted() {
		if txn.IsKeyDone(key) { // TODO remove in product
			if vv, err := kv.db.Get(ctx, key, types.NewKVReadOptionWithExactVersion(txn.ID.Version())); !errors.IsNotExistsErr(err) {
				assert.Must(vv.Version == txn.ID.Version())
				glog.Fatalf("txn-%d value of key '%s' still exists after rollbacked", vv.Version, key)
			}
		}
		val.SetAborted()
		return val, err, !opt.ReadExactVersion, utils.SafeDecr(val.Version)
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
func (kv *KVCC) Set(ctx context.Context, key string, val types.Value, opt types.KVCCWriteOption) error {
	var (
		isTxnRecord = opt.IsTxnRecord()
		txnId       = types.TxnId(val.Version)
	)
	assert.Must((!isTxnRecord && key != "") || (isTxnRecord && key == ""))
	assert.Must(val.IsDirty())
	assert.Must(!val.IsWriteOfKey() || !isTxnRecord)
	assert.Must(val.IsWriteOfKey() || isTxnRecord)

	// cache may lost after restarted, so ignore too stale write
	if err := utils.CheckOldMan(val.Version, kv.StaleWriteThreshold); err != nil {
		return err
	}

	if isTxnRecord {
		kv.lm.Lock(txnId)
		defer kv.lm.Unlock(txnId)

		inserted, txn, err := kv.txnManager.InsertTxnIfNotExists(txnId)
		if err != nil {
			return err
		}
		if inserted {
			glog.V(OCCVerboseLevel).Infof("[KVCC::Set::setTxnRecord] created new txn-%d", txnId)
		}
		return txn.SetTxnRecord(ctx, val, opt)
	}

	var txn *transaction.Transaction
	if val.IsFirstWriteOfKey() {
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
		assert.Must(val.IsWriteOfKey())
		var err error
		if txn, err = kv.txnManager.GetTxn(txnId); err != nil {
			return errors.Annotatef(err, "key: '%s'", key)
		}
	}

	txn.Lock()
	if txn.GetTxnState() != types.TxnStateUncommitted {
		if txn.IsCommitted() {
			glog.Fatalf("txn-%d write key '%s' after committed", txnId, key)
		}
		assert.Must(txn.IsAborted())
		glog.V(OCCVerboseLevel).Infof("[KVCC::setKey] want to insert key '%s' to txn-%d after rollbacked", key, txnId)
		txn.Unlock()

		return errors.ErrWriteKeyAfterTabletTxnRollbacked
	}

	w, err := kv.tsCache.AddWriter(key, txn)
	if err != nil {
		txn.Unlock()

		return err
	}
	if err = kv.db.Set(ctx, key, val, opt.ToKVWriteOption()); err == nil && txn.MustAddWrittenKeyUnsafe(key, val.Meta.ToDB()) {
		glog.V(OCCVerboseLevel).Infof("[KVCC::setKey] added new key '%s' to txn-%d", key, txnId)
	}
	w.SetResult(err)
	w.Done()
	txn.Unlock()

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

func (kv *KVCC) Close() error {
	kv.tsCache.m.Clear()
	kv.txnManager.Close()
	return kv.db.Close()
}

func (kv *KVCC) addMaxReadVersionForceFetchLatest(key string, val types.Value, getMaxReadVersion, readExactVersion bool) types.ValueCC {
	if !getMaxReadVersion || readExactVersion {
		return val.WithMaxReadVersion(0)
	}
	return val.WithMaxReadVersion(kv.tsCache.GetMaxReaderVersion(key))
}
