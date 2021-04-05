package kvcc

import (
	"context"
	"fmt"
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
		)),
		tsCache: NewTimestampCache(),
	}
	cc.lm.Initialize(128)
	return cc
}

func (kv *KVCC) Get(ctx context.Context, key string, opt types.KVCCReadOption) (types.ValueCC, error) {
	assert.Must((!opt.IsTxnRecord() && key != "") || (opt.IsTxnRecord() && key == "" && opt.IsReadExactVersion()))

	if !opt.IsUpdateTimestampCache() && !opt.IsGetMaxReadVersion() {
		val, err := kv.db.Get(ctx, key, opt.ToKVReadOption())
		//noinspection ALL
		return val.WithMaxReadVersion(0), err
	}

	if opt.IsTxnRecord() {
		assert.Must(opt.IsGetMaxReadVersion())
		if !utils.IsTooOld(opt.ExactVersion, kv.StaleWriteThreshold) {
			if inserted, txn, err := kv.txnManager.InsertTxnIfNotExists(types.TxnId(opt.ExactVersion), kv.db); err == nil {
				if inserted {
					glog.V(70).Infof("[KVCC::Get::GetTxnRecord] created new txn-%d", txn.ID)
				}
				return txn.GetTxnRecord(ctx, opt)
			}
		}
		txnId := types.TxnId(opt.ExactVersion)
		kv.lm.RLock(txnId)
		assert.Must(utils.IsTooOld(opt.ExactVersion, kv.StaleWriteThreshold))
		kv.lm.RUnlock(txnId) // NOTE: this is enough, no need defer kv.lm.RUnlock(txnId)

		val, err := kv.db.Get(ctx, "", opt.ToKVReadOption())
		return val.WithMaxReadVersion(types.MaxTxnVersion), err // NOTE: ignore even if not update timestamp cache
	}

	if opt.IsReadModifyWrite() {
		assert.Must(key != "")
		if !kv.SupportReadModifyWriteTxn() {
			return types.EmptyValueCC, errors.Annotatef(errors.ErrInvalidConfig, "can't support read for write transaction with current config: %v", kv.TabletTxnConfig)
		}
		//assert.Must(!opt.IsReadExactVersion())
		if opt.ReaderVersion < kv.tsCache.GetMaxReaderVersion(key) {
			return kv.addMaxReadVersionForceFetchLatest(key, types.EmptyValueCC, opt.IsGetMaxReadVersion()), errors.Annotatef(errors.ErrWriteReadConflict, "read for write txn version < kv.tsCache.GetMaxReaderVersion(key: '%s')", key)
		}
		if opt.IsReadModifyWriteFirstReadOfKey() {
			w, err := kv.txnManager.PushReadModifyWriteReaderOnKey(key, opt)
			if err != nil {
				return kv.addMaxReadVersionForceFetchLatest(key, types.EmptyValueCC, opt.IsGetMaxReadVersion()), err
			}
			if waitErr := w.Wait(ctx, utils.MaxDuration(time.Second/2, kv.StaleWriteThreshold/2)); waitErr != nil {
				glog.V(8).Infof("KVCC:Get failed to wait read modify write queue event of key '%s', txn version: %d, err: %v", key, opt.ReaderVersion, waitErr)
				return kv.addMaxReadVersionForceFetchLatest(key, types.EmptyValueCC, opt.IsGetMaxReadVersion()), errors.Annotatef(errors.ErrReadModifyWriteWaitFailed, waitErr.Error())
			}
			if glog.V(60) {
				glog.Infof("txn-%d key '%s' get enter, cost %s, time since notified: %s", opt.ReaderVersion, key, bench.Elapsed(), time.Duration(time.Now().UnixNano()-(w.NotifyTime)))
			}
		}
	}
	var dbReadVersion = opt.GetKVReadVersion()
	for try := 1; ; try++ {
		if val, err, retry := kv.get(ctx, key, &opt, &dbReadVersion, try); !retry {
			return val, err
		}
	}
}

func (kv *KVCC) get(ctx context.Context, key string, opt *types.KVCCReadOption, dbReadVersion *uint64, try int) (valCC types.ValueCC, err error, retry bool) {
	assert.Must(opt.ReaderVersion >= opt.MinAllowedSnapshotVersion)
	var (
		// inputs
		snapshotRead = opt.IsSnapshotRead()
		exactVersion = opt.IsReadExactVersion()

		// outputs
		retriedTooManyTimes, minSnapshotVersionViolated bool
		val                                             types.Value
		maxReadVersion                                  uint64
	)

	if val, err = kv.getValue(ctx, key, opt, snapshotRead, exactVersion, dbReadVersion,
		&retriedTooManyTimes, &minSnapshotVersionViolated, &maxReadVersion); err != nil && !errors.IsNotExistsErr(err) && err != errors.ErrMinAllowedSnapshotVersionViolated {
		glog.Errorf("txn-%d get key '%s' failed: '%v', minAllowedSnapshotVersion: %d, cost: %s", opt.ReaderVersion, key, err, opt.MinAllowedSnapshotVersion, bench.Elapsed())
	} else if glog.V(60) {
		var errDesc string
		if err != nil {
			errDesc = ", " + err.Error()
		}
		glog.Infof("txn-%d get key '%s' succeeded, dirty: %v%s, cost: %s", opt.ReaderVersion, key, valCC.IsDirty(), errDesc, bench.Elapsed())
	}
	assert.Must(err != nil || (exactVersion && val.Version == opt.ExactVersion) || (!exactVersion && val.Version <= opt.ReaderVersion))

	if !exactVersion && val.IsAborted() {
		assert.Must(err == nil && val.Version != 0 && opt.IsUpdateTimestampCache())
		*dbReadVersion = val.Version - 1
		if try < consts.MaxRetryTxnGet {
			return types.EmptyValueCC, nil, true
		}
	}

	if snapshotRead {
		assert.Must(opt.ReaderVersion != 0)
		valCC = val.WithSnapshotVersion(opt.ReaderVersion)
	} else {
		valCC = val.WithSnapshotVersion(0)
	}

	if !opt.IsGetMaxReadVersion() {
		assert.Must(valCC.MaxReadVersion == 0)
	} else if (exactVersion && maxReadVersion > opt.ExactVersion) || (!exactVersion && maxReadVersion > opt.ReaderVersion) {
		valCC.MaxReadVersion = maxReadVersion
	} else {
		valCC.MaxReadVersion = kv.tsCache.GetMaxReaderVersion(key)
	}

	if snapshotRead && valCC.IsDirty() && !retry {
		assert.Must(err == nil)
		if minSnapshotVersionViolated {
			assert.Must(valCC.Version-1 < opt.MinAllowedSnapshotVersion)
			valCC.V, err = nil, errors.ErrMinAllowedSnapshotVersionViolated //  errors.ReplaceErr(err,
		} else if retriedTooManyTimes {
			assert.Must(valCC.Version-1 >= opt.MinAllowedSnapshotVersion)
			valCC.V, err = nil, errors.ErrSnapshotReadRetriedTooManyTimes // errors.ReplaceErr(err,
		}
		assert.Must(err != nil)
	}
	assert.Must(!snapshotRead || (valCC.Version <= valCC.SnapshotVersion && opt.MinAllowedSnapshotVersion <= valCC.SnapshotVersion))
	assert.Must(err != nil || valCC.Version != 0)
	return valCC, err, false
}

func (kv *KVCC) getValue(ctx context.Context, key string, opt *types.KVCCReadOption, snapshotRead, exactVersion bool,
	dbReadVersion *uint64, retriedTooManyTimes, minSnapshotVersionViolated *bool, pMaxReadVersion *uint64) (val types.Value, err error) {
	var (
		w                    *transaction.Writer
		writingWritersBefore transaction.WritingWriters
		maxReadVersion       uint64
	)

	w, writingWritersBefore, maxReadVersion, err = kv.tsCache.FindWriters(key, opt)
	*pMaxReadVersion = maxReadVersion
	if err != nil {
		if errors.IsNotExistsErr(err) {
			assert.Must(w == nil && exactVersion)
			return val, err
		}
		assert.Must(snapshotRead && maxReadVersion == 0 && err == errors.ErrMinAllowedSnapshotVersionViolated)
		return types.NewValue(nil, w.ID.Version()), err
	}

	w.WaitWritten()
	if opt.IsMetaOnly() && w != nil {
		vv, state := w.ToValue(), w.GetTxnState()
		if state.IsCommitted() {
			vv.SetCommitted()
			return vv, nil
		}
		if state.IsAborted() {
			assert.Must(vv.IsDirty())
			vv.SetAborted()
			return vv, errors.ErrKeyOrVersionNotExist
		}
		// TODO is this dangerous?
		if w.Succeeded() {
			return vv, nil
		}
	}

	if !snapshotRead || w != nil {
		if val, err = kv.db.Get(ctx, key, opt.WithKVReadVersion(*dbReadVersion)); err != nil && !errors.IsNotExistsErr(err) {
			return val, err
		}
		if w == nil {
			kv.checkGotValue(ctx, key, &val, err, *opt, exactVersion)
			return val, err
		}
		assert.Must((err == nil || errors.IsNotExistsErr(err)) && w != nil)

		writerVersion, state := w.ID.Version(), w.GetTxnState()
		if state.IsCommitted() {
			assert.Must(val.Version == writerVersion && err == nil)
			val = val.WithCommitted()
			return val, nil
		}
		if exactVersion {
			assert.Must(val.Version == writerVersion || errors.IsNotExistsErr(err))
			if state.IsAborted() {
				val.SetAborted()
				return val, errors.ErrKeyOrVersionNotExist
			}
			return val, err
		}
		assert.Must(!errors.IsNotExistsErr(err) || val.Version == 0)
		if assert.Must(val.Version <= writerVersion); val.Version == writerVersion {
			if state.IsAborted() {
				assert.Must(val.IsDirty())
				val.SetAborted()
			}
		} else {
			assert.Must(!w.Succeeded() || w.IsAborted()) // Rollbacking was set before remove version in KV::RollbackKey() // TODO what if !w.Succeeded?
			if chkErr := writingWritersBefore.CheckRead(ctx, val.Version, consts.DefaultReadTimeout/10); chkErr != nil {
				return types.EmptyValue, chkErr
			}
			kv.checkGotValue(ctx, key, &val, err, *opt, exactVersion)
		}

		if val.IsDirty() && opt.IsSnapshotRead() {
			*minSnapshotVersionViolated = true
		}
		return val, err
	}

	for i, readerVersion := 0, uint64(0); ; {
		assert.Must(opt.ReaderVersion >= opt.MinAllowedSnapshotVersion)
		if val, err = kv.db.Get(ctx, key, opt.WithKVReadVersion(*dbReadVersion)); err != nil || val.IsCommitted() {
			kv.checkGotValue(ctx, key, &val, err, *opt, exactVersion)
			return val, err
		}
		if readerVersion = val.Version - 1; readerVersion < opt.MinAllowedSnapshotVersion {
			kv.checkGotValue(ctx, key, &val, err, *opt, exactVersion)
			*minSnapshotVersionViolated = true
			return val, err
		}
		if i == consts.MaxRetrySnapshotRead-1 {
			kv.checkGotValue(ctx, key, &val, err, *opt, exactVersion)
			*retriedTooManyTimes = true
			return val, err
		}
		i, opt.ReaderVersion = i+1, readerVersion // NOTE: order can't change, guarantee the invariant: val.Version == floor(opt.ReaderVersion)
	}
}

func (kv *KVCC) checkGotValue(ctx context.Context, key string, val *types.Value, err error, opt types.KVCCReadOption, exactVersion bool) {
	if val.IsCommitted() || (err != nil && (!exactVersion || !errors.IsNotExistsErr(err))) {
		return
	}
	assert.Must(err == nil || (errors.IsNotExistsErr(err) && exactVersion))

	var (
		txn    *transaction.Transaction
		getErr error
	)

	if exactVersion { // include err == nil or not exists
		if txn, getErr = kv.txnManager.GetTxn(types.TxnId(opt.ExactVersion)); getErr != nil {
			glog.V(1).Infof("[KVCC:checkGotValue][txn-%d][key-'%s'] failed to get txn for exact version %d", opt.ReaderVersion, key, opt.ExactVersion)
			return
		}
	} else {
		assert.Must(err == nil && val.Version != 0)
		if txn, getErr = kv.txnManager.GetTxn(types.TxnId(val.Version)); getErr != nil {
			glog.V(1).Infof("[KVCC:checkGotValue][txn-%d][key-'%s'] failed to get txn for read value %d", opt.ReaderVersion, key, val.Version)
			return
		}
	}
	assert.Must(txn != nil)
	if opt.IsWaitWhenReadDirty() {
		if waitErr := txn.WaitTerminateWithTimeout(ctx, consts.DefaultReadTimeout/10); waitErr != nil {
			glog.V(8).Infof("[KVCC:checkGotValue][txn-%d][key-'%s'] failed to wait terminate for txn-%d", opt.ReaderVersion, key, txn.ID.Version())
			return
		}
	}

	switch state := txn.GetTxnState(); state {
	case types.TxnStateCommitted:
		assert.Must(!errors.IsNotExistsErr(err))
		val.SetCommitted()
	case types.TxnStateRollbacking, types.TxnStateRollbacked:
		if txn.IsKeyDone(key) { // TODO remove in product
			if vv, err := kv.db.Get(ctx, key, types.NewKVReadOption(txn.ID.Version()).WithExactVersion()); !errors.IsNotExistsErr(err) {
				assert.Must(vv.Version == txn.ID.Version())
				glog.Fatalf("txn-%d value of key '%s' still exists after rollbacked", vv.Version, key)
			}
		}
		val.SetAborted()
	case types.TxnStateUncommitted:
	default:
		panic(fmt.Sprintf("impossible txn state: %s", state))
	}
}

func (kv *KVCC) UpdateMeta(ctx context.Context, key string, version uint64, opt types.KVCCUpdateMetaOption) (err error) {
	assert.Must(key != "")
	if !opt.IsClearWriteIntent() {
		return errors.ErrNotSupported
	}
	inserted, txn := kv.txnManager.MustInsertTxnIfNotExists(types.TxnId(version), kv.db)
	if inserted {
		glog.V(OCCVerboseLevel).Infof("[KVCC::UpdateMeta] created new txn-%d", txn.ID)
	}
	err = txn.ClearWriteIntent(ctx, key, opt)
	if opt.IsReadModifyWrite() {
		kv.txnManager.SignalReadModifyWriteKeyEvent(types.TxnId(version), transaction.NewReadModifyWriteKeyEvent(key,
			transaction.GetReadModifyWriteKeyEventTypeClearWriteIntent(err == nil)))
	}
	return err
}

func (kv *KVCC) RollbackKey(ctx context.Context, key string, version uint64, opt types.KVCCRollbackKeyOption) (err error) {
	inserted, txn := kv.txnManager.MustInsertTxnIfNotExists(types.TxnId(version), kv.db)
	if inserted {
		glog.V(OCCVerboseLevel).Infof("[KVCC::RollbackKey] created new txn-%d", txn.ID)
	}
	if err = txn.RollbackKey(ctx, key, opt); err == nil {
		kv.tsCache.RemoveVersion(key, version)
	}
	if opt.IsReadModifyWrite() {
		kv.txnManager.SignalReadModifyWriteKeyEvent(types.TxnId(version), transaction.NewReadModifyWriteKeyEvent(key,
			transaction.GetReadModifyWriteKeyEventTypeRemoveVersion(err == nil)))
	}
	return err
}

func (kv *KVCC) RemoveTxnRecord(ctx context.Context, version uint64, opt types.KVCCRemoveTxnRecordOption) error {
	inserted, txn := kv.txnManager.MustInsertTxnIfNotExists(types.TxnId(version), kv.db)
	if inserted {
		glog.V(OCCVerboseLevel).Infof("[KVCC::RemoveTxnRecord] created new txn-%d", txn.ID)
	}
	return txn.RemoveTxnRecord(ctx, opt)
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

		inserted, txn, err := kv.txnManager.InsertTxnIfNotExists(txnId, kv.db)
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
		inserted, txn, err = kv.txnManager.InsertTxnIfNotExists(txnId, kv.db)
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
	if txn.AddUnsafe(key) {
		glog.V(OCCVerboseLevel).Infof("[KVCC::setKey] added key '%s' to txn-%d", key, txnId)
	}

	w, err := kv.tsCache.AddWriter(key, val.Meta.ToDB(), txn)
	if err != nil {
		txn.Unlock()

		return err
	}
	err = kv.db.Set(ctx, key, val, opt.ToKVWriteOption())
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

func (kv *KVCC) addMaxReadVersionForceFetchLatest(key string, val types.ValueCC, getMaxReadVersion bool) types.ValueCC {
	if !getMaxReadVersion {
		return val.WithMaxReadVersion(0)
	}
	return val.WithMaxReadVersion(kv.tsCache.GetMaxReaderVersion(key))
}
