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
		exactVersion      = opt.IsReadExactVersion()
		getMaxReadVersion = opt.IsGetMaxReadVersion()
		snapshotRead      = opt.IsSnapshotRead()

		// outputs
		val                                             types.Value
		retriedTooManyTimes, minSnapshotVersionViolated bool
	)

	w, writingWritersBefore, maxReadVersion, err := kv.tsCache.FindWriters(key, opt)
	if err != nil {
		assert.Must(maxReadVersion == 0 && err == errors.ErrMinAllowedSnapshotVersionViolated && w != nil && snapshotRead)
		return kv.addMaxReadVersionForceFetchLatest(key, types.NewValue(nil, w.ID.Version()).WithSnapshotVersion(opt.ReaderVersion), opt.IsGetMaxReadVersion()), err, false
	}
	w.WaitWritten()
	if !snapshotRead || w != nil {
		if val, err = kv.db.Get(ctx, key, opt.WithKVReadVersion(*dbReadVersion)); (err == nil || errors.IsNotExistsErr(err)) && w != nil {
			assert.Must(!errors.IsNotExistsErr(err) || val.Version == 0)
			if writerVersion := w.ID.Version(); w.IsCommitted() {
				assert.Must(val.Version == writerVersion)
				val = val.WithNoWriteIntent()
			} else {
				assert.Must(val.Version <= writerVersion)
				if val.Version < writerVersion {
					assert.Must(!w.Succeeded() || w.IsAborted()) // Rollbacking was set before remove version in KV::Set() // TODO what if !w.Succeeded?
					if chkErr := writingWritersBefore.CheckRead(ctx, val.Version, consts.DefaultReadTimeout/10); chkErr != nil {
						val, err = types.EmptyValue, chkErr
					}
				}
				if val.IsDirty() && opt.IsSnapshotRead() {
					minSnapshotVersionViolated = true
				}
			}
		}
	} else {
		assert.Must(!getMaxReadVersion)
		for i, readerVersion := 0, uint64(0); ; {
			assert.Must(opt.ReaderVersion >= opt.MinAllowedSnapshotVersion)
			if val, err = kv.db.Get(ctx, key, opt.WithKVReadVersion(*dbReadVersion)); err != nil || !val.IsDirty() {
				break
			}
			if readerVersion = val.Version - 1; readerVersion < opt.MinAllowedSnapshotVersion {
				minSnapshotVersionViolated = true
				break
			}
			if i == consts.MaxRetrySnapshotRead-1 {
				retriedTooManyTimes = true
				break
			}
			i, opt.ReaderVersion = i+1, readerVersion
		}
	}
	if snapshotRead {
		assert.Must(opt.ReaderVersion != 0)
		valCC = val.WithSnapshotVersion(opt.ReaderVersion)
	} else {
		valCC = val.WithSnapshotVersion(0)
	}

	assert.Must(err != nil || (exactVersion && valCC.Version == opt.ExactVersion) || (!exactVersion && valCC.Version <= opt.ReaderVersion))
	assert.Must(!valCC.IsDirty() || err == nil)

	defer func() {
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
	}()

	if err != nil || !valCC.IsDirty() || !opt.IsWaitWhenReadDirty() {
		if glog.V(60) {
			if err != nil {
				glog.Errorf("txn-%d get key '%s' failed: '%v', minAllowedSnapshotVersion: %d, cost: %s", opt.ReaderVersion, key, err, opt.MinAllowedSnapshotVersion, bench.Elapsed())
			} else {
				glog.Infof("txn-%d get key '%s' succeeded, dirty: %v, cost: %s", opt.ReaderVersion, key, valCC.IsDirty(), bench.Elapsed())
			}
		}
		return valCC, err, false
	}
	assert.Must(key != "") // must not be txn record
	waitFor, err := kv.txnManager.GetTxn(types.TxnId(valCC.Version))
	if err != nil {
		assert.Must(false) // TODO remove in product
		glog.Errorf("[KVCC:get][txn-%d] cannot find txn of read value of version %d for key '%s'", opt.ReaderVersion, valCC.Version, key)
		return valCC, nil, try < consts.MaxRetryTxnGet // Let upper layer handle this if try >= consts.MaxRetryTxnGet
	}
	if waitErr := waitFor.WaitTerminateWithTimeout(ctx, consts.DefaultReadTimeout/10); waitErr != nil {
		glog.V(8).Infof("[KVCC:get][txn-%d][key-'%s'] failed to wait terminate for txn-%d", key, waitFor.ID.Version())
		return valCC, nil, false // Let upper layer handle this.
	}
	state := waitFor.GetTxnState()
	switch {
	case state.IsCommitted():
		return valCC.WithNoWriteIntent(), nil, false
	case state.IsAborted():
		if waitFor.IsKeyDone(key) { // TODO remove in product
			if vv, err := kv.db.Get(ctx, key, types.NewKVReadOption(valCC.Version).WithExactVersion()); !errors.IsNotExistsErr(err) {
				assert.Must(vv.Version == valCC.Version)
				glog.Fatalf("txn-%d value of key '%s' still exists after rollbacked", vv.Version, key)
			}
		}
		*dbReadVersion = valCC.Version - 1
		return valCC, nil, try < consts.MaxRetryTxnGet // Let upper layer handle this.
	default:
		panic(fmt.Sprintf("impossible txn state: %s", state))
	}
}

// Set must be non-blocking in current io framework.
func (kv *KVCC) Set(ctx context.Context, key string, val types.Value, opt types.KVCCWriteOption) error {
	var (
		isTxnRecord = opt.IsTxnRecord()
		txnId       = types.TxnId(val.Version)
	)
	assert.Must((!isTxnRecord && key != "") || (isTxnRecord && key == ""))
	if !val.IsDirty() {
		inserted, txn := kv.txnManager.MustInsertTxnIfNotExists(txnId, kv.db)
		if inserted {
			glog.V(OCCVerboseLevel).Infof("[KVCC::Set::ClearOrRemoveVersion] created new txn-%d", txn.ID)
		}
		if isTxnRecord {
			return txn.RemoveTxnRecord(ctx, val, opt)
		}
		var (
			isClearWriteIntent = opt.IsClearWriteIntent()
			isRollbackKey      = opt.IsRollbackKey()
			err                error
		)
		assert.Must(isClearWriteIntent || isRollbackKey)
		assert.Must(!isClearWriteIntent || !isRollbackKey)
		if err = txn.DoneKey(ctx, key, val, opt); err == nil && isRollbackKey {
			kv.tsCache.RemoveVersion(key, val.Version)
		}
		if opt.IsReadModifyWrite() {
			if isClearWriteIntent {
				kv.txnManager.SignalReadModifyWriteKeyEvent(txnId, transaction.NewReadModifyWriteKeyEvent(key,
					transaction.GetReadModifyWriteKeyEventTypeClearWriteIntent(err == nil)))
			} else {
				kv.txnManager.SignalReadModifyWriteKeyEvent(txnId, transaction.NewReadModifyWriteKeyEvent(key,
					transaction.GetReadModifyWriteKeyEventTypeRemoveVersion(err == nil)))
			}
		}
		return err
	}

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
	defer txn.Unlock()

	if txn.GetTxnState() != types.TxnStateUncommitted {
		if txn.IsCommitted() {
			glog.Fatalf("txn-%d write key '%s' after committed", txnId, key)
		}
		assert.Must(txn.IsAborted())
		glog.V(OCCVerboseLevel).Infof("[KVCC::setKey] want to insert key '%s' to txn-%d after rollbacked", key, txnId)
		return errors.ErrWriteKeyAfterTabletTxnRollbacked
	}
	if txn.AddUnsafe(key) {
		glog.V(OCCVerboseLevel).Infof("[KVCC::setKey] added key '%s' to txn-%d", key, txnId)
	}

	w, err := kv.tsCache.TryLock(key, txn)
	if err != nil {
		return err
	}
	err = kv.db.Set(ctx, key, val, opt.ToKVWriteOption())
	w.SetResult(err)
	w.Unlock()

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
