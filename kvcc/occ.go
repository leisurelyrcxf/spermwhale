package kvcc

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/bench"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/kvcc/transaction"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

type TimestampCache struct {
	m concurrency.ConcurrentMap
	t concurrency.ConcurrentTxnMap
}

func NewTimestampCache() *TimestampCache {
	tc := &TimestampCache{}
	tc.m.Initialize(64)
	tc.t.Initialize(64)
	return tc
}

func (cache *TimestampCache) GetMaxReadVersion(key types.TxnKeyUnion) uint64 {
	if key := key.Key; key != "" {
		return cache.GetMaxReadVersionOfKey(key)
	}
	v, ok := cache.t.Get(key.TxnId)
	if !ok {
		return 0
	}
	return v.(uint64)
}

func (cache *TimestampCache) GetMaxReadVersionOfKey(key string) uint64 {
	v, ok := cache.m.Get(key)
	if !ok {
		return 0
	}
	return v.(uint64)
}

func (cache *TimestampCache) UpdateMaxReadVersion(key types.TxnKeyUnion, version uint64) (success bool, maxVal uint64) {
	if key := key.Key; key != "" {
		b, v := cache.m.SetIf(key, version, func(prev interface{}, exist bool) bool {
			if !exist {
				return true
			}
			return version > prev.(uint64)
		})
		assert.Must(v.(uint64) >= version)
		return b, v.(uint64)
	}

	assert.Must(key.TxnId > 0)
	b, v := cache.t.SetIf(key.TxnId, version, func(prev interface{}, exist bool) bool {
		if !exist {
			return true
		}
		return version > prev.(uint64)
	})
	assert.Must(v.(uint64) >= version)
	return b, v.(uint64)
}

// KV with concurrency control
type KVCC struct {
	types.TabletTxnConfig

	db types.KV

	txnManager *transaction.Manager
	lm         concurrency.TxnLockManager
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

	kvcc := &KVCC{
		TabletTxnConfig: cfg,
		db:              db,
		txnManager: transaction.NewManager(
			cfg,
			consts.MaxReadModifyWriteQueueCapacityPerKey,
			consts.ReadModifyWriteQueueMaxReadersRatio,
			utils.MaxDuration(2*time.Second, cfg.StaleWriteThreshold)),
		tsCache: NewTimestampCache(),
	}
	kvcc.lm.Initialize()
	return kvcc
}

func (kv *KVCC) Get(ctx context.Context, key string, opt types.KVCCReadOption) (types.ValueCC, error) {
	assert.Must((!opt.IsTxnRecord() && key != "") || (opt.IsTxnRecord() && key == "" && opt.IsGetExactVersion()))

	if opt.IsNotUpdateTimestampCache() && opt.IsNotGetMaxReadVersion() {
		val, err := kv.db.Get(ctx, key, opt.ToKVReadOption())
		//noinspection ALL
		return val.WithMaxReadVersion(0), err
	}

	if opt.IsReadModifyWrite() {
		assert.Must(key != "")
		if !kv.SupportReadModifyWriteTxn() {
			return types.EmptyValueCC, errors.Annotatef(errors.ErrInvalidConfig, "can't support read for write transaction with current config: %v", kv.TabletTxnConfig)
		}
		//assert.Must(!opt.IsGetExactVersion())
		if opt.ReaderVersion < kv.tsCache.GetMaxReadVersionOfKey(key) {
			return types.EmptyValueCC, errors.Annotatef(errors.ErrWriteReadConflict, "read for write txn version < kv.tsCache.GetMaxReadVersion(key: '%s')", key)
		}
		if opt.IsReadModifyWriteFirstReadOfKey() {
			w, err := kv.txnManager.PushReadModifyWriteReaderOnKey(key, opt)
			if err != nil {
				return types.EmptyValueCC, err
			}
			if waitErr := w.Wait(ctx, utils.MaxDuration(time.Second/2, consts.DefaultReadTimeout/10)); waitErr != nil {
				glog.V(8).Infof("KVCC:Get failed to wait read modify write queue event of key '%s', txn version: %d, err: %v", key, opt.ReaderVersion, waitErr)
				return types.EmptyValueCC, errors.Annotatef(errors.ErrReadModifyWriteWaitFailed, waitErr.Error())
			}
			if glog.V(60) {
				glog.Infof("txn-%d key '%s' get enter, cost %s, time since notified: %s", opt.ReaderVersion, key, bench.Elapsed(), time.Duration(time.Now().UnixNano()-(w.NotifyTime)))
			}
		}
	}
	for try := 1; ; try++ {
		val, err, retry := kv.get(ctx, key, opt, try)
		if !retry {
			return val, err
		}
		assert.Must(err != nil)
	}
}

func (kv *KVCC) get(ctx context.Context, key string, opt types.KVCCReadOption, try int) (valCC types.ValueCC, err error, retry bool) {
	assert.Must(opt.ReaderVersion >= opt.MinAllowedSnapshotVersion)
	var (
		// inputs
		exactVersion         = opt.IsGetExactVersion()
		updateTimestampCache = !opt.IsNotUpdateTimestampCache()
		getMaxReadVersion    = !opt.IsNotGetMaxReadVersion()
		snapshotRead         = opt.IsSnapshotRead()
		txnKey               = types.TxnKeyUnion{Key: key, TxnId: types.TxnId(opt.ExactVersion)}

		// outputs
		val                                             types.Value
		retriedTooManyTimes, minSnapshotVersionViolated bool
	)

	kv.lm.RLock(txnKey) // guarantee mutual exclusion with set, note this is different from row lock in 2PL
	if !snapshotRead {
		val, err = kv.db.Get(ctx, key, opt.ToKVReadOption())
	} else {
		assert.Must(!getMaxReadVersion)
		for i, readerVersion := 0, uint64(0); ; {
			assert.Must(opt.ReaderVersion >= opt.MinAllowedSnapshotVersion)
			if val, err = kv.db.Get(ctx, key, opt.ToKVReadOption()); err != nil || !val.IsDirty() {
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
		val = val.WithSnapshotVersion(opt.ReaderVersion)
	}
	assert.Must(err != nil || (exactVersion && val.Version == opt.ExactVersion) || (!exactVersion && val.Version <= opt.ReaderVersion))

	var maxReadVersion uint64
	if updateTimestampCache {
		_, maxReadVersion = kv.tsCache.UpdateMaxReadVersion(txnKey, opt.ReaderVersion)
	}
	kv.lm.RUnlock(txnKey)

	defer func() {
		if snapshotRead && valCC.IsDirty() && !retry {
			if minSnapshotVersionViolated {
				assert.Must(val.Version-1 < opt.MinAllowedSnapshotVersion)
				valCC.V, err = nil, errors.ReplaceErr(err, errors.ErrReadVersionViolatesMinAllowedSnapshot)
			} else if retriedTooManyTimes {
				assert.Must(val.Version-1 >= opt.MinAllowedSnapshotVersion)
				valCC.V, err = nil, errors.ReplaceErr(err, errors.ErrSnapshotReadRetriedTooManyTimes)
			}
			assert.Must(err != nil)
		}
		assert.Must(!snapshotRead || (val.Version <= val.SnapshotVersion && opt.MinAllowedSnapshotVersion <= val.SnapshotVersion))
	}()

	if err != nil || !val.IsDirty() || !opt.IsWaitWhenReadDirty() {
		if glog.V(60) {
			if err != nil {
				glog.Errorf("txn-%d get %s failed: '%v', minAllowedSnapshotVersion: %d, cost: %s", opt.ReaderVersion, txnKey, err, opt.MinAllowedSnapshotVersion, bench.Elapsed())
			} else {
				glog.Infof("txn-%d get %s succeeded, dirty: %v, cost: %s", opt.ReaderVersion, txnKey, val.IsDirty(), bench.Elapsed())
			}
		}
		return kv.addMaxReadVersion(txnKey, val, getMaxReadVersion, maxReadVersion), err, false
	}
	assert.Must(key != "") // must not be txn record
	waiter, event, err := kv.txnManager.RegisterKeyEventWaiter(types.TxnId(val.Version), key)
	if err != nil {
		valCC := kv.addMaxReadVersion(txnKey, val, getMaxReadVersion, maxReadVersion)
		if code := errors.GetErrorCode(err); code != consts.ErrCodeWriteIntentQueueFull {
			if glog.V(4) {
				glog.Infof("txn-%d get register key event for key '%s' failed: '%v', cost: %s", opt.ReaderVersion, key, err, bench.Elapsed())
			}
			assert.Must(code == consts.ErrCodeTabletWriteTransactionNotFound)                        // TODO remove this in product
			if code == consts.ErrCodeTabletWriteTransactionNotFound && try < consts.MaxRetryTxnGet { // retryable
				return valCC, err, true
			}
		}
		return valCC, nil, false // Let upper layer handle this.
	}
	if waiter != nil {
		var waitErr error
		event, waitErr = waiter.Wait(ctx, consts.DefaultReadTimeout/10)
		valCC = kv.addMaxReadVersionForceFetchLatest(txnKey, val, getMaxReadVersion)
		if waitErr != nil {
			glog.V(8).Infof("KVCC:get failed to wait event of dirty key '%s' @version-%d, err: %v", key, val.Version, waitErr)
			return valCC, nil, false // Let upper layer handle this.
		}
		if glog.V(60) {
			glog.Infof("txn-%d wait dirty key '%s' written by txn-%d waited event %s, cost: %s", opt.ReaderVersion, key, val.Version, event.Type, bench.Elapsed())
		}
	} else {
		valCC = kv.addMaxReadVersion(txnKey, val, getMaxReadVersion, maxReadVersion)
		if glog.V(60) {
			glog.Infof("txn-%d wait dirty key '%s' written by txn-%d get event %s immediately, cost: %s", opt.ReaderVersion, key, val.Version, event.Type, bench.Elapsed())
		}
	}

	assert.Must(event.Key == key)
	switch event.Type {
	case transaction.KeyEventTypeClearWriteIntent:
		return valCC.WithNoWriteIntent(), nil, false
	case transaction.KeyEventTypeRemoveVersionFailed:
		return valCC, nil, false // Let upper layer handle this.
	case transaction.KeyEventTypeVersionRemoved:
		// TODO remove this in product
		vv, err := kv.db.Get(ctx, key, types.NewKVReadOption(val.Version).WithExactVersion())
		if !errors.IsNotExistsErr(err) {
			assert.Must(vv.Version == val.Version)
			glog.Fatalf("txn-%d value of key '%s' still exists after rollbacked", vv.Version, key)
		}
		if try < consts.MaxRetryTxnGet {
			return valCC, errors.ErrReadUncommittedDataPrevTxnKeyRollbacked, true
		}
		return valCC, nil, false // Let upper layer handle this.
	default:
		panic(fmt.Sprintf("impossible event type: '%s'", event.Type))
	}
}

// Set must be non-blocking in current io framework.
func (kv *KVCC) Set(ctx context.Context, key string, val types.Value, opt types.KVCCWriteOption) error {

	var (
		isTxnRecord = opt.IsTxnRecord()
		txnId       = types.TxnId(val.Version)
		txnKey      = types.TxnKeyUnion{Key: key, TxnId: txnId}
	)
	assert.Must((!isTxnRecord && key != "") || (isTxnRecord && key == ""))
	if !val.IsDirty() {
		if isTxnRecord {
			assert.Must(opt.IsRemoveVersion())
			if err := kv.db.Set(ctx, key, val, opt.ToKVWriteOption()); err != nil && glog.V(10) {
				glog.Errorf("txn-%d set %s failed: %v, remove: %v", txnId, txnKey, err, opt.IsRemoveVersion())
				return err
			}
			return nil
		}

		assert.Must(opt.IsClearWriteIntent() || opt.IsRollbackKey())
		inserted, txn := kv.txnManager.InsertTxnIfNotExists(txnId, kv.db)
		if inserted {
			glog.V(70).Infof("[KVCC::Set::ClearOrRemoveVersion] created new txn-%d", txn.ID)
		}
		err := txn.DoneKey(ctx, key, val, opt)

		if opt.IsReadModifyWrite() {
			if opt.IsClearWriteIntent() {
				kv.txnManager.SignalReadModifyWriteKeyEvent(txnId, transaction.NewReadModifyWriteKeyEvent(key,
					transaction.GetReadModifyWriteKeyEventTypeClearWriteIntent(err == nil)))
			} else if opt.IsRollbackKey() {
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
		kv.lm.Lock(txnKey) // TODO make write parallel?
		defer kv.lm.Unlock(txnKey)

		if val.Version < kv.tsCache.GetMaxReadVersion(txnKey) {
			return errors.ErrWriteReadConflict
		}

		if err := kv.db.Set(ctx, key, val, opt.ToKVWriteOption()); err != nil {
			if glog.V(4) {
				glog.Errorf("txn-%d set %s failed: '%v", txnId, txnKey, err)
			}
			return err
		}
		if glog.V(60) {
			glog.Infof("txn-%d set txn-record succeeded, cost %s", txnId, bench.Elapsed())
		}
		return nil
	}

	var txn *transaction.Transaction
	if val.IsFirstWriteOfKey() {
		var inserted bool
		if inserted, txn = kv.txnManager.InsertTxnIfNotExists(txnId, kv.db); inserted {
			glog.V(70).Infof("[KVCC::Set::setKey] created new txn-%d", txnId)
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

	if txn.TxnState != types.TxnStateUncommitted {
		if txn.IsCommitted() {
			glog.Fatalf("txn-%d write key '%s' after committed", txnId, key)
		}
		assert.Must(txn.IsAborted())
		glog.V(70).Infof("[KVCC::setKey] want to insert key '%s' to txn-%d after rollbacked", key, txnId)
		return errors.ErrWriteKeyAfterTabletTxnRollbacked
	}
	assert.Must(!txn.WrittenKeys.Done)
	inserted, keyDone := txn.WrittenKeys.AddUnsafe(key)
	assert.Must(!keyDone)
	if inserted {
		glog.V(70).Infof("[KVCC::setKey] inserted key '%s' to txn-%d", key, txnId)
	}

	// guarantee mutual exclusion with get,
	// note this is different from row lock in 2PL because it gets
	// unlocked immediately after read finish, which is not allowed in 2PL
	kv.lm.Lock(txnKey) // TODO make write parallel?

	if val.Version < kv.tsCache.GetMaxReadVersion(txnKey) {
		kv.lm.Unlock(txnKey)
		return errors.ErrWriteReadConflict
	}
	// ignore write-write conflict, handling write-write conflict is not necessary for concurrency control
	err := kv.db.Set(ctx, key, val, opt.ToKVWriteOption())
	kv.lm.Unlock(txnKey)

	if err != nil {
		if glog.V(10) {
			glog.Errorf("txn-%d set key '%s' failed: '%v", txnId, key, err)
		}
		return err
	}

	if opt.IsReadModifyWrite() {
		kv.txnManager.SignalReadModifyWriteKeyEvent(txnId, transaction.NewReadModifyWriteKeyEvent(key, transaction.ReadModifyWriteKeyEventTypeKeyWritten))
	}
	if glog.V(60) {
		glog.Infof("txn-%d set key '%s' (internal_version: %d) succeeded, cost %s", txnId, key, val.InternalVersion, bench.Elapsed())
	}
	return nil
}

func (kv *KVCC) Close() error {
	kv.tsCache.m.Clear()
	kv.txnManager.Close()
	return kv.db.Close()
}

func (kv *KVCC) addMaxReadVersion(txnKey types.TxnKeyUnion, val types.Value, getMaxReadVersion bool, maxReadVersion uint64) types.ValueCC {
	if !getMaxReadVersion {
		return val.WithMaxReadVersion(0)
	}
	if maxReadVersion != 0 {
		return val.WithMaxReadVersion(maxReadVersion)
	}
	return val.WithMaxReadVersion(kv.tsCache.GetMaxReadVersion(txnKey)) // TODO maybe return max reader version to user?
}

func (kv *KVCC) addMaxReadVersionForceFetchLatest(txnKey types.TxnKeyUnion, val types.Value, getMaxReadVersion bool) types.ValueCC {
	if !getMaxReadVersion {
		return val.WithMaxReadVersion(0)
	}
	return val.WithMaxReadVersion(kv.tsCache.GetMaxReadVersion(txnKey))
}
