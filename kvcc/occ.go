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
}

func NewTimestampCache() *TimestampCache {
	tc := &TimestampCache{}
	tc.m.Initialize(64)
	return tc
}

func (cache *TimestampCache) GetMaxReadVersion(key string) uint64 {
	v, ok := cache.m.Get(key)
	if !ok {
		return 0
	}
	return v.(uint64)
}

func (cache *TimestampCache) UpdateMaxReadVersion(key string, version uint64) (success bool, maxVal uint64) {
	b, v := cache.m.SetIf(key, version, func(prev interface{}, exist bool) bool {
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
	lm         *concurrency.LockManager
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

	return &KVCC{
		TabletTxnConfig: cfg,
		db:              db,
		txnManager: transaction.NewManager(
			consts.MaxReadForWriteQueueCapacityPerKey,
			consts.ReadForWriteQueueMaxReadersRatio,
			utils.MaxDuration(5*time.Second, cfg.StaleWriteThreshold)),
		lm:      concurrency.NewLockManager(),
		tsCache: NewTimestampCache(),
	}
}

func (kv *KVCC) Get(ctx context.Context, key string, opt types.KVCCReadOption) (types.ValueCC, error) {
	if opt.IsNotUpdateTimestampCache() && opt.IsNotGetMaxReadVersion() {
		val, err := kv.db.Get(ctx, key, opt.ToKVReadOption())
		//noinspection ALL
		return val.WithMaxReadVersion(0), err
	}

	const readForWriteWaitTimeout = consts.DefaultReadTimeout / 10
	if opt.IsReadForWrite() {
		assert.Must(!opt.IsGetExactVersion())
		if opt.ReaderVersion < kv.tsCache.GetMaxReadVersion(key) {
			return types.EmptyValueCC, errors.Annotatef(errors.ErrWriteReadConflict, "read for write txn version < kv.tsCache.GetMaxReadVersion(key)")
		}
		if opt.IsReadForWriteFirstReadOfKey() {
			w, err := kv.txnManager.PushReadForWriteReaderOnKey(key, opt)
			if err != nil {
				return types.EmptyValueCC, err
			}
			if waitErr := w.Wait(ctx, readForWriteWaitTimeout); waitErr != nil {
				return types.EmptyValueCC, errors.Annotatef(errors.ErrReadForWriteWaitFailed, waitErr.Error())
			}
			glog.V(60).Infof("txn-%d get enter, cost %s, time since notified: %s", opt.ReaderVersion, bench.Elapsed(), time.Duration(time.Now().UnixNano()-(w.NotifyTime)))
		}
	}
	for i := 0; ; i++ {
		if val, err := kv.get(ctx, key, opt); err == nil ||
			!errors.IsRetryableTabletGetErr(err) || i >= consts.MaxRetryTxnGet {
			assert.Must(i < consts.MaxRetryTxnGet)
			return val, err // TODO
			//return val, errors.CASError(err, consts.ErrCodeTabletWriteTransactionNotFound, nil)
		}
	}
}

func (kv *KVCC) get(ctx context.Context, key string, opt types.KVCCReadOption) (types.ValueCC, error) {
	var (
		exactVersion         = opt.IsGetExactVersion()
		updateTimestampCache = !opt.IsNotUpdateTimestampCache()
		getMaxReadVersion    = !opt.IsNotGetMaxReadVersion()
	)

	// guarantee mutual exclusion with set,
	// note this is different from row lock in 2PL because it gets
	// unlocked immediately after read finish, which is not allowed in 2PL
	kv.lm.RLock(key)

	var maxReadVersion uint64
	if updateTimestampCache {
		_, maxReadVersion = kv.tsCache.UpdateMaxReadVersion(key, opt.ReaderVersion)
	}

	//kv.lm.RUnlock(key) if put here performance will down for read-for-write txn, reason unknown.
	val, err := kv.db.Get(ctx, key, opt.ToKVReadOption())
	assert.Must(err != nil || (exactVersion && val.Version == opt.ExactVersion) || (!exactVersion && val.Version <= opt.ReaderVersion))
	var valCC types.ValueCC
	if getMaxReadVersion {
		if maxReadVersion <= opt.ReaderVersion {
			maxReadVersion = kv.tsCache.GetMaxReadVersion(key)
		}
		valCC = val.WithMaxReadVersion(maxReadVersion)
	} else {
		valCC = val.WithMaxReadVersion(0)
	}
	if err != nil || !valCC.HasWriteIntent() || !opt.IsWaitNoWriteIntent() {
		kv.lm.RUnlock(key)
		glog.V(60).Infof("txn-%d get finished, cost: %s", opt.ReaderVersion, bench.Elapsed())
		return valCC, err
	}
	kv.lm.RUnlock(key)

	waiter, event, err := kv.txnManager.RegisterKeyEventWaiter(types.TxnId(valCC.Version), key)
	if err != nil {
		if errors.GetErrorCode(err) == consts.ErrCodeTabletWriteTransactionNotFound {
			return valCC, err
		}
		return valCC, nil // Let upper layer handle this.
	}
	if waiter != nil {
		var waitErr error
		if event, waitErr = waiter.Wait(ctx, consts.DefaultReadTimeout/100); waitErr != nil {
			glog.V(8).Infof("KVCC:get failed to wait event of key with write intent: '%s'@version%d, err: %v", key, valCC.Version, waitErr)
			return valCC, nil // Let upper layer handle this.
		}
	}
	assert.Must(event.Key == key)
	switch event.Type {
	case transaction.KeyEventTypeClearWriteIntent:
		glog.V(60).Infof("txn-%d get finished, cost: %s", opt.ReaderVersion, bench.Elapsed())
		return valCC.WithNoWriteIntent(), nil
	case transaction.KeyEventTypeRemoveVersionFailed:
		// TODO maybe wait until rollbacked?
		return valCC, errors.ErrReadUncommittedDataPrevTxnToBeRollbacked
	case transaction.KeyEventTypeVersionRemoved:
		// TODO remove this in product
		vv, _ := kv.db.Get(ctx, key, types.NewKVReadOption(valCC.Version+1))
		assert.Must(vv.Version < valCC.Version)
		return valCC, errors.ErrReadUncommittedDataPrevTxnHasBeenRollbacked
	default:
		panic(fmt.Sprintf("impossible event type: '%s'", event.Type))
	}
}

// Set must be non-blocking in current io framework.
func (kv *KVCC) Set(ctx context.Context, key string, val types.Value, opt types.KVCCWriteOption) error {
	txnId := types.TxnId(val.Version)
	if !val.HasWriteIntent() {
		var (
			isWrittenKey        = !opt.IsReadForWriteRollbackOrClearReadKey() // clear or rollbacked written key
			checkWrittenKeyDone bool
			err                 error
		)

		if opt.IsClearWriteIntent() {
			// NOTE: OK even if opt.IsReadForWriteRollbackOrClearReadKey() or kv.db.Set failed
			// TODO needs test against kv.db.Set() failed.
			kv.txnManager.SignalKeyEvent(txnId, transaction.NewKeyEvent(key, transaction.KeyEventTypeClearWriteIntent), false)
		}

		if isWrittenKey {
			err = kv.db.Set(ctx, key, val, opt.ToKVWriteOption())
			checkWrittenKeyDone = err == nil && !opt.IsWriteByDifferentTransaction()
		}

		if opt.IsClearWriteIntent() {
			if checkWrittenKeyDone {
				assert.Must(isWrittenKey)
				kv.txnManager.DoneWrittenKeyWriteIntentCleared(txnId, key)
			}
			if opt.IsReadForWrite() {
				kv.txnManager.SignalReadForWriteKeyEvent(txnId, transaction.NewReadForWriteKeyEvent(key,
					transaction.GetReadForWriteKeyEventTypeClearWriteIntent(err == nil)))
			}
			glog.V(60).Infof("txn-%d write intent cleared, cost: %s", txnId, bench.Elapsed())
		} else if opt.IsRollbackKey() {
			if isWrittenKey {
				kv.txnManager.SignalKeyEvent(txnId, transaction.NewKeyEvent(key,
					transaction.GetKeyEventTypeRemoveVersion(err == nil)), checkWrittenKeyDone)
			}
			if opt.IsReadForWrite() {
				kv.txnManager.SignalReadForWriteKeyEvent(txnId, transaction.NewReadForWriteKeyEvent(key,
					transaction.GetReadForWriteKeyEventTypeRemoveVersion(err == nil)))
			}
		}
		return err
	}

	if opt.IsFirstWriteOfKey() {
		kv.txnManager.AddWriteTransactionWrittenKey(txnId)
	}

	// cache may lost after restarted, so ignore too stale write
	if err := utils.CheckOldMan(val.Version, kv.StaleWriteThreshold); err != nil {
		return err
	}

	// guarantee mutual exclusion with get,
	// note this is different from row lock in 2PL because it gets
	// unlocked immediately after read finish, which is not allowed in 2PL
	kv.lm.Lock(key) // TODO make write parallel

	if val.Version < kv.tsCache.GetMaxReadVersion(key) {
		kv.lm.Unlock(key)
		return errors.ErrWriteReadConflict
	}

	// ignore write-write conflict, handling write-write conflict is not necessary for concurrency control
	err := kv.db.Set(ctx, key, val, opt.ToKVWriteOption())
	kv.lm.Unlock(key)

	if err == nil && opt.IsReadForWrite() {
		kv.txnManager.SignalReadForWriteKeyEvent(txnId, transaction.NewReadForWriteKeyEvent(key, transaction.ReadForWriteKeyEventTypeKeyWritten))
	}
	if !opt.IsTxnRecord() {
		glog.V(60).Infof("txn-%d set key finished, cost %s", txnId, bench.Elapsed())
	}
	return err
}

func (kv *KVCC) Close() error {
	kv.tsCache.m.Clear()
	kv.txnManager.Close()
	return kv.db.Close()
}
