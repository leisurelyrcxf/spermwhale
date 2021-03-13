package kvcc

import (
	"context"
	"time"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/event/readforwrite"
	"github.com/leisurelyrcxf/spermwhale/event/writeintent"
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

// KV with concurrent control
type KVCC struct {
	types.TabletTxnConfig

	lm *concurrency.LockManager

	tsCache *TimestampCache

	writeIntentManager  *writeintent.Manager
	readForWriteManager *readforwrite.Manager

	db types.KV
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
		TabletTxnConfig:     cfg,
		lm:                  concurrency.NewLockManager(),
		tsCache:             NewTimestampCache(),
		writeIntentManager:  writeintent.NewManager(),
		readForWriteManager: readforwrite.NewManager(),
		db:                  db,
	}
}

func (kv *KVCC) Get(ctx context.Context, key string, opt types.KVCCReadOption) (types.ValueCC, error) {
	if opt.IsNotUpdateTimestampCache() && opt.IsNotGetMaxReadVersion() {
		val, err := kv.db.Get(ctx, key, opt.ToKVReadOption())
		//noinspection ALL
		return val.WithMaxReadVersion(0), err
	}

	const readForWriteWaitTimeout = consts.DefaultReadTimeout * 3
	if opt.IsReadForWriteFirstRead() {
		w, err := kv.readForWriteManager.AppendReader(key, opt.ReaderVersion)
		if err != nil {
			return types.EmptyValueCC, err
		}
		if waitErr := w.Wait(ctx, readForWriteWaitTimeout); waitErr != nil {
			return types.EmptyValueCC, errors.Annotatef(errors.ErrReadForWriteWaitFailed, waitErr.Error())
		}
	}
	for i := 0; ; i++ {
		if val, err := kv.get(ctx, key, opt); err == nil ||
			errors.GetErrorCode(err) != consts.ErrCodeReadUncommittedDataPrevTxnHasBeenRollbacked ||
			i >= consts.MaxRetryTxnGet {
			return val, err
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
		return valCC, err
	}
	kv.lm.RUnlock(key)

	waiter, err := kv.writeIntentManager.RegisterWaiter(key, valCC.Version)
	if err != nil {
		glog.V(8).Infof("KVCC:get register waiter failed '%s'@version%d: %v", key, valCC.Version, err)
		return valCC, nil
	}
	e, waitErr := waiter.Wait(ctx, consts.DefaultReadTimeout/100)
	if waitErr != nil {
		glog.V(8).Infof("KVCC:get failed to wait event of key with write intent: '%s'@version%d, err: %v", key, valCC.Version, waitErr)
		return valCC, nil // Let upper layer handle this.
	}
	switch e {
	case writeintent.Removed:
		return types.EmptyValueCC, errors.ErrReadUncommittedDataPrevTxnHasBeenRollbacked
	case writeintent.Cleared:
		return valCC.WithNoWriteIntent(), nil
	default:
		panic("impossible code")
	}
}

func (kv *KVCC) Set(ctx context.Context, key string, val types.Value, opt types.KVCCWriteOption) error {
	if !val.HasWriteIntent() {
		// Don't care the result of kv.db.Set(), TODO needs test against kv.db.Set() failed.
		if opt.IsClearWriteIntent() {
			kv.writeIntentManager.FireEvent(key, val.Version, writeintent.Cleared)
		} else if opt.IsRemoveVersion() {
			kv.writeIntentManager.FireEvent(key, val.Version, writeintent.Removed)
		}
		// TODO kv.writeIntentManager.GC(key, val.Version)
		err := kv.db.Set(ctx, key, val, opt.ToKVWriteOption())
		if opt.IsReadForWrite() {
			if opt.IsClearWriteIntent() {
				kv.readForWriteManager.Signal(key, val.Version, readforwrite.WriteIntentCleared)
			} else if opt.IsRemoveVersion() {
				kv.readForWriteManager.Signal(key, val.Version, readforwrite.VersionRemoved)
			}
		}
		return err
	}

	// cache may lost after restarted, so ignore too stale write
	if err := utils.CheckOldMan(val.Version, kv.StaleWriteThreshold); err != nil {
		return err
	}

	// guarantee mutual exclusion with get,
	// note this is different from row lock in 2PL because it gets
	// unlocked immediately after read finish, which is not allowed in 2PL
	kv.lm.Lock(key)
	defer kv.lm.Unlock(key) // TODO make write parallel

	if val.Version < kv.tsCache.GetMaxReadVersion(key) {
		return errors.ErrWriteReadConflict
	}

	// ignore write-write conflict, handling write-write conflict is not necessary for concurrency control
	return kv.db.Set(ctx, key, val, opt.ToKVWriteOption())
	//if err == nil && opt.IsReadForWrite() {
	//	TODO signal earlier?
	//}
}

func (kv *KVCC) Close() error {
	kv.tsCache.m.Clear()
	kv.writeIntentManager.Close()
	kv.readForWriteManager.Close()
	return kv.db.Close()
}
