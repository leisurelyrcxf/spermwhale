package tablet

import (
	"context"
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"

	"github.com/leisurelyrcxf/spermwhale/utils"

	"github.com/leisurelyrcxf/spermwhale/errors"

	"github.com/leisurelyrcxf/spermwhale/mvcc"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
)

type TimestampCache struct {
	m concurrency.ConcurrentMap
}

func NewTimestampCache() *TimestampCache {
	return &TimestampCache{
		m: concurrency.NewConcurrentMap(64),
	}
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
	types.TxnConfig

	lm *concurrency.LockManager

	tsCache *TimestampCache

	db mvcc.DB
}

func NewKVCC(db mvcc.DB, cfg types.TxnConfig) *KVCC {
	return newKVCC(db, cfg, false)
}

func NewKVCCForTesting(db mvcc.DB, cfg types.TxnConfig) *KVCC {
	return newKVCC(db, cfg, true)
}

func newKVCC(db mvcc.DB, cfg types.TxnConfig, testing bool) *KVCC {
	// Wait until uncertainty passed because timestamp cache is
	// invalid during starting (lost last stored values),
	// this is to prevent stale write violating stabilizability
	if !testing {
		time.Sleep(cfg.GetWaitTimestampCacheInvalidTimeout())
	}

	return &KVCC{
		TxnConfig: cfg,
		lm:        concurrency.NewLockManager(),
		tsCache:   NewTimestampCache(),
		db:        db,
	}
}

func (kv *KVCC) Get(ctx context.Context, key string, opt types.ReadOption) (types.Value, error) {
	if opt.NotUpdateTimestampCache && !opt.GetMaxReadVersion {
		return kv.db.Get(ctx, key, opt)
	}

	// guarantee mutual exclusion with set,
	// note this is different from row lock in 2PL because it gets
	// unlocked immediately after read finish, which is not allowed in 2PL
	kv.lm.RLock(key)
	defer kv.lm.RUnlock(key)

	var maxReadVersion uint64
	if !opt.NotUpdateTimestampCache {
		readVersion := opt.Version
		if opt.ExactVersion {
			types.SafeIncr(&readVersion) // prevent future write of opt.Version
		}
		_, maxReadVersion = kv.tsCache.UpdateMaxReadVersion(key, readVersion)
	}
	//kv.lm.RUnlock(key) if put here performance will down for read-for-write txn, reason unknown.
	val, err := kv.db.Get(ctx, key, opt)
	assert.Must(err != nil || (opt.ExactVersion && val.Version == opt.Version) || (!opt.ExactVersion && val.Version <= opt.Version))
	if opt.GetMaxReadVersion {
		//noinspection ALL
		if maxReadVersion > 0 {
			return val.WithMaxReadVersion(maxReadVersion), err
		}
		return val.WithMaxReadVersion(kv.tsCache.GetMaxReadVersion(key)), err
	}
	return val, err
}

func (kv *KVCC) Set(ctx context.Context, key string, val types.Value, opt types.WriteOption) error {
	if !val.WriteIntent {
		return kv.db.Set(ctx, key, val, opt)
	}

	// cache may lost after restarted, so ignore too stale write
	if err := utils.CheckTooStale(val.Version, kv.StaleWriteThreshold); err != nil {
		return err
	}

	// guarantee mutual exclusion with set,
	// note this is different from row lock in 2PL because it gets
	// unlocked immediately after read finish, which is not allowed in 2PL
	kv.lm.Lock(key)
	defer kv.lm.Unlock(key)

	if val.Version < kv.tsCache.GetMaxReadVersion(key) {
		return errors.ErrTransactionConflict
	}

	// ignore write-write conflict, handling write-write conflict is not necessary for concurrency control
	return kv.db.Set(ctx, key, val, opt)
}

func (kv *KVCC) Close() error {
	kv.tsCache.m.Clear()
	return kv.db.Close()
}
