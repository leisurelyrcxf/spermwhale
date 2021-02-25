package tablet

import (
	"context"
	"time"

	"github.com/leisurelyrcxf/spermwhale/errors"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/data_struct"
	"github.com/leisurelyrcxf/spermwhale/mvcc"
	"github.com/leisurelyrcxf/spermwhale/sync2"
	"github.com/leisurelyrcxf/spermwhale/types"
)

type TimestampCache struct {
	m data_struct.ConcurrentMap
}

func NewTimestampCache() *TimestampCache {
	return &TimestampCache{
		m: data_struct.NewConcurrentMap(64),
	}
}

func (cache *TimestampCache) GetMaxReadVersion(key string) uint64 {
	v, ok := cache.m.Get(key)
	if !ok {
		return 0
	}
	return v.(uint64)
}

func (cache *TimestampCache) UpdateMaxReadVersion(key string, version uint64) (success bool, prev uint64) {
	b, v := cache.m.SetIf(key, version, func(prev interface{}, exist bool) bool {
		if !exist {
			return true
		}
		return version > prev.(uint64)
	})
	if v == nil {
		return b, 0
	}
	return b, v.(uint64)
}

// KV with concurrent control
type OCCPhysical struct {
	staleWriteThr, maxClockDrift time.Duration

	lm *sync2.LockManager

	oracle  *physical.Oracle
	tsCache *TimestampCache

	db mvcc.DB
}

func NewKVCC(db mvcc.DB, staleWriteThr, maxClockDrift time.Duration) *OCCPhysical {
	// Wait until uncertainty passed because timestamp cache is
	// invalid during starting (lost last stored values),
	// this is to prevent stale write violating stabilizability
	time.Sleep(consts.GetWaitTimestampCacheInvalidTimeout(staleWriteThr, maxClockDrift))

	return &OCCPhysical{
		staleWriteThr: staleWriteThr,
		maxClockDrift: maxClockDrift,
		lm:            sync2.NewLockManager(),
		oracle:        physical.NewOracle(),
		tsCache:       NewTimestampCache(),
		db:            db,
	}
}

func (kv *OCCPhysical) Get(ctx context.Context, key string, opt types.ReadOption) (types.Value, error) {
	if opt.ExactVersion {
		return kv.db.Get(ctx, key, opt)
	}
	// guarantee mutual exclusion with set,
	// note this is different from row lock in 2PL because it gets
	// unlocked immediately after read finish, which is not allowed in 2PL
	kv.lm.RLock(key)
	defer kv.lm.RUnlock(key)

	kv.tsCache.UpdateMaxReadVersion(key, opt.Version)
	return kv.db.Get(ctx, key, opt)
}

func (kv *OCCPhysical) Set(ctx context.Context, key string, val types.Value, opt types.WriteOption) error {
	if !val.WriteIntent {
		return kv.db.Set(ctx, key, val, opt)
	}

	// guarantee mutual exclusion with set,
	// note this is different from row lock in 2PL because it gets
	// unlocked immediately after read finish, which is not allowed in 2PL
	kv.lm.Lock(key)
	defer kv.lm.Unlock(key)

	// cache may lost after restarted, so ignore too stale write
	// TODO change to HLCTimestamp
	currentTS, err := kv.oracle.FetchTimestamp(ctx)
	if err != nil {
		glog.Fatalf("failed to fetch timestamp: %v", err)
	}
	if val.Version < currentTS && currentTS-val.Version > uint64(kv.staleWriteThr) {
		return errors.ErrStaleWrite
	}
	maxReadVersion := kv.tsCache.GetMaxReadVersion(key)
	if val.Version < maxReadVersion {
		return errors.ErrVersionConflict
	}
	// TODO check clock uncertainty and verify if this is the same transaction
	// ignore write-write conflict, handling write-write conflict is not necessary for concurrency control
	return kv.db.Set(ctx, key, val, opt)
}
