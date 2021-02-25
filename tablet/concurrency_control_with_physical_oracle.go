package tablet

import (
	"context"
	"time"

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
type KVCCPhysical struct {
	lm *sync2.LockManager

	oracle  *physical.Oracle
	tsCache *TimestampCache

	db mvcc.DB
}

func NewKVCC(db mvcc.DB) *KVCCPhysical {
	// Wait until uncertainty passed because timestamp cache is
	// invalid during starting (lost last stored values),
	// this is to prevent stale write violating stabilizability
	time.Sleep(consts.WaitTimestampCacheInvalidTimeout)

	return &KVCCPhysical{
		lm:      sync2.NewLockManager(),
		oracle:  physical.NewOracle(),
		tsCache: NewTimestampCache(),
		db:      db,
	}
}

func (kv *KVCCPhysical) Get(ctx context.Context, key string, version uint64) (types.Value, error) {
	// guarantee mutual exclusion with set,
	// note this is different from row lock in 2PL because it gets
	// unlocked immediately after read finish, which is not allowed in 2PL
	kv.lm.RLock(key)
	defer kv.lm.RUnlock(key)

	kv.tsCache.UpdateMaxReadVersion(key, version)
	return kv.db.Get(ctx, key, version)
}

func (kv *KVCCPhysical) Set(ctx context.Context, key string, val string, opt types.WriteOption) error {
	if opt.ClearWriteIntent {
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
	if opt.Version < currentTS && currentTS-opt.Version > uint64(consts.TooStaleWriteThreshold) {
		return consts.ErrStaleWrite
	}
	maxVersion := kv.tsCache.GetMaxReadVersion(key)
	if opt.Version < maxVersion {
		return &types.Error{
			Code: consts.ErrCodeVersionConflict,
			Msg:  consts.ErrMsgVersionConflict,
		}
	}
	// ignore write-write conflict, handling write-write conflict is not necessary for concurrency control
	return kv.db.Set(ctx, key, val, opt)
}
