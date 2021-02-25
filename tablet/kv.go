package tablet

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"

	"github.com/leisurelyrcxf/spermwhale/data_struct"
	"github.com/leisurelyrcxf/spermwhale/mvcc"
	"github.com/leisurelyrcxf/spermwhale/proto/tabletpb"
	"github.com/leisurelyrcxf/spermwhale/sync2"
	"github.com/leisurelyrcxf/spermwhale/types"
)

const (
	ErrCodeVersionConflict = 1
)

const (
	ErrMsgVersionConflict = "version conflict"
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

type KV struct {
	tabletpb.UnimplementedKVServer

	lm      *sync2.LockManager
	tsCache *TimestampCache
	db      mvcc.DB
}

func NewKV(db mvcc.DB) *KV {
	return &KV{
		lm:      sync2.NewLockManager(),
		tsCache: NewTimestampCache(),
		db:      db,
	}
}

func (kv *KV) Get(_ context.Context, req *tabletpb.GetRequest) (*tabletpb.GetResponse, error) {
	vv, err := kv.get(req.Key, req.Version)
	if err != nil {
		return &tabletpb.GetResponse{
			Err: &commonpb.Error{
				Code: -1,
				Msg:  err.Error(),
			},
		}, nil
	}
	return &tabletpb.GetResponse{
		V: &commonpb.Value{
			Meta: &commonpb.ValueMeta{
				WriteIntent: vv.WriteIntent,
				Version:     vv.Version,
			},
			Val: vv.V,
		},
	}, nil
}

func (kv *KV) Set(_ context.Context, req *tabletpb.SetRequest) (*tabletpb.SetResponse, error) {
	err := kv.set(req.Key, req.Value.Val, req.Value.Meta.Version, req.Value.Meta.WriteIntent)
	return &tabletpb.SetResponse{Err: commonpb.ToPBError(err)}, nil
}

func (kv *KV) get(key string, version uint64) (types.Value, error) {
	// guarantee mutual exclusion with set,
	// note this is different from row lock in 2PL because it gets
	// unlocked immediately after read finish, which is not allowed in 2PL
	kv.lm.RLock(key)
	defer kv.lm.RUnlock(key)

	kv.tsCache.UpdateMaxReadVersion(key, version)
	return kv.db.Get(key, version)
}

func (kv *KV) set(key string, val string, version uint64, writeIntent bool) *types.Error {
	kv.lm.Lock(key)
	defer kv.lm.Unlock(key)

	maxVersion := kv.tsCache.GetMaxReadVersion(key)
	if version < maxVersion {
		return &types.Error{
			Code: ErrCodeVersionConflict,
			Msg:  ErrMsgVersionConflict,
		}
	}
	kv.db.Set(key, val, version, writeIntent)
	return nil
}

func (kv *KV) mustEmbedUnimplementedKVServer() {}
