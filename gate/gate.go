package gate

import (
	"context"
	"sync"

	"github.com/leisurelyrcxf/spermwhale/kvcc"

	"github.com/leisurelyrcxf/spermwhale/errors"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"

	"github.com/leisurelyrcxf/spermwhale/topo"

	"github.com/leisurelyrcxf/spermwhale/types"
)

type Shard struct {
	types.KVCC

	ID int
}

func NewShard(g *topo.Group) (*Shard, error) {
	cli, err := kvcc.NewClient(g.ServerAddr)
	if err != nil {
		return nil, err
	}
	return &Shard{
		KVCC: cli,
		ID:   g.Id,
	}, nil
}

// TODO the routing logic is not the key point of this project,
// thus here is the simplest implementation. Cannot be used in product.
type Gate struct {
	shardsReady bool
	shards      []*Shard
	shardsRW    sync.RWMutex

	store *topo.Store
}

func NewGate(store *topo.Store) (*Gate, error) {
	g := &Gate{store: store}
	if err := g.syncShards(); err != nil {
		return nil, err
	}
	if err := g.watchShards(); err != nil {
		return nil, err
	}
	return g, nil
}

func (g *Gate) Get(ctx context.Context, key string, opt types.KVCCReadOption) (types.ValueCC, error) {
	s, err := g.Route(types.TxnKeyUnion{Key: key, TxnId: types.TxnId(opt.ExactVersion)})
	if err != nil {
		return types.EmptyValueCC, err
	}
	return s.Get(ctx, key, opt)
}

func (g *Gate) Set(ctx context.Context, key string, val types.Value, opt types.KVCCWriteOption) error {
	s, err := g.Route(types.TxnKeyUnion{Key: key, TxnId: types.TxnId(val.Version)})
	if err != nil {
		return err
	}
	return s.Set(ctx, key, val, opt)
}

func (g *Gate) UpdateMeta(ctx context.Context, key string, version uint64, opt types.KVCCUpdateMetaOption) error {
	assert.Must(key != "")
	s, err := g.Route(types.TxnKeyUnion{Key: key})
	if err != nil {
		return err
	}
	return s.UpdateMeta(ctx, key, version, opt)
}

func (g *Gate) RollbackKey(ctx context.Context, key string, version uint64, opt types.KVCCRollbackKeyOption) error {
	assert.Must(key != "")
	s, err := g.Route(types.TxnKeyUnion{Key: key})
	if err != nil {
		return err
	}
	return s.RollbackKey(ctx, key, version, opt)
}

func (g *Gate) RemoveTxnRecord(ctx context.Context, version uint64, opt types.KVCCRemoveTxnRecordOption) error {
	s, err := g.Route(types.TxnKeyUnion{TxnId: types.TxnId(version)})
	if err != nil {
		return err
	}
	return s.RemoveTxnRecord(ctx, version, opt)
}

func (g *Gate) KeyVersionCount(ctx context.Context, key string) (int64, error) {
	s, err := g.Route(types.TxnKeyUnion{Key: key})
	if err != nil {
		return 0, err
	}
	return s.KeyVersionCount(ctx, key)
}

func (g *Gate) Close() (err error) {
	for _, s := range g.shards {
		err = errors.Wrap(err, s.Close())
	}
	return err
}

func (g *Gate) Route(key types.TxnKeyUnion) (*Shard, error) {
	g.shardsRW.RLock()
	defer g.shardsRW.RUnlock()

	if !g.shardsReady {
		return nil, errors.ErrShardsNotReady
	}
	var id = key.Hash() % uint64(len(g.shards))
	if g.shards[id] == nil {
		glog.Fatalf("g.shards[%d] == nil", id)
	}
	return g.shards[id], nil
}

func (g *Gate) MustRoute(key types.TxnKeyUnion) *Shard {
	s, err := g.Route(key)
	assert.MustNoError(err)
	return s
}

func (g *Gate) GetShards() []*Shard {
	return g.shards
}

func (g *Gate) watchShards() error {
	watchFuture, err := g.store.Client().WatchOnce(g.store.GroupDir())
	if err != nil && !errors.IsNotSupportedErr(err) {
		return err
	}

	if errors.IsNotSupportedErr(err) {
		if g.shardsReady {
			return nil
		}
		return errors.Annotatef(errors.ErrShardsNotReady, "reason: %v", err)
	}

	go func() {
		for {
			<-watchFuture

			if err := g.syncShards(); err != nil {
				glog.Fatalf("update shards failed: '%v'", err)
			}
			var err error
			if watchFuture, err = g.store.Client().WatchOnce(g.store.GroupDir()); err != nil {
				glog.Fatalf("watch once failed: '%v'", err)
			}
		}
	}()

	return nil
}

func (g *Gate) syncShards() error {
	groups, err := g.store.ListGroup()
	if err != nil {
		return err
	}

	g.shardsRW.Lock()
	defer g.shardsRW.Unlock()

	for _, group := range groups {
		if err := g.syncShard(group); err != nil {
			return err
		}
	}
	g.checkShards()
	return nil
}

func (g *Gate) syncShard(group *topo.Group) error {
	if group.Id < 0 {
		return errors.Errorf("group.Id < 0")
	}
	if group.Id >= len(g.shards) {
		for i, count := 0, group.Id-len(g.shards)+1; i < count; i++ {
			g.shards = append(g.shards, nil)
		}
	}
	assert.Must(group.Id < len(g.shards))

	shard, err := NewShard(group)
	if err != nil {
		glog.Errorf("synchronizing group %d to %s failed: '%v'", group.Id, group.ServerAddr, err)
		return err
	}
	g.shards[shard.ID] = shard
	glog.V(6).Infof("synchronized group %d to %s successfully", shard.ID, group.ServerAddr)
	return nil
}

func (g *Gate) checkShards() {
	for i := 0; i < len(g.shards); i++ {
		if g.shards[i] == nil {
			glog.Warningf("shard %d not ready", i)
			g.shardsReady = false
		}
	}
	g.shardsReady = true
}

// Use a gate as a kv server
type ReadOnlyKV struct {
	*Gate
}

func NewReadOnlyKV(gate *Gate) *ReadOnlyKV {
	return &ReadOnlyKV{gate}
}

func (kv *ReadOnlyKV) Get(ctx context.Context, key string, opt types.KVReadOption) (types.Value, error) {
	val, err := kv.Gate.Get(ctx, key, *opt.ToKVCC())
	//noinspection ALL
	return val.Value, err
}

func (kv *ReadOnlyKV) Set(context.Context, string, types.Value, types.KVWriteOption) error {
	return errors.Annotatef(errors.ErrNotSupported, "this is an read-only kv server")
}

func (kv *ReadOnlyKV) KeyVersionCount(ctx context.Context, key string) (int64, error) {
	return kv.Gate.KeyVersionCount(ctx, key)
}

func (kv *ReadOnlyKV) UpdateMeta(context.Context, string, uint64, types.KVUpdateMetaOption) error {
	return errors.Annotatef(errors.ErrNotSupported, "by ReadOnlyKV")
}

func (kv *ReadOnlyKV) RollbackKey(context.Context, string, uint64) error {
	return errors.Annotatef(errors.ErrNotSupported, "by ReadOnlyKV")
}

func (kv *ReadOnlyKV) RemoveTxnRecord(context.Context, uint64) error {
	return errors.ErrNotSupported
}
