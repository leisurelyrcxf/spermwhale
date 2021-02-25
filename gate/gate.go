package gate

import (
	"context"
	"sync"

	"github.com/leisurelyrcxf/spermwhale/kv"

	"github.com/leisurelyrcxf/spermwhale/errors"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"

	"github.com/leisurelyrcxf/spermwhale/models"

	"github.com/leisurelyrcxf/spermwhale/types"
)

type Shard struct {
	*kv.Client

	id int
}

func NewShard(g *models.Group) (*Shard, error) {
	cli, err := kv.NewClient(g.ServerAddr)
	if err != nil {
		return nil, err
	}
	return &Shard{
		Client: cli,
		id:     g.Id,
	}, nil
}

// TODO the routing logic is not the key point of this project,
// thus here is the simplest implementation. Cannot be used in product.
type Gate struct {
	shardsReady bool
	shards      []*Shard
	shardsRW    sync.RWMutex

	store *models.Store
}

func NewGate(store *models.Store) (*Gate, error) {
	g := &Gate{store: store}
	if err := g.syncShards(); err != nil {
		return nil, err
	}
	if err := g.watchShards(); err != nil {
		return nil, err
	}
	return g, nil
}

func (g *Gate) Get(ctx context.Context, key string, opt types.ReadOption) (types.Value, error) {
	s, err := g.route(key)
	if err != nil {
		return types.EmptyValue, err
	}
	return s.Get(ctx, key, opt)
}

func (g *Gate) Set(ctx context.Context, key string, val types.Value, opt types.WriteOption) error {
	s, err := g.route(key)
	if err != nil {
		return err
	}
	return s.Set(ctx, key, val, opt)
}

func (g *Gate) Close() (err error) {
	for _, s := range g.shards {
		err = errors.Wrap(err, s.Close())
	}
	return err
}

func (g *Gate) route(key string) (*Shard, error) {
	g.shardsRW.RLock()
	defer g.shardsRW.RUnlock()

	if !g.shardsReady {
		return nil, errors.ErrShardsNotReady
	}
	var id = Hash([]byte(key)) % len(g.shards)
	if g.shards[id] == nil {
		glog.Fatalf("g.shards[%d] == nil", id)
	}
	return g.shards[id], nil
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
	g.shardsRW.Lock()
	defer g.shardsRW.Unlock()

	groups, err := g.store.ListGroup()
	if err != nil {
		return err
	}
	for _, group := range groups {
		if err := g.syncShard(group); err != nil {
			return err
		}
	}
	g.checkShards()
	return nil
}

func (g *Gate) syncShard(group *models.Group) error {
	if group.Id < 0 {
		return errors.Errorf("group.Id < 0")
	}
	if group.Id >= len(g.shards) {
		for i := 0; i < group-len(g.shards)+1; i++ {
			g.shards = append(g.shards, nil)
		}
	}
	assert.Must(group.Id < len(g.shards))

	shard, err := NewShard(group)
	if err != nil {
		return err
	}
	g.shards[shard.id] = shard
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
