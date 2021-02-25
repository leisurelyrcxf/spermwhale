package gate

import (
	"bytes"
	"context"
	"hash/crc32"

	"github.com/golang/glog"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/tablet"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils/errors"
)

type Shard struct {
	tablet.Client

	id int
}

type Shards []*Shard

func NewShards(shardCount int) Shards {
	return make([]*Shard, shardCount)
}

func (ss Shards) AddShard(s *Shard) {
	if s.id >= len(ss) || s.id < 0 {
		glog.Fatalf("invalid shard id %d", s.id)
	}
	if ss[s.id] != nil {
		glog.Fatalf("shard id %d already exists", s.id)
	}
	ss[s.id] = s

}

func (ss Shards) Get(ctx context.Context, key string, version uint64) (types.Value, error) {
	s, err := ss.route(key)
	if err != nil {
		return types.Value{}, err
	}
	return s.Get(ctx, key, version)
}

func (ss Shards) Set(ctx context.Context, key, val string, version uint64, writeIntent bool) error {
	s, err := ss.route(key)
	if err != nil {
		return err
	}
	return s.Set(ctx, key, val, version, writeIntent)
}

func (ss Shards) Close() (err error) {
	for _, s := range ss {
		err = errors.Wrap(err, s.Close())
	}
	return
}

func (ss Shards) route(key string) (*Shard, error) {
	var id = hash([]byte(key)) % len(ss)
	if ss[id] == nil {
		return nil, errors.Annotatef(consts.ErrShardNotExists, "id: %d", id)
	}
	return ss[id], nil
}

func hash(key []byte) uint32 {
	const (
		TagBeg = '{'
		TagEnd = '}'
	)
	if beg := bytes.IndexByte(key, TagBeg); beg >= 0 {
		if end := bytes.IndexByte(key[beg+1:], TagEnd); end >= 0 {
			key = key[beg+1 : beg+1+end]
		}
	}
	return crc32.ChecksumIEEE(key)
}
