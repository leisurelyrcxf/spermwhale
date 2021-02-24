package tablet

import (
	"context"
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/mvcc"

	"github.com/leisurelyrcxf/spermwhale/proto/kvpb"
	"github.com/leisurelyrcxf/spermwhale/types"
	"google.golang.org/grpc"
)

var (
	NilGetResponse = fmt.Errorf("nil get response")
	NilSetResponse = fmt.Errorf("nil set response")
)

type Client struct {
	kv   kvpb.KVClient
	conn *grpc.ClientConn
}

func NewClient(serverAddr string) (*Client, error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: conn,
		kv:   kvpb.NewKVClient(conn),
	}, nil
}

func (c *Client) Get(ctx context.Context, key string, version uint64) (types.VersionedValue, error) {
	resp, err := c.kv.Get(ctx, &kvpb.GetRequest{
		Key:     key,
		Version: version,
	})
	if err != nil {
		return types.VersionedValue{}, err
	}
	if resp == nil {
		return types.VersionedValue{}, NilGetResponse
	}
	if resp.Err != nil {
		return types.VersionedValue{}, resp.Err.Error()
	}
	if resp.V == nil {
		return types.VersionedValue{}, NilGetResponse
	}
	return resp.V.ToVersionedValue(), nil
}

func (c *Client) Set(ctx context.Context, key, val string, version uint64) error {
	resp, err := c.kv.Set(ctx, &kvpb.SetRequest{
		Key: key,
		Value: &kvpb.VersionedValue{
			Value:   val,
			Version: version,
		},
	})
	if err != nil {
		return err
	}
	if resp == nil {
		return NilSetResponse
	}
	if resp.Err != nil {
		return resp.Err.Error()
	}
	return nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

type KV struct {
	kvpb.UnimplementedKVServer
	db *mvcc.DB
}

func (kv *KV) Get(_ context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	vv, err := kv.db.Get(req.Key, req.Version)
	if err != nil {
		return &kvpb.GetResponse{
			Err: &kvpb.Error{
				Code: -1,
				Msg:  err.Error(),
			},
		}, nil
	}
	return &kvpb.GetResponse{
		V: &kvpb.VersionedValue{
			Value:   vv.Value,
			Version: vv.Version,
		},
	}, nil
}
func (kv *KV) Set(_ context.Context, req *kvpb.SetRequest) (*kvpb.SetResponse, error) {
	kv.db.Set(req.Key, req.Value.Value, req.Value.Version)
	return &kvpb.SetResponse{}, nil
}

func (kv *KV) mustEmbedUnimplementedKVServer() {}
