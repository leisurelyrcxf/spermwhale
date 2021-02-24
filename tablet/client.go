package tablet

import (
	"context"
	"fmt"

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

func (c *Client) Get(ctx context.Context, key string, version uint64) (types.Value, error) {
	resp, err := c.kv.Get(ctx, &kvpb.GetRequest{
		Key:     key,
		Version: version,
	})
	if err != nil {
		return types.Value{}, err
	}
	if resp == nil {
		return types.Value{}, NilGetResponse
	}
	if resp.Err != nil {
		return types.Value{}, resp.Err.Error()
	}
	if resp.V == nil {
		return types.Value{}, NilGetResponse
	}
	return resp.V.ToValue(), nil
}

func (c *Client) Set(ctx context.Context, key, val string, version uint64, writeIntent bool) error {
	resp, err := c.kv.Set(ctx, &kvpb.SetRequest{
		Key: key,
		Value: &kvpb.Value{
			Meta: &kvpb.ValueMeta{
				WriteIntent: writeIntent,
				Version:     version,
			},
			Val: val,
		},
	})
	if err != nil {
		return err
	}
	if resp == nil {
		return NilSetResponse
	}
	return resp.Err.Error()
}

func (c *Client) Close() error {
	return c.conn.Close()
}
