package tablet

import (
	"context"
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"

	"github.com/leisurelyrcxf/spermwhale/proto/tabletpb"
	"github.com/leisurelyrcxf/spermwhale/types"
	"google.golang.org/grpc"
)

var (
	NilGetResponse = fmt.Errorf("nil get response")
	NilSetResponse = fmt.Errorf("nil set response")
)

type Client struct {
	kv   tabletpb.KVClient
	conn *grpc.ClientConn
}

func NewClient(serverAddr string) (*Client, error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: conn,
		kv:   tabletpb.NewKVClient(conn),
	}, nil
}

func (c *Client) Get(ctx context.Context, key string, opt types.ReadOption) (types.Value, error) {
	resp, err := c.kv.Get(ctx, &tabletpb.GetRequest{
		Key: key,
		Opt: commonpb.ToPBReadOption(opt),
	})
	if err != nil {
		return types.EmptyValue, err
	}
	if resp == nil {
		return types.EmptyValue, NilGetResponse
	}
	if resp.Err != nil {
		return types.EmptyValue, resp.Err.Error()
	}
	if resp.V == nil {
		return types.EmptyValue, NilGetResponse
	}
	return resp.V.Value(), nil
}

func (c *Client) Set(ctx context.Context, key string, val types.Value, opt types.WriteOption) error {
	resp, err := c.kv.Set(ctx, &tabletpb.SetRequest{
		Key:   key,
		Value: commonpb.ToPBValue(val),
		Opt:   commonpb.ToPBWriteOption(opt),
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
