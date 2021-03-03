package kv

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/errors"

	"github.com/leisurelyrcxf/spermwhale/proto/kvpb"
	"github.com/leisurelyrcxf/spermwhale/types"
	"google.golang.org/grpc"
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

func (c *Client) Get(ctx context.Context, key string, opt types.KVReadOption) (types.Value, error) {
	resp, err := c.kv.Get(ctx, &kvpb.KVGetRequest{
		Key: key,
		Opt: opt.ToPB(),
	})
	if err != nil {
		return types.EmptyValue, err
	}
	if resp == nil {
		return types.EmptyValue, errors.Annotatef(errors.ErrNilResponse, "kv::Client::Get resp == nil")
	}
	if resp.Err != nil {
		if resp.V == nil {
			return types.EmptyValue, errors.NewErrorFromPB(resp.Err)
		}
		return types.NewValueFromPB(resp.V), errors.NewErrorFromPB(resp.Err)
	}
	if resp.V == nil {
		return types.EmptyValue, errors.Annotatef(errors.ErrNilResponse, "kv::Client::Get resp.V == nil")
	}
	return types.NewValueFromPB(resp.V), nil
}

func (c *Client) Set(ctx context.Context, key string, val types.Value, opt types.KVWriteOption) error {
	resp, err := c.kv.Set(ctx, &kvpb.KVSetRequest{
		Key:   key,
		Value: val.ToPB(),
		Opt:   opt.ToPB(),
	})
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.Annotatef(errors.ErrNilResponse, "TabletClient::Set resp == nil")
	}
	return errors.NewErrorFromPB(resp.Err)
}

func (c *Client) Close() error {
	return c.conn.Close()
}
