package kv

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/errors"

	"github.com/leisurelyrcxf/spermwhale/proto/tabletpb"
	"github.com/leisurelyrcxf/spermwhale/types"
	"google.golang.org/grpc"
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
		Opt: opt.ToPB(),
	})
	if err != nil {
		return types.EmptyValue, err
	}
	if resp == nil {
		return types.EmptyValue, errors.Annotatef(errors.ErrNilResponse, "TabletClient::Get resp == nil")
	}
	if resp.Err != nil {
		return types.EmptyValue, errors.NewErrorFromPB(resp.Err)
	}
	if resp.V == nil {
		return types.EmptyValue, errors.Annotatef(errors.ErrNilResponse, "TabletClient::Get resp.V == nil")
	}
	return types.NewValueFromPB(resp.V), nil
}

func (c *Client) Set(ctx context.Context, key string, val types.Value, opt types.WriteOption) error {
	resp, err := c.kv.Set(ctx, &tabletpb.SetRequest{
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
