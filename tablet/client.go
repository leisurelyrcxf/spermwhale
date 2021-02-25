package tablet

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/errors"

	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"

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
		Opt: commonpb.ToPBReadOption(opt),
	})
	if err != nil {
		return types.EmptyValue, err
	}
	if resp == nil {
		return types.EmptyValue, errors.Annotatef(errors.ErrNilResponse, "TabletClient::Get resp == nil")
	}
	if resp.Err != nil {
		return types.EmptyValue, resp.Err.Error()
	}
	if resp.V == nil {
		return types.EmptyValue, errors.Annotatef(errors.ErrNilResponse, "TabletClient::Get resp.V == nil")
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
		return errors.Annotatef(errors.ErrNilResponse, "TabletClient::Set resp == nil")
	}
	return resp.Err.Error()
}

func (c *Client) Close() error {
	return c.conn.Close()
}
