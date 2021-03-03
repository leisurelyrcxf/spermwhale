package kvcc

import (
	"context"

	"google.golang.org/grpc"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/proto/kvccpb"
	"github.com/leisurelyrcxf/spermwhale/types"
)

type Client struct {
	kv   kvccpb.KVCCClient
	conn *grpc.ClientConn
}

func NewClient(serverAddr string) (*Client, error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: conn,
		kv:   kvccpb.NewKVCCClient(conn),
	}, nil
}

func (c *Client) Get(ctx context.Context, key string, opt types.KVCCReadOption) (types.ValueCC, error) {
	resp, err := c.kv.Get(ctx, &kvccpb.KVCCGetRequest{
		Key: key,
		Opt: opt.ToPB(),
	})
	if err != nil {
		return types.EmptyValueCC, err
	}
	if resp == nil {
		return types.EmptyValueCC, errors.Annotatef(errors.ErrNilResponse, "TabletClient::Get resp == nil")
	}
	if resp.Err != nil {
		if resp.V == nil {
			return types.EmptyValueCC, errors.NewErrorFromPB(resp.Err)
		}
		return types.NewValueCCFromPB(resp.V), errors.NewErrorFromPB(resp.Err)
	}
	if resp.V == nil {
		return types.EmptyValueCC, errors.Annotatef(errors.ErrNilResponse, "TabletClient::Get resp.V == nil")
	}
	return types.NewValueCCFromPB(resp.V), nil
}

func (c *Client) Set(ctx context.Context, key string, val types.Value, opt types.KVCCWriteOption) error {
	resp, err := c.kv.Set(ctx, &kvccpb.KVCCSetRequest{
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
