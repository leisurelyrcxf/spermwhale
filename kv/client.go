package kv

import (
	"context"

	"google.golang.org/grpc"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/proto/kvpb"
	"github.com/leisurelyrcxf/spermwhale/types"
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
		return errors.Annotatef(errors.ErrNilResponse, "kv::Client::Set resp == nil")
	}
	return errors.NewErrorFromPB(resp.Err)
}

func (c *Client) KeyVersionCount(ctx context.Context, key string) (int64, error) {
	resp, err := c.kv.KeyVersionCount(ctx, &kvpb.KVVersionCountRequest{Key: key})
	if err != nil {
		return 0, err
	}
	if resp == nil {
		return 0, errors.Annotatef(errors.ErrNilResponse, "kv::Client::KeyVersionCount resp == nil")
	}
	return resp.VersionCount, nil
}

func (c *Client) UpdateMeta(ctx context.Context, key string, version uint64, opt types.KVUpdateMetaOption) error {
	resp, err := c.kv.UpdateMeta(ctx, &kvpb.KVUpdateMetaRequest{
		Key:     key,
		Version: version,
		Opt:     opt.ToPB(),
	})
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.Annotatef(errors.ErrNilResponse, "TabletClient::UpdateMeta resp == nil")
	}
	return errors.NewErrorFromPB(resp.Err)
}
func (c *Client) RollbackKey(ctx context.Context, key string, version uint64) error {
	resp, err := c.kv.RollbackKey(ctx, &kvpb.KVRollbackKeyRequest{
		Key:     key,
		Version: version,
	})
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.Annotatef(errors.ErrNilResponse, "TabletClient::RollbackKey resp == nil")
	}
	return errors.NewErrorFromPB(resp.Err)
}

func (c *Client) RemoveTxnRecord(ctx context.Context, version uint64) error {
	resp, err := c.kv.RemoveTxnRecord(ctx, &kvpb.KVRemoveTxnRecordRequest{
		Version: version,
	})
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.Annotatef(errors.ErrNilResponse, "TabletClient::RemoveTxnRecord resp == nil")
	}
	return errors.NewErrorFromPB(resp.Err)
}

func (c *Client) Close() error {
	return c.conn.Close()
}
