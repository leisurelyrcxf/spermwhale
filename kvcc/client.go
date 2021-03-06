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

func (c *Client) KeyVersionCount(ctx context.Context, key string) (int64, error) {
	resp, err := c.kv.KeyVersionCount(ctx, &kvccpb.KVCCVersionCountRequest{Key: key})
	if err != nil {
		return 0, err
	}
	if resp == nil {
		return 0, errors.Annotatef(errors.ErrNilResponse, "TabletClient::KeyVersionCount resp == nil")
	}
	if resp.Err != nil {
		return 0, errors.NewErrorFromPB(resp.Err)
	}
	return resp.VersionCount, nil
}

func (c *Client) UpdateMeta(ctx context.Context, key string, version uint64, opt types.KVCCUpdateMetaOption) error {
	resp, err := c.kv.UpdateMeta(ctx, &kvccpb.KVCCUpdateMetaRequest{
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

func (c *Client) RollbackKey(ctx context.Context, key string, version uint64, opt types.KVCCRollbackKeyOption) error {
	resp, err := c.kv.RollbackKey(ctx, &kvccpb.KVCCRollbackKeyRequest{
		Key:     key,
		Version: version,
		Opt:     opt.ToPB(),
	})
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.Annotatef(errors.ErrNilResponse, "TabletClient::RollbackKey resp == nil")
	}
	return errors.NewErrorFromPB(resp.Err)
}

func (c *Client) RemoveTxnRecord(ctx context.Context, version uint64, opt types.KVCCRemoveTxnRecordOption) error {
	resp, err := c.kv.RemoveTxnRecord(ctx, &kvccpb.KVCCRemoveTxnRecordRequest{
		Version: version,
		Opt:     opt.ToPB(),
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
