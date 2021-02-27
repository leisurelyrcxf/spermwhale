package txn

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/errors"

	"github.com/leisurelyrcxf/spermwhale/proto/txnpb"
	"github.com/leisurelyrcxf/spermwhale/types"
	"google.golang.org/grpc"
)

type Client struct {
	c    txnpb.TxnClient
	conn *grpc.ClientConn
}

func NewClient(serverAddr string) (*Client, error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: conn,
		c:    txnpb.NewTxnClient(conn),
	}, nil
}

func (c *Client) Begin(ctx context.Context) (types.TxnId, error) {
	resp, err := c.c.Begin(ctx, &txnpb.BeginRequest{})
	if err != nil {
		return 0, err
	}
	if resp == nil {
		return 0, errors.Annotatef(errors.ErrNilResponse, "TxnClient::Begin resp == nil")
	}
	if resp.Err != nil {
		return 0, resp.Err.Error()
	}
	return types.TxnId(resp.TxnId), nil
}

func (c *Client) Get(ctx context.Context, key string, txnID types.TxnId) (types.Value, error) {
	resp, err := c.c.Get(ctx, &txnpb.TxnGetRequest{
		Key:   key,
		TxnId: uint64(txnID),
	})
	if err != nil {
		return types.EmptyValue, err
	}
	if resp == nil {
		return types.EmptyValue, errors.Annotatef(errors.ErrNilResponse, "TxnClient::Get resp == nil")
	}
	if resp.Err != nil {
		return types.EmptyValue, resp.Err.Error()
	}
	if resp.V == nil {
		return types.EmptyValue, errors.Annotatef(errors.ErrNilResponse, "TxnClient::Get resp.V == nil")
	}
	return resp.V.Value(), nil
}

func (c *Client) Set(ctx context.Context, key string, val []byte, txnID types.TxnId) error {
	resp, err := c.c.Set(ctx, &txnpb.TxnSetRequest{
		Key:   key,
		Value: val,
		TxnId: uint64(txnID),
	})
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.Annotatef(errors.ErrNilResponse, "TxnClient::Set resp == nil")
	}
	return resp.Err.Error()
}

func (c *Client) Commit(ctx context.Context, txnID types.TxnId) error {
	resp, err := c.c.Commit(ctx, &txnpb.CommitRequest{
		TxnId: uint64(txnID),
	})
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.Annotatef(errors.ErrNilResponse, "TxnClient::Commit resp == nil")
	}
	return resp.Err.Error()
}

func (c *Client) Rollback(ctx context.Context, txnID types.TxnId) error {
	resp, err := c.c.Rollback(ctx, &txnpb.RollbackRequest{
		TxnId: uint64(txnID),
	})
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.Annotatef(errors.ErrNilResponse, "TxnClient::Rollback resp == nil")
	}
	return resp.Err.Error()
}

func (c *Client) Close() error {
	return c.conn.Close()
}

type ClientTxnManager struct {
	c *Client
}

func NewClientTxnManager(c *Client) *ClientTxnManager {
	return &ClientTxnManager{c: c}
}

func (m *ClientTxnManager) BeginTransaction(ctx context.Context) (types.Txn, error) {
	txnID, err := m.c.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return NewClientTxn(txnID, m.c), nil
}

func (m *ClientTxnManager) Close() error {
	return m.c.Close()
}

type ClientTxn struct {
	id types.TxnId
	c  *Client
}

func NewClientTxn(id types.TxnId, c *Client) *ClientTxn {
	return &ClientTxn{id: id, c: c}
}

func (txn *ClientTxn) GetId() types.TxnId {
	return txn.id
}

func (txn *ClientTxn) Get(ctx context.Context, key string) (types.Value, error) {
	return txn.c.Get(ctx, key, txn.id)
}

func (txn *ClientTxn) Set(ctx context.Context, key string, val []byte) error {
	return txn.c.Set(ctx, key, val, txn.id)
}

func (txn *ClientTxn) Commit(ctx context.Context) error {
	return txn.c.Commit(ctx, txn.id)
}

func (txn *ClientTxn) Rollback(ctx context.Context) error {
	return txn.c.Rollback(ctx, txn.id)
}
