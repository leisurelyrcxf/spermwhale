package txn

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/assert"

	"github.com/leisurelyrcxf/spermwhale/errors"

	"github.com/leisurelyrcxf/spermwhale/proto/txnpb"
	"github.com/leisurelyrcxf/spermwhale/types"
	"google.golang.org/grpc"
)

type Client struct {
	c    txnpb.TxnServiceClient
	conn *grpc.ClientConn
}

func NewClient(serverAddr string) (*Client, error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: conn,
		c:    txnpb.NewTxnServiceClient(conn),
	}, nil
}

func (c *Client) Begin(ctx context.Context) (TransactionInfo, error) {
	resp, err := c.c.Begin(ctx, &txnpb.BeginRequest{})
	if err != nil {
		return InvalidTransactionInfo(0), err
	}
	if resp == nil {
		return InvalidTransactionInfo(0), errors.Annotatef(errors.ErrNilResponse, "TxnClient::Begin resp == nil")
	}
	if resp.Err != nil {
		return InvalidTransactionInfo(0), errors.NewErrorFromPB(resp.Err)
	}
	if resp.Txn == nil {
		return InvalidTransactionInfo(0), errors.Annotatef(errors.ErrNilResponse, "TxnClient::Begin resp.Txn == nil")
	}
	return NewTransactionInfoFromPB(resp.Txn), nil
}

func (c *Client) Get(ctx context.Context, key string, txnID types.TxnId) (types.Value, TransactionInfo, error) {
	resp, err := c.c.Get(ctx, &txnpb.TxnGetRequest{
		Key:   key,
		TxnId: uint64(txnID),
	})
	if err != nil {
		return types.EmptyValue, InvalidTransactionInfo(txnID), err
	}
	if resp == nil {
		return types.EmptyValue, InvalidTransactionInfo(txnID), errors.Annotatef(errors.ErrNilResponse, "TxnClient::Get resp == nil")
	}
	if resp.Txn == nil {
		return types.EmptyValue, InvalidTransactionInfo(txnID), errors.Annotatef(errors.ErrNilResponse, "resp.Txn == nil")
	}
	txnInfo := NewTransactionInfoFromPB(resp.Txn)
	assert.Must(txnInfo.ID == txnID)
	if resp.Err != nil {
		return types.EmptyValue, txnInfo, errors.NewErrorFromPB(resp.Err)
	}
	if resp.V == nil {
		return types.EmptyValue, txnInfo, errors.Annotatef(errors.ErrNilResponse, "TxnClient::Get resp.V == nil")
	}
	if resp.V.Meta == nil {
		return types.EmptyValue, txnInfo, errors.Annotatef(errors.ErrNilResponse, "TxnClient::Get resp.V.Meta == nil")
	}
	return types.NewValueFromPB(resp.V), txnInfo, nil
}

func (c *Client) Set(ctx context.Context, key string, val []byte, txnID types.TxnId) (TransactionInfo, error) {
	resp, err := c.c.Set(ctx, &txnpb.TxnSetRequest{
		Key:   key,
		Value: val,
		TxnId: uint64(txnID),
	})
	if err != nil {
		return InvalidTransactionInfo(txnID), err
	}
	if resp == nil {
		return InvalidTransactionInfo(txnID), errors.Annotatef(errors.ErrNilResponse, "TxnClient::Set resp == nil")
	}
	if resp.Txn == nil {
		return InvalidTransactionInfo(txnID), errors.Annotatef(errors.ErrNilResponse, "TxnClient::Set resp.Txn == nil")
	}
	assert.Must(types.TxnId(resp.Txn.Id) == txnID)
	return NewTransactionInfoFromPB(resp.Txn), errors.NewErrorFromPB(resp.Err)
}

func (c *Client) Commit(ctx context.Context, txnID types.TxnId) (TransactionInfo, error) {
	resp, err := c.c.Commit(ctx, &txnpb.CommitRequest{
		TxnId: uint64(txnID),
	})
	if err != nil {
		return InvalidTransactionInfo(txnID), err
	}
	if resp == nil {
		return InvalidTransactionInfo(txnID), errors.Annotatef(errors.ErrNilResponse, "TxnClient::Commit resp == nil")
	}
	if resp.Txn == nil {
		return InvalidTransactionInfo(txnID), errors.Annotatef(errors.ErrNilResponse, "TxnClient::Commit resp.Txn == nil")
	}
	assert.Must(types.TxnId(resp.Txn.Id) == txnID)
	return NewTransactionInfoFromPB(resp.Txn), errors.NewErrorFromPB(resp.Err)
}

func (c *Client) Rollback(ctx context.Context, txnID types.TxnId) (TransactionInfo, error) {
	resp, err := c.c.Rollback(ctx, &txnpb.RollbackRequest{
		TxnId: uint64(txnID),
	})
	if err != nil {
		return InvalidTransactionInfo(txnID), err
	}
	if resp == nil {
		return InvalidTransactionInfo(txnID), errors.Annotatef(errors.ErrNilResponse, "TxnClient::Rollback resp == nil")
	}
	if resp.Txn == nil {
		return InvalidTransactionInfo(txnID), errors.Annotatef(errors.ErrNilResponse, "TxnClient::Rollback resp.Txn == nil")
	}
	return NewTransactionInfoFromPB(resp.Txn), errors.NewErrorFromPB(resp.Err)
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
	txnInfo, err := m.c.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &ClientTxn{
		TransactionInfo: txnInfo,
		c:               m.c,
	}, nil
}

func (m *ClientTxnManager) Close() error {
	return m.c.Close()
}

type ClientTxn struct {
	TransactionInfo

	c *Client
}

func (txn *ClientTxn) Get(ctx context.Context, key string) (types.Value, error) {
	val, txnInfo, err := txn.c.Get(ctx, key, txn.ID)
	assert.Must(txn.ID == txnInfo.ID)
	txn.TransactionInfo = txnInfo
	return val, err
}

func (txn *ClientTxn) Set(ctx context.Context, key string, val []byte) error {
	txnInfo, err := txn.c.Set(ctx, key, val, txn.ID)
	assert.Must(txn.ID == txnInfo.ID)
	txn.TransactionInfo = txnInfo
	return err
}

func (txn *ClientTxn) Commit(ctx context.Context) error {
	txnInfo, err := txn.c.Commit(ctx, txn.ID)
	assert.Must(txn.ID == txnInfo.ID)
	txn.TransactionInfo = txnInfo
	return err
}

func (txn *ClientTxn) Rollback(ctx context.Context) error {
	txnInfo, err := txn.c.Rollback(ctx, txn.ID)
	assert.Must(txn.ID == txnInfo.ID)
	txn.TransactionInfo = txnInfo
	return err
}
