package txn

import (
	"context"
	"fmt"

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

func (c *Client) Begin(ctx context.Context, typ types.TxnType) (TransactionInfo, error) {
	resp, err := c.c.Begin(ctx, &txnpb.BeginRequest{Type: typ.ToPB()})
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

func (c *Client) Get(ctx context.Context, key string, txnID types.TxnId, opt types.TxnReadOption) (types.Value, TransactionInfo, error) {
	resp, err := c.c.Get(ctx, &txnpb.TxnGetRequest{
		Key:   key,
		TxnId: uint64(txnID),
		Opt:   opt.ToPB(),
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

func (c *Client) MGet(ctx context.Context, keys []string, txnID types.TxnId, opt types.TxnReadOption) ([]types.Value, TransactionInfo, error) {
	resp, err := c.c.MGet(ctx, &txnpb.TxnMGetRequest{
		Keys:  keys,
		TxnId: uint64(txnID),
		Opt:   opt.ToPB(),
	})
	if err != nil {
		return nil, InvalidTransactionInfo(txnID), err
	}
	if resp == nil {
		return nil, InvalidTransactionInfo(txnID), errors.Annotatef(errors.ErrNilResponse, "TxnClient::MGet resp == nil")
	}
	if resp.Txn == nil {
		return nil, InvalidTransactionInfo(txnID), errors.Annotatef(errors.ErrNilResponse, "TxnClient::MGet resp.Txn == nil")
	}
	txnInfo := NewTransactionInfoFromPB(resp.Txn)
	assert.Must(txnInfo.ID == txnID)
	if resp.Err != nil {
		return nil, txnInfo, errors.NewErrorFromPB(resp.Err)
	}
	if resp.Values == nil {
		return nil, txnInfo, errors.Annotatef(errors.ErrNilResponse, "TxnClient::MGet resp.V == nil")
	}
	if len(resp.Values) != len(keys) {
		return nil, txnInfo, errors.Annotatef(errors.ErrInvalidResponse, fmt.Sprintf("TxnClient::MGet resp length not match, expect %d, got %d", len(keys), len(resp.Values)))
	}
	values := make([]types.Value, 0, len(keys))
	for _, v := range resp.Values {
		if v.Meta == nil {
			return nil, txnInfo, errors.Annotatef(errors.ErrNilResponse, "TxnClient::MGet resp.V.Meta == nil")
		}
		values = append(values, types.NewValueFromPB(v))
	}
	return values, txnInfo, nil
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
	c            *Client
	recordValues bool
}

func NewClientTxnManager(c *Client) *ClientTxnManager {
	return &ClientTxnManager{c: c}
}

func (m *ClientTxnManager) SetRecordValues(b bool) *ClientTxnManager {
	m.recordValues = b
	return m
}

func (m *ClientTxnManager) BeginTransaction(ctx context.Context, typ types.TxnType) (types.Txn, error) {
	txnInfo, err := m.c.Begin(ctx, typ)
	if err != nil {
		return nil, err
	}
	assert.Must(txnInfo.TxnType == typ)
	txn := newClientTxn(txnInfo, m.c)
	if !m.recordValues {
		return txn, nil
	}
	return types.NewRecordValuesTxn(txn), nil
}

func (m *ClientTxnManager) Close() error {
	return m.c.Close()
}

type ClientTxn struct {
	TransactionInfo

	c *Client
}

func newClientTxn(txnInfo TransactionInfo, c *Client) *ClientTxn {
	return &ClientTxn{
		TransactionInfo: txnInfo,
		c:               c,
	}
}

func (txn *ClientTxn) Get(ctx context.Context, key string, opt types.TxnReadOption) (types.Value, error) {
	val, txnInfo, err := txn.c.Get(ctx, key, txn.ID, opt)
	assert.Must(txn.ID == txnInfo.ID)
	txn.TransactionInfo = txnInfo
	return val, err
}

func (txn *ClientTxn) MGet(ctx context.Context, keys []string, opt types.TxnReadOption) ([]types.Value, error) {
	values, txnInfo, err := txn.c.MGet(ctx, keys, txn.ID, opt)
	assert.Must(txn.ID == txnInfo.ID)
	txn.TransactionInfo = txnInfo
	return values, err
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

func (txn *ClientTxn) GetReadValues() map[string]types.Value {
	return types.InvalidReadValues
}

func (txn *ClientTxn) GetWriteValues() map[string]types.Value {
	return types.InvalidWriteValues
}
