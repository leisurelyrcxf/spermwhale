package txn

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/types/basic"

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

func (c *Client) Begin(ctx context.Context, opt types.TxnOption) (TransactionInfo, error) {
	resp, err := c.c.Begin(ctx, &txnpb.BeginRequest{Opt: opt.ToPB()})
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

func (c *Client) Get(ctx context.Context, key string, txnID types.TxnId) (types.TValue, TransactionInfo, error) {
	resp, err := c.c.Get(ctx, &txnpb.TxnGetRequest{
		Key:   key,
		TxnId: uint64(txnID),
	})
	if err != nil {
		return types.EmptyTValue, InvalidTransactionInfo(txnID), err
	}
	if resp == nil {
		return types.EmptyTValue, InvalidTransactionInfo(txnID), errors.Annotatef(errors.ErrNilResponse, "TxnClient::Get resp == nil")
	}
	if resp.Txn == nil {
		return types.EmptyTValue, InvalidTransactionInfo(txnID), errors.Annotatef(errors.ErrNilResponse, "resp.Txn == nil")
	}
	txnInfo := NewTransactionInfoFromPB(resp.Txn)
	assert.Must(txnInfo.ID == txnID)
	if resp.Err != nil {
		if resp.TValue == nil {
			return types.EmptyTValue, txnInfo, errors.NewErrorFromPB(resp.Err)
		}
		return types.NewTValueFromPB(resp.TValue), txnInfo, errors.NewErrorFromPB(resp.Err)
	}
	if err := resp.TValue.Validate(); err != nil {
		return types.EmptyTValue, txnInfo, errors.Annotatef(errors.ErrInvalidResponse, err.Error())
	}
	return types.NewTValueFromPB(resp.TValue), txnInfo, nil
}

func (c *Client) MGet(ctx context.Context, keys []string, txnID types.TxnId) ([]types.TValue, TransactionInfo, error) {
	if err := types.ValidateMGetRequest(keys); err != nil {
		return nil, InvalidTransactionInfo(txnID), err
	}

	resp, err := c.c.MGet(ctx, &txnpb.TxnMGetRequest{
		Keys:  keys,
		TxnId: uint64(txnID),
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
		if err := txnpb.TValues(resp.TValues).Validate(len(keys)); err != nil {
			return nil, txnInfo, errors.NewErrorFromPB(resp.Err)
		}
		return types.NewTValuesFromPB(resp.TValues), txnInfo, errors.NewErrorFromPB(resp.Err)
	}
	if err := txnpb.TValues(resp.TValues).Validate(len(keys)); err != nil {
		return nil, txnInfo, errors.Annotatef(errors.ErrInvalidResponse, err.Error())
	}
	return types.NewTValuesFromPB(resp.TValues), txnInfo, nil
}

func (c *Client) Set(ctx context.Context, key string, val []byte, txnID types.TxnId) (TransactionInfo, error) {
	resp, err := c.c.Set(ctx, &txnpb.TxnSetRequest{
		Key:   key,
		Value: val,
		TxnId: txnID.Version(),
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

func (c *Client) MSet(ctx context.Context, keys []string, values [][]byte, txnID types.TxnId) (TransactionInfo, error) {
	if err := types.ValidateMSetRequest(keys, values); err != nil {
		return InvalidTransactionInfo(txnID), err
	}
	resp, err := c.c.MSet(ctx, &txnpb.TxnMSetRequest{
		Keys:   keys,
		Values: values,
		TxnId:  txnID.Version(),
	})
	if err != nil {
		return InvalidTransactionInfo(txnID), err
	}
	if resp == nil {
		return InvalidTransactionInfo(txnID), errors.Annotatef(errors.ErrNilResponse, "TxnClient::MSet resp == nil")
	}
	if resp.Txn == nil {
		return InvalidTransactionInfo(txnID), errors.Annotatef(errors.ErrNilResponse, "TxnClient::MSet resp.Txn == nil")
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

func (m *ClientTxnManager) BeginTransaction(ctx context.Context, opt types.TxnOption) (types.Txn, error) {
	txnInfo, err := m.c.Begin(ctx, opt)
	if err != nil {
		return nil, err
	}
	assert.Must(txnInfo.TxnType == opt.TxnType)
	assert.Must(txnInfo.TxnSnapshotReadOption.Equals(opt.SnapshotReadOption))
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

	preventedFutureWrite basic.Set
	c                    *Client
}

func newClientTxn(txnInfo TransactionInfo, c *Client) *ClientTxn {
	return &ClientTxn{
		TransactionInfo: txnInfo,
		c:               c,
	}
}

func (txn *ClientTxn) Get(ctx context.Context, key string) (types.TValue, error) {
	val, txnInfo, err := txn.c.Get(ctx, key, txn.ID)
	assert.Must(txn.ID == txnInfo.ID)
	txn.TransactionInfo = txnInfo
	if val.IsFutureWritePrevented() {
		txn.preventedFutureWrite.Insert(key)
	}
	return val, err
}

func (txn *ClientTxn) MGet(ctx context.Context, keys []string) ([]types.TValue, error) {
	values, txnInfo, err := txn.c.MGet(ctx, keys, txn.ID)
	assert.Must(txn.ID == txnInfo.ID)
	txn.TransactionInfo = txnInfo
	assert.Must(len(values) == 0 || len(values) == len(keys))
	for idx, val := range values {
		if val.IsFutureWritePrevented() {
			txn.preventedFutureWrite.Insert(keys[idx])
		}
	}
	return values, err
}

func (txn *ClientTxn) Set(ctx context.Context, key string, val []byte) error {
	if txn.preventedFutureWrite.Contains(key) {
		return errors.ErrWriteReadConflictFutureWritePrevented
	}
	txnInfo, err := txn.c.Set(ctx, key, val, txn.ID)
	assert.Must(txn.ID == txnInfo.ID)
	txn.TransactionInfo = txnInfo
	return err
}

func (txn *ClientTxn) MSet(ctx context.Context, keys []string, values [][]byte) error {
	for _, key := range keys {
		if txn.preventedFutureWrite.Contains(key) {
			return errors.ErrWriteReadConflictFutureWritePrevented
		}
	}
	txnInfo, err := txn.c.MSet(ctx, keys, values, txn.ID)
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

func (txn *ClientTxn) GetReadValues() map[string]types.TValue {
	panic(errors.ErrNotSupported)
}

func (txn *ClientTxn) GetWriteValues() map[string]types.Value {
	panic(errors.ErrNotSupported)
}
