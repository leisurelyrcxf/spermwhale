package smart_txn_client

import (
	"context"
	"math/rand"
	"time"

	"github.com/leisurelyrcxf/spermwhale/errors"

	"github.com/leisurelyrcxf/spermwhale/types"
)

type Txn struct {
	id       uint64
	delegate types.Txn
}

func NewTxn(id uint64) *Txn {
	return &Txn{id: id}
}

func (txn *Txn) Get(ctx context.Context, key string) (types.Value, error) {
	return txn.delegate.Get(ctx, key, txn.id)
}

func (txn *Txn) Set(ctx context.Context, key string, val []byte) error {
	return txn.delegate.Set(ctx, key, val, txn.id)
}

func (txn *Txn) Commit(ctx context.Context) error {
	return txn.delegate.Commit(ctx, txn.id)
}

func (txn *Txn) Rollback(ctx context.Context) error {
	return txn.delegate.Rollback(ctx, txn.id)
}

func (txn *Txn) Close() error {
	return txn.delegate.Close()
}

type SmartClient struct {
	delegate types.Txn
}

func NewSmartClient(delegate types.Txn) *SmartClient {
	return &SmartClient{delegate: delegate}
}

func (c *SmartClient) DoTransaction(ctx context.Context, f func(ctx context.Context, txn *Txn) error) error {
	for i := 0; i < 100; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			break
		}
		txn, err := c.beginTransaction(ctx)
		if err != nil {
			return err
		}

		if err = f(ctx, txn); err == nil {
			return nil
		}
		if !errors.IsRetryableErr(err) {
			return err
		}
		rand.Seed(time.Now().UnixNano())
		time.Sleep(time.Millisecond * rand.Intn(10))
	}
	return errors.ErrTxnRetriedTooManyTimes
}

func (c *SmartClient) Close() error {
	return c.delegate.Close()
}

func (c *SmartClient) beginTransaction(ctx context.Context) (*Txn, error) {
	txnID, err := c.delegate.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return NewTxn(txnID), nil
}
