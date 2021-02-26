package smart_txn_client

import (
	"context"
	"math/rand"
	"strconv"
	"time"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
)

type SmartClient struct {
	types.TxnManager
}

func NewSmartClient(tm types.TxnManager) *SmartClient {
	return &SmartClient{TxnManager: tm}
}

func (c *SmartClient) DoTransaction(ctx context.Context, f func(ctx context.Context, txn types.Txn) error) error {
	return c.DoTransactionEx(ctx, func(ctx context.Context, txn types.Txn) (err error, retry bool) {
		return f(ctx, txn), true
	})
}

func (c *SmartClient) DoTransactionEx(ctx context.Context, f func(ctx context.Context, txn types.Txn) (err error, retry bool)) error {
	for i := 0; i < 100; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			break
		}
		tx, err := c.TxnManager.BeginTransaction(ctx)
		if err != nil {
			if errors.IsRetryableErr(err) {
				continue
			}
			return err
		}

		err, retry := f(ctx, tx)
		if err == nil {
			return tx.Commit(ctx)
		}
		_ = tx.Rollback(ctx)
		if !retry {
			return err
		}
		if !errors.IsRetryableErr(err) {
			return err
		}
		rand.Seed(time.Now().UnixNano())
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(4)))
	}
	return errors.ErrTxnRetriedTooManyTimes
}

func (c *SmartClient) Set(ctx context.Context, key string, val []byte) error {
	return c.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
		return txn.Set(ctx, key, val)
	})
}

func (c *SmartClient) Get(ctx context.Context, key string) (val types.Value, _ error) {
	if err := c.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) (err error) {
		val, err = txn.Get(ctx, key)
		return
	}); err != nil {
		return types.EmptyValue, err
	}
	return val, nil
}

func (c *SmartClient) SetInt(ctx context.Context, key string, intVal int) error {
	return c.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
		return txn.Set(ctx, key, []byte(strconv.Itoa(intVal)))
	})
}

func (c *SmartClient) GetInt(ctx context.Context, key string) (int, error) {
	var val types.Value
	if err := c.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) (err error) {
		val, err = txn.Get(ctx, key)
		return
	}); err != nil {
		return 0, err
	}
	return val.Int()
}

func (c *SmartClient) Close() error {
	return c.TxnManager.Close()
}
