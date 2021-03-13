package smart_txn_client

import (
	"context"
	"math/rand"
	"strconv"
	"time"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
)

const defaultMaxRetry = 1000

type SmartClient struct {
	types.TxnManager
	maxRetry int
}

func NewSmartClient(tm types.TxnManager, maxRetry int) *SmartClient {
	if maxRetry <= 0 {
		maxRetry = defaultMaxRetry
	}
	return &SmartClient{TxnManager: tm, maxRetry: maxRetry}
}

func (c *SmartClient) DoTransaction(ctx context.Context, f func(ctx context.Context, txn types.Txn) error) error {
	_, err := c.DoTransactionExOfType(ctx, types.TxnTypeDefault, f)
	if err != nil {
		glog.Errorf("[DoTransaction] do transaction failed: '%v'", err)
	}
	return err
}

func (c *SmartClient) DoTransactionOfType(ctx context.Context, typ types.TxnType, f func(ctx context.Context, txn types.Txn) error) error {
	_, err := c.DoTransactionExOfType(ctx, typ, f)
	if err != nil {
		glog.Errorf("[DoTransaction] do transaction failed: '%v'", err)
	}
	return err
}

func (c *SmartClient) DoTransactionEx(ctx context.Context, f func(ctx context.Context, txn types.Txn) error) (types.Txn, error) {
	return c.DoTransactionExOfType(ctx, types.TxnTypeDefault, f)
}

func (c *SmartClient) DoTransactionExOfType(ctx context.Context, typ types.TxnType, f func(ctx context.Context, txn types.Txn) error) (types.Txn, error) {
	return c.DoTransactionRaw(ctx, typ, func(ctx context.Context, txn types.Txn) (err error, retry bool) {
		return f(ctx, txn), true
	}, nil, nil)
}

func (c *SmartClient) DoTransactionRaw(ctx context.Context, typ types.TxnType, f func(ctx context.Context, txn types.Txn) (err error, retry bool),
	beforeCommit, beforeRollback func() error) (types.Txn, error) {
	for i := 0; i < c.maxRetry; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		tx, err := c.TxnManager.BeginTransaction(ctx, typ)
		if err != nil {
			if errors.IsRetryableTransactionManagerErr(err) {
				continue
			}
			return nil, err
		}

		err, retry := f(ctx, tx)
		if err == nil {
			if beforeCommit != nil {
				if err := beforeCommit(); err != nil {
					return tx, err
				}
			}
			err := tx.Commit(ctx)
			if err == nil {
				return tx, nil
			}
			if !retry {
				return tx, err
			}
			if !tx.GetState().IsAborted() && !errors.IsRetryableTransactionErr(err) {
				return tx, err
			}
			rand.Seed(time.Now().UnixNano())
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(4)))
			continue
		}
		if beforeRollback != nil {
			if err := beforeRollback(); err != nil {
				return tx, err
			}
		}
		if !tx.GetState().IsAborted() {
			_ = tx.Rollback(ctx)
		}
		if !retry || !errors.IsRetryableTransactionErr(err) {
			return tx, err
		}
		rand.Seed(time.Now().UnixNano())
		time.Sleep(time.Millisecond * time.Duration(1+rand.Intn(9)))
	}
	return nil, errors.Annotatef(errors.ErrTxnRetriedTooManyTimes, "after retried %d times", c.maxRetry)
}

func (c *SmartClient) Set(ctx context.Context, key string, val []byte) error {
	return c.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
		return txn.Set(ctx, key, val)
	})
}

func (c *SmartClient) Get(ctx context.Context, key string) (val types.Value, _ error) {
	if err := c.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) (err error) {
		val, err = txn.Get(ctx, key, types.NewTxnReadOption())
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
		val, err = txn.Get(ctx, key, types.NewTxnReadOption())
		return
	}); err != nil {
		return 0, err
	}
	return val.Int()
}

func (c *SmartClient) Close() error {
	return c.TxnManager.Close()
}
