package smart_txn_client

import (
	"context"
	"math/rand"
	"strconv"
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"

	"github.com/leisurelyrcxf/spermwhale/utils"

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
	_, _, err := c.DoTransactionOfTypeEx(ctx, types.TxnTypeDefault, f)
	if err != nil {
		glog.Errorf("[DoTransaction] do transaction failed: '%v'", err)
	}
	return err
}

func (c *SmartClient) DoTransactionOfType(ctx context.Context, typ types.TxnType, f func(ctx context.Context, txn types.Txn) error) error {
	_, _, err := c.DoTransactionOfTypeEx(ctx, typ, f)
	if err != nil {
		glog.Errorf("[DoTransaction] do transaction failed: '%v'", err)
	}
	return err
}

func (c *SmartClient) DoTransactionEx(ctx context.Context, f func(ctx context.Context, txn types.Txn) error) (types.Txn, error) {
	txn, _, err := c.DoTransactionOfTypeEx(ctx, types.TxnTypeDefault, f)
	return txn, err
}

func (c *SmartClient) DoTransactionOfTypeEx(ctx context.Context, typ types.TxnType, f func(ctx context.Context, txn types.Txn) error) (types.Txn, int, error) {
	return c.DoTransactionRaw(ctx, typ, func(ctx context.Context, txn types.Txn) (err error, retry bool) {
		return f(ctx, txn), true
	}, nil, nil)
}

func (c *SmartClient) DoTransactionRaw(ctx context.Context, typ types.TxnType, f func(ctx context.Context, txn types.Txn) (err error, retry bool),
	beforeCommit, beforeRollback func() error) (types.Txn, int, error) {
	var snapshotVersion = uint64(0)
	for i := 0; i < c.maxRetry; i++ {
		if err := ctx.Err(); err != nil {
			return nil, i + 1, err
		}
		tx, err := c.TxnManager.BeginTransaction(ctx, typ, snapshotVersion)
		if err != nil {
			if errors.IsRetryableTransactionManagerErr(err) {
				continue
			}
			return nil, i + 1, err
		}

		err, retry := f(ctx, tx)
		if err == nil {
			if beforeCommit != nil {
				if err := beforeCommit(); err != nil {
					return tx, i + 1, err
				}
			}
			err := tx.Commit(ctx)
			if err == nil {
				return tx, i + 1, nil
			}
			if !retry { //TODO FIXME
				return tx, i + 1, err
			}
			if !tx.GetState().IsAborted() && !errors.IsRetryableTransactionErr(err) {
				return tx, i + 1, err
			}
			rand.Seed(time.Now().UnixNano())
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(4)))
			continue
		}
		if beforeRollback != nil {
			if err := beforeRollback(); err != nil {
				return tx, i + 1, err
			}
		}
		if !tx.GetState().IsAborted() {
			_ = tx.Rollback(ctx)
		}
		if !retry || !errors.IsRetryableTransactionErr(err) {
			return tx, i + 1, err
		}
		if errors.IsSnapshotReadTabletErr(err) {
			snapshotVersion = tx.GetSnapshotVersion()
			assert.Must(snapshotVersion > 0)
		}
		time.Sleep(utils.RandomPeriod(time.Millisecond, 1, 9))
	}
	return nil, c.maxRetry, errors.Annotatef(errors.ErrTxnRetriedTooManyTimes, "after retried %d times", c.maxRetry)
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

func (c *SmartClient) MGet(ctx context.Context, keys []string, txnType types.TxnType) (values []types.Value, _ error) {
	if _, _, err := c.DoTransactionOfTypeEx(ctx, txnType, func(ctx context.Context, txn types.Txn) (err error) {
		values, err = txn.MGet(ctx, keys, types.NewTxnReadOption())
		return
	}); err != nil {
		return nil, err
	}
	return values, nil
}

func (c *SmartClient) SetInt(ctx context.Context, key string, intVal int) error {
	return c.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
		return txn.Set(ctx, key, []byte(strconv.Itoa(intVal)))
	})
}

func (c *SmartClient) GetInt(ctx context.Context, key string) (int, error) {
	val, err := c.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	return val.Int()
}

func (c *SmartClient) MGetInts(ctx context.Context, keys []string, txnType types.TxnType) ([]int, error) {
	values, err := c.MGet(ctx, keys, txnType)
	if err != nil {
		return nil, err
	}
	ints := make([]int, 0, len(values))
	for _, v := range values {
		x, err := v.Int()
		if err != nil {
			return nil, err
		}
		ints = append(ints, x)
	}
	return ints, nil
}

func (c *SmartClient) Close() error {
	return c.TxnManager.Close()
}
