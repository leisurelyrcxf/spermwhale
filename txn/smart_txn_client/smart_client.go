package smart_txn_client

import (
	"context"
	"math/rand"
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"

	"github.com/leisurelyrcxf/spermwhale/consts"

	"github.com/leisurelyrcxf/spermwhale/utils"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
)

const defaultMaxRetry = 1000

type SmartClient struct {
	types.TxnManager
	maxRetry             int
	retryOnCommitFailure bool
}

func NewSmartClient(tm types.TxnManager, maxRetry int) *SmartClient {
	if maxRetry <= 0 {
		maxRetry = defaultMaxRetry
	}
	return &SmartClient{TxnManager: tm, maxRetry: maxRetry, retryOnCommitFailure: true}
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

func (c *SmartClient) DoTransactionOfTypeEx(ctx context.Context, typ types.TxnType, f func(ctx context.Context, txn types.Txn) error) (_ types.Txn, retryTimes int, _ error) {
	return c.DoTransactionRaw(ctx, types.NewTxnOption(typ), func(ctx context.Context, txn types.Txn) (err error, retry bool) {
		return f(ctx, txn), true
	}, nil, nil, VoidOnRetry)
}

func (c *SmartClient) DoTransactionOfOption(ctx context.Context, opt types.TxnOption, f func(ctx context.Context, txn types.Txn) error) (
	txn types.Txn, retryTimes int, retryDetails types.RetryDetails, err error) {
	return c.DoTransactionRawFetchRetryDetails(ctx, opt, func(ctx context.Context, txn types.Txn) (err error, retry bool) {
		return f(ctx, txn), true
	}, nil, nil)
}

func (c *SmartClient) DoTransactionRawFetchRetryDetails(ctx context.Context, opt types.TxnOption, f func(ctx context.Context, txn types.Txn) (err error, retry bool),
	beforeCommit, beforeRollback func() error) (txn types.Txn, retryTimes int, retryDetails types.RetryDetails, err error) {
	retryDetails = make(types.RetryDetails)
	txn, retryTimes, err = c.DoTransactionRaw(ctx, opt, f, beforeCommit, beforeRollback, func(err error) {
		assert.Must(err != nil)
		retryDetails[errors.GetErrorKey(err)]++
	})
	if errors.GetErrorCode(err) == consts.ErrCodeTxnRetriedTooManyTimes || err == ctx.Err() {
		err = errors.Annotatef(err, "retry_details: {%s}", retryDetails)
	}
	return
}

func VoidOnRetry(err error) {}

func (c *SmartClient) DoTransactionRaw(ctx context.Context, opt types.TxnOption, f func(ctx context.Context, txn types.Txn) (err error, retry bool),
	beforeCommit, beforeRollback func() error, onRetry func(err error)) (_ types.Txn, retryTimes int, _ error) {
	for i := 0; i < c.maxRetry; i++ {
		if err := ctx.Err(); err != nil {
			return nil, i + 1, err
		}
		tx, err := c.TxnManager.BeginTransaction(ctx, opt)
		if err != nil {
			if errors.IsRetryableTransactionManagerErr(err) {
				onRetry(err)
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
			if !c.retryOnCommitFailure {
				return tx, i + 1, err
			}
			if !tx.GetState().IsAborted() && !errors.IsRetryableTransactionErr(err) {
				return tx, i + 1, err
			}
			rand.Seed(time.Now().UnixNano())
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(4)))
			onRetry(err)
			continue
		}
		if !tx.GetState().IsAborted() {
			if beforeRollback != nil {
				if err := beforeRollback(); err != nil {
					return tx, i + 1, err
				}
			}
			_ = tx.Rollback(ctx)
		}
		if !retry || !errors.IsRetryableTransactionErr(err) {
			return tx, i + 1, err
		}
		if errors.IsSnapshotReadTabletErr(err) {
			opt.SnapshotReadOption.SetSnapshotVersion(tx.GetSnapshotReadOption().SnapshotVersion, true)
		}
		time.Sleep(utils.RandomPeriod(time.Millisecond, 1, 9))
		glog.V(401).Infof("[SmartClient::DoTransactionRaw] user function returns err: %v, retrying for %dth round...", err.Error(), i+2)
		onRetry(err)
	}
	return nil, c.maxRetry, errors.Annotatef(errors.ErrTxnRetriedTooManyTimes, "after retried %d times", c.maxRetry)
}

func (c *SmartClient) Set(ctx context.Context, key string, val []byte) error {
	return c.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
		return txn.Set(ctx, key, val)
	})
}

func (c *SmartClient) Get(ctx context.Context, key string) (val types.TValue, _ error) {
	if err := c.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) (err error) {
		val, err = txn.Get(ctx, key)
		return
	}); err != nil {
		return types.EmptyTValue, err
	}
	return val, nil
}

func (c *SmartClient) MGet(ctx context.Context, keys []string, txnType types.TxnType) (values []types.TValue, _ error) {
	if _, _, err := c.DoTransactionOfTypeEx(ctx, txnType, func(ctx context.Context, txn types.Txn) (err error) {
		values, err = txn.MGet(ctx, keys)
		return
	}); err != nil {
		return nil, err
	}
	return values, nil
}

func (c *SmartClient) MSet(ctx context.Context, keys []string, values [][]byte, txnType types.TxnType) error {
	_, _, err := c.DoTransactionOfTypeEx(ctx, txnType, func(ctx context.Context, txn types.Txn) error {
		return txn.MSet(ctx, keys, values)
	})
	return err
}

func (c *SmartClient) SetInt(ctx context.Context, key string, intVal int) error {
	return c.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
		return txn.Set(ctx, key, types.NewIntValue(intVal).V)
	})
}

func (c *SmartClient) MSetInts(ctx context.Context, keys []string, values []int) error {
	vs := make([][]byte, len(values))
	for i, v := range values {
		vs[i] = types.NewIntValue(v).V
	}
	return c.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
		return txn.MSet(ctx, keys, vs)
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
