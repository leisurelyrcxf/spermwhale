package integration_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/types"

	"github.com/leisurelyrcxf/spermwhale/txn/smart_txn_client"

	testifyassert "github.com/stretchr/testify/assert"

	integration "github.com/leisurelyrcxf/spermwhale/integration_test"
)

func TestTransaction(t *testing.T) {
	testifyassert.True(t, testTransaction(t))
}

func testTransaction(t *testing.T) (b bool) {
	ts := integration.NewTestSuite(t)
	if !testifyassert.NotNil(t, ts) {
		return
	}
	defer ts.CloseWithResult(&b)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	const (
		goRoutineNumber = 10
		delta           = 6
	)
	for i := 0; i < goRoutineNumber; i++ {
		go func() {
			if !ts.NoError(ts.TxnClient.DoTransaction(ctx, func(ctx context.Context, txn *smart_txn_client.Txn) error {
				val, err := txn.Get(ctx, "k1")
				if err != nil {
					return err
				}
				v1, err := val.Int()
				if !ts.NoError(err) {
					return err
				}
				v1 += delta

				if err := txn.Set(ctx, "k1", types.IntValue(v1).V); err != nil {
					return err
				}
				return txn.Commit(ctx)
			})) {
				return
			}
		}()
	}

	val, err := ts.KVClient.Get(ctx, "k1", types.NewReadOption(math.MaxUint64))
	if !ts.NoError(err) {
		return
	}
	return ts.Equal(goRoutineNumber*delta, val.MustInt())
}
