package txn

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"testing"
	"time"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/leisurelyrcxf/spermwhale/mvcc/impl/memory"
	"github.com/leisurelyrcxf/spermwhale/tablet"
	"github.com/leisurelyrcxf/spermwhale/txn/smart_txn_client"
	"github.com/leisurelyrcxf/spermwhale/types"
)

var defaultTxnConfig = types.TxnConfig{
	StaleWriteThreshold: time.Millisecond * 5,
	MaxClockDrift:       time.Millisecond,
}

func TestTxn(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 5))

	for _, threshold := range []int{5, 10, 100, 1000} {
		for i := 0; i < 100; i++ {
			if !testifyassert.True(t, testTxn(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestTxn failed @round %d", i)
				return
			}
		}
	}
}

func testTxn(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testTxn @round %d", round)

	db := memory.NewDB()
	kvcc := tablet.NewKVCCForTesting(db, defaultTxnConfig.SetStaleWriteThreshold(staleWriteThreshold))
	m := NewTransactionManager(kvcc, defaultTxnConfig, 10)
	sc := smart_txn_client.NewSmartClient(m)
	assert := testifyassert.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	const (
		initialValue    = 101
		goRoutineNumber = 10000
		delta           = 6
	)
	err := sc.SetInt(ctx, "k1", initialValue)
	if !assert.NoError(err) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < goRoutineNumber; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			assert.NoError(sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
				val, err := txn.Get(ctx, "k1")
				if err != nil {
					return err
				}
				v1, err := val.Int()
				if !assert.NoError(err) {
					return err
				}
				v1 += delta

				return txn.Set(ctx, "k1", types.IntValue(v1).V)
			}))
		}()
	}

	wg.Wait()
	val, err := sc.GetInt(ctx, "k1")
	if !assert.NoError(err) {
		return
	}
	t.Logf("val: %d", val)
	if !assert.Equal(goRoutineNumber*delta+initialValue, val) {
		return
	}

	return true
}
