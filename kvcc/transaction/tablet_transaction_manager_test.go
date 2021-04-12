package transaction

import (
	"flag"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"
	"github.com/leisurelyrcxf/spermwhale/testutils"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

func TestInvalidWaiters(t *testing.T) {
	assert := types.NewAssertion(t)

	assert.True(invalidKeyWaiters != nil)
	assert.True(isInvalidKeyWaiters(invalidKeyWaiters))
	assert.True(isInvalidKeyWaiters([]*KeyEventWaiter{}))
	assert.True(!isInvalidKeyWaiters(nil))
}

const (
	rounds = 100
)

func TestManagerInsert(t *testing.T) {
	testutils.RunTestForNRounds(t, rounds, testManagerInsert)
}

func testManagerInsert(t types.T) (b bool) {
	var (
		txnIds = []types.TxnId{types.TxnId(1111), types.TxnId(2222)}
		tm     = NewManager(types.TestTableTxnManagerCfg, nil)
	)
	tm.cfg.TxnInsertThreshold = math.MaxInt64
	defer tm.Close()

	var (
		assert = types.NewAssertion(t)
		wg     sync.WaitGroup
	)

	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			_, _, err := tm.InsertTxnIfNotExists(txnIds[i&1])
			assert.NoError(err)
		}(i)
	}

	wg.Wait()
	if !assert.Equal(int64(2), tm.writeTxns.TotalTransactionInserted.Get()) {
		return
	}
	if !assert.Equal(int64(2), tm.writeTxns.ContainedTransactionCount.Get()) {
		return
	}
	return true
}

func TestManagerGC(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", "10000")
	testutils.RunTestForNRounds(t, 100, testManagerGC)
}

func testManagerGC(t types.T) (b bool) {
	cfg := types.TestTableTxnManagerCfg
	cfg.MinGCThreadMinInterrupt = 0
	cfg.TxnLifeSpan = 100 * time.Millisecond
	cfg.TxnInsertThreshold = cfg.TxnLifeSpan
	var (
		oracle = physical.NewOracle()
		tm     = NewManager(cfg, nil)
		txnIds = []types.TxnId{types.TxnId(oracle.MustFetchTimestamp()), types.TxnId(oracle.MustFetchTimestamp())}
	)
	if !testifyassert.True(t, !utils.IsTooOld(txnIds[0].Version(), tm.cfg.TxnInsertThreshold)) {
		return false
	}
	if !testifyassert.NotEqual(t, txnIds[0], txnIds[1]) {
		return false
	}
	defer tm.Close()

	var (
		assert = types.NewAssertion(t)

		insertTxnIfNotExists = func(id types.TxnId, idx int) (inserted bool, txn *Transaction, err error) {
			inserted, obj := tm.writeTxns.InsertIfNotExists(id, func() interface{} {
				if utils.IsTooOld(id.Version(), tm.cfg.TxnInsertThreshold) {
					return nil
				}
				{
					// different
					if idx > 10 {
						time.Sleep(time.Millisecond * 100)
					}
				}
				return tm.newTransaction(id)
			})
			if obj == nil {
				return false, nil, errors.ErrStaleWriteInsertTooOldTxn
			}
			return inserted, obj.(*Transaction), nil
		}

		wg sync.WaitGroup
	)

	for i := 0; i < 1000000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			if inserted, txn, _ := insertTxnIfNotExists(txnIds[i&1], i); inserted {
				wg.Add(1)
				go func() {
					defer wg.Done()

					time.Sleep(time.Millisecond * 10)
					txn.SetAborted("test no txn will inserted after %s", "removed")
				}()
			}
		}(i)
	}

	wg.Wait()
	if !assert.LessOrEqual(tm.writeTxns.TotalTransactionInserted.Get(), int64(2)) {
		return
	}
	if !assert.Equal(int64(0), tm.writeTxns.AliveTransactionCount.Get()) {
		return
	}
	for i, count := 0, tm.writeTxns.ContainedTransactionCount.Get(); i < 1000 && count > 0; i, count = i+1, tm.writeTxns.ContainedTransactionCount.Get() {
		t.Logf("ContainedTransactionCount: %d", count)
		time.Sleep(time.Millisecond)
	}
	if !assert.Equal(int64(0), tm.writeTxns.ContainedTransactionCount.Get()) {
		return
	}
	return true
}
