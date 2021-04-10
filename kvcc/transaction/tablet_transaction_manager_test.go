package transaction

import (
	"sync"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/utils"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"
	"github.com/leisurelyrcxf/spermwhale/testutils"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/basic"
)

func (tm *Manager) removeTxnById(txnId types.TxnId) (alreadyRemoved bool) {
	txn, err := tm.GetTxn(txnId)
	if err != nil {
		// already removed
		return true
	}
	tm.removeTxn(txn)
	return false
}

func (tm *Manager) removeTxnPrimitive(txn *Transaction) {
	tm.writeTxns.RemoveWhen(txn.ID, txn.ID.After(tm.cfg.TxnLifeSpan))
}

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
	defer tm.Close()

	var (
		assert = types.NewAssertion(t)

		txnObjectsCount      basic.AtomicInt32
		insertTxnIfNotExists = func(id types.TxnId) (inserted bool, txn *Transaction) {
			inserted, obj := tm.writeTxns.InsertIfNotExists(id, func() interface{} {
				txn := newTransaction(id, nil, func(transaction *Transaction) {
					tm.removeTxnPrimitive(transaction)
				})
				txnObjectsCount.Add(1)
				return txn
			})
			return inserted, obj.(*Transaction)
		}

		wg sync.WaitGroup
	)

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			insertTxnIfNotExists(txnIds[i&1])
		}(i)
	}

	wg.Wait()
	return assert.Equal(int32(2), txnObjectsCount.Get())
}

func TestManagerGC(t *testing.T) {
	testutils.RunTestForNRounds(t, 1, testManagerGC)
}

func testManagerGC(t types.T) (b bool) {
	cfg := types.TestTableTxnManagerCfg
	cfg.TxnLifeSpan = 100 * time.Millisecond
	var (
		oracle = physical.NewOracle()
		txnIds = []types.TxnId{types.TxnId(oracle.MustFetchTimestamp()), types.TxnId(oracle.MustFetchTimestamp())}
		tm     = NewManager(cfg, nil)
	)
	if !testifyassert.NotEqual(t, txnIds[0], txnIds[1]) {
		return false
	}
	defer tm.Close()

	var (
		assert = types.NewAssertion(t)

		txnObjectsCount      basic.AtomicInt32
		insertTxnIfNotExists = func(id types.TxnId, idx int) {
			tm.writeTxns.InsertIfNotExists(id, func() interface{} {
				if utils.IsTooOld(id.Version(), tm.cfg.TxnLifeSpan) {
					return nil
				}
				if idx > 10 {
					time.Sleep(time.Second)
				}
				txn := newTransaction(id, nil, func(transaction *Transaction) {
					tm.removeTxnPrimitive(transaction)
				})
				txnObjectsCount.Add(1)
				txn.unref(txn)
				return txn
			})
		}

		wg sync.WaitGroup
	)

	for i := 0; i < 10000000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			insertTxnIfNotExists(txnIds[i&1], i)
		}(i)
	}

	wg.Wait()
	return assert.Equal(int32(2), txnObjectsCount.Get())
}
