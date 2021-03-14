package transaction

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
)

func TestPriorityQueue_PushNotify(t *testing.T) {
	assert := types.NewAssertion(t)

	const (
		key1    = "k1"
		key2    = "k2"
		timeout = time.Second * 10
		txnNum  = 5
	)

	ctx := context.Background()

	tm := NewManager()
	defer tm.Close()

	txnIds := make([]int, txnNum)
	for i := 0; i < txnNum; i++ {
		txnIds[i] = i
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(txnIds), func(i, j int) {
		txnIds[i], txnIds[j] = txnIds[j], txnIds[i]
	})
	t.Logf("txn ids: %v", txnIds)

	var (
		executedTxns = make(chan types.TxnId, 10)
		wg           sync.WaitGroup
	)
	for i := 0; i < txnNum; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			for txnId := types.TxnId(txnIds[i]); ; txnId += 10 {
				cond, err := tm.PushReadForWriteReaderOnKey(key1, txnId)
				if errors.GetErrorCode(err) == consts.ErrCodeWriteReadConflict {
					t.Logf("txn %d rollbacked due to %v", txnId, err)
					continue
				}
				if !assert.NoError(err) {
					return
				}
				if cond != nil {
					if err := cond.Wait(ctx, timeout); !assert.NoError(err) {
						return
					}
				}
				time.Sleep(time.Second)
				tm.NotifyReadForWriteKeyDone(key1, txnId)
				executedTxns <- txnId
				break
			}
		}(i)
	}

	wg.Wait()
	close(executedTxns)

	executedTxnArray := make([]types.TxnId, 0, txnNum)
	for txn := range executedTxns {
		executedTxnArray = append(executedTxnArray, txn)
	}
	t.Logf("executed order: %v", executedTxnArray)
}
