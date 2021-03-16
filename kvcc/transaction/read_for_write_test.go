package transaction

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

const defaultLogDir = "/tmp/spermwhale"

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

func TestPriorityQueue_Timeouted(t *testing.T) {
	//_ = flag.Set("alsologtostderr", fmt.Sprintf("%t", true))
	testifyassert.NoError(t, utils.MkdirIfNotExists(defaultLogDir))
	_ = flag.Set("v", fmt.Sprintf("%d", 11))
	_ = flag.Set("log_dir", defaultLogDir)

	assert := types.NewAssertion(t)

	const (
		key1         = "k1"
		txnNum       = 500
		taskDuration = time.Millisecond * 100
		txnIdDelta   = 1000
	)
	if !assert.Greater(txnIdDelta, txnNum) {
		return
	}

	ctx := context.Background()

	tm := NewManager()
	defer tm.Close()

	txnIds := make([]int, txnNum)
	for i := 0; i < txnNum; i++ {
		txnIds[i] = i + 1
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(txnIds), func(i, j int) {
		txnIds[i], txnIds[j] = txnIds[j], txnIds[i]
	})

	var (
		executedTxns = make(chan types.TxnId, 10)
		wg           sync.WaitGroup
	)
	for i := 0; i < txnNum; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			for txnId := types.TxnId(txnIds[i]); ; txnId += txnIdDelta {
				cond, err := tm.PushReadForWriteReaderOnKey(key1, txnId)
				if errors.GetErrorCode(err) == consts.ErrCodeWriteReadConflict || errors.GetErrorCode(err) == consts.ErrCodeReadForWriteQueueFull {
					//t.Logf("txn %d rollbacked due to %v, retrying...", txnId, err)
					//time.Sleep(time.Millisecond)
					continue
				}
				if !assert.NoError(err) {
					return
				}
				if cond != nil {
					if err := cond.Wait(ctx, taskDuration); err != nil {
						//t.Logf("txn %d wait timeouted, retrying...", txnId)
						tm.NotifyReadForWriteKeyDone(key1, txnId)
						//time.Sleep(time.Millisecond)
						continue
					}
				}
				time.Sleep(taskDuration)
				executedTxns <- txnId
				tm.NotifyReadForWriteKeyDone(key1, txnId)
				break
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(executedTxns)
	}()

	executedTxnArray := make([]types.TxnId, 0, txnNum)
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)

		for txn := range executedTxns {
			executedTxnArray = append(executedTxnArray, txn)
		}
	}()
	<-collectorDone

	for i := 1; i < len(executedTxnArray); i++ {
		prev, cur := executedTxnArray[i-1], executedTxnArray[i]
		if !assert.Less(prev.Version(), cur.Version()) {
			t.Errorf("executed order violated: %v", executedTxnArray)
			return
		}
	}
	t.Logf("executed order: %v", executedTxnArray)
}

func TestPriorityQueue_HeadNonTerminate(t *testing.T) {
	_ = flag.Set("alsologtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 11))
	_ = flag.Set("log_dir", "/tmp/spw")
	assert := types.NewAssertion(t)

	const (
		key1    = "k1"
		key2    = "k2"
		timeout = time.Second * 4
		txnNum  = 5
	)

	ctx := context.Background()

	tm := NewManager()
	defer tm.Close()

	txnIds := make([]int, txnNum)
	for i := 0; i < txnNum; i++ {
		txnIds[i] = i + 1
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
					t.Logf("txn %d rollbacked due to %v, retrying...", txnId, err)
					continue
				}
				if !assert.NoError(err) {
					return
				}
				if cond != nil {
					if err := cond.Wait(ctx, timeout); err != nil {
						t.Logf("txn %d wait timeouted, retrying...", txnId)
						tm.NotifyReadForWriteKeyDone(key1, txnId)
						continue
					}
				} else {
					return
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
