package transaction

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"

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
		queueCapacity = 100
		key1          = "k1"
		timeout       = time.Second * 10
		taskDuration  = time.Second
		txnNum        = 5
	)

	ctx := context.Background()

	tm := NewManager(queueCapacity, consts.ReadForWriteQueueMaxReadersRatio, time.Minute)
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
		executedTxns = make(readers, txnNum)
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
				if err := cond.Wait(ctx, timeout); !assert.NoError(err) {
					return
				}
				time.Sleep(taskDuration)
				tm.NotifyReadForWriteKeyDone(key1, txnId)
				executedTxns[i] = &reader{
					id:               txnId,
					readForWriteCond: *cond,
				}
				break
			}
		}(i)
	}

	wg.Wait()

	sort.Sort(executedTxns)
	for i := 1; i < len(executedTxns); i++ {
		prev, cur := executedTxns[i-1], executedTxns[i]
		if !assert.Less(prev.notifyTime, cur.notifyTime) {
			t.Errorf("executed order violated: %v", executedTxns.PrintString())
			return
		}
	}
	t.Logf("executed order: %v", executedTxns.PrintString())
}

func TestPriorityQueue_Timeouted(t *testing.T) {
	_ = flag.Set("alsologtostderr", fmt.Sprintf("%t", true))
	testifyassert.NoError(t, utils.MkdirIfNotExists(defaultLogDir))
	_ = flag.Set("v", fmt.Sprintf("%d", 110))
	_ = flag.Set("log_dir", defaultLogDir)

	assert := types.NewAssertion(t)

	const (
		queueCapacity                    = 50
		arriveInterval                   = time.Second * 5 / queueCapacity
		readForWriteQueueMaxReadersRatio = 0.5

		key1         = "k1"
		txnNum       = 500
		taskDuration = time.Millisecond * 100
		timeout      = taskDuration * 10
	)

	ctx := context.Background()

	o := physical.NewOracle()
	tm := NewManager(queueCapacity, readForWriteQueueMaxReadersRatio, time.Minute)
	defer tm.Close()

	var (
		executedTxns = make(readers, txnNum)
		wg           sync.WaitGroup
	)
	for i := 0; i < txnNum; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			for {
				txnId := types.TxnId(o.MustFetchTimestamp())
				cond, err := tm.PushReadForWriteReaderOnKey(key1, txnId)
				if errors.GetErrorCode(err) == consts.ErrCodeWriteReadConflict || errors.GetErrorCode(err) == consts.ErrCodeReadForWriteQueueFull {
					//t.Logf("txn %d rollbacked due to %v, retrying...", txnId, err)
					time.Sleep(arriveInterval)
					continue
				}
				if !assert.NoError(err) {
					return
				}
				if err := cond.Wait(ctx, timeout); err != nil {
					//t.Logf("txn %d wait timeouted, retrying...", txnId)
					tm.NotifyReadForWriteKeyDone(key1, txnId)
					time.Sleep(arriveInterval)
					continue
				}
				time.Sleep(taskDuration)
				tm.NotifyReadForWriteKeyDone(key1, txnId)
				executedTxns[i] = &reader{
					id:               txnId,
					readForWriteCond: *cond,
				}
				break
			}
		}(i)
	}

	wg.Wait()

	sort.Sort(executedTxns)
	for i := 1; i < len(executedTxns); i++ {
		prev, cur := executedTxns[i-1], executedTxns[i]
		if !assert.Less(prev.notifyTime, cur.notifyTime) {
			t.Errorf("executed order violated: %v", executedTxns.PrintString())
			return
		}
	}
	t.Logf("executed order: %v", executedTxns.PrintString())
}

func TestPriorityQueue_HeadNonTerminate(t *testing.T) {
	_ = flag.Set("alsologtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 11))
	_ = flag.Set("log_dir", "/tmp/spw")
	assert := types.NewAssertion(t)

	const (
		queueCapacity = 100
		key1          = "k1"
		timeout       = time.Second * 5
		taskDuration  = time.Second
		txnNum        = 5
	)

	ctx := context.Background()

	tm := NewManager(queueCapacity, consts.ReadForWriteQueueMaxReadersRatio, timeout)
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
		executedTxns = make(readers, txnNum)
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
				if cond.notifyTime > 0 {
					executedTxns[i] = &reader{
						id:               txnId,
						readForWriteCond: *cond,
					}
					break
				}
				if err := cond.Wait(ctx, timeout); err != nil {
					t.Logf("txn %d wait timeouted, retrying...", txnId)
					tm.NotifyReadForWriteKeyDone(key1, txnId)
					continue
				}
				time.Sleep(taskDuration)
				tm.NotifyReadForWriteKeyDone(key1, txnId)
				executedTxns[i] = &reader{
					id:               txnId,
					readForWriteCond: *cond,
				}
				break
			}
		}(i)
	}

	wg.Wait()

	sort.Sort(executedTxns)
	for i := 1; i < len(executedTxns); i++ {
		prev, cur := executedTxns[i-1], executedTxns[i]
		if !assert.Less(prev.notifyTime, cur.notifyTime) {
			t.Errorf("executed order violated: %v", executedTxns.PrintString())
			return
		}
	}
	t.Logf("executed order: %v", executedTxns.PrintString())
}
