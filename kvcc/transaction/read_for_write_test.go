package transaction

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

const defaultLogDir = "/tmp/spermwhale"

type ExecuteInfo struct {
	reader
	retryTimes int
}

type ExecuteInfos struct {
	infos []ExecuteInfo

	*testifyassert.Assertions
	t *testing.T
}

func NewExecuteInfos(t *testing.T, length int) ExecuteInfos {
	return ExecuteInfos{
		infos:      make([]ExecuteInfo, length),
		Assertions: types.NewAssertion(t),
		t:          t,
	}
}
func (rs ExecuteInfos) Len() int           { return len(rs.infos) }
func (rs ExecuteInfos) Swap(i, j int)      { rs.infos[i], rs.infos[j] = rs.infos[j], rs.infos[i] }
func (rs ExecuteInfos) Less(i, j int) bool { return rs.infos[i].id < rs.infos[j].id }
func (rs ExecuteInfos) AverageTryTimes() float64 {
	var totalRetryTimes int
	for _, e := range rs.infos {
		totalRetryTimes += e.retryTimes
	}
	return float64(totalRetryTimes) / float64(len(rs.infos))
}
func (rs ExecuteInfos) Check() {
	sort.Sort(rs)
	for i := 1; i < len(rs.infos); i++ {
		prev, cur := rs.infos[i-1], rs.infos[i]
		if !rs.Assertions.Less(prev.NotifyTime, cur.NotifyTime) {
			rs.t.Errorf("executed order violated: %v", rs.PrintString())
			return
		}
	}
	rs.t.Logf("Executed order: %v", rs.PrintString())
	rs.t.Logf("Average retry times: %.2f", rs.AverageTryTimes())
}
func (rs ExecuteInfos) PrintString() string {
	strs := make([]string, len(rs.infos))
	for i := range rs.infos {
		strs[i] = fmt.Sprintf("%d", rs.infos[i].id)
	}
	return strings.Join(strs, ",")
}

func TestPriorityQueue_Timeouted(t *testing.T) {
	_ = flag.Set("alsologtostderr", fmt.Sprintf("%t", true))
	testifyassert.NoError(t, utils.MkdirIfNotExists(defaultLogDir))
	_ = flag.Set("v", fmt.Sprintf("%d", 50))
	_ = flag.Set("log_dir", defaultLogDir)

	const (
		key1   = "k1"
		txnNum = 10000

		queueLength        = time.Second * 5
		queueCapacity      = 1000
		arriveIntervalUnit = queueLength / queueCapacity

		taskDuration = arriveIntervalUnit * 4
		timeout      = queueLength

		readForWriteQueueMaxReadersRatio = float64(timeout) / float64(queueLength) / 2
	)

	ctx := context.Background()
	o := physical.NewOracle()
	tm := NewManager(queueCapacity, readForWriteQueueMaxReadersRatio, time.Minute)
	defer tm.Close()

	var (
		executedTxns = NewExecuteInfos(t, txnNum)
		wg           sync.WaitGroup
	)
	for i := 0; i < txnNum; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			for retryTimes := 1; ; retryTimes++ {
				txnId := types.TxnId(o.MustFetchTimestamp())
				cond, err := tm.PushReadForWriteReaderOnKey(key1, txnId)
				if errors.GetErrorCode(err) == consts.ErrCodeWriteReadConflict || errors.GetErrorCode(err) == consts.ErrCodeReadForWriteQueueFull {
					//t.Logf("txn %d rollbacked due to %v, retrying...", txnId, err)
					rand.Seed(time.Now().UnixNano())
					time.Sleep(arriveIntervalUnit * time.Duration(1+rand.Intn(9)))
					continue
				}
				if !testifyassert.NoError(t, err) {
					return
				}
				if err := cond.Wait(ctx, timeout); err != nil {
					//t.Logf("txn %d wait timeouted, retrying...", txnId)
					tm.SignalReadForWriteKeyEvent(key1, txnId)
					rand.Seed(time.Now().UnixNano())
					time.Sleep(arriveIntervalUnit * time.Duration(1+rand.Intn(10)))
					continue
				}
				time.Sleep(taskDuration)
				tm.SignalReadForWriteKeyEvent(key1, txnId)
				executedTxns.infos[i] = ExecuteInfo{
					reader: reader{
						id:               txnId,
						readForWriteCond: *cond,
					},
					retryTimes: retryTimes,
				}
				break
			}
		}(i)
		time.Sleep(arriveIntervalUnit)
	}

	wg.Wait()

	executedTxns.Check()
}

func TestPriorityQueue_PushNotify(t *testing.T) {
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
		executedTxns = NewExecuteInfos(t, txnNum)
		wg           sync.WaitGroup
	)
	for i := 0; i < txnNum; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			for retryTimes, txnId := 1, types.TxnId(txnIds[i]); ; retryTimes, txnId = retryTimes+1, txnId+10 {
				cond, err := tm.PushReadForWriteReaderOnKey(key1, txnId)
				if errors.GetErrorCode(err) == consts.ErrCodeWriteReadConflict {
					t.Logf("txn %d rollbacked due to %v", txnId, err)
					continue
				}
				if !testifyassert.NoError(t, err) {
					return
				}
				if err := cond.Wait(ctx, timeout); !testifyassert.NoError(t, err) {
					return
				}
				time.Sleep(taskDuration)
				tm.SignalReadForWriteKeyEvent(key1, txnId)
				executedTxns.infos[i] = ExecuteInfo{
					reader: reader{
						id:               txnId,
						readForWriteCond: *cond,
					},
					retryTimes: retryTimes,
				}
				break
			}
		}(i)
	}

	wg.Wait()
	executedTxns.Check()
}

func TestPriorityQueue_HeadNonTerminate(t *testing.T) {
	_ = flag.Set("alsologtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 11))
	_ = flag.Set("log_dir", "/tmp/spw")

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
		executedTxns = NewExecuteInfos(t, txnNum)
		wg           sync.WaitGroup
	)
	for i := 0; i < txnNum; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			for retryTimes, txnId := 1, types.TxnId(txnIds[i]); ; retryTimes, txnId = retryTimes+1, txnId+10 {
				cond, err := tm.PushReadForWriteReaderOnKey(key1, txnId)
				if errors.GetErrorCode(err) == consts.ErrCodeWriteReadConflict {
					t.Logf("txn %d rollbacked due to %v, retrying...", txnId, err)
					continue
				}
				if !testifyassert.NoError(t, err) {
					return
				}
				if cond.NotifyTime > 0 {
					executedTxns.infos[i] = ExecuteInfo{
						reader: reader{
							id:               txnId,
							readForWriteCond: *cond,
						},
						retryTimes: retryTimes,
					}
					break
				}
				if err := cond.Wait(ctx, timeout); err != nil {
					t.Logf("txn %d wait timeouted, retrying...", txnId)
					tm.SignalReadForWriteKeyEvent(key1, txnId)
					continue
				}
				time.Sleep(taskDuration)
				tm.SignalReadForWriteKeyEvent(key1, txnId)
				executedTxns.infos[i] = ExecuteInfo{
					reader: reader{
						id:               txnId,
						readForWriteCond: *cond,
					},
					retryTimes: retryTimes,
				}
				break
			}
		}(i)
	}

	wg.Wait()
	executedTxns.Check()
}
