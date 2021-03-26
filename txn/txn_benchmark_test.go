package txn

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/kvcc"
	"github.com/leisurelyrcxf/spermwhale/txn/smart_txn_client"
	"github.com/leisurelyrcxf/spermwhale/types"
)

func BenchmarkTxnLostUpdate(b *testing.B) {
	b.N = 100
	start := time.Now()
	for i := 0; i < b.N; i++ {
		benchmarkTxnLostUpdate(b, false)
	}
	b.ReportMetric(float64(time.Since(start))/float64(time.Millisecond)/float64(b.N), "ms/op")
}

func BenchmarkTxnLostUpdateWaitNoWriteIntent(b *testing.B) {
	b.N = 100
	start := time.Now()
	for i := 0; i < b.N; i++ {
		benchmarkTxnLostUpdate(b, true)
	}
	b.ReportMetric(float64(time.Since(start))/float64(time.Millisecond)/float64(b.N), "ms/op")
}

func benchmarkTxnLostUpdate(b *testing.B, waitNoWriteIntent bool) (ret bool) {
	const (
		staleWriteThreshold = time.Second
		initialValue        = 101
		goRoutineNumber     = 100
		delta               = 6
	)

	start := time.Now()
	kvc := kvcc.NewKVCCForTesting(newTestMemoryDB(time.Millisecond*10, FailurePatternNone, 0), defaultTabletTxnConfig.WithStaleWriteThreshold(staleWriteThreshold))
	tm := NewTransactionManager(kvc, defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold)).SetRecordValuesTxn(true)
	sc := smart_txn_client.NewSmartClient(tm, 0)
	defer sc.Close()
	assert := types.NewAssertion(b)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	err := sc.SetInt(ctx, "k1", initialValue)
	if !assert.NoError(err) {
		return
	}
	txns := make(ExecuteInfos, goRoutineNumber)
	var wg sync.WaitGroup
	for i := 0; i < goRoutineNumber; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			var (
				readOpt = types.NewTxnReadOption()
			)
			if waitNoWriteIntent {
				readOpt = readOpt.WithWaitNoWriteIntent()
			}
			if tx, _, err := sc.DoTransactionOfTypeEx(ctx, types.TxnTypeReadForWrite, func(ctx context.Context, txn types.Txn) error {
				val, err := txn.Get(ctx, "k1", readOpt)
				if err != nil {
					return err
				}
				v1, err := val.Int()
				if !assert.NoError(err) {
					return err
				}
				return txn.Set(ctx, "k1", types.NewIntValue(v1+delta).V)
			}); assert.NoError(err) {
				txns[i] = NewExecuteInfo(tx, 0, 0)
			}
		}(i)
	}

	wg.Wait()
	val, err := sc.GetInt(ctx, "k1")
	if !assert.NoError(err) {
		return
	}
	b.Logf("val: %d, cost: %s", val, time.Since(start))
	if !assert.Equal(goRoutineNumber*delta+initialValue, val) {
		return
	}
	return txns.CheckReadForWriteOnly(assert, "k1")
}
