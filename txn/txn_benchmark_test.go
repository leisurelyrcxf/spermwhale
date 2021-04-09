package txn

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/types"
)

const BenchRounds = 10

func BenchmarkTxnLostUpdate(b *testing.B) {
	b.N = BenchRounds
	start := time.Now()
	for i := 0; i < b.N; i++ {
		benchmarkTxnLostUpdate(b, false)
	}
	b.ReportMetric(float64(time.Since(start))/float64(time.Millisecond)/float64(b.N), "ms/op")
}

func BenchmarkTxnLostUpdateWaitNoWriteIntent(b *testing.B) {
	b.N = BenchRounds
	start := time.Now()
	for i := 0; i < b.N; i++ {
		benchmarkTxnLostUpdate(b, true)
	}
	b.ReportMetric(float64(time.Since(start))/float64(time.Millisecond)/float64(b.N), "ms/op")
}

func benchmarkTxnLostUpdate(b *testing.B, waitNoWriteIntent bool) (ret bool) {
	const (
		latency            = time.Millisecond * 10
		failurePattern     = FailurePatternNone
		failureProbability = 0
	)

	const (
		initialValue = 101
		goRoutineNum = 100
		delta        = 6
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	ts := NewEmbeddedTestCase(b, 1, nil).SetTxnType(types.TxnTypeReadModifyWrite.CondWaitWhenReadDirty(waitNoWriteIntent)).
		SetGoRoutineNum(goRoutineNum).
		SetSimulatedLatency(latency).SetWriteFailureProb(failurePattern).SetFailurePattern(failureProbability)
	if !ts.True(ts.GenTestEnv()) {
		return
	}
	sc := ts.scs[0]
	if err := sc.SetInt(ctx, "k1", initialValue); !ts.NoError(err) {
		return
	}
	var (
		start = time.Now()
		wg    sync.WaitGroup
	)
	for i := 0; i < ts.GoRoutineNum; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			ts.True(ts.DoTransaction(ctx, i, sc, func(ctx context.Context, txn types.Txn) error {
				val, err := txn.Get(ctx, "k1")
				if err != nil {
					return err
				}
				v1, err := val.Int()
				if !ts.NoError(err) {
					return err
				}
				return txn.Set(ctx, "k1", types.NewIntValue(v1+delta).V)
			}))
		}(i)
	}

	wg.Wait()

	val, err := sc.GetInt(ctx, "k1")
	if !ts.NoError(err) {
		return
	}
	if !ts.Less(int64(time.Since(start)), int64(time.Second*5)) {
		return
	}
	if !ts.Equal(ts.GoRoutineNum*delta+initialValue, val) {
		return
	}
	if !ts.True(ts.CheckSerializability()) {
		return
	}
	ts.LogExecuteInfos(float64(time.Since(start)) / float64(time.Second))
	return ts.CheckReadModifyWriteOnly("k1")
}
