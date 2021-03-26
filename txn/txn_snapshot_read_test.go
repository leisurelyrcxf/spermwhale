package txn

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/types"
)

func TestTxnRead(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, func(ctx context.Context, ts *TestCase) bool {
		return testTxnSnapshotRead(ctx, ts, true, false, false)
	}).SetGoRoutineNum(25).SetTxnNumPerGoRoutine(2000).Run()
}

func TestTxnSnapshotRead(t *testing.T) {
	NewEmbeddedSnapshotReadTestCase(t, rounds, func(ctx context.Context, ts *TestCase) bool {
		return testTxnSnapshotRead(ctx, ts, false, false, false)
	}).SetGoRoutineNum(25).SetTxnNumPerGoRoutine(2000).Run()
}

func TestTxnSnapshotReadInteractive(t *testing.T) {
	NewEmbeddedSnapshotReadTestCase(t, rounds, func(ctx context.Context, ts *TestCase) bool {
		return testTxnSnapshotRead(ctx, ts, true, false, false)
	}).SetGoRoutineNum(25).SetTxnNumPerGoRoutine(2000).Run()
}

func TestTxnSnapshotReadInteractiveMixedMGetAndGet(t *testing.T) {
	NewEmbeddedSnapshotReadTestCase(t, rounds, func(ctx context.Context, ts *TestCase) bool {
		return testTxnSnapshotRead(ctx, ts, true, true, false)
	}).SetGoRoutineNum(25).SetTxnNumPerGoRoutine(2000).Run()
}

func TestTxnSnapshotReadInteractiveMixedGetAndMGet(t *testing.T) {
	NewEmbeddedSnapshotReadTestCase(t, rounds, func(ctx context.Context, ts *TestCase) bool {
		return testTxnSnapshotRead(ctx, ts, true, true, true)
	}).SetGoRoutineNum(25).SetTxnNumPerGoRoutine(2000).Run()
}

func testTxnSnapshotRead(ctx context.Context, ts *TestCase, interactive, mixedMGetAndGet, mgetThenGet bool) (b bool) {
	const (
		key1, key2, key3 = "k1", "k2", "k3"
	)
	var allKeys = []string{key1, key2, key3}
	sc := ts.scs[0]
	if !ts.NoError(sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
		ts.NoError(txn.Set(ctx, key1, types.NewIntValue(1).V))
		ts.NoError(txn.Set(ctx, key2, types.NewIntValue(1).V))
		ts.NoError(txn.Set(ctx, key3, types.NewIntValue(1).V))
		return nil
	})) {
		return
	}
	time.Sleep(time.Millisecond * 100) // wait write intent cleared
	var wg sync.WaitGroup
	for i := 0; i < ts.GoRoutineNum; i++ {
		if i&3 == 0 {
			ts.SetExtraRound(i)
		}

		wg.Add(1)
		go func(goRoutineIdx int) {
			defer wg.Done()

			start := time.Now()
			rounds := ts.TxnNumPerGoRoutine
			if goRoutineIdx&3 == 0 {
				rounds = rounds * 10
			}
			for j := 0; j < rounds; j++ {
				if goRoutineIdx&3 == 0 {
					ts.True(ts.DoReadOnlyTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
						if !interactive {
							_, err := txn.MGet(ctx, allKeys, ts.ReadOpt)
							return err
						}
						if !mixedMGetAndGet {
							for _, key := range allKeys {
								if _, err := txn.Get(ctx, key, ts.ReadOpt); err != nil {
									return err
								}
							}
							return nil
						}
						if mgetThenGet {
							if _, err := txn.MGet(ctx, []string{key1, key2}, ts.ReadOpt); err != nil {
								return err
							}
							_, err := txn.Get(ctx, key3, ts.ReadOpt)
							return err
						}
						if _, err := txn.Get(ctx, key1, ts.ReadOpt); err != nil {
							return err
						}
						_, err := txn.MGet(ctx, []string{key2, key3}, ts.ReadOpt)
						return err
					}))
				} else if goRoutineIdx&3 == 1 {
					ts.True(ts.DoTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
						return txn.Set(ctx, key1, types.NewIntValue(j).V)
					}))
				} else if goRoutineIdx&3 == 2 {
					ts.True(ts.DoTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
						return txn.Set(ctx, key2, types.NewIntValue(j*10).V)
					}))
				} else {
					ts.True(ts.DoTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
						return txn.Set(ctx, key3, types.NewIntValue(j*100).V)
					}))
				}
			}

			if i < 4 {
				ts.t.Logf("cost %s per round @goRoutine %d", time.Since(start)/time.Duration(rounds), goRoutineIdx)
			}
		}(i)
	}

	wg.Wait()
	return true
}
