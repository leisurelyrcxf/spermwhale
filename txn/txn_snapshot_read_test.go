package txn

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/utils"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"

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

func TestTxnSnapshotReadInteractiveWaitWhenReadDirty(t *testing.T) {
	NewEmbeddedSnapshotReadTestCase(t, rounds, func(ctx context.Context, ts *TestCase) bool {
		return testTxnSnapshotRead(ctx, ts, true, false, false)
	}).AddReadOnlyTxnType(types.TxnTypeWaitWhenReadDirty).SetGoRoutineNum(25).SetTxnNumPerGoRoutine(2000).Run()
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

func TestTxnSnapshotReadWaitWhenReadDirtyMoreKeys(t *testing.T) {
	NewEmbeddedSnapshotReadTestCase(t, rounds, func(ctx context.Context, ts *TestCase) bool {
		return testTxnSnapshotReadMoreKeys(ctx, ts, false, false, false)
	}).AddReadOnlyTxnType(types.TxnTypeWaitWhenReadDirty).SetGoRoutineNum(25).SetTxnNumPerGoRoutine(2000).Run()
}

func TestTxnSnapshotReadInteractiveWaitWhenReadDirtyMoreKeys(t *testing.T) {
	NewEmbeddedSnapshotReadTestCase(t, rounds, func(ctx context.Context, ts *TestCase) bool {
		return testTxnSnapshotReadMoreKeys(ctx, ts, true, false, false)
	}).AddReadOnlyTxnType(types.TxnTypeWaitWhenReadDirty).SetGoRoutineNum(25).SetTxnNumPerGoRoutine(2000).Run()
}

func TestTxnSnapshotReadInteractiveWriteIndex(t *testing.T) {
	NewEmbeddedSnapshotReadTestCase(t, rounds, func(ctx context.Context, ts *TestCase) bool {
		return testTxnSnapshotReadInteractiveWriteIndex(ctx, ts)
	}).SetGoRoutineNum(25).SetTxnNumPerGoRoutine(2000).Run()
}

func TestTxnSnapshotReadInteractiveWriteIndexWaitWhenReadDirty(t *testing.T) {
	NewEmbeddedSnapshotReadTestCase(t, rounds, func(ctx context.Context, ts *TestCase) bool {
		return testTxnSnapshotReadInteractiveWriteIndex(ctx, ts)
	}).AddReadOnlyTxnType(types.TxnTypeWaitWhenReadDirty).SetGoRoutineNum(25).SetTxnNumPerGoRoutine(2000).Run()
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
							_, err := txn.MGet(ctx, allKeys)
							return err
						}
						if !mixedMGetAndGet {
							for _, key := range allKeys {
								if _, err := txn.Get(ctx, key); err != nil {
									return err
								}
							}
							return nil
						}
						if mgetThenGet {
							if _, err := txn.MGet(ctx, []string{key1, key2}); err != nil {
								return err
							}
							_, err := txn.Get(ctx, key3)
							return err
						}
						if _, err := txn.Get(ctx, key1); err != nil {
							return err
						}
						_, err := txn.MGet(ctx, []string{key2, key3})
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

func testTxnSnapshotReadMoreKeys(ctx context.Context, ts *TestCase, interactive, mixedMGetAndGet, mgetThenGet bool) (b bool) {
	const (
		key1, key2, key3, key4, key5, key6 = "k1", "k2", "k3", "k4", "k5", "k6"
	)
	var allKeys = []string{key1, key2, key3, key4, key5, key6}
	sc := ts.scs[0]
	if !ts.NoError(sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
		for _, key := range allKeys {
			if err := txn.Set(ctx, key, types.NewIntValue(1).V); !ts.NoError(err) {
				return err
			}
		}
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
							_, err := txn.MGet(ctx, allKeys)
							return err
						}
						if !mixedMGetAndGet {
							for _, key := range allKeys {
								if _, err := txn.Get(ctx, key); err != nil {
									return err
								}
							}
							return nil
						}
						if mgetThenGet {
							if _, err := txn.MGet(ctx, []string{key1, key2, key3}); err != nil {
								return err
							}
							if _, err := txn.Get(ctx, key4); err != nil {
								return err
							}
							if _, err := txn.MGet(ctx, []string{key5, key6}); err != nil {
								return err
							}
							return nil
						}
						if _, err := txn.Get(ctx, key1); err != nil {
							return err
						}
						_, err := txn.MGet(ctx, []string{key2, key3, key4, key5, key6})
						return err
					}))
				} else if goRoutineIdx&3 == 1 {
					ts.True(ts.DoTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
						if err := txn.Set(ctx, key1, types.NewIntValue(j).V); err != nil {
							return err
						}
						return txn.Set(ctx, key2, types.NewIntValue(j).V)
					}))
				} else if goRoutineIdx&3 == 2 {
					ts.True(ts.DoTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
						if err := txn.Set(ctx, key3, types.NewIntValue(j*10).V); err != nil {
							return err
						}
						return txn.Set(ctx, key4, types.NewIntValue(j).V)
					}))
				} else {
					ts.True(ts.DoTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
						if err := txn.Set(ctx, key5, types.NewIntValue(j*10).V); err != nil {
							return err
						}
						return txn.Set(ctx, key6, types.NewIntValue(j).V)
					}))
				}
			}

			if i < 4 {
				ts.t.Logf("%s cost %s per round @goRoutine %d", ts.t.Name(), time.Since(start)/time.Duration(rounds), goRoutineIdx)
			}
		}(i)
	}

	wg.Wait()
	return true
}

type IndexValue struct {
	IndexKey string
	Val      int
}

func NewIndexValue(indexKey string, val int) []byte {
	bytes, err := json.Marshal(IndexValue{
		IndexKey: indexKey,
		Val:      val,
	})
	assert.MustNoError(err)
	return bytes
}

func testTxnSnapshotReadInteractiveWriteIndex(ctx context.Context, ts *TestCase) (b bool) {
	const (
		key1 = "kkk"
	)
	sc := ts.scs[0]
	var (
		firstKeyWritten     = make(chan struct{})
		firstKeyWrittenOnce sync.Once
	)
	time.Sleep(time.Millisecond * 100) // wait write intent cleared
	var wg sync.WaitGroup
	for i := 0; i < ts.GoRoutineNum; i++ {
		if i&1 == 0 {
			ts.SetExtraRound(i)
		}

		wg.Add(1)
		go func(goRoutineIdx int) {
			defer wg.Done()

			start := time.Now()
			rounds := ts.TxnNumPerGoRoutine
			if goRoutineIdx&1 == 0 {
				rounds = rounds * 10
			}
			for j := 0; j < rounds; j++ {
				if goRoutineIdx&1 == 0 {
					<-firstKeyWritten
					ts.True(ts.DoReadOnlyTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
						v1, err := txn.Get(ctx, key1)
						if err != nil {
							return err
						}
						var s1 IndexValue
						if err := json.Unmarshal(v1.V, &s1); !ts.NoError(err) {
							return err
						}
						key2 := s1.IndexKey
						if !ts.NotEmpty(key2) {
							return errors.ErrAssertFailed
						}
						v2, err := txn.Get(ctx, key2)
						if err != nil {
							return err
						}
						var s2 IndexValue
						if err := json.Unmarshal(v2.V, &s2); !ts.NoError(err) {
							return err
						}
						key3 := s2.IndexKey
						if !ts.NotEmpty(key3) {
							return errors.ErrAssertFailed
						}
						v3, err := txn.Get(ctx, key3)
						if err != nil {
							return err
						}
						var s3 IndexValue
						if err := json.Unmarshal(v3.V, &s3); !ts.NoError(err) {
							return err
						}
						ts.Empty(s3.IndexKey)
						ts.Equal(s1.Val*10, s2.Val)
						ts.Equal(s2.Val*10, s3.Val)
						//ts.t.Logf("%s: %d, %s: %d, %s: %d", key1, s1.Val, key2, s2.Val, key3, s3.Val)
						return nil
					}))
				} else if goRoutineIdx&1 == 1 {
					ts.True(ts.DoTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
						rand.Seed(time.Now().UnixNano())
						choice := rand.Intn(1000)
						k3 := utils.RandomKey(3)
						if err := txn.Set(ctx, k3, NewIndexValue("", choice*100)); err != nil {
							return err
						}
						k2 := utils.RandomKey(4)
						if err := txn.Set(ctx, k2, NewIndexValue(k3, choice*10)); err != nil {
							return err
						}
						if err := txn.Set(ctx, key1, NewIndexValue(k2, choice)); err != nil {
							return err
						}
						firstKeyWrittenOnce.Do(func() {
							time.Sleep(time.Millisecond * 100)
							close(firstKeyWritten)
						})
						return nil
					}))
				}
			}

			if i < 4 {
				ts.t.Logf("%s cost %s per round @goRoutine %d", ts.t.Name(), time.Since(start)/time.Duration(rounds), goRoutineIdx)
			}
		}(i)
	}

	wg.Wait()
	return true
}
