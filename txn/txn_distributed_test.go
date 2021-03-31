package txn

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/txn/smart_txn_client"
	"github.com/leisurelyrcxf/spermwhale/types"
	testifyassert "github.com/stretchr/testify/assert"
)

func TestDistributedTxnLostUpdate(t *testing.T) {
	NewTestCase(t, rounds, testDistributedTxnLostUpdate).Run()
}

func TestDistributedTxnReadConsistency(t *testing.T) {
	NewTestCase(t, rounds, testDistributedTxnReadConsistency).Run()
}

func TestDistributedTxnReadConsistencySnapshotRead(t *testing.T) {
	NewTestCase(t, rounds, testDistributedTxnReadConsistency).SetReadOnlyTxnType(types.TxnTypeSnapshotRead).Run()
}

func TestDistributedTxnReadConsistencyDeadlock(t *testing.T) {
	NewTestCase(t, rounds, testDistributedTxnReadConsistencyDeadlock).Run()
}
func TestDistributedTxnReadConsistencyDeadlockReadModifyWriteWaitWhenReadDirty(t *testing.T) {
	NewTestCase(t, rounds, testDistributedTxnReadConsistencyDeadlock).SetTxnType(types.TxnTypeReadModifyWrite | types.TxnTypeWaitWhenReadDirty).SetLogLevel(10).Run()
}

func TestDistributedTxnWriteSkew(t *testing.T) {
	NewTestCase(t, rounds, testDistributedTxnWriteSkew).Run()
}

func TestDistributedTxnExtraWriteSimple(t *testing.T) {
	NewTestCase(t, rounds, testDistributedTxnExtraWriteSimple).Run()
}

func TestDistributedTxnExtraWriteComplex(t *testing.T) {
	NewTestCase(t, rounds, testDistributedTxnExtraWriteComplex).Run()
}

func testDistributedTxnLostUpdate(ctx context.Context, ts *TestCase) (b bool) {
	const (
		key          = "k1"
		initialValue = 101
		delta        = 6
	)
	sc1 := ts.scs[0]
	if err := sc1.SetInt(ctx, key, initialValue); !ts.NoError(err) {
		return
	}
	var wg sync.WaitGroup
	for i := 0; i < ts.GoRoutineNum; i++ {
		wg.Add(1)

		go func(goRoutineIndex int) {
			defer wg.Done()

			for i := 0; i < ts.TxnNumPerGoRoutine; i++ {
				ts.DoTransaction(ctx, goRoutineIndex, ts.scs[0], func(ctx context.Context, txn types.Txn) error {
					val, err := txn.Get(ctx, key)
					if err != nil {
						return err
					}
					v1, err := val.Int()
					if !ts.NoError(err) {
						return err
					}
					return txn.Set(ctx, key, types.NewIntValue(v1+delta).V)
				})
			}
		}(i)
	}

	wg.Wait()
	val, err := ts.scs[1].GetInt(ctx, "k1")
	if !ts.NoError(err) {
		return
	}
	ts.t.Logf("val: %d", val)
	if !ts.Equal(ts.GoRoutineNum*ts.TxnNumPerGoRoutine*delta+initialValue, val) {
		return
	}
	return true
}

func testDistributedTxnReadConsistency(ctx context.Context, ts *TestCase) (b bool) {
	const (
		delta                          = 6
		key1, key2                     = "k1", "k22"
		k1InitialValue, k2InitialValue = 100, 200
		valueSum                       = k1InitialValue + k2InitialValue
	)
	sc1, sc2 := ts.scs[0], ts.scs[1]
	if err := sc1.SetInt(ctx, key1, k1InitialValue); !ts.NoError(err) {
		return
	}
	if val, err := sc1.GetInt(ctx, key1); !ts.NoError(err) || !ts.Equal(k1InitialValue, val) {
		return
	}
	if err := sc1.SetInt(ctx, key2, k2InitialValue); !ts.NoError(err) {
		return
	}
	if val, err := sc1.GetInt(ctx, key2); !ts.NoError(err) || !ts.Equal(k2InitialValue, val) {
		return
	}

	ts.SetExtraRound(0)
	var wg sync.WaitGroup
	for i := 0; i < ts.GoRoutineNum; i++ {
		wg.Add(1)

		go func(goRoutineIndex int) {
			defer wg.Done()

			start := time.Now()
			rounds := ts.TxnNumPerGoRoutine
			if goRoutineIndex == 0 {
				rounds *= 10
			}
			for round := 0; round < rounds; round++ {
				if goRoutineIndex == 0 {
					var (
						values  []types.Value
						readTxn types.Txn
						err     error
					)
					if ts.True(ts.DoReadOnlyTransaction(ctx, goRoutineIndex, sc1, func(ctx context.Context, txn types.Txn) (err error) {
						if values, err = txn.MGet(ctx, []string{key1, key2, key1, key2, key1, key2}); err == nil {
							readTxn = txn
						}
						return
					})); ts.NoError(err) && ts.Len(values, 6) {
						txn := ts.executedTxnsPerGoRoutine[goRoutineIndex][len(ts.executedTxnsPerGoRoutine[goRoutineIndex])-1]
						ts.Equal(readTxn.GetId(), txn.GetId())
						if !readTxn.GetType().IsSnapshotRead() {
							continue
						}
						var ints = make([]int, len(values))
						for idx, val := range values {
							ts.True(!val.IsDirty())
							ts.Equal(types.TxnInternalVersion(1), val.InternalVersion)
							ts.Equal(txn.GetSnapshotReadOption().SnapshotVersion, val.SnapshotVersion)
							x, err := val.Int()
							if !ts.NoError(err) {
								return
							}
							ints[idx] = x
						}
						ts.Equal(values[0].Version, values[2].Version)
						ts.Equal(values[2].Version, values[4].Version)
						ts.Equal(values[0].Flag, values[2].Flag)
						ts.Equal(values[2].Flag, values[4].Flag)
						ts.Equal(values[1].Version, values[3].Version)
						ts.Equal(values[3].Version, values[5].Version)
						ts.Equal(values[1].Flag, values[3].Flag)
						ts.Equal(values[3].Flag, values[5].Flag)
						v0, v1 := ints[0], ints[1]
						ts.Equal(valueSum, v0+v1)
						v2, v3 := ints[2], ints[3]
						ts.Equal(valueSum, v2+v3)
						v4, v5 := ints[4], ints[5]
						ts.Equal(valueSum, v4+v5)
						ts.Equal(v0, v2)
						ts.Equal(v2, v4)
						ts.Equal(v1, v3)
						ts.Equal(v3, v5)
					}
				} else {
					ts.True(ts.DoTransaction(ctx, goRoutineIndex, sc2, func(ctx context.Context, txn types.Txn) error {
						{
							key1Val, err := txn.Get(ctx, key1)
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !ts.NoError(err) {
								return err
							}
							v1 -= delta
							if err := txn.Set(ctx, key1, types.NewIntValue(v1).V); err != nil {
								return err
							}
						}

						{
							key2Val, err := txn.Get(ctx, key2)
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !ts.NoError(err) {
								return err
							}
							v2 += delta
							if err := txn.Set(ctx, key2, types.NewIntValue(v2).V); err != nil {
								return err
							}
						}
						return nil
					}))
				}
			}
			ts.t.Logf("cost %s per txn @goRoutine %d", time.Since(start)/time.Duration(rounds), goRoutineIndex)
		}(i)
	}

	wg.Wait()

	value1, err := sc1.GetInt(ctx, key1)
	if !ts.NoError(err) {
		return
	}
	if !ts.Equal(k1InitialValue-(ts.GoRoutineNum-1)*ts.TxnNumPerGoRoutine*delta, value1) {
		return
	}

	value2, err := sc2.GetInt(ctx, key2)
	if !ts.NoError(err) {
		return
	}
	if !ts.Equal(k2InitialValue+(ts.GoRoutineNum-1)*ts.TxnNumPerGoRoutine*delta, value2) {
		return
	}
	return true
}

func testDistributedTxnReadConsistencyDeadlock(ctx context.Context, ts *TestCase) (b bool) {
	const (
		delta                          = 6
		key1, key2                     = "k1", "k22"
		k1InitialValue, k2InitialValue = 100, 200
		valueSum                       = k1InitialValue + k2InitialValue
	)
	if !ts.True(ts.MustRouteToDifferentShards(key1, key2)) {
		return false
	}
	sc1, sc2 := ts.scs[0], ts.scs[1]
	if err := sc1.SetInt(ctx, key1, k1InitialValue); !ts.NoError(err) {
		return
	}
	if val, err := sc1.GetInt(ctx, key1); !ts.NoError(err) || !ts.Equal(k1InitialValue, val) {
		return
	}
	if err := sc2.SetInt(ctx, key2, k2InitialValue); !ts.NoError(err) {
		return
	}
	if val, err := sc2.GetInt(ctx, key2); !ts.NoError(err) || !ts.Equal(k2InitialValue, val) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < ts.GoRoutineNum; i++ {
		wg.Add(1)

		go func(goRoutineIndex int) {
			defer wg.Done()

			start := time.Now()
			for round := 0; round < ts.TxnNumPerGoRoutine; round++ {
				if goRoutineIndex == 0 {
					ts.True(ts.DoReadOnlyTransaction(ctx, goRoutineIndex, sc1, func(ctx context.Context, txn types.Txn) error {
						key1Val, err := txn.Get(ctx, key1)
						if err != nil {
							return err
						}
						v1, err := key1Val.Int()
						if !ts.NoError(err) {
							return err
						}

						key2Val, err := txn.Get(ctx, key2)
						if err != nil {
							return err
						}
						v2, err := key2Val.Int()
						if !ts.NoError(err) {
							return err
						}
						if !ts.Equal(valueSum, v1+v2) {
							return errors.ErrAssertFailed
						}
						return nil
					}))
				} else if goRoutineIndex&1 == 1 {
					ts.True(ts.DoTransaction(ctx, goRoutineIndex, sc2, func(ctx context.Context, txn types.Txn) error {
						{
							key1Val, err := txn.Get(ctx, key1)
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !ts.NoError(err) {
								return err
							}
							v1 -= delta
							if err := txn.Set(ctx, key1, types.NewIntValue(v1).V); err != nil {
								return err
							}
						}

						{
							key2Val, err := txn.Get(ctx, key2)
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !ts.NoError(err) {
								return err
							}
							v2 += delta
							if err := txn.Set(ctx, key2, types.NewIntValue(v2).V); err != nil {
								return err
							}
						}
						return nil
					}))
				} else {
					ts.True(ts.DoTransaction(ctx, goRoutineIndex, sc1, func(ctx context.Context, txn types.Txn) error {
						{
							key2Val, err := txn.Get(ctx, key2)
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !ts.NoError(err) {
								return err
							}
							v2 += delta
							if err := txn.Set(ctx, key2, types.NewIntValue(v2).V); err != nil {
								return err
							}
						}

						{
							key1Val, err := txn.Get(ctx, key1)
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !ts.NoError(err) {
								return err
							}
							v1 -= delta
							if err := txn.Set(ctx, key1, types.NewIntValue(v1).V); err != nil {
								return err
							}
						}
						return nil
					}))
				}
			}
			ts.t.Logf("%s cost %v per txn @goRoutine %d", ts.t.Name(), time.Now().Sub(start)/time.Duration(ts.TxnNumPerGoRoutine), goRoutineIndex)
		}(i)
	}

	wg.Wait()
	value1, err := sc1.GetInt(ctx, key1)
	if !ts.NoError(err) {
		return
	}
	if !ts.Equal(k1InitialValue-(ts.GoRoutineNum-1)*ts.TxnNumPerGoRoutine*delta, value1) {
		return
	}
	value2, err := sc2.GetInt(ctx, key2)
	if !ts.NoError(err) {
		return
	}
	if !ts.Equal(k2InitialValue+(ts.GoRoutineNum-1)*ts.TxnNumPerGoRoutine*delta, value2) {
		return
	}
	return true
}

func testDistributedTxnWriteSkew(ctx context.Context, ts *TestCase) (b bool) {
	const (
		delta                          = 6
		key1, key2                     = "k1", "k22"
		k1InitialValue, k2InitialValue = 1000, 1000
	)
	var (
		constraint = func(v1, v2 int) bool { return v1+v2 > 0 }
	)
	if !ts.MustRouteToDifferentShards(key1, key2) {
		return
	}
	sc1, sc2 := ts.scs[0], ts.scs[1]
	if err := sc1.SetInt(ctx, key1, k1InitialValue); !ts.NoError(err) {
		return
	}
	if val, err := sc2.GetInt(ctx, key1); !ts.NoError(err) || !ts.Equal(k1InitialValue, val) {
		return
	}
	if err := sc2.SetInt(ctx, key2, k2InitialValue); !ts.NoError(err) {
		return
	}
	if val, err := sc1.GetInt(ctx, key2); !ts.NoError(err) || !ts.Equal(k2InitialValue, val) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < ts.GoRoutineNum; i++ {
		wg.Add(1)

		go func(goRoutineIndex int) {
			defer wg.Done()

			start := time.Now()
			for round := 0; round < ts.TxnNumPerGoRoutine; round++ {
				if goRoutineIndex == 0 || goRoutineIndex == 1 {
					ts.True(ts.DoTransaction(ctx, goRoutineIndex, sc1, func(ctx context.Context, txn types.Txn) error {
						key1Val, err := txn.Get(ctx, key1)
						if err != nil {
							return err
						}
						v1, err := key1Val.Int()
						if !ts.NoError(err) {
							return err
						}

						key2Val, err := txn.Get(ctx, key2)
						if err != nil {
							return err
						}
						v2, err := key2Val.Int()
						if !ts.NoError(err) {
							return err
						}
						if !ts.True(constraint(v1, v2)) {
							return errors.ErrAssertFailed
						}
						return nil
					}))
				} else {
					ts.True(ts.DoTransaction(ctx, goRoutineIndex, sc1, func(ctx context.Context, txn types.Txn) error {
						if goRoutineIndex < (ts.GoRoutineNum-2)/2+2 {
							key1Val, err := txn.Get(ctx, key1)
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !ts.NoError(err) {
								return err
							}
							key2Val, err := txn.Get(ctx, key2)
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !ts.NoError(err) {
								return err
							}

							v1 -= delta
							if constraint(v1, v2) {
								if err := txn.Set(ctx, key1, types.NewIntValue(v1).V); err != nil {
									return err
								}
							}
						} else {
							key2Val, err := txn.Get(ctx, key2)
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !ts.NoError(err) {
								return err
							}
							key1Val, err := txn.Get(ctx, key1)
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !ts.NoError(err) {
								return err
							}

							v2 -= delta
							if constraint(v1, v2) {
								if err := txn.Set(ctx, key2, types.NewIntValue(v2).V); err != nil {
									return err
								}
							}
						}
						return nil
					}))
				}
			}
			ts.t.Logf("%s cost %v per txn @goRoutine %d", ts.t.Name(), time.Now().Sub(start)/time.Duration(ts.TxnNumPerGoRoutine), goRoutineIndex)
		}(i)
	}

	wg.Wait()

	value1, err := sc1.GetInt(ctx, key1)
	if !ts.NoError(err) {
		return
	}
	value2, err := sc2.GetInt(ctx, key2)
	if !ts.NoError(err) {
		return
	}
	if !ts.True(constraint(value1, value2)) {
		return
	}
	if !ts.Equal((k1InitialValue+k2InitialValue)%delta, value1+value2) {
		return
	}
	ts.t.Logf("value1: %d, value2: %d", value1, value2)
	return true
}

func testDistributedTxnExtraWriteSimple(ctx context.Context, ts *TestCase) (b bool) {
	const (
		transferAmount                 = 6
		key1, key2                     = "k1", "k22"
		k1InitialValue, k2InitialValue = 100, 200
		key1ExtraDelta, key2ExtraDelta = 10, 20
	)
	if !ts.MustRouteToDifferentShards(key1, key2) {
		return
	}
	sc1, sc2 := ts.scs[0], ts.scs[1]
	if err := sc1.SetInt(ctx, key1, k1InitialValue); !ts.NoError(err) {
		return
	}
	if val, err := sc1.GetInt(ctx, key1); !ts.NoError(err) || !ts.Equal(k1InitialValue, val) {
		return
	}
	if err := sc1.SetInt(ctx, key2, k2InitialValue); !ts.NoError(err) {
		return
	}
	if val, err := sc1.GetInt(ctx, key2); !ts.NoError(err) || !ts.Equal(k2InitialValue, val) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < ts.GoRoutineNum; i++ {
		wg.Add(1)

		go func(goRoutineIndex int) {
			defer wg.Done()

			start := time.Now()
			for round := 0; round < ts.TxnNumPerGoRoutine; round++ {
				if goRoutineIndex == 0 {
					ts.True(ts.DoTransaction(ctx, goRoutineIndex, sc1, func(ctx context.Context, txn types.Txn) error {
						{
							const k1 = key1
							key1Val, err := txn.Get(ctx, k1)
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !ts.NoError(err) {
								return err
							}
							if err := txn.Set(ctx, k1, types.NewIntValue(v1+key1ExtraDelta).V); err != nil {
								return err
							}
						}

						{
							const k2 = key2
							key2Val, err := txn.Get(ctx, k2)
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !ts.NoError(err) {
								return err
							}
							if err := txn.Set(ctx, k2, types.NewIntValue(v2+key2ExtraDelta).V); err != nil {
								return err
							}
						}
						return nil
					}))
				} else {
					ts.True(ts.DoTransaction(ctx, goRoutineIndex, sc1, func(ctx context.Context, txn types.Txn) error {
						{
							const k1 = key1
							key1Val, err := txn.Get(ctx, k1)
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !ts.NoError(err) {
								return err
							}
							if err := txn.Set(ctx, k1, types.NewIntValue(v1-transferAmount).V); err != nil {
								return err
							}
						}

						{
							const k2 = key2
							key2Val, err := txn.Get(ctx, k2)
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !ts.NoError(err) {
								return err
							}
							if err := txn.Set(ctx, k2, types.NewIntValue(v2+transferAmount).V); err != nil {
								return err
							}
						}
						return nil
					}))
				}
			}
			ts.t.Logf("%s cost %v per txn @goRoutine %d", ts.t.Name(), time.Now().Sub(start)/time.Duration(ts.TxnNumPerGoRoutine), goRoutineIndex)
		}(i)
	}

	wg.Wait()
	{
		value1, err := sc1.GetInt(ctx, key1)
		if !ts.NoError(err) {
			return
		}
		if !ts.Equal(k1InitialValue-(ts.GoRoutineNum-1)*ts.TxnNumPerGoRoutine*transferAmount+1*ts.TxnNumPerGoRoutine*key1ExtraDelta, value1) {
			return
		}

		value2, err := sc1.GetInt(ctx, key2)
		if !ts.NoError(err) {
			return
		}
		if !ts.Equal(k2InitialValue+(ts.GoRoutineNum-1)*ts.TxnNumPerGoRoutine*transferAmount+1*ts.TxnNumPerGoRoutine*key2ExtraDelta, value2) {
			return
		}
	}
	{
		value1, err := sc2.GetInt(ctx, key1)
		if !ts.NoError(err) {
			return
		}
		if !ts.Equal(k1InitialValue-(ts.GoRoutineNum-1)*ts.TxnNumPerGoRoutine*transferAmount+1*ts.TxnNumPerGoRoutine*key1ExtraDelta, value1) {
			return
		}

		value2, err := sc2.GetInt(ctx, key2)
		if !ts.NoError(err) {
			return
		}
		if !ts.Equal(k2InitialValue+(ts.GoRoutineNum-1)*ts.TxnNumPerGoRoutine*transferAmount+1*ts.TxnNumPerGoRoutine*key2ExtraDelta, value2) {
			return
		}
	}
	return true
}

func testDistributedTxnExtraWriteComplex(ctx context.Context, ts *TestCase) (b bool) {
	const (
		delta                                          = 6
		key1, key2, key3                               = "k1", "k22", "k3"
		k1InitialValue, k2InitialValue, k3InitialValue = 100, 200, 300
		key1ExtraDelta, key2ExtraDelta, key3ExtraDelta = 10, 20, 30
	)
	if !ts.MustRouteToDifferentShards(key1, key2) {
		return
	}
	sc1, sc2 := ts.scs[0], ts.scs[1]
	if err := sc1.SetInt(ctx, key1, k1InitialValue); !ts.NoError(err) {
		return
	}
	if val, err := sc2.GetInt(ctx, key1); !ts.NoError(err) || !ts.Equal(k1InitialValue, val) {
		return
	}
	if err := sc2.SetInt(ctx, key2, k2InitialValue); !ts.NoError(err) {
		return
	}
	if val, err := sc1.GetInt(ctx, key2); !ts.NoError(err) || !ts.Equal(k2InitialValue, val) {
		return
	}
	if err := sc1.SetInt(ctx, key3, k3InitialValue); !ts.NoError(err) {
		return
	}
	if val, err := sc2.GetInt(ctx, key3); !ts.NoError(err) || !ts.Equal(k3InitialValue, val) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < ts.GoRoutineNum; i++ {
		wg.Add(1)

		go func(goRoutineIndex int) {
			defer wg.Done()

			start := time.Now()
			for round := 0; round < ts.TxnNumPerGoRoutine; round++ {
				if goRoutineIndex == 0 {
					var (
						readValues  = map[string]types.Value{}
						writeValues = map[string]types.Value{}
					)
					ts.True(ts.DoTransaction(ctx, goRoutineIndex, sc1, func(ctx context.Context, txn types.Txn) error {
						// key1 += key1ExtraDelta
						key1Val, err := txn.Get(ctx, key1)
						if err != nil {
							return err
						}
						v1, err := key1Val.Int()
						if !ts.NoError(err) {
							return err
						}
						readValues[key1] = key1Val
						v1 += key1ExtraDelta
						writtenVal1 := types.NewIntValue(v1)
						if err := txn.Set(ctx, key1, writtenVal1.V); err != nil {
							return err
						}
						writeValues[key1] = writtenVal1.WithVersion(txn.GetId().Version())
						return nil
					}))
				} else if goRoutineIndex == 1 {
					// key2 += key2ExtraDelta
					var (
						readValues  = map[string]types.Value{}
						writeValues = map[string]types.Value{}
					)
					ts.True(ts.DoTransaction(ctx, goRoutineIndex, sc1, func(ctx context.Context, txn types.Txn) error {
						key2Val, err := txn.Get(ctx, key2)
						if err != nil {
							return err
						}
						v2, err := key2Val.Int()
						if !ts.NoError(err) {
							return err
						}
						readValues[key2] = key2Val
						v2 += key2ExtraDelta
						writtenVal2 := types.NewIntValue(v2)
						if err := txn.Set(ctx, key2, writtenVal2.V); err != nil {
							return err
						}
						writeValues[key2] = writtenVal2.WithVersion(txn.GetId().Version())
						return nil
					}))
				} else if goRoutineIndex == 2 {
					// key3 += key3ExtraDelta
					var (
						readValues  = map[string]types.Value{}
						writeValues = map[string]types.Value{}
					)
					ts.True(ts.DoTransaction(ctx, goRoutineIndex, sc1, func(ctx context.Context, txn types.Txn) error {
						key3Val, err := txn.Get(ctx, key3)
						if err != nil {
							return err
						}
						v3, err := key3Val.Int()
						if !ts.NoError(err) {
							return err
						}
						readValues[key3] = key3Val
						v3 += key3ExtraDelta
						writtenVal3 := types.NewIntValue(v3)
						if err := txn.Set(ctx, key3, writtenVal3.V); err != nil {
							return err
						}
						writeValues[key3] = writtenVal3.WithVersion(txn.GetId().Version())
						return nil
					}))
				} else if goRoutineIndex == 3 {
					// read key1 key2 key3
					var readValues = map[string]types.Value{}
					ts.True(ts.DoTransaction(ctx, goRoutineIndex, sc1, func(ctx context.Context, txn types.Txn) error {
						key1Val, err := txn.Get(ctx, key1)
						if err != nil {
							return err
						}
						readValues[key1] = key1Val
						key2Val, err := txn.Get(ctx, key2)
						if err != nil {
							return err
						}
						readValues[key2] = key2Val
						key3Val, err := txn.Get(ctx, key3)
						if err != nil {
							return err
						}
						readValues[key3] = key3Val
						return nil
					}))
				} else {
					// transfer delta from key1 to key2
					var (
						readValues  = map[string]types.Value{}
						writeValues = map[string]types.Value{}
					)
					ts.True(ts.DoTransaction(ctx, goRoutineIndex, sc1, func(ctx context.Context, txn types.Txn) error {
						{
							key1Val, err := txn.Get(ctx, key1)
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !ts.NoError(err) {
								return err
							}
							readValues[key1] = key1Val
							v1 -= delta
							writtenVal1 := types.NewIntValue(v1)
							if err := txn.Set(ctx, key1, writtenVal1.V); err != nil {
								return err
							}
							writeValues[key1] = writtenVal1.WithVersion(txn.GetId().Version())
						}

						{
							key2Val, err := txn.Get(ctx, key2)
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !ts.NoError(err) {
								return err
							}
							readValues[key2] = key2Val
							v2 += delta
							writtenVal2 := types.NewIntValue(v2)
							if err := txn.Set(ctx, key2, writtenVal2.V); err != nil {
								return err
							}
							writeValues[key2] = writtenVal2.WithVersion(txn.GetId().Version())
						}
						return nil
					}))
				}
			}
			ts.t.Logf("%s cost %v per txn @goRoutine %d", ts.t.Name(), time.Now().Sub(start)/time.Duration(ts.TxnNumPerGoRoutine), goRoutineIndex)
		}(i)
	}

	wg.Wait()

	value1, err := sc1.GetInt(ctx, key1)
	if !ts.NoError(err) {
		return
	}
	if !ts.Equal(k1InitialValue-(ts.GoRoutineNum-4)*ts.TxnNumPerGoRoutine*delta+1*ts.TxnNumPerGoRoutine*key1ExtraDelta, value1) {
		return
	}
	value2, err := sc2.GetInt(ctx, key2)
	if !ts.NoError(err) {
		return
	}
	if !ts.Equal(k2InitialValue+(ts.GoRoutineNum-4)*ts.TxnNumPerGoRoutine*delta+1*ts.TxnNumPerGoRoutine*key2ExtraDelta, value2) {
		return
	}
	value3, err := sc2.GetInt(ctx, key3)
	if !ts.NoError(err) {
		return
	}
	if !ts.Equal(k3InitialValue+1*ts.TxnNumPerGoRoutine*key3ExtraDelta, value3) {
		return
	}
	return true
}

func TestDistributedTxnConsistencyStandalone(t *testing.T) {
	t.Skip("needs processes be up")
	const (
		port1 = 9999
		port2 = 19999
	)
	cli1, err := NewClient(fmt.Sprintf("localhost:%d", port1))
	if !testifyassert.NoError(t, err) {
		return
	}
	cli2, err := NewClient(fmt.Sprintf("localhost:%d", port2))
	if !testifyassert.NoError(t, err) {
		return
	}
	ctm1, ctm2 := NewClientTxnManager(cli1), NewClientTxnManager(cli2)
	sc1 := smart_txn_client.NewSmartClient(ctm1, 10000)
	sc2 := smart_txn_client.NewSmartClient(ctm2, 10000)
	NewTestCase(t, rounds, testDistributedTxnExtraWriteSimple).SetSmartClients(sc1, sc2).Run()
}
