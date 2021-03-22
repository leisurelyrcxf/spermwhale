package txn

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/utils"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/gate"
	"github.com/leisurelyrcxf/spermwhale/txn/smart_txn_client"
	"github.com/leisurelyrcxf/spermwhale/types"
	testifyassert "github.com/stretchr/testify/assert"
)

func TestDistributedTxnLostUpdate(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 5))

	for _, threshold := range []int{10000} {
		for i := 0; i < rounds; i++ {
			if !testifyassert.True(t, testDistributedTxnLostUpdate(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestDistributedTxnLostUpdate failed @round %d", i)
				return
			}
		}
	}
}

func testDistributedTxnLostUpdate(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testDistributedTxnLostUpdate @round %d", round)
	assert := types.NewAssertion(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	const (
		initialValue      = 101
		goRoutineNumber   = 6
		roundPerGoRoutine = 1000
		delta             = 6
	)
	var (
		txnManagerCfg = defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold)
		tabletCfg     = defaultTabletTxnConfig.WithStaleWriteThreshold(staleWriteThreshold)
	)
	gAte, stopper := createGate(t, tabletCfg)
	defer stopper()
	if !assert.NotNil(gAte) {
		return
	}
	tm := NewTransactionManager(gAte, txnManagerCfg)
	sc := smart_txn_client.NewSmartClient(tm, 0)
	if err := sc.SetInt(ctx, "k1", initialValue); !assert.NoError(err) {
		return
	}
	if val, err := sc.GetInt(ctx, "k1"); !assert.NoError(err) || !assert.Equal(initialValue, val) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < goRoutineNumber; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			start := time.Now()
			for i := 0; i < roundPerGoRoutine; i++ {
				if !assert.NoError(sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
					val, err := txn.Get(ctx, "k1", types.NewTxnReadOption())
					if err != nil {
						return err
					}
					v1, err := val.Int()
					if !assert.NoError(err) {
						return err
					}
					v1 += delta

					return txn.Set(ctx, "k1", types.NewIntValue(v1).V)
				})) {
					return
				}
			}
			t.Logf("cost %v per round", time.Now().Sub(start)/roundPerGoRoutine)
		}()
	}

	wg.Wait()
	val, err := sc.GetInt(ctx, "k1")
	if !assert.NoError(err) {
		return
	}
	t.Logf("val: %d", val)
	if !assert.Equal(goRoutineNumber*roundPerGoRoutine*delta+initialValue, val) {
		return
	}

	return true
}

func TestDistributedTxnReadConsistency(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 6))

	for _, threshold := range []int{10000} {
		for i := 0; i < rounds; i++ {
			if !testifyassert.True(t, testDistributedTxnReadConsistency(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestDistributedTxnConsistency failed @round %d", i)
				return
			}
		}
	}
}

func testDistributedTxnReadConsistency(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testDistributedTxnReadConsistency @round %d", round)
	assert := types.NewAssertion(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	const (
		goRoutineNumber                = 6
		roundPerGoRoutine              = 1000
		delta                          = 6
		key1, key2                     = "k1", "k22"
		k1InitialValue, k2InitialValue = 100, 200
		valueSum                       = k1InitialValue + k2InitialValue
	)
	var (
		txnManagerCfg = defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold)
		tabletCfg     = defaultTabletTxnConfig.WithStaleWriteThreshold(staleWriteThreshold)
	)
	gAte, stopper := createGate(t, tabletCfg)
	defer stopper()
	//t.Logf("%v shard: %d, %v shard: %d", key1, gAte.MustRoute(key1).ID, key2, gAte.MustRoute(key2).ID)
	if !assert.NotEqual(gAte.MustRoute(key1).ID, gAte.MustRoute(key2).ID) {
		return
	}
	if !assert.NotNil(gAte) {
		return
	}
	tm := NewTransactionManager(gAte, txnManagerCfg)
	sc := smart_txn_client.NewSmartClient(tm, 10000)
	defer sc.Close()
	if err := sc.SetInt(ctx, key1, k1InitialValue); !assert.NoError(err) {
		return
	}
	if val, err := sc.GetInt(ctx, key1); !assert.NoError(err) || !assert.Equal(k1InitialValue, val) {
		return
	}
	if err := sc.SetInt(ctx, key2, k2InitialValue); !assert.NoError(err) {
		return
	}
	if val, err := sc.GetInt(ctx, key2); !assert.NoError(err) || !assert.Equal(k2InitialValue, val) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < goRoutineNumber; i++ {
		wg.Add(1)

		go func(goRoutineIndex int) {
			defer wg.Done()

			start := time.Now()
			rounds := roundPerGoRoutine
			if goRoutineIndex == 0 {
				rounds *= 10
			}
			for round := 0; round < rounds; round++ {
				if goRoutineIndex == 0 {
					var (
						txn    types.Txn
						values []types.Value
						err    error
					)
					if txn, err = sc.DoTransactionExOfType(ctx, types.TxnTypeSnapshotRead, func(ctx context.Context, txn types.Txn) (err error) {
						values, err = txn.MGet(ctx, []string{key1, key2, key1, key2, key1, key2}, types.NewTxnReadOption())
						return
					}); assert.NoError(err) && assert.Len(values, 6) {
						var ints = make([]int, len(values))
						for idx, val := range values {
							assert.True(!val.HasWriteIntent())
							assert.Equal(types.TxnInternalVersion(1), val.InternalVersion)
							assert.Equal(txn.(*Txn).SnapshotVersion, val.SnapshotVersion)
							x, err := val.Int()
							if !assert.NoError(err) {
								return
							}
							ints[idx] = x
						}
						assert.Equal(values[0].Version, values[2].Version)
						assert.Equal(values[2].Version, values[4].Version)
						assert.Equal(values[0].Flag, values[2].Flag)
						assert.Equal(values[2].Flag, values[4].Flag)
						assert.Equal(values[1].Version, values[3].Version)
						assert.Equal(values[3].Version, values[5].Version)
						assert.Equal(values[1].Flag, values[3].Flag)
						assert.Equal(values[3].Flag, values[5].Flag)
						v0, v1 := ints[0], ints[1]
						assert.Equal(valueSum, v0+v1)
						v2, v3 := ints[2], ints[3]
						assert.Equal(valueSum, v2+v3)
						v4, v5 := ints[4], ints[5]
						assert.Equal(valueSum, v4+v5)
						assert.Equal(v0, v2)
						assert.Equal(v2, v4)
						assert.Equal(v1, v3)
						assert.Equal(v3, v5)
					}
				} else {
					assert.NoError(sc.DoTransactionOfType(ctx, types.TxnTypeReadForWrite, func(ctx context.Context, txn types.Txn) error {
						{
							key1Val, err := txn.Get(ctx, key1, types.NewTxnReadOption())
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !assert.NoError(err) {
								return err
							}
							v1 -= delta
							if err := txn.Set(ctx, key1, types.NewIntValue(v1).V); err != nil {
								return err
							}
						}

						{
							key2Val, err := txn.Get(ctx, key2, types.NewTxnReadOption())
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !assert.NoError(err) {
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
			t.Logf("cost %v per round @go routine %d", time.Now().Sub(start)/time.Duration(rounds), goRoutineIndex)
		}(i)
	}

	wg.Wait()

	value1, err := sc.GetInt(ctx, key1)
	if !assert.NoError(err) {
		return
	}
	if !assert.Equal(k1InitialValue-(goRoutineNumber-1)*roundPerGoRoutine*delta, value1) {
		return
	}

	value2, err := sc.GetInt(ctx, key2)
	if !assert.NoError(err) {
		return
	}
	if !assert.Equal(k2InitialValue+(goRoutineNumber-1)*roundPerGoRoutine*delta, value2) {
		return
	}

	return true
}

func TestDistributedTxnReadConsistencyDeadlock(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 6))

	for _, threshold := range []int{10000} {
		for i := 0; i < rounds; i++ {
			if !testifyassert.True(t, testDistributedTxnReadConsistencyDeadlock(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestDistributedTxnConsistencyDeadlock failed @round %d", i)
				return
			}
		}
	}
}

func testDistributedTxnReadConsistencyDeadlock(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testDistributedTxnReadConsistencyDeadlock @round %d", round)
	assert := types.NewAssertion(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	const (
		goRoutineNumber                = 6
		roundPerGoRoutine              = 1000
		delta                          = 6
		key1, key2                     = "k1", "k22"
		k1InitialValue, k2InitialValue = 100, 200
		valueSum                       = k1InitialValue + k2InitialValue
	)
	var (
		txnManagerCfg = defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold)
		tabletCfg     = defaultTabletTxnConfig.WithStaleWriteThreshold(staleWriteThreshold)
	)
	gAte, stopper := createGate(t, tabletCfg)
	defer stopper()
	//t.Logf("%v shard: %d, %v shard: %d", key1, gAte.MustRoute(key1).ID, key2, gAte.MustRoute(key2).ID)
	if !assert.NotEqual(gAte.MustRoute(key1).ID, gAte.MustRoute(key2).ID) {
		return
	}
	if !assert.NotNil(gAte) {
		return
	}
	tm := NewTransactionManager(gAte, txnManagerCfg)
	sc := smart_txn_client.NewSmartClient(tm, 10000)
	defer sc.Close()
	if err := sc.SetInt(ctx, key1, k1InitialValue); !assert.NoError(err) {
		return
	}
	if val, err := sc.GetInt(ctx, key1); !assert.NoError(err) || !assert.Equal(k1InitialValue, val) {
		return
	}
	if err := sc.SetInt(ctx, key2, k2InitialValue); !assert.NoError(err) {
		return
	}
	if val, err := sc.GetInt(ctx, key2); !assert.NoError(err) || !assert.Equal(k2InitialValue, val) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < goRoutineNumber; i++ {
		wg.Add(1)

		go func(goRoutineIndex int) {
			defer wg.Done()

			start := time.Now()
			for round := 0; round < roundPerGoRoutine; round++ {
				if goRoutineIndex == 0 {
					assert.NoError(sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
						key1Val, err := txn.Get(ctx, key1, types.NewTxnReadOption())
						if err != nil {
							return err
						}
						v1, err := key1Val.Int()
						if !assert.NoError(err) {
							return err
						}

						key2Val, err := txn.Get(ctx, key2, types.NewTxnReadOption())
						if err != nil {
							return err
						}
						v2, err := key2Val.Int()
						if !assert.NoError(err) {
							return err
						}
						if !assert.Equal(valueSum, v1+v2) {
							return errors.ErrAssertFailed
						}
						return nil
					}))
				} else if goRoutineIndex&1 == 1 {
					assert.NoError(sc.DoTransactionOfType(ctx, types.TxnTypeReadForWrite, func(ctx context.Context, txn types.Txn) error {
						{
							key1Val, err := txn.Get(ctx, key1, types.NewTxnReadOption())
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !assert.NoError(err) {
								return err
							}
							v1 -= delta
							if err := txn.Set(ctx, key1, types.NewIntValue(v1).V); err != nil {
								return err
							}
						}

						{
							key2Val, err := txn.Get(ctx, key2, types.NewTxnReadOption())
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !assert.NoError(err) {
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
					assert.NoError(sc.DoTransactionOfType(ctx, types.TxnTypeReadForWrite, func(ctx context.Context, txn types.Txn) error {
						{
							key2Val, err := txn.Get(ctx, key2, types.NewTxnReadOption())
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !assert.NoError(err) {
								return err
							}
							v2 += delta
							if err := txn.Set(ctx, key2, types.NewIntValue(v2).V); err != nil {
								return err
							}
						}

						{
							key1Val, err := txn.Get(ctx, key1, types.NewTxnReadOption())
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !assert.NoError(err) {
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
			t.Logf("cost %v per round", time.Now().Sub(start)/roundPerGoRoutine)
		}(i)
	}

	wg.Wait()

	value1, err := sc.GetInt(ctx, key1)
	if !assert.NoError(err) {
		return
	}
	if !assert.Equal(k1InitialValue-(goRoutineNumber-1)*roundPerGoRoutine*delta, value1) {
		return
	}

	value2, err := sc.GetInt(ctx, key2)
	if !assert.NoError(err) {
		return
	}
	if !assert.Equal(k2InitialValue+(goRoutineNumber-1)*roundPerGoRoutine*delta, value2) {
		return
	}

	return true
}

func TestDistributedTxnConsistencyExtraWrite(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 5))

	for _, threshold := range []int{10000} {
		for i := 0; i < 10; i++ {
			if !testifyassert.True(t, testDistributedTxnConsistencyExtraWrite(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestDistributedTxnConsistencyExtraWrite failed @round %d", i)
				return
			}
		}
	}
}

func testDistributedTxnConsistencyExtraWrite(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testDistributedTxnConsistencyExtraWrite @round %d", round)
	assert := types.NewAssertion(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	const (
		goRoutineNumber                                = 8
		roundPerGoRoutine                              = 1000
		delta                                          = 6
		key1, key2, key3                               = "k1", "k22", "k3"
		k1InitialValue, k2InitialValue, k3InitialValue = 100, 200, 300
		key1ExtraDelta, key2ExtraDelta, key3ExtraDelta = 10, 20, 30
	)
	var (
		txnManagerCfg = defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold)
		tabletCfg     = defaultTabletTxnConfig.WithStaleWriteThreshold(staleWriteThreshold)
	)
	gAte, stopper := createGate(t, tabletCfg)
	defer stopper()
	//t.Logf("%v shard: %d, %v shard: %d", key1, gAte.MustRoute(key1).ID, key2, gAte.MustRoute(key2).ID)
	if !assert.NotEqual(gAte.MustRoute(key1).ID, gAte.MustRoute(key2).ID) {
		return
	}
	if !assert.NotNil(gAte) {
		return
	}
	tm := NewTransactionManager(gAte, txnManagerCfg)
	sc := smart_txn_client.NewSmartClient(tm, 10000)
	if err := sc.SetInt(ctx, key1, k1InitialValue); !assert.NoError(err) {
		return
	}
	if val, err := sc.GetInt(ctx, key1); !assert.NoError(err) || !assert.Equal(k1InitialValue, val) {
		return
	}
	if err := sc.SetInt(ctx, key2, k2InitialValue); !assert.NoError(err) {
		return
	}
	if val, err := sc.GetInt(ctx, key2); !assert.NoError(err) || !assert.Equal(k2InitialValue, val) {
		return
	}
	if err := sc.SetInt(ctx, key3, k3InitialValue); !assert.NoError(err) {
		return
	}
	if val, err := sc.GetInt(ctx, key3); !assert.NoError(err) || !assert.Equal(k3InitialValue, val) {
		return
	}

	txns := make([][]ExecuteInfo, goRoutineNumber)
	var wg sync.WaitGroup
	for i := 0; i < goRoutineNumber; i++ {
		wg.Add(1)

		go func(goRoutineIndex int) {
			defer wg.Done()

			start := time.Now()
			for round := 0; round < roundPerGoRoutine; round++ {
				if goRoutineIndex == 0 {
					var (
						readValues  = map[string]types.Value{}
						writeValues = map[string]types.Value{}
					)
					if tx, err := sc.DoTransactionEx(ctx, func(ctx context.Context, txn types.Txn) error {
						// key1 += key1ExtraDelta
						key1Val, err := txn.Get(ctx, key1, types.NewTxnReadOption())
						if err != nil {
							return err
						}
						v1, err := key1Val.Int()
						if !assert.NoError(err) {
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
					}); assert.NoError(err) {
						txns[goRoutineIndex] = append(txns[goRoutineIndex], ExecuteInfo{
							ID:          tx.GetId().Version(),
							State:       tx.GetState(),
							ReadValues:  readValues,
							WriteValues: writeValues,
						})
					}
				} else if goRoutineIndex == 1 {
					// key2 += key2ExtraDelta
					var (
						readValues  = map[string]types.Value{}
						writeValues = map[string]types.Value{}
					)
					if tx, err := sc.DoTransactionEx(ctx, func(ctx context.Context, txn types.Txn) error {
						key2Val, err := txn.Get(ctx, key2, types.NewTxnReadOption())
						if err != nil {
							return err
						}
						v2, err := key2Val.Int()
						if !assert.NoError(err) {
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
					}); assert.NoError(err) {
						txns[goRoutineIndex] = append(txns[goRoutineIndex], ExecuteInfo{
							ID:          tx.GetId().Version(),
							State:       tx.GetState(),
							ReadValues:  readValues,
							WriteValues: writeValues,
						})
					}
				} else if goRoutineIndex == 2 {
					// key3 += key3ExtraDelta
					var (
						readValues  = map[string]types.Value{}
						writeValues = map[string]types.Value{}
					)
					if tx, err := sc.DoTransactionEx(ctx, func(ctx context.Context, txn types.Txn) error {
						key3Val, err := txn.Get(ctx, key3, types.NewTxnReadOption())
						if err != nil {
							return err
						}
						v3, err := key3Val.Int()
						if !assert.NoError(err) {
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
					}); assert.NoError(err) {
						txns[goRoutineIndex] = append(txns[goRoutineIndex], ExecuteInfo{
							ID:          tx.GetId().Version(),
							State:       tx.GetState(),
							ReadValues:  readValues,
							WriteValues: writeValues,
						})
					}
				} else if goRoutineIndex == 3 {
					// read key1 key2 key3
					var readValues = map[string]types.Value{}
					if tx, err := sc.DoTransactionEx(ctx, func(ctx context.Context, txn types.Txn) error {
						key1Val, err := txn.Get(ctx, key1, types.NewTxnReadOption())
						if err != nil {
							return err
						}
						readValues[key1] = key1Val
						key2Val, err := txn.Get(ctx, key2, types.NewTxnReadOption())
						if err != nil {
							return err
						}
						readValues[key2] = key2Val
						key3Val, err := txn.Get(ctx, key3, types.NewTxnReadOption())
						if err != nil {
							return err
						}
						readValues[key3] = key3Val
						return nil
					}); assert.NoError(err) {
						txns[goRoutineIndex] = append(txns[goRoutineIndex], ExecuteInfo{
							ID:          tx.GetId().Version(),
							State:       tx.GetState(),
							ReadValues:  readValues,
							WriteValues: nil,
						})
					}
				} else {
					// transfer delta from key1 to key2
					var (
						readValues  = map[string]types.Value{}
						writeValues = map[string]types.Value{}
					)
					if tx, err := sc.DoTransactionEx(ctx, func(ctx context.Context, txn types.Txn) error {
						{
							key1Val, err := txn.Get(ctx, key1, types.NewTxnReadOption().WithWaitNoWriteIntent())
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !assert.NoError(err) {
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
							key2Val, err := txn.Get(ctx, key2, types.NewTxnReadOption().WithWaitNoWriteIntent())
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !assert.NoError(err) {
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
					}); assert.NoError(err) {
						txns[goRoutineIndex] = append(txns[goRoutineIndex], ExecuteInfo{
							ID:          tx.GetId().Version(),
							State:       tx.GetState(),
							ReadValues:  readValues,
							WriteValues: writeValues,
						})
					}
				}
			}
			t.Logf("cost %v per round @goroutine %d", time.Now().Sub(start)/roundPerGoRoutine, goRoutineIndex)
		}(i)
	}

	wg.Wait()

	value1, err := sc.GetInt(ctx, key1)
	if !assert.NoError(err) {
		return
	}
	if !assert.Equal(k1InitialValue-(goRoutineNumber-4)*roundPerGoRoutine*delta+1*roundPerGoRoutine*key1ExtraDelta, value1) {
		return
	}

	value2, err := sc.GetInt(ctx, key2)
	if !assert.NoError(err) {
		return
	}
	if !assert.Equal(k2InitialValue+(goRoutineNumber-4)*roundPerGoRoutine*delta+1*roundPerGoRoutine*key2ExtraDelta, value2) {
		return
	}

	value3, err := sc.GetInt(ctx, key3)
	if !assert.NoError(err) {
		return
	}
	if !assert.Equal(k3InitialValue+1*roundPerGoRoutine*key3ExtraDelta, value3) {
		return
	}

	allTxns := make([]ExecuteInfo, 0, len(txns)*roundPerGoRoutine)
	for _, txnsOneGoRoutine := range txns {
		allTxns = append(allTxns, txnsOneGoRoutine...)
	}
	sort.Sort(ExecuteInfos(allTxns))
	for i := 0; i < len(allTxns); i++ {
		findWriteVersion := func(cur int, key string, readVal types.Value) types.Value {
			for j := i - 1; j > 0; j-- {
				ptx := allTxns[j]
				if writeVal, ok := ptx.WriteValues[key]; ok {
					return writeVal
				}
			}
			return types.EmptyValue
		}
		for key, readVal := range allTxns[i].ReadValues {
			writeVal := findWriteVersion(i, key, readVal)
			if writeVal.IsEmpty() {
				continue
			}
			if !assert.Equal(readVal.Version, writeVal.Version) {
				return
			}
		}
	}

	return true
}

func TestDistributedTxnWriteSkew(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 5))

	for _, threshold := range []int{10000} {
		for i := 0; i < rounds; i++ {
			if !testifyassert.True(t, testDistributedTxnWriteSkew(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestDistributedTxnWriteSkew failed @round %d", i)
				return
			}
		}
	}
}

func testDistributedTxnWriteSkew(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testDistributedTxnWriteSkew @round %d", round)
	assert := types.NewAssertion(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	const (
		goRoutineNumber                = 6
		roundPerGoRoutine              = 1000
		delta                          = 6
		key1, key2                     = "k1", "k22"
		k1InitialValue, k2InitialValue = 1000, 1000
	)
	var (
		txnManagerCfg = defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold)
		tabletCfg     = defaultTabletTxnConfig.WithStaleWriteThreshold(staleWriteThreshold)
		constraint    = func(v1, v2 int) bool { return v1+v2 > 0 }
	)
	gAte, stopper := createGate(t, tabletCfg)
	if !assert.NotNil(gAte) {
		return
	}
	defer stopper()
	//t.Logf("%v shard: %d, %v shard: %d", key1, gAte.MustRoute(key1).ID, key2, gAte.MustRoute(key2).ID)
	if !assert.NotEqual(gAte.MustRoute(key1).ID, gAte.MustRoute(key2).ID) {
		return
	}
	if !assert.NotNil(gAte) {
		return
	}
	tm := NewTransactionManager(gAte, txnManagerCfg)
	sc := smart_txn_client.NewSmartClient(tm, 0)
	if err := sc.SetInt(ctx, key1, k1InitialValue); !assert.NoError(err) {
		return
	}
	if val, err := sc.GetInt(ctx, key1); !assert.NoError(err) || !assert.Equal(k1InitialValue, val) {
		return
	}
	if err := sc.SetInt(ctx, key2, k2InitialValue); !assert.NoError(err) {
		return
	}
	if val, err := sc.GetInt(ctx, key2); !assert.NoError(err) || !assert.Equal(k2InitialValue, val) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < goRoutineNumber; i++ {
		wg.Add(1)

		go func(goRoutineIndex int) {
			defer wg.Done()

			start := time.Now()
			for round := 0; round < roundPerGoRoutine; round++ {
				if goRoutineIndex == 0 || goRoutineIndex == 1 {
					assert.NoError(sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
						key1Val, err := txn.Get(ctx, key1, types.NewTxnReadOption())
						if err != nil {
							return err
						}
						v1, err := key1Val.Int()
						if !assert.NoError(err) {
							return err
						}

						key2Val, err := txn.Get(ctx, key2, types.NewTxnReadOption())
						if err != nil {
							return err
						}
						v2, err := key2Val.Int()
						if !assert.NoError(err) {
							return err
						}
						if !assert.True(constraint(v1, v2)) {
							return errors.ErrAssertFailed
						}
						return nil
					}))
				} else {
					assert.NoError(sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
						if goRoutineIndex < (goRoutineNumber-2)/2+2 {
							key1Val, err := txn.Get(ctx, key1, types.NewTxnReadOption())
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !assert.NoError(err) {
								return err
							}
							key2Val, err := txn.Get(ctx, key2, types.NewTxnReadOption())
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !assert.NoError(err) {
								return err
							}

							v1 -= delta
							if constraint(v1, v2) {
								if err := txn.Set(ctx, key1, types.NewIntValue(v1).V); err != nil {
									return err
								}
							}
						} else {
							key2Val, err := txn.Get(ctx, key2, types.NewTxnReadOption())
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !assert.NoError(err) {
								return err
							}
							key1Val, err := txn.Get(ctx, key1, types.NewTxnReadOption())
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !assert.NoError(err) {
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
			t.Logf("cost %v per round", time.Now().Sub(start)/roundPerGoRoutine)
		}(i)
	}

	wg.Wait()

	value1, err := sc.GetInt(ctx, key1)
	if !assert.NoError(err) {
		return
	}
	value2, err := sc.GetInt(ctx, key2)
	if !assert.NoError(err) {
		return
	}
	if !assert.True(constraint(value1, value2)) {
		return
	}
	if !assert.Equal((k1InitialValue+k2InitialValue)%delta, value1+value2) {
		return
	}
	t.Logf("value1: %d, value2: %d", value1, value2)
	return true
}

func TestDistributedTxnConsistencyIntegrate(t *testing.T) {
	testDistributedTxnConsistencyIntegrate(t, types.TxnTypeDefault, types.NewTxnReadOption())
}

func TestDistributedTxnConsistencyIntegrateReadForWriteWaitNoWriteIntent(t *testing.T) {
	testDistributedTxnConsistencyIntegrate(t, types.TxnTypeReadForWrite, types.NewTxnReadOption().WithWaitNoWriteIntent())
}

func testDistributedTxnConsistencyIntegrate(t *testing.T, txnType types.TxnType, readOpt types.TxnReadOption) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 5))

	for _, threshold := range []int{10000} {
		for i := 0; i < rounds; i++ {
			if !testifyassert.True(t, testDistributedTxnConsistencyIntegrateOneRound(t, i, txnType, readOpt, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestDistributedTxnConsistencyIntegrate failed @round %d", i)
				return
			}
		}
	}
}

func testDistributedTxnConsistencyIntegrateOneRound(t *testing.T, round int, txnType types.TxnType, readOpt types.TxnReadOption, staleWriteThreshold time.Duration) (b bool) {
	assert := types.NewAssertion(t)
	t.Logf("testDistributedTxnConsistencyIntegrate @round %d", round)
	var (
		txnManagerCfg = defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold)
		tabletCfg     = defaultTabletTxnConfig.WithStaleWriteThreshold(staleWriteThreshold)
	)
	txnServers, clientTMs, stopper := createCluster(t, txnManagerCfg, tabletCfg)
	defer stopper()
	if !assert.Len(txnServers, 2) {
		return
	}
	//t.Logf("%v shard: %d, %v shard: %d", key1, gAte.MustRoute(key1).ID, key2, gAte.MustRoute(key2).ID)
	gAte := txnServers[0].tm.kv.(*gate.Gate)
	const key1, key2 = "k1", "k22"
	if !assert.NotEqual(gAte.MustRoute(key1).ID, gAte.MustRoute(key2).ID) {
		return
	}
	tm1, tm2 := clientTMs[0], clientTMs[1]
	sc1 := smart_txn_client.NewSmartClient(tm1, 10000)
	sc2 := smart_txn_client.NewSmartClient(tm2, 10000)

	return testDistributedTxnConsistencyOneRound(t, txnType, readOpt, []*smart_txn_client.SmartClient{sc1, sc2})
}

func TestDistributedTxnConsistencyStandalone(t *testing.T) {
	t.Skip("needs processes be upped")
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 5))

	for i := 0; i < rounds; i++ {
		if !testifyassert.True(t, testDistributedTxnConsistencyStandalone(t, i)) {
			t.Errorf("TestDistributedTxnConsistencyStandalone failed @round %d", i)
			return
		}
	}
}

func testDistributedTxnConsistencyStandalone(t *testing.T, round int) (b bool) {
	assert := types.NewAssertion(t)
	t.Logf("testDistributedTxnConsistencyStandalone @round %d", round)
	const (
		port1 = 9999
		port2 = 19999
	)
	cli1, err := NewClient(fmt.Sprintf("localhost:%d", port1))
	if !assert.NoError(err) {
		return
	}
	cli2, err := NewClient(fmt.Sprintf("localhost:%d", port2))
	if !assert.NoError(err) {
		return
	}
	ctm1, ctm2 := NewClientTxnManager(cli1), NewClientTxnManager(cli2)
	sc1 := smart_txn_client.NewSmartClient(ctm1, 10000)
	sc2 := smart_txn_client.NewSmartClient(ctm2, 10000)
	return testDistributedTxnConsistencyIntegrateConvenient(t, []*smart_txn_client.SmartClient{sc1, sc2})
}

func TestDistributedTxnConsistencyIntegrateRedis(t *testing.T) {
	_ = flag.Set("alsologtostderr", fmt.Sprintf("%t", true))
	testifyassert.NoError(t, utils.MkdirIfNotExists(consts.DefaultTestLogDir))
	_ = flag.Set("v", fmt.Sprintf("%d", 150))
	_ = flag.Set("log_dir", consts.DefaultTestLogDir)

	for _, threshold := range []int{10000} {
		for i := 0; i < rounds; i++ {
			if !testifyassert.True(t, testDistributedTxnConsistencyIntegrateRedis(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("testDistributedTxnConsistencyIntegrateRedis failed @round %d", i)
				return
			}
		}
	}
}

func testDistributedTxnConsistencyIntegrateRedis(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	assert := types.NewAssertion(t)
	t.Logf("testDistributedTxnConsistencyIntegrateRedis @round %d", round)
	var (
		txnManagerCfg = defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold)
		tabletCfg     = defaultTabletTxnConfig.WithStaleWriteThreshold(staleWriteThreshold)
	)
	tms, clientTMs, stopper := createClusterEx(t, types.DBTypeRedis, txnManagerCfg, tabletCfg)
	if !assert.Len(tms, 2) {
		return
	}
	defer stopper()
	//t.Logf("%v shard: %d, %v shard: %d", key1, gAte.MustRoute(key1).ID, key2, gAte.MustRoute(key2).ID)
	gAte := tms[0].tm.kv.(*gate.Gate)
	const key1, key2 = "k1", "k22"
	if !assert.NotEqual(gAte.MustRoute(key1).ID, gAte.MustRoute(key2).ID) {
		return
	}
	tm1, tm2 := clientTMs[0], clientTMs[1]
	sc1 := smart_txn_client.NewSmartClient(tm1, 10000)
	sc2 := smart_txn_client.NewSmartClient(tm2, 10000)

	return testDistributedTxnConsistencyIntegrateConvenient(t, []*smart_txn_client.SmartClient{sc1, sc2})
}

func testDistributedTxnConsistencyIntegrateConvenient(t *testing.T, scs []*smart_txn_client.SmartClient) (b bool) {
	return testDistributedTxnConsistencyOneRound(t, types.TxnTypeDefault, types.NewTxnReadOption(), scs)
}

func testDistributedTxnConsistencyOneRound(t *testing.T, txnType types.TxnType, readOpt types.TxnReadOption, scs []*smart_txn_client.SmartClient) (b bool) {
	assert := types.NewAssertion(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	const (
		goRoutineNumber                = 6
		roundPerGoRoutine              = 1000
		delta                          = 6
		key1, key2                     = "k1", "k22"
		k1InitialValue, k2InitialValue = 100, 200
		valueSum                       = k1InitialValue + k2InitialValue
		key1ExtraDelta, key2ExtraDelta = 10, 20
	)
	sc1, sc2 := scs[0], scs[1]
	if err := sc1.SetInt(ctx, key1, k1InitialValue); !assert.NoError(err) {
		return
	}
	if val, err := sc1.GetInt(ctx, key1); !assert.NoError(err) || !assert.Equal(k1InitialValue, val) {
		return
	}
	if err := sc1.SetInt(ctx, key2, k2InitialValue); !assert.NoError(err) {
		return
	}
	if val, err := sc1.GetInt(ctx, key2); !assert.NoError(err) || !assert.Equal(k2InitialValue, val) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < goRoutineNumber; i++ {
		wg.Add(1)

		go func(goRoutineIndex int) {
			defer wg.Done()

			start := time.Now()
			for round := 0; round < roundPerGoRoutine; round++ {
				if goRoutineIndex == 0 {
					assert.NoError(sc1.DoTransactionOfType(ctx, txnType, func(ctx context.Context, txn types.Txn) error {
						{
							key1Val, err := txn.Get(ctx, key1, readOpt)
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !assert.NoError(err) {
								return err
							}
							v1 += key1ExtraDelta
							if err := txn.Set(ctx, key1, types.NewIntValue(v1).V); err != nil {
								return err
							}
						}

						{
							key2Val, err := txn.Get(ctx, key2, readOpt)
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !assert.NoError(err) {
								return err
							}
							v2 += key2ExtraDelta
							if err := txn.Set(ctx, key2, types.NewIntValue(v2).V); err != nil {
								return err
							}
						}
						return nil
					}))
				} else {
					assert.NoError(sc2.DoTransactionOfType(ctx, txnType, func(ctx context.Context, txn types.Txn) error {
						{
							key1Val, err := txn.Get(ctx, key1, readOpt)
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !assert.NoError(err) {
								return err
							}
							v1 -= delta
							if err := txn.Set(ctx, key1, types.NewIntValue(v1).V); err != nil {
								return err
							}
						}

						{
							key2Val, err := txn.Get(ctx, key2, readOpt)
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !assert.NoError(err) {
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
			t.Logf("cost %v per round", time.Now().Sub(start)/roundPerGoRoutine)
		}(i)
	}

	wg.Wait()

	{
		value1, err := sc1.GetInt(ctx, key1)
		if !assert.NoError(err) {
			return
		}
		if !assert.Equal(k1InitialValue-(goRoutineNumber-1)*roundPerGoRoutine*delta+1*roundPerGoRoutine*key1ExtraDelta, value1) {
			return
		}

		value2, err := sc1.GetInt(ctx, key2)
		if !assert.NoError(err) {
			return
		}
		if !assert.Equal(k2InitialValue+(goRoutineNumber-1)*roundPerGoRoutine*delta+1*roundPerGoRoutine*key2ExtraDelta, value2) {
			return
		}
	}
	{
		value1, err := sc2.GetInt(ctx, key1)
		if !assert.NoError(err) {
			return
		}
		if !assert.Equal(k1InitialValue-(goRoutineNumber-1)*roundPerGoRoutine*delta+1*roundPerGoRoutine*key1ExtraDelta, value1) {
			return
		}

		value2, err := sc2.GetInt(ctx, key2)
		if !assert.NoError(err) {
			return
		}
		if !assert.Equal(k2InitialValue+(goRoutineNumber-1)*roundPerGoRoutine*delta+1*roundPerGoRoutine*key2ExtraDelta, value2) {
			return
		}
	}

	return true
}
