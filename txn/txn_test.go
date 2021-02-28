package txn

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/leisurelyrcxf/spermwhale/gate"

	"github.com/leisurelyrcxf/spermwhale/types/concurrency"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/mvcc/impl/memory"
	"github.com/leisurelyrcxf/spermwhale/tablet"
	"github.com/leisurelyrcxf/spermwhale/txn/smart_txn_client"
	"github.com/leisurelyrcxf/spermwhale/types"
)

var defaultTxnConfig = types.TxnConfig{
	StaleWriteThreshold: time.Millisecond * 5,
	MaxClockDrift:       time.Millisecond,
}

type MyT struct {
	t *testing.T
}

func NewT(t *testing.T) MyT {
	return MyT{
		t: t,
	}
}

func (t MyT) Errorf(format string, args ...interface{}) {
	if isMain() {
		t.t.Errorf(format, args...)
		return
	}
	print(fmt.Sprintf(format, args...))
	_ = os.Stderr.Sync()
	os.Exit(1)
}

func isMain() bool {
	ss := string(debug.Stack())
	return strings.Contains(ss, "testing.(*T).Run")
}

func TestTxnLostUpdate(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 1))

	for _, threshold := range []int{10} {
		for i := 0; i < 2; i++ {
			if !testifyassert.True(t, testTxnLostUpdate(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestTxnLostUpdate failed @round %d, staleWriteThreshold: %s", i, time.Millisecond*time.Duration(threshold))
				return
			}
		}
	}
}

func testTxnLostUpdate(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testTxnLostUpdate @round %d, staleWriteThreshold: %s", round, staleWriteThreshold)

	db := memory.NewDB()
	kvcc := tablet.NewKVCCForTesting(db, defaultTxnConfig.WithStaleWriteThreshold(staleWriteThreshold))
	m := NewTransactionManager(kvcc, defaultTxnConfig, 10, 20)
	sc := smart_txn_client.NewSmartClient(m, 0)
	assert := testifyassert.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	const (
		initialValue    = 101
		goRoutineNumber = 10000
		delta           = 6
	)
	err := sc.SetInt(ctx, "k1", initialValue)
	if !assert.NoError(err) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < goRoutineNumber; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			assert.NoError(sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
				val, err := txn.Get(ctx, "k1")
				if err != nil {
					return err
				}
				v1, err := val.Int()
				if !assert.NoError(err) {
					return err
				}
				v1 += delta

				return txn.Set(ctx, "k1", types.IntValue(v1).V)
			}))
		}()
	}

	wg.Wait()
	val, err := sc.GetInt(ctx, "k1")
	if !assert.NoError(err) {
		return
	}
	t.Logf("val: %d", val)
	if !assert.Equal(goRoutineNumber*delta+initialValue, val) {
		return
	}

	return true
}

func TestTxnReadAfterWrite(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 0))

	for _, threshold := range []int{1000, 100, 10, 5} {
		for i := 0; i < 100; i++ {
			if !testifyassert.True(t, testTxnReadAfterWrite(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestTxnReadAfterWrite failed @round %d", i)
				return
			}
		}
	}
}

func testTxnReadAfterWrite(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testTxnReadAfterWrite @round %d", round)

	db := memory.NewDB()
	kvcc := tablet.NewKVCCForTesting(db, defaultTxnConfig.WithStaleWriteThreshold(staleWriteThreshold))
	m := NewTransactionManager(kvcc, defaultTxnConfig, 20, 30)
	sc := smart_txn_client.NewSmartClient(m, 0)
	assert := testifyassert.New(NewT(t))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	const (
		initialValue    = 101
		goRoutineNumber = 10000
		delta           = 6
	)
	err := sc.SetInt(ctx, "k1", initialValue)
	if !assert.NoError(err) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < goRoutineNumber; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			assert.NoError(sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
				val, err := txn.Get(ctx, "k1")
				if err != nil {
					return err
				}
				v1, err := val.Int()
				if !assert.NoError(err) {
					return err
				}
				v1 += delta

				if err := txn.Set(ctx, "k1", types.IntValue(v1).V); err != nil {
					return err
				}

				val2, err := txn.Get(ctx, "k1")
				if err != nil {
					return err
				}
				v2, err := val2.Int()
				if !assert.NoError(err) {
					return err
				}
				if !assert.Equalf(v1, v2, "read_version: %d, txn_version: %d", val2.Version, txn.GetId()) {
					return errors.ErrAssertFailed
				}
				return nil
			}))
		}()
	}

	wg.Wait()
	val, err := sc.GetInt(ctx, "k1")
	if !assert.NoError(err) {
		return
	}
	t.Logf("val: %d", val)
	if !assert.Equal(goRoutineNumber*delta+initialValue, val) {
		return
	}

	return true
}

func TestTxnLostUpdateWithSomeAborted(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 10-1))

	for _, threshold := range []int{5, 10, 100, 1000} {
		for i := 0; i < 100; i++ {
			if !testifyassert.True(t, testTxnLostUpdateWithSomeAborted(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestTxnLostUpdateWithSomeAborted failed @round %d", i)
				return
			}
		}
	}
}

func testTxnLostUpdateWithSomeAborted(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testTxnLostUpdateWithSomeAborted @round %d", round)

	db := memory.NewDB()
	kvcc := tablet.NewKVCCForTesting(db, defaultTxnConfig.WithStaleWriteThreshold(staleWriteThreshold))
	m := NewTransactionManager(kvcc, defaultTxnConfig, 20, 30)
	sc := smart_txn_client.NewSmartClient(m, 0)
	assert := testifyassert.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	const (
		initialValue    = 101
		goRoutineNumber = 10000
		delta           = 6
	)
	err := sc.SetInt(ctx, "k1", initialValue)
	if !assert.NoError(err) {
		return
	}

	var (
		wg       sync.WaitGroup
		goodTxns concurrency.AtomicUint64
	)
	for i := 0; i < goRoutineNumber; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			if i%100 == 0 {
				assert.Equal(errors.ErrInject, sc.DoTransactionEx(ctx, func(ctx context.Context, txn types.Txn) (error, bool) {
					val, err := txn.Get(ctx, "k1")
					if err != nil {
						return err, true
					}
					v1, err := val.Int()
					if !assert.NoError(err) {
						return err, true
					}
					v1 += delta

					return txn.Set(ctx, "k1", types.IntValue(v1).V), true
				}, func() error {
					return errors.ErrInject
				}, nil))
			} else {
				goodTxns.Add(1)
				assert.NoError(sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
					val, err := txn.Get(ctx, "k1")
					if err != nil {
						return err
					}
					v1, err := val.Int()
					if !assert.NoError(err) {
						return err
					}
					v1 += delta

					return txn.Set(ctx, "k1", types.IntValue(v1).V)
				}))
			}
		}(i)
	}

	wg.Wait()
	val, err := sc.GetInt(ctx, "k1")
	if !assert.NoError(err) {
		return
	}
	t.Logf("val: %d", val)
	if !assert.Equal(int(goodTxns.Get())*delta+initialValue, val) {
		return
	}

	return true
}

func TestTxnLostUpdateWithSomeAborted2(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 10-1))

	for _, threshold := range []int{5, 10, 100, 1000} {
		for i := 0; i < 100; i++ {
			if !testifyassert.True(t, testTxnLostUpdateWithSomeAborted2(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestTxnLostUpdateWithSomeAborted2 failed @round %d", i)
				return
			}
		}
	}
}

func testTxnLostUpdateWithSomeAborted2(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testTxnLostUpdateWithSomeAborted2 @round %d", round)

	db := memory.NewDB()
	kvcc := tablet.NewKVCCForTesting(db, defaultTxnConfig.WithStaleWriteThreshold(staleWriteThreshold))
	m := NewTransactionManager(kvcc, defaultTxnConfig, 20, 30)
	sc := smart_txn_client.NewSmartClient(m, 0)
	assert := testifyassert.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	const (
		initialValue    = 101
		goRoutineNumber = 10000
		delta           = 6
	)
	err := sc.SetInt(ctx, "k1", initialValue)
	if !assert.NoError(err) {
		return
	}

	var (
		wg       sync.WaitGroup
		goodTxns concurrency.AtomicUint64
	)
	for i := 0; i < goRoutineNumber; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			if i%100 == 0 {
				err := sc.DoTransactionEx(ctx, func(ctx context.Context, txn types.Txn) (error, bool) {
					val, err := txn.Get(ctx, "k1")
					if err != nil {
						return err, true
					}
					v1, err := val.Int()
					if !assert.NoError(err) {
						return err, true
					}
					v1 += delta

					return txn.Set(ctx, "k1", types.IntValue(v1).V), true
				}, nil, func() error {
					return errors.ErrInject
				})
				if err == nil {
					goodTxns.Add(1)
				} else {
					assert.Equal(errors.ErrInject, err)
				}
			} else {
				goodTxns.Add(1)
				assert.NoError(sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
					val, err := txn.Get(ctx, "k1")
					if err != nil {
						return err
					}
					v1, err := val.Int()
					if !assert.NoError(err) {
						return err
					}
					v1 += delta

					return txn.Set(ctx, "k1", types.IntValue(v1).V)
				}))
			}
		}(i)
	}

	wg.Wait()
	val, err := sc.GetInt(ctx, "k1")
	if !assert.NoError(err) {
		return
	}
	t.Logf("val: %d", val)
	if !assert.Equal(int(goodTxns.Get())*delta+initialValue, val) {
		return
	}

	return true
}

func TestDistributedTxnLostUpdate(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 5))

	for _, threshold := range []int{10000} {
		for i := 0; i < 10; i++ {
			if !testifyassert.True(t, testDistributedTxnLostUpdate(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestDistributedTxnLostUpdate failed @round %d", i)
				return
			}
		}
	}
}

func testDistributedTxnLostUpdate(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testDistributedTxnLostUpdate @round %d", round)
	assert := testifyassert.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	const (
		initialValue      = 101
		goRoutineNumber   = 6
		roundPerGoRoutine = 1000
		delta             = 6
	)
	var (
		cfg = types.TxnConfig{}.WithStaleWriteThreshold(staleWriteThreshold)
	)
	gAte, stopper := createGate(t, cfg)
	defer stopper()
	if !assert.NotNil(gAte) {
		return
	}
	tm := NewTransactionManager(gAte, cfg, 20, 30)
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
					val, err := txn.Get(ctx, "k1")
					if err != nil {
						return err
					}
					v1, err := val.Int()
					if !assert.NoError(err) {
						return err
					}
					v1 += delta

					return txn.Set(ctx, "k1", types.IntValue(v1).V)
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

func TestDistributedTxnConsistency(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 5))

	for _, threshold := range []int{10000} {
		for i := 0; i < 10; i++ {
			if !testifyassert.True(t, testDistributedTxnConsistency(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestDistributedTxnConsistency failed @round %d", i)
				return
			}
		}
	}
}

func testDistributedTxnConsistency(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testDistributedTxnConsistency @round %d", round)
	assert := testifyassert.New(NewT(t))

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
		cfg = types.TxnConfig{}.WithStaleWriteThreshold(staleWriteThreshold)
	)
	gAte, stopper := createGate(t, cfg)
	defer stopper()
	//t.Logf("%v shard: %d, %v shard: %d", key1, gAte.MustRoute(key1).ID, key2, gAte.MustRoute(key2).ID)
	if !assert.NotEqual(gAte.MustRoute(key1).ID, gAte.MustRoute(key2).ID) {
		return
	}
	if !assert.NotNil(gAte) {
		return
	}
	tm := NewTransactionManager(gAte, cfg, 20, 30)
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
				if goRoutineIndex == 0 {
					assert.NoError(sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
						key1Val, err := txn.Get(ctx, key1)
						if err != nil {
							return err
						}
						v1, err := key1Val.Int()
						if !assert.NoError(err) {
							return err
						}

						key2Val, err := txn.Get(ctx, key2)
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
				} else {
					assert.NoError(sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
						{
							key1Val, err := txn.Get(ctx, key1)
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !assert.NoError(err) {
								return err
							}
							v1 -= delta
							if err := txn.Set(ctx, key1, types.IntValue(v1).V); err != nil {
								return err
							}
						}

						{
							key2Val, err := txn.Get(ctx, key2)
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !assert.NoError(err) {
								return err
							}
							v2 += delta
							if err := txn.Set(ctx, key2, types.IntValue(v2).V); err != nil {
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

func TestDistributedTxnConsistency2(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 5))

	for _, threshold := range []int{10000} {
		for i := 0; i < 10; i++ {
			if !testifyassert.True(t, testDistributedTxnConsistency2(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestDistributedTxnConsistency2 failed @round %d", i)
				return
			}
		}
	}
}

func testDistributedTxnConsistency2(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testDistributedTxnConsistency2 @round %d", round)
	assert := testifyassert.New(NewT(t))

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
	var (
		cfg = types.TxnConfig{}.WithStaleWriteThreshold(staleWriteThreshold)
	)
	gAte, stopper := createGate(t, cfg)
	defer stopper()
	//t.Logf("%v shard: %d, %v shard: %d", key1, gAte.MustRoute(key1).ID, key2, gAte.MustRoute(key2).ID)
	if !assert.NotEqual(gAte.MustRoute(key1).ID, gAte.MustRoute(key2).ID) {
		return
	}
	if !assert.NotNil(gAte) {
		return
	}
	tm := NewTransactionManager(gAte, cfg, 20, 30)
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
				if goRoutineIndex == 0 {
					assert.NoError(sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
						{
							key1Val, err := txn.Get(ctx, key1)
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !assert.NoError(err) {
								return err
							}
							v1 += key1ExtraDelta
							if err := txn.Set(ctx, key1, types.IntValue(v1).V); err != nil {
								return err
							}
						}

						{
							key2Val, err := txn.Get(ctx, key2)
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !assert.NoError(err) {
								return err
							}
							v2 += key2ExtraDelta
							if err := txn.Set(ctx, key2, types.IntValue(v2).V); err != nil {
								return err
							}
						}
						return nil
					}))
				} else {
					assert.NoError(sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
						{
							key1Val, err := txn.Get(ctx, key1)
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !assert.NoError(err) {
								return err
							}
							v1 -= delta
							if err := txn.Set(ctx, key1, types.IntValue(v1).V); err != nil {
								return err
							}
						}

						{
							key2Val, err := txn.Get(ctx, key2)
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !assert.NoError(err) {
								return err
							}
							v2 += delta
							if err := txn.Set(ctx, key2, types.IntValue(v2).V); err != nil {
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
	if !assert.Equal(k1InitialValue-(goRoutineNumber-1)*roundPerGoRoutine*delta+1*roundPerGoRoutine*key1ExtraDelta, value1) {
		return
	}

	value2, err := sc.GetInt(ctx, key2)
	if !assert.NoError(err) {
		return
	}
	if !assert.Equal(k2InitialValue+(goRoutineNumber-1)*roundPerGoRoutine*delta+1*roundPerGoRoutine*key2ExtraDelta, value2) {
		return
	}

	return true
}

func TestDistributedTxnWriteSkew(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 5))

	for _, threshold := range []int{10000} {
		for i := 0; i < 10; i++ {
			if !testifyassert.True(t, testDistributedTxnWriteSkew(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestDistributedTxnWriteSkew failed @round %d", i)
				return
			}
		}
	}
}

func testDistributedTxnWriteSkew(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testDistributedTxnWriteSkew @round %d", round)
	assert := testifyassert.New(NewT(t))

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
		cfg        = types.TxnConfig{}.WithStaleWriteThreshold(staleWriteThreshold)
		constraint = func(v1, v2 int) bool { return v1+v2 > 0 }
	)
	gAte, stopper := createGate(t, cfg)
	defer stopper()
	//t.Logf("%v shard: %d, %v shard: %d", key1, gAte.MustRoute(key1).ID, key2, gAte.MustRoute(key2).ID)
	if !assert.NotEqual(gAte.MustRoute(key1).ID, gAte.MustRoute(key2).ID) {
		return
	}
	if !assert.NotNil(gAte) {
		return
	}
	tm := NewTransactionManager(gAte, cfg, 20, 30)
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
						key1Val, err := txn.Get(ctx, key1)
						if err != nil {
							return err
						}
						v1, err := key1Val.Int()
						if !assert.NoError(err) {
							return err
						}

						key2Val, err := txn.Get(ctx, key2)
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
							key1Val, err := txn.Get(ctx, key1)
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !assert.NoError(err) {
								return err
							}
							key2Val, err := txn.Get(ctx, key2)
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !assert.NoError(err) {
								return err
							}

							v1 -= delta
							if constraint(v1, v2) {
								if err := txn.Set(ctx, key1, types.IntValue(v1).V); err != nil {
									return err
								}
							}
						} else {
							key2Val, err := txn.Get(ctx, key2)
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !assert.NoError(err) {
								return err
							}
							key1Val, err := txn.Get(ctx, key1)
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !assert.NoError(err) {
								return err
							}

							v2 -= delta
							if constraint(v1, v2) {
								if err := txn.Set(ctx, key2, types.IntValue(v2).V); err != nil {
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
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 5))

	for _, threshold := range []int{10000} {
		for i := 0; i < 10; i++ {
			if !testifyassert.True(t, testDistributedTxnConsistencyIntegrate(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestDistributedTxnConsistencyIntegrate failed @round %d", i)
				return
			}
		}
	}
}

func testDistributedTxnConsistencyIntegrate(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testDistributedTxnConsistencyIntegrate @round %d", round)
	assert := testifyassert.New(NewT(t))

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
	var (
		cfg = types.TxnConfig{}.WithStaleWriteThreshold(staleWriteThreshold)
	)
	tms, clientTMs, stopper := createCluster(t, cfg)
	defer stopper()
	if !assert.Len(tms, 2) {
		return
	}
	//t.Logf("%v shard: %d, %v shard: %d", key1, gAte.MustRoute(key1).ID, key2, gAte.MustRoute(key2).ID)
	gAte := tms[0].tm.kv.(*gate.Gate)
	if !assert.NotEqual(gAte.MustRoute(key1).ID, gAte.MustRoute(key2).ID) {
		return
	}
	tm1, tm2 := clientTMs[0], clientTMs[1]
	sc1 := smart_txn_client.NewSmartClient(tm1, 10000)
	sc2 := smart_txn_client.NewSmartClient(tm2, 10000)
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
					assert.NoError(sc1.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
						{
							key1Val, err := txn.Get(ctx, key1)
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !assert.NoError(err) {
								return err
							}
							v1 += key1ExtraDelta
							if err := txn.Set(ctx, key1, types.IntValue(v1).V); err != nil {
								return err
							}
						}

						{
							key2Val, err := txn.Get(ctx, key2)
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !assert.NoError(err) {
								return err
							}
							v2 += key2ExtraDelta
							if err := txn.Set(ctx, key2, types.IntValue(v2).V); err != nil {
								return err
							}
						}
						return nil
					}))
				} else {
					assert.NoError(sc2.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
						{
							key1Val, err := txn.Get(ctx, key1)
							if err != nil {
								return err
							}
							v1, err := key1Val.Int()
							if !assert.NoError(err) {
								return err
							}
							v1 -= delta
							if err := txn.Set(ctx, key1, types.IntValue(v1).V); err != nil {
								return err
							}
						}

						{
							key2Val, err := txn.Get(ctx, key2)
							if err != nil {
								return err
							}
							v2, err := key2Val.Int()
							if !assert.NoError(err) {
								return err
							}
							v2 += delta
							if err := txn.Set(ctx, key2, types.IntValue(v2).V); err != nil {
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
