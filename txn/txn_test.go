package txn

import (
	"context"
	"flag"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/gate"
	"github.com/leisurelyrcxf/spermwhale/kv/impl/memory"
	"github.com/leisurelyrcxf/spermwhale/kvcc"
	"github.com/leisurelyrcxf/spermwhale/txn/smart_txn_client"
	"github.com/leisurelyrcxf/spermwhale/txn/ttypes"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
)

func TestTxnLostUpdate(t *testing.T) {
	testTxnLostUpdate(t, types.TxnTypeDefault, types.NewTxnReadOption(), []time.Duration{time.Millisecond * 10})
}

func TestTxnLostUpdateReadForWrite(t *testing.T) {
	testTxnLostUpdate(t, types.TxnTypeReadForWrite, types.NewTxnReadOption(), []time.Duration{time.Second})
}

func TestTxnLostUpdateWaitNoWriteIntent(t *testing.T) {
	testTxnLostUpdate(t, types.TxnTypeDefault, types.NewTxnReadOption().WithWaitNoWriteIntent(), []time.Duration{time.Second})
}

func TestTxnLostUpdateReadForWriteWaitNoWriteIntent(t *testing.T) {
	testTxnLostUpdate(t, types.TxnTypeReadForWrite, types.NewTxnReadOption().WithWaitNoWriteIntent(), []time.Duration{time.Second})
}

func testTxnLostUpdate(t *testing.T, txnType types.TxnType, readOpt types.TxnReadOption, staleWriteThresholds []time.Duration) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 5))

	for _, staleWriteThreshold := range staleWriteThresholds {
		for i := 0; i < rounds; i++ {
			if !testifyassert.True(t, testTxnLostUpdateOneRound(t, i, txnType, readOpt, staleWriteThreshold)) {
				t.Errorf("testTxnLostUpdate failed @round %d, staleWriteThreshold: %s, type: %s, readOpt: %v", i, staleWriteThreshold, txnType, readOpt)
				return
			}
		}
	}
}

func testTxnLostUpdateOneRound(t *testing.T, round int, txnType types.TxnType, readOpt types.TxnReadOption, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testTxnLostUpdate @round %d, staleWriteThreshold: %s", round, staleWriteThreshold)

	const (
		initialValue    = 101
		goRoutineNumber = 10000
		delta           = 6
	)
	db := memory.NewMemoryDB()
	kvc := kvcc.NewKVCCForTesting(db, defaultTabletTxnConfig.WithStaleWriteThreshold(staleWriteThreshold))
	m := NewTransactionManager(kvc, defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold))
	sc := smart_txn_client.NewSmartClient(m, 0)
	defer sc.Close()
	assert := types.NewAssertion(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	err := sc.SetInt(ctx, "k1", initialValue)
	if !assert.NoError(err) {
		return
	}
	txns := make([]ExecuteInfo, goRoutineNumber)
	var wg sync.WaitGroup
	for i := 0; i < goRoutineNumber; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			var readValue, writeValue types.Value
			if tx, err := sc.DoTransactionRaw(ctx, txnType, func(ctx context.Context, txn types.Txn) (error, bool) {
				val, err := txn.Get(ctx, "k1", readOpt)
				if err != nil {
					return err, true
				}
				v1, err := val.Int()
				if !assert.NoError(err) {
					return err, false
				}
				readValue = val
				v1 += delta
				writeValue = types.IntValue(v1).WithVersion(txn.GetId().Version())
				return txn.Set(ctx, "k1", writeValue.V), true
			}, nil, nil); assert.NoError(err) {
				txns[i] = ExecuteInfo{
					ID:          tx.GetId().Version(),
					State:       tx.GetState(),
					ReadValues:  map[string]types.Value{"k1": readValue},
					WriteValues: map[string]types.Value{"k1": writeValue},
				}
			}
		}(i)
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

	sort.Sort(ExecuteInfos(txns))
	for i := 1; i < len(txns); i++ {
		prev, cur := txns[i-1], txns[i]
		if !assert.Equal(prev.ID, cur.ReadValues["k1"].Version) || !assert.Equal(prev.WriteValues["k1"].MustInt(), cur.ReadValues["k1"].MustInt()) {
			return
		}
	}
	return true
}

func TestTxnLostUpdateWriteAfterWrite(t *testing.T) {
	testTxnLostUpdateWriteAfterWrite(t, types.TxnTypeDefault, types.NewTxnReadOption(), []time.Duration{time.Millisecond * 10})
}

func testTxnLostUpdateWriteAfterWrite(t *testing.T, txnType types.TxnType, readOpt types.TxnReadOption, staleWriteThresholds []time.Duration) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 6))

	for _, staleWriteThreshold := range staleWriteThresholds {
		for i := 0; i < rounds; i++ {
			if !testifyassert.True(t, testTxnLostUpdateWriteAfterWriteOneRound(t, i, txnType, readOpt, staleWriteThreshold)) {
				t.Errorf("testTxnLostUpdateWriteAfterWriteOneRound failed @round %d, staleWriteThreshold: %s, type: %s, readOpt: %v", i, staleWriteThreshold, txnType, readOpt)
				return
			}
		}
	}
}

func testTxnLostUpdateWriteAfterWriteOneRound(t *testing.T, round int, txnType types.TxnType, readOpt types.TxnReadOption, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testTxnLostUpdateWriteAfterWriteOneRound @round %d, staleWriteThreshold: %s", round, staleWriteThreshold)

	const (
		initialValue     = 101
		goRoutineNumber  = 10000
		delta            = 6
		writeTimesPerTxn = 4
		key              = "k1"
	)
	if goRoutineNumber&1 != 0 {
		panic("goRoutineNumber&1 != 0")
	}
	db := memory.NewMemoryDB()
	kvc := kvcc.NewKVCCForTesting(db, defaultTabletTxnConfig.WithStaleWriteThreshold(staleWriteThreshold))
	m := NewTransactionManager(kvc, defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold))
	sc := smart_txn_client.NewSmartClient(m, 0)
	defer sc.Close()
	assert := types.NewAssertion(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if !assert.NoError(sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
		for i := 0; i < writeTimesPerTxn; i++ {
			if err := txn.Set(ctx, key, types.IntValue(initialValue).V); err != nil {
				assert.NoError(err)
				return err
			}
		}
		return nil
	})) {
		return
	}
	txns := make(ExecuteInfos, goRoutineNumber)
	var wg sync.WaitGroup
	for i := 0; i < goRoutineNumber; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			var readValues, writeValues = map[string]types.Value{}, map[string]types.Value{}
			if tx, err := sc.DoTransactionRaw(ctx, txnType, func(ctx context.Context, txn types.Txn) (error, bool) {
				if i&1 == 1 {
					val, err := txn.Get(ctx, key, readOpt)
					if err != nil {
						return err, true
					}
					v1, err := val.Int()
					if !assert.NoError(err) {
						return err, false
					}
					readValues[key] = val
					for i := 0; i < writeTimesPerTxn; i++ {
						v1 += delta
						writeValues[key] = types.IntValue(v1).WithVersion(txn.GetId().Version())
						if err := txn.Set(ctx, key, writeValues[key].V); err != nil {
							return err, true
						}
						time.Sleep(time.Microsecond)
					}
					return nil, true
				} else {
					val, err := txn.Get(ctx, key, readOpt)
					if err == nil {
						readValues[key] = val
					}
					return err, true
				}
			}, nil, nil); assert.NoError(err) {
				txns[i] = ExecuteInfo{
					ID:          tx.GetId().Version(),
					State:       tx.GetState(),
					WriteValues: writeValues,
					ReadValues:  readValues,
				}
			}
		}(i)
	}

	wg.Wait()
	val, err := sc.GetInt(ctx, key)
	if !assert.NoError(err) {
		return
	}
	t.Logf("val: %d", val)
	if !assert.Equal((goRoutineNumber/2)*delta*writeTimesPerTxn+initialValue, val) {
		return
	}

	txns.Check(assert)
	for _, cur := range txns {
		if _, isWrite := cur.WriteValues[key]; !isWrite {
			continue
		}
		val, err := kvc.Get(ctx, key, types.NewKVCCReadOption(cur.ID).WithExactVersion(cur.ID).WithNotGetMaxReadVersion().WithNotGetMaxReadVersion())
		if !assert.NoError(err) {
			return
		}
		if !assert.Equal(cur.ID, val.Version) {
			return
		}
		if !assert.Equal(types.TxnInternalVersion(writeTimesPerTxn), val.InternalVersion) {
			return
		}
		if !assert.False(val.HasWriteIntent()) {
			return
		}
		if !assert.Equal(types.TxnInternalVersion(writeTimesPerTxn), cur.ReadValues[key].InternalVersion) {
			return false
		}
	}

	return true
}

func TestTxnReadForWrite2Keys(t *testing.T) {
	testTxnReadForWrite2Keys(t, types.TxnTypeDefault, types.NewTxnReadOption(), []time.Duration{time.Millisecond * 10})
}

func TestTxnReadForWrite2KeysWaitNoWriteIntent(t *testing.T) {
	testTxnReadForWrite2Keys(t, types.TxnTypeDefault, types.NewTxnReadOption().WithWaitNoWriteIntent(), []time.Duration{time.Millisecond * 10})
}

func TestTxnReadForWrite2KeysReadForWrite(t *testing.T) {
	testTxnReadForWrite2Keys(t, types.TxnTypeReadForWrite, types.NewTxnReadOption(), []time.Duration{time.Second})
}

func TestTxnReadForWrite2KeysReadForWriteWaitNoWriteIntent(t *testing.T) {
	testTxnReadForWrite2Keys(t, types.TxnTypeReadForWrite, types.NewTxnReadOption().WithWaitNoWriteIntent(), []time.Duration{time.Second})
}

func testTxnReadForWrite2Keys(t *testing.T, txnType types.TxnType, readOpt types.TxnReadOption, staleWriteThresholds []time.Duration) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 8))

	for _, threshold := range staleWriteThresholds {
		for i := 0; i < rounds; i++ {
			if !testifyassert.True(t, testTxnReadForWrite2KeysOneRound(t, i, txnType, readOpt, threshold)) {
				t.Errorf("TestTxnReadForWrite2Keys failed @round %d, staleWriteThreshold: %s", i, threshold)
				return
			}
		}
	}
}

func testTxnReadForWrite2KeysOneRound(t *testing.T, round int, txnType types.TxnType, readOpt types.TxnReadOption, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testTxnReadForWrite2Keys @round %d, staleWriteThreshold: %s", round, staleWriteThreshold)

	db := memory.NewMemoryDB()
	kvc := kvcc.NewKVCCForTesting(db, defaultTabletTxnConfig.WithStaleWriteThreshold(staleWriteThreshold))
	m := NewTransactionManager(kvc, defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold))
	sc := smart_txn_client.NewSmartClient(m, 0)
	defer sc.Close()
	assert := types.NewAssertion(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	const (
		key1InitialValue = 10
		key2InitialValue = 100
		goRoutineNumber  = 10000
		delta            = 6
	)
	err := sc.SetInt(ctx, "k1", key1InitialValue)
	if !assert.NoError(err) {
		return
	}
	err = sc.SetInt(ctx, "k2", key2InitialValue)
	if !assert.NoError(err) {
		return
	}

	txns := make([]ExecuteInfo, goRoutineNumber)

	var wg sync.WaitGroup
	for i := 0; i < goRoutineNumber; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			var (
				readValues  = map[string]types.Value{}
				writeValues = map[string]types.Value{}
			)
			if tx, err := sc.DoTransactionExOfType(ctx, txnType, func(ctx context.Context, txn types.Txn) error {
				{
					val, err := txn.Get(ctx, "k1", readOpt)
					if err != nil {
						return err
					}
					v1, err := val.Int()
					if !assert.NoError(err) {
						return err
					}
					readValues["k1"] = val
					v1 += delta
					writeValues["k1"] = types.IntValue(v1).WithVersion(txn.GetId().Version())
					if err := txn.Set(ctx, "k1", writeValues["k1"].V); err != nil {
						return err
					}
				}
				{
					val, err := txn.Get(ctx, "k2", readOpt)
					if err != nil {
						return err
					}
					v2, err := val.Int()
					if !assert.NoError(err) {
						return err
					}
					readValues["k2"] = val
					v2 += delta
					writeValues["k2"] = types.IntValue(v2).WithVersion(txn.GetId().Version())
					if err := txn.Set(ctx, "k2", writeValues["k2"].V); err != nil {
						return err
					}
				}
				return nil
			}); assert.NoError(err) {
				txns[i] = ExecuteInfo{
					ID:          tx.GetId().Version(),
					State:       tx.GetState(),
					ReadValues:  readValues,
					WriteValues: writeValues,
				}
			}
		}(i)
	}

	wg.Wait()
	{
		v1, err := sc.GetInt(ctx, "k1")
		if !assert.NoError(err) {
			return
		}
		t.Logf("v1: %d", v1)
		if !assert.Equal(goRoutineNumber*delta+key1InitialValue, v1) {
			return
		}
	}
	{
		v2, err := sc.GetInt(ctx, "k2")
		if !assert.NoError(err) {
			return
		}
		t.Logf("v2: %d", v2)
		if !assert.Equal(goRoutineNumber*delta+key2InitialValue, v2) {
			return
		}
	}

	sort.Sort(ExecuteInfos(txns))
	for i := 1; i < len(txns); i++ {
		prev, cur := txns[i-1], txns[i]
		if !assert.Equal(prev.ID, cur.ReadValues["k1"].Version) || !assert.Equal(prev.WriteValues["k1"].MustInt(), cur.ReadValues["k1"].MustInt()) {
			return
		}
		if !assert.Equal(prev.ID, cur.ReadValues["k2"].Version) || !assert.Equal(prev.WriteValues["k2"].MustInt(), cur.ReadValues["k2"].MustInt()) {
			return
		}
	}
	return true
}

func TestTxnReadForWrite2KeysDeadlock(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 4))

	for _, threshold := range []int{1000} {
		for i := 0; i < rounds; i++ {
			if !testifyassert.True(t, testTxnReadForWrite2KeysDeadlockOneRound(t, i, types.TxnTypeDefault, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestTxnReadForWrite2KeysDeadlock failed @round %d, staleWriteThreshold: %s", i, time.Millisecond*time.Duration(threshold))
				return
			}
		}
	}
}

func testTxnReadForWrite2KeysDeadlockOneRound(t *testing.T, round int, txnType types.TxnType, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testTxnReadForWrite2KeysDeadlockOneRound @round %d, staleWriteThreshold: %s", round, staleWriteThreshold)

	db := memory.NewMemoryDB()
	kvc := kvcc.NewKVCCForTesting(db, defaultTabletTxnConfig.WithStaleWriteThreshold(staleWriteThreshold))
	m := NewTransactionManager(kvc, defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold))
	sc := smart_txn_client.NewSmartClient(m, 0)
	defer sc.Close()
	assert := types.NewAssertion(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	const (
		key1InitialValue = 10
		key2InitialValue = 100
		goRoutineNumber  = 10000
		delta            = 6
	)
	err := sc.SetInt(ctx, "k1", key1InitialValue)
	if !assert.NoError(err) {
		return
	}
	err = sc.SetInt(ctx, "k2", key2InitialValue)
	if !assert.NoError(err) {
		return
	}

	txns := make([]ExecuteInfo, goRoutineNumber)

	var wg sync.WaitGroup
	for i := 0; i < goRoutineNumber; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			var (
				readValues  = map[string]types.Value{}
				writeValues = map[string]types.Value{}
				readOpt     = types.NewTxnReadOption()
			)

			if tx, err := sc.DoTransactionExOfType(ctx, txnType, func(ctx context.Context, txn types.Txn) error {
				if i&1 == 1 {
					{
						val, err := txn.Get(ctx, "k1", readOpt)
						if err != nil {
							return err
						}
						v1, err := val.Int()
						if !assert.NoError(err) {
							return err
						}
						readValues["k1"] = val
						v1 += delta
						writeValues["k1"] = types.IntValue(v1).WithVersion(txn.GetId().Version())
						if err := txn.Set(ctx, "k1", writeValues["k1"].V); err != nil {
							return err
						}
					}
					{
						val, err := txn.Get(ctx, "k2", readOpt)
						if err != nil {
							return err
						}
						v2, err := val.Int()
						if !assert.NoError(err) {
							return err
						}
						readValues["k2"] = val
						v2 += delta
						writeValues["k2"] = types.IntValue(v2).WithVersion(txn.GetId().Version())
						if err := txn.Set(ctx, "k2", writeValues["k2"].V); err != nil {
							return err
						}
					}
				} else {
					{
						val, err := txn.Get(ctx, "k2", readOpt)
						if err != nil {
							return err
						}
						v2, err := val.Int()
						if !assert.NoError(err) {
							return err
						}
						readValues["k2"] = val
						v2 += delta
						writeValues["k2"] = types.IntValue(v2).WithVersion(txn.GetId().Version())
						if err := txn.Set(ctx, "k2", writeValues["k2"].V); err != nil {
							return err
						}
					}
					{
						val, err := txn.Get(ctx, "k1", readOpt)
						if err != nil {
							return err
						}
						v1, err := val.Int()
						if !assert.NoError(err) {
							return err
						}
						readValues["k1"] = val
						v1 += delta
						writeValues["k1"] = types.IntValue(v1).WithVersion(txn.GetId().Version())
						if err := txn.Set(ctx, "k1", writeValues["k1"].V); err != nil {
							return err
						}
					}
				}
				return nil
			}); assert.NoError(err) {
				txns[i] = ExecuteInfo{
					ID:          tx.GetId().Version(),
					State:       tx.GetState(),
					ReadValues:  readValues,
					WriteValues: writeValues,
				}
			}
		}(i)
	}

	wg.Wait()
	{
		v1, err := sc.GetInt(ctx, "k1")
		if !assert.NoError(err) {
			return
		}
		t.Logf("v1: %d", v1)
		if !assert.Equal(goRoutineNumber*delta+key1InitialValue, v1) {
			return
		}
	}
	{
		v2, err := sc.GetInt(ctx, "k2")
		if !assert.NoError(err) {
			return
		}
		t.Logf("v2: %d", v2)
		if !assert.Equal(goRoutineNumber*delta+key2InitialValue, v2) {
			return
		}
	}

	sort.Sort(ExecuteInfos(txns))
	for i := 1; i < len(txns); i++ {
		prev, cur := txns[i-1], txns[i]
		for _, key := range []string{"k1", "k2"} {
			prevVal, err := db.Get(ctx, key, types.NewKVReadOption(prev.ID).WithExactVersion())
			if !assert.NoError(err) || !assert.Equal(prev.ID, prevVal.Version) || !assert.Equal(prev.WriteValues[key].MustInt(), prevVal.MustInt()) {
				return
			}
			if !assert.Equal(prev.ID, cur.ReadValues[key].Version) || !assert.Equal(prev.WriteValues[key].MustInt(), cur.ReadValues[key].MustInt()) {
				return
			}
		}
	}
	return true
}

func TestTxnReadForWriteNKeysOneRound(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 4))

	for _, threshold := range []int{1000} {
		for i := 0; i < rounds; i++ {
			if !testifyassert.True(t, testTxnReadForWriteNKeysOneRound(t, i, types.TxnTypeDefault, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestTxnReadForWriteNKeysOneRound failed @round %d, staleWriteThreshold: %s", i, time.Millisecond*time.Duration(threshold))
				return
			}
		}
	}
}

func testTxnReadForWriteNKeysOneRound(t *testing.T, round int, txnType types.TxnType, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testTxnReadForWriteNKeysOneRound @round %d, staleWriteThreshold: %s", round, staleWriteThreshold)

	db := memory.NewMemoryDB()
	kvc := kvcc.NewKVCCForTesting(db, defaultTabletTxnConfig.WithStaleWriteThreshold(staleWriteThreshold))
	m := NewTransactionManager(kvc, defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold))
	sc := smart_txn_client.NewSmartClient(m, 0)
	defer sc.Close()
	assert := types.NewAssertion(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	const (
		key1InitialValue = 10
		goRoutineNumber  = 10000
		delta            = 6
		N                = 10
	)

	for i := 0; i < N; i++ {
		var key = fmt.Sprintf("k%d", i)
		if err := sc.SetInt(ctx, key, key1InitialValue); !assert.NoError(err) {
			return
		}
	}

	txns := make([]ExecuteInfo, goRoutineNumber)

	var wg sync.WaitGroup
	for i := 0; i < goRoutineNumber; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			var (
				readValues  = map[string]types.Value{}
				writeValues = map[string]types.Value{}
				readOpt     = types.NewTxnReadOption()
			)

			if tx, err := sc.DoTransactionExOfType(ctx, txnType, func(ctx context.Context, txn types.Txn) error {
				for i := 0; i < N; i++ {
					var key = fmt.Sprintf("k%d", i)
					val, err := txn.Get(ctx, key, readOpt)
					if err != nil {
						return err
					}
					v, err := val.Int()
					if !assert.NoError(err) {
						return err
					}
					readValues[key] = val
					v += delta
					writeValues[key] = types.IntValue(v).WithVersion(txn.GetId().Version())
					if err := txn.Set(ctx, key, writeValues[key].V); err != nil {
						return err
					}
				}
				return nil
			}); assert.NoError(err) {
				txns[i] = ExecuteInfo{
					ID:          tx.GetId().Version(),
					State:       tx.GetState(),
					ReadValues:  readValues,
					WriteValues: writeValues,
				}
			}
		}(i)
	}

	wg.Wait()
	{
		v1, err := sc.GetInt(ctx, "k1")
		if !assert.NoError(err) {
			return
		}
		t.Logf("v1: %d", v1)
		if !assert.Equal(goRoutineNumber*delta+key1InitialValue, v1) {
			return
		}
	}

	sort.Sort(ExecuteInfos(txns))
	for i := 1; i < len(txns); i++ {
		prev, cur := txns[i-1], txns[i]
		for _, key := range []string{"k1", "k2"} {
			prevVal, err := db.Get(ctx, key, types.NewKVReadOption(prev.ID).WithExactVersion())
			if !assert.NoError(err) || !assert.Equal(prev.ID, prevVal.Version) || !assert.Equal(prev.WriteValues[key].MustInt(), prevVal.MustInt()) {
				return
			}
			if !assert.Equal(prev.ID, cur.ReadValues[key].Version) || !assert.Equal(prev.WriteValues[key].MustInt(), cur.ReadValues[key].MustInt()) {
				return
			}
		}
	}
	return true
}

func TestTxnLostUpdateModAdd(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 1))

	for _, threshold := range []int{10} {
		for i := 0; i < rounds; i++ {
			if !testifyassert.True(t, testTxnLostUpdateModAdd(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestTxnLostUpdateModAdd failed @round %d, staleWriteThreshold: %s", i, time.Millisecond*time.Duration(threshold))
				return
			}
		}
	}
}

func testTxnLostUpdateModAdd(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("")
	t.Logf("testTxnLostUpdate @round %d, staleWriteThreshold: %s", round, staleWriteThreshold)

	db := memory.NewMemoryDB()
	kvcc := kvcc.NewKVCCForTesting(db, defaultTabletTxnConfig.WithStaleWriteThreshold(staleWriteThreshold))
	m := NewTransactionManager(kvcc, defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold))
	sc := smart_txn_client.NewSmartClient(m, 0)
	assert := types.NewAssertion(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	const (
		initialValue1   = 101
		initialValue2   = 222
		goRoutineNumber = 10000
		delta           = 6
	)
	err := sc.SetInt(ctx, "k1", initialValue1)
	if !assert.NoError(err) {
		return
	}
	err = sc.SetInt(ctx, "k22", initialValue2)
	if !assert.NoError(err) {
		return
	}

	txns := make([]ExecuteInfo, goRoutineNumber)

	f1, f2, f3 := func(x int) int {
		return (x + 5) % 11
	}, func(x int) int {
		return (x + 8) % 19
	}, func(x int) int {
		return (x + 7) % 23
	}
	selector := func(i int, x int) (func(int) int, int) {
		if i%3 == 0 {
			return f1, f1(x)
		}
		if i%3 == 1 {
			return f2, f2(x)
		}
		return f3, f3(x)
	}

	var wg sync.WaitGroup
	for i := 0; i < goRoutineNumber; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			var (
				g1, g2      func(int) int
				readValues  = map[string]types.Value{}
				writeValues = map[string]types.Value{}
			)
			if tx, err := sc.DoTransactionEx(ctx, func(ctx context.Context, txn types.Txn) error {
				{
					val, err := txn.Get(ctx, "k1", types.NewTxnReadOption())
					if err != nil {
						return err
					}
					readValues["k1"] = val
					v1, err := val.Int()
					if !assert.NoError(err) {
						return err
					}
					g1, v1 = selector(i, v1)
					writeValue := types.IntValue(v1).WithVersion(txn.GetId().Version())
					writeValues["k1"] = writeValue
					if err := txn.Set(ctx, "k1", writeValue.V); err != nil {
						return err
					}
				}

				{
					val, err := txn.Get(ctx, "k22", types.NewTxnReadOption())
					if err != nil {
						return err
					}
					readValues["k22"] = val
					v2, err := val.Int()
					if !assert.NoError(err) {
						return err
					}
					g2, v2 = selector(i+1, v2)
					writeValue := types.IntValue(v2).WithVersion(txn.GetId().Version())
					writeValues["k22"] = writeValue
					if err := txn.Set(ctx, "k22", writeValue.V); err != nil {
						return err
					}
				}
				return nil
			}); assert.NoError(err) {
				txns[i] = ExecuteInfo{
					ID:             tx.GetId().Version(),
					State:          tx.GetState(),
					ReadValues:     readValues,
					WriteValues:    writeValues,
					AdditionalInfo: [2]func(int) int{g1, g2},
				}
			}
		}(i)
	}

	wg.Wait()
	val1, err := sc.GetInt(ctx, "k1")
	if !assert.NoError(err) {
		return
	}
	val2, err := sc.GetInt(ctx, "k22")
	if !assert.NoError(err) {
		return
	}
	t.Logf("res_v1: %d, res_v2: %d", val1, val2)

	sort.Sort(ExecuteInfos(txns))
	for i := 1; i < len(txns); i++ {
		prev, cur := txns[i-1], txns[i]
		if !assert.Equal(prev.ID, cur.ReadValues["k1"].Version) || !assert.Equal(prev.WriteValues["k1"].MustInt(), cur.ReadValues["k1"].MustInt()) {
			return
		}
		if !assert.Equal(prev.ID, cur.ReadValues["k22"].Version) || !assert.Equal(prev.WriteValues["k22"].MustInt(), cur.ReadValues["k22"].MustInt()) {
			return
		}
	}

	v1, v2 := initialValue1, initialValue2
	for _, txn := range txns {
		g1, g2 := txn.AdditionalInfo.([2]func(int) int)[0], txn.AdditionalInfo.([2]func(int) int)[1]
		v1 = g1(v1)
		v2 = g2(v2)
	}
	t.Logf("replay_res_v1 :%d, replay_res_v2 :%d", v1, v2)
	return assert.Equal(v1, val1) && assert.Equal(v2, val2)
}

func TestTxnReadWriteAfterWrite(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 0))

	for _, threshold := range []int{10} {
		for i := 0; i < rounds; i++ {
			if !testifyassert.True(t, testTxnReadWriteAfterWrite(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestTxnReadWriteAfterWrite failed @round %d", i)
				return
			}
		}
	}
}

func testTxnReadWriteAfterWrite(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testTxnReadWriteAfterWrite @round %d", round)

	db := memory.NewMemoryDB()
	kvcc := kvcc.NewKVCCForTesting(db, defaultTabletTxnConfig.WithStaleWriteThreshold(staleWriteThreshold))
	m := NewTransactionManager(kvcc, defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold))
	sc := smart_txn_client.NewSmartClient(m, 0)
	assert := types.NewAssertion(t)

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
				val, err := txn.Get(ctx, "k1", types.NewTxnReadOption())
				if err != nil {
					return err
				}
				v1, err := val.Int()
				if !assert.NoError(err) {
					return err
				}
				v1 += delta

				if err := txn.Set(ctx, "k1", types.IntValue(v1-1).V); err != nil {
					return err
				}
				if err := txn.Set(ctx, "k1", types.IntValue(v1-3).V); err != nil {
					return err
				}
				if err := txn.Set(ctx, "k1", types.IntValue(v1).V); err != nil {
					return err
				}

				val2, err := txn.Get(ctx, "k1", types.NewTxnReadOption())
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
	_ = flag.Set("v", fmt.Sprintf("%d", 1))

	for _, threshold := range []int{10, 100} {
		for i := 0; i < rounds; i++ {
			if !testifyassert.True(t, testTxnLostUpdateWithSomeAborted(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestTxnLostUpdateWithSomeAborted failed @round %d", i)
				return
			}
		}
	}
}

func testTxnLostUpdateWithSomeAborted(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testTxnLostUpdateWithSomeAborted @round %d", round)

	db := memory.NewMemoryDB()
	var (
		txnManagerCfg = defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold)
		tabletCfg     = defaultTabletTxnConfig.WithStaleWriteThreshold(staleWriteThreshold)
	)
	kvc := kvcc.NewKVCCForTesting(db, tabletCfg)
	m := NewTransactionManager(kvc, txnManagerCfg)
	sc := smart_txn_client.NewSmartClient(m, 0)
	assert := types.NewAssertion(t)

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
				_, err := sc.DoTransactionRaw(ctx, types.TxnTypeDefault, func(ctx context.Context, txn types.Txn) (error, bool) {
					val, err := txn.Get(ctx, "k1", types.NewTxnReadOption())
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
				}, nil)
				assert.Equal(errors.ErrInject, err)
			} else {
				goodTxns.Add(1)
				assert.NoError(sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
					val, err := txn.Get(ctx, "k1", types.NewTxnReadOption())
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
	_ = flag.Set("v", fmt.Sprintf("%d", 1))

	for _, threshold := range []int{5, 10, 100, 1000} {
		for i := 0; i < rounds; i++ {
			if !testifyassert.True(t, testTxnLostUpdateWithSomeAborted2(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestTxnLostUpdateWithSomeAborted2 failed @round %d", i)
				return
			}
		}
	}
}

func testTxnLostUpdateWithSomeAborted2(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testTxnLostUpdateWithSomeAborted2 @round %d", round)

	var (
		txnManagerCfg = defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold)
		tabletCfg     = defaultTabletTxnConfig.WithStaleWriteThreshold(staleWriteThreshold)
	)
	db := memory.NewMemoryDB()
	kvcc := kvcc.NewKVCCForTesting(db, tabletCfg)
	m := NewTransactionManager(kvcc, txnManagerCfg)
	sc := smart_txn_client.NewSmartClient(m, 0)
	assert := types.NewAssertion(t)

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
				_, err := sc.DoTransactionRaw(ctx, types.TxnTypeDefault, func(ctx context.Context, txn types.Txn) (error, bool) {
					val, err := txn.Get(ctx, "k1", types.NewTxnReadOption())
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
					val, err := txn.Get(ctx, "k1", types.NewTxnReadOption())
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
							if err := txn.Set(ctx, key1, types.IntValue(v1).V); err != nil {
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
							if err := txn.Set(ctx, key1, types.IntValue(v1).V); err != nil {
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
							if err := txn.Set(ctx, key2, types.IntValue(v2).V); err != nil {
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
							if err := txn.Set(ctx, key2, types.IntValue(v2).V); err != nil {
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
							if err := txn.Set(ctx, key1, types.IntValue(v1).V); err != nil {
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
						writtenVal1 := types.IntValue(v1)
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
						writtenVal2 := types.IntValue(v2)
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
						writtenVal3 := types.IntValue(v3)
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
							writtenVal1 := types.IntValue(v1)
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
							writtenVal2 := types.IntValue(v2)
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
								if err := txn.Set(ctx, key1, types.IntValue(v1).V); err != nil {
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
		for i := 0; i < rounds; i++ {
			if !testifyassert.True(t, testDistributedTxnConsistencyIntegrate(t, i, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestDistributedTxnConsistencyIntegrate failed @round %d", i)
				return
			}
		}
	}
}

func testDistributedTxnConsistencyIntegrate(t *testing.T, round int, staleWriteThreshold time.Duration) (b bool) {
	assert := types.NewAssertion(t)
	t.Logf("testDistributedTxnConsistencyIntegrate @round %d", round)
	var (
		txnManagerCfg = defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold)
		tabletCfg     = defaultTabletTxnConfig.WithStaleWriteThreshold(staleWriteThreshold)
	)
	tms, clientTMs, stopper := createCluster(t, txnManagerCfg, tabletCfg)
	defer stopper()
	if !assert.Len(tms, 2) {
		return
	}
	//t.Logf("%v shard: %d, %v shard: %d", key1, gAte.MustRoute(key1).ID, key2, gAte.MustRoute(key2).ID)
	gAte := tms[0].tm.kv.(*gate.Gate)
	const key1, key2 = "k1", "k22"
	if !assert.NotEqual(gAte.MustRoute(key1).ID, gAte.MustRoute(key2).ID) {
		return
	}
	tm1, tm2 := clientTMs[0], clientTMs[1]
	sc1 := smart_txn_client.NewSmartClient(tm1, 10000)
	sc2 := smart_txn_client.NewSmartClient(tm2, 10000)

	return testDistributedTxnConsistencyIntegrateFunc(t, []*smart_txn_client.SmartClient{sc1, sc2})
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
	return testDistributedTxnConsistencyIntegrateFunc(t, []*smart_txn_client.SmartClient{sc1, sc2})
}

func TestDistributedTxnConsistencyIntegrateRedis(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 5))

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

	return testDistributedTxnConsistencyIntegrateFunc(t, []*smart_txn_client.SmartClient{sc1, sc2})
}

func testDistributedTxnConsistencyIntegrateFunc(t *testing.T, scs []*smart_txn_client.SmartClient) (b bool) {
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
					assert.NoError(sc1.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
						{
							key1Val, err := txn.Get(ctx, key1, types.NewTxnReadOption())
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
							key2Val, err := txn.Get(ctx, key2, types.NewTxnReadOption())
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
							key1Val, err := txn.Get(ctx, key1, types.NewTxnReadOption())
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
							key2Val, err := txn.Get(ctx, key2, types.NewTxnReadOption())
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

func TestTxnEncode(t *testing.T) {
	assert := types.NewAssertion(t)

	txn := NewTxn(123, types.TxnTypeReadForWrite, kvcc.NewKVCCForTesting(memory.NewMemoryDB(), defaultTabletTxnConfig), defaultTxnManagerConfig, &TransactionStore{}, nil, nil)
	txn.State = types.TxnStateRollbacking
	txn.InitializeWrittenKeys(ttypes.KeyVersions{"k1": 111, "k2": 222}, true)
	bytes := txn.Encode()
	t.Logf("txn:     %s", string(bytes))

	newTxn, err := DecodeTxn(123, bytes)
	assert.NoError(err)
	t.Logf("new_txn: %s", string(newTxn.Encode()))
	assert.Equal(txn.ID, newTxn.ID)
	assert.Equal(txn.Type, newTxn.Type)
	assert.Equal(txn.State, newTxn.State)
	assert.Equal(txn.GetWrittenKey2LastVersion(), newTxn.GetWrittenKey2LastVersion())
}
