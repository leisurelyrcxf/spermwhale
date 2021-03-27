package txn

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/kv/impl/memory"
	"github.com/leisurelyrcxf/spermwhale/kvcc"
	"github.com/leisurelyrcxf/spermwhale/txn/smart_txn_client"
	"github.com/leisurelyrcxf/spermwhale/txn/ttypes"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
)

func TestTxnLostUpdate(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnLostUpdate).SetStaleWriteThreshold(time.Millisecond * 10).SetStaleWriteThreshold(time.Millisecond * 10).Run()
}

func TestTxnLostUpdateReadModifyWrite(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnLostUpdate).SetTxnType(types.TxnTypeReadModifyWrite).Run()
}

func TestTxnLostUpdateWaitWhenReadDirty(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnLostUpdate).SetTxnType(types.TxnTypeWaitWhenReadDirty).SetStaleWriteThreshold(time.Millisecond * 10).Run()
}

func TestTxnLostUpdateReadModifyWriteWaitWhenReadDirty(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnLostUpdate).SetTxnType(types.TxnTypeReadModifyWrite | types.TxnTypeWaitWhenReadDirty).Run()
}

func TestTxnLostUpdateInjectErr(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnLostUpdate).SetStaleWriteThreshold(time.Millisecond * 10).SetStaleWriteThreshold(time.Millisecond * 10).
		SetGoRoutineNum(100).SetFailurePattern(FailurePatternAll).SetFailureProbability(10).Run()
}

func TestTxnLostUpdateWriteAfterWrite(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnLostUpdateWriteAfterWrite).SetStaleWriteThreshold(time.Millisecond * 10).Run()
}

func TestTxnLostUpdateWriteAfterWriteSnapshotRead(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnLostUpdateWriteAfterWrite).SetStaleWriteThreshold(time.Millisecond * 10).
		SetReadOnlyTxnType(types.TxnTypeSnapshotRead).Run()
}

func testTxnLostUpdate(ctx context.Context, ts *TestCase) (b bool) {
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
				ts.True(ts.DoTransaction(ctx, goRoutineIndex, sc1, func(ctx context.Context, txn types.Txn) error {
					val, err := txn.Get(ctx, key)
					if err != nil {
						return err
					}
					v1, err := val.Int()
					if !ts.NoError(err) {
						return err
					}
					return txn.Set(ctx, key, types.NewIntValue(v1+delta).V)
				}))
			}
		}(i)
	}

	wg.Wait()
	val, err := sc1.GetInt(ctx, key)
	if !ts.NoError(err) {
		return
	}
	ts.t.Logf("val: %d", val)
	if !ts.Equal(ts.GoRoutineNum*ts.TxnNumPerGoRoutine*delta+initialValue, val) {
		return
	}
	return ts.CheckSerializability() && ts.allExecutedTxns.CheckReadForWriteOnly(ts.Assertions, key)
}

func testTxnLostUpdateWriteAfterWrite(ctx context.Context, ts *TestCase) (b bool) {
	const (
		initialValue     = 101
		delta            = 6
		writeTimesPerTxn = 6
		key              = "k1"
	)
	if !ts.Truef(ts.GoRoutineNum&1 == 0, "goRoutineNumber&1 != 0") {
		return
	}
	sc := ts.scs[0]
	ts.SetExtraRound(0)
	if !ts.True(ts.DoTransaction(ctx, 0, sc, func(ctx context.Context, txn types.Txn) error {
		for i := 0; i < writeTimesPerTxn; i++ {
			if err := txn.Set(ctx, key, types.NewIntValue(initialValue).V); err != nil {
				ts.NoError(err)
				return err
			}
		}
		return nil
	})) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < ts.GoRoutineNum; i++ {
		wg.Add(1)

		go func(goRoutineIdx int) {
			defer wg.Done()

			if goRoutineIdx&1 == 1 {
				ts.True(ts.DoTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
					val, err := txn.Get(ctx, key)
					if err != nil {
						return err
					}
					v, err := val.Int()
					if !ts.NoError(err) {
						return err
					}
					for i := 0; i < writeTimesPerTxn; i++ {
						v += delta
						if err := txn.Set(ctx, key, types.NewIntValue(v).V); err != nil {
							return err
						}
					}
					return nil
				}))
			} else {
				ts.True(ts.DoReadOnlyTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
					_, err := txn.Get(ctx, key)
					return err
				}))
			}
		}(i)
	}

	wg.Wait()
	val, err := sc.GetInt(ctx, key)
	if !ts.NoError(err) {
		return
	}
	ts.t.Logf("key value '%s': %d", key, val)
	if !ts.Equal((ts.GoRoutineNum/2)*delta*writeTimesPerTxn+initialValue, val) {
		return
	}

	if !ts.CheckSerializability() {
		return
	}
	kv := sc.TxnManager.(*TransactionManager).kv
	for idx, cur := range ts.allExecutedTxns {
		if _, isWrite := cur.WriteValues[key]; !isWrite {
			continue
		}
		val, err := kv.Get(ctx, key, types.NewKVCCReadOption(cur.ID).WithExactVersion(cur.ID).WithNotGetMaxReadVersion().WithNotGetMaxReadVersion())
		if !ts.NoError(err) {
			return
		}
		if !ts.Equal(cur.ID, val.Version) {
			return
		}
		if !ts.Equal(types.TxnInternalVersion(writeTimesPerTxn), val.InternalVersion) {
			return
		}
		if !ts.False(val.IsDirty()) {
			return
		}
		if idx >= 1 {
			if !ts.Equal(types.TxnInternalVersion(writeTimesPerTxn), cur.ReadValues[key].InternalVersion) {
				return false
			}
		}
	}

	return true
}

func TestTxnLostUpdateWriteAfterWriteOverflow(t *testing.T) {
	const (
		initialValue    = 101
		goRoutineNumber = 10000
		key             = "k1"
	)
	if goRoutineNumber&1 != 0 {
		panic("goRoutineNumber&1 != 0")
	}
	db := memory.NewMemoryDB()
	kvc := kvcc.NewKVCCForTesting(db, defaultTabletTxnConfig.WithStaleWriteThreshold(time.Second))
	m := NewTransactionManager(kvc, defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(time.Second))
	sc := smart_txn_client.NewSmartClient(m, 0)
	defer sc.Close()
	assert := types.NewAssertion(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	assert.NoError(sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
		for i := 0; i < consts.MaxTxnInternalVersion; i++ {
			if err := txn.Set(ctx, key, types.NewIntValue(initialValue).V); err != nil {
				return err
			}
		}
		return nil
	}))
	val, err := sc.Get(ctx, key)
	assert.NoError(err)
	assert.Equal(types.TxnInternalVersionMax, val.InternalVersion)
	assert.Equal(types.TxnInternalVersion(254), val.InternalVersion)
	assert.Equal(errors.ErrTransactionInternalVersionOverflow, sc.DoTransaction(ctx, func(ctx context.Context, txn types.Txn) error {
		for i := 0; i < consts.PositiveInvalidTxnInternalVersion; i++ {
			if err := txn.Set(ctx, key, types.NewIntValue(initialValue).V); err != nil {
				return err
			}
		}
		return nil
	}))
}

func TestTxnReadModifyWrite2Keys(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWrite2KeysOneRound).Run()
}

func TestTxnReadModifyWrite2KeysWaitWhenReadDirty(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWrite2KeysOneRound).SetTxnType(types.TxnTypeWaitWhenReadDirty).Run()
}

func TestTxnReadModifyWrite2KeysReadModifyWrite(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWrite2KeysOneRound).SetTxnType(types.TxnTypeReadModifyWrite).Run()
}

func TestTxnReadModifyWrite2KeysReadModifyWriteWaitWhenReadDirty(t *testing.T) {
	NewEmbeddedTestCase(t, rounds, testTxnReadModifyWrite2KeysOneRound).SetTxnType(types.TxnTypeReadModifyWrite | types.TxnTypeWaitWhenReadDirty).Run()
}

func testTxnReadModifyWrite2KeysOneRound(ctx context.Context, ts *TestCase) (b bool) {
	const (
		key1InitialValue = 10
		key2InitialValue = 100
		delta            = 6
	)
	sc := ts.scs[0]
	err := sc.SetInt(ctx, "k1", key1InitialValue)
	if !ts.NoError(err) {
		return
	}
	err = sc.SetInt(ctx, "k2", key2InitialValue)
	if !ts.NoError(err) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < ts.GoRoutineNum; i++ {
		wg.Add(1)
		go func(goRoutineIdx int) {
			defer wg.Done()

			ts.True(ts.DoTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
				{
					val, err := txn.Get(ctx, "k1")
					if err != nil {
						return err
					}
					v1, err := val.Int()
					if !ts.NoError(err) {
						return err
					}
					if err := txn.Set(ctx, "k1", types.NewIntValue(v1+delta).V); err != nil {
						return err
					}
				}
				{
					val, err := txn.Get(ctx, "k2")
					if err != nil {
						return err
					}
					v2, err := val.Int()
					if !ts.NoError(err) {
						return err
					}
					if err := txn.Set(ctx, "k2", types.NewIntValue(v2+delta).V); err != nil {
						return err
					}
				}
				return nil
			}))
		}(i)
	}

	wg.Wait()
	{
		v1, err := sc.GetInt(ctx, "k1")
		if !ts.NoError(err) {
			return
		}
		ts.t.Logf("v1: %d", v1)
		if !ts.Equal(ts.GoRoutineNum*delta+key1InitialValue, v1) {
			return
		}
	}
	{
		v2, err := sc.GetInt(ctx, "k2")
		if !ts.NoError(err) {
			return
		}
		ts.t.Logf("v2: %d", v2)
		if !ts.Equal(ts.GoRoutineNum*delta+key2InitialValue, v2) {
			return
		}
	}

	if !ts.CheckSerializability() {
		return
	}
	txns := ts.allExecutedTxns
	for i := 1; i < len(txns); i++ {
		prev, cur := txns[i-1], txns[i]
		if readVal := cur.ReadValues["k1"]; !ts.Equal(prev.ID, readVal.Version) || !ts.EqualValue(prev.WriteValues["k1"], readVal.WithNoWriteIntent()) {
			return
		}
		if readVal := cur.ReadValues["k2"]; !ts.Equal(prev.ID, readVal.Version) || !ts.EqualValue(prev.WriteValues["k2"], readVal.WithNoWriteIntent()) {
			return
		}
	}
	return true
}

func TestTxnReadModifyWrite2KeysDeadlock(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 4))

	for _, threshold := range []int{1000} {
		for i := 0; i < rounds; i++ {
			if !testifyassert.True(t, testTxnReadModifyWrite2KeysDeadlockOneRound(t, i, types.TxnTypeDefault, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestTxnReadModifyWrite2KeysDeadlock failed @round %d, staleWriteThreshold: %s", i, time.Millisecond*time.Duration(threshold))
				return
			}
		}
	}
}

func testTxnReadModifyWrite2KeysDeadlockOneRound(t *testing.T, round int, txnType types.TxnType, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testTxnReadModifyWrite2KeysDeadlockOneRound @round %d, staleWriteThreshold: %s", round, staleWriteThreshold)

	db := memory.NewMemoryDB()
	kvc := kvcc.NewKVCCForTesting(db, defaultTabletTxnConfig.WithStaleWriteThreshold(staleWriteThreshold))
	m := NewTransactionManager(kvc, defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold)).SetRecordValuesTxn(true)
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

			if tx, _, err := sc.DoTransactionOfTypeEx(ctx, txnType, func(ctx context.Context, txn types.Txn) error {
				if i&1 == 1 {
					{
						val, err := txn.Get(ctx, "k1")
						if err != nil {
							return err
						}
						v1, err := val.Int()
						if !assert.NoError(err) {
							return err
						}
						if err := txn.Set(ctx, "k1", types.NewIntValue(v1+delta).V); err != nil {
							return err
						}
					}
					{
						val, err := txn.Get(ctx, "k2")
						if err != nil {
							return err
						}
						v2, err := val.Int()
						if !assert.NoError(err) {
							return err
						}
						if err := txn.Set(ctx, "k2", types.NewIntValue(v2+delta).V); err != nil {
							return err
						}
					}
				} else {
					{
						val, err := txn.Get(ctx, "k2")
						if err != nil {
							return err
						}
						v2, err := val.Int()
						if !assert.NoError(err) {
							return err
						}
						if err := txn.Set(ctx, "k2", types.NewIntValue(v2+delta).V); err != nil {
							return err
						}
					}
					{
						val, err := txn.Get(ctx, "k1")
						if err != nil {
							return err
						}
						v1, err := val.Int()
						if !assert.NoError(err) {
							return err
						}
						if err := txn.Set(ctx, "k1", types.NewIntValue(v1+delta).V); err != nil {
							return err
						}
					}
				}
				return nil
			}); assert.NoError(err) {
				txns[i] = NewExecuteInfo(tx, 0, 0)
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

func TestTxnReadModifyWriteNKeysOneRound(t *testing.T) {
	_ = flag.Set("logtostderr", fmt.Sprintf("%t", true))
	_ = flag.Set("v", fmt.Sprintf("%d", 4))

	for _, threshold := range []int{1000} {
		for i := 0; i < rounds; i++ {
			if !testifyassert.True(t, testTxnReadModifyWriteNKeysOneRound(t, i, types.TxnTypeDefault, time.Millisecond*time.Duration(threshold))) {
				t.Errorf("TestTxnReadModifyWriteNKeysOneRound failed @round %d, staleWriteThreshold: %s", i, time.Millisecond*time.Duration(threshold))
				return
			}
		}
	}
}

func testTxnReadModifyWriteNKeysOneRound(t *testing.T, round int, txnType types.TxnType, staleWriteThreshold time.Duration) (b bool) {
	t.Logf("testTxnReadModifyWriteNKeysOneRound @round %d, staleWriteThreshold: %s", round, staleWriteThreshold)

	db := memory.NewMemoryDB()
	kvc := kvcc.NewKVCCForTesting(db, defaultTabletTxnConfig.WithStaleWriteThreshold(staleWriteThreshold))
	m := NewTransactionManager(kvc, defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold)).SetRecordValuesTxn(true)
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

			if tx, _, err := sc.DoTransactionOfTypeEx(ctx, txnType, func(ctx context.Context, txn types.Txn) error {
				for i := 0; i < N; i++ {
					var key = fmt.Sprintf("k%d", i)
					val, err := txn.Get(ctx, key)
					if err != nil {
						return err
					}
					v, err := val.Int()
					if !assert.NoError(err) {
						return err
					}
					if err := txn.Set(ctx, key, types.NewIntValue(v+delta).V); err != nil {
						return err
					}
				}
				return nil
			}); assert.NoError(err) {
				txns[i] = NewExecuteInfo(tx, 0, 0)
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
	m := NewTransactionManager(kvcc, defaultTxnManagerConfig.WithWoundUncommittedTxnThreshold(staleWriteThreshold)).SetRecordValuesTxn(true)
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
					val, err := txn.Get(ctx, "k1")
					if err != nil {
						return err
					}
					readValues["k1"] = val
					v1, err := val.Int()
					if !assert.NoError(err) {
						return err
					}
					g1, v1 = selector(i, v1)
					writeValue := types.NewIntValue(v1).WithVersion(txn.GetId().Version())
					writeValues["k1"] = writeValue
					if err := txn.Set(ctx, "k1", writeValue.V); err != nil {
						return err
					}
				}

				{
					val, err := txn.Get(ctx, "k22")
					if err != nil {
						return err
					}
					readValues["k22"] = val
					v2, err := val.Int()
					if !assert.NoError(err) {
						return err
					}
					g2, v2 = selector(i+1, v2)
					writeValue := types.NewIntValue(v2).WithVersion(txn.GetId().Version())
					writeValues["k22"] = writeValue
					if err := txn.Set(ctx, "k22", writeValue.V); err != nil {
						return err
					}
				}
				return nil
			}); assert.NoError(err) {
				txns[i] = NewExecuteInfo(tx, 0, 0).WithAdditionalInfo([2]func(int) int{g1, g2})
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
				val, err := txn.Get(ctx, "k1")
				if err != nil {
					return err
				}
				v1, err := val.Int()
				if !assert.NoError(err) {
					return err
				}
				v1 += delta

				if err := txn.Set(ctx, "k1", types.NewIntValue(v1-1).V); err != nil {
					return err
				}
				if err := txn.Set(ctx, "k1", types.NewIntValue(v1-3).V); err != nil {
					return err
				}
				if err := txn.Set(ctx, "k1", types.NewIntValue(v1).V); err != nil {
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
				_, _, err := sc.DoTransactionRaw(ctx, types.NewDefaultTxnOption(), func(ctx context.Context, txn types.Txn) (error, bool) {
					val, err := txn.Get(ctx, "k1")
					if err != nil {
						return err, true
					}
					v1, err := val.Int()
					if !assert.NoError(err) {
						return err, true
					}
					return txn.Set(ctx, "k1", types.NewIntValue(v1+delta).V), true
				}, func() error {
					return errors.ErrInject
				}, nil)
				assert.Equal(errors.ErrInject, err)
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
					return txn.Set(ctx, "k1", types.NewIntValue(v1+delta).V)
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
				_, _, err := sc.DoTransactionRaw(ctx, types.NewDefaultTxnOption(), func(ctx context.Context, txn types.Txn) (error, bool) {
					val, err := txn.Get(ctx, "k1")
					if err != nil {
						return err, true
					}
					v1, err := val.Int()
					if !assert.NoError(err) {
						return err, true
					}
					return txn.Set(ctx, "k1", types.NewIntValue(v1+delta).V), true
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
					return txn.Set(ctx, "k1", types.NewIntValue(v1+delta).V)
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

func TestTxnEncode(t *testing.T) {
	assert := types.NewAssertion(t)

	txn := NewTxn(123, types.TxnTypeReadModifyWrite, kvcc.NewKVCCForTesting(memory.NewMemoryDB(), defaultTabletTxnConfig), defaultTxnManagerConfig, &TransactionStore{}, nil, nil)
	txn.TxnState = types.TxnStateRollbacking
	txn.InitializeWrittenKeys(ttypes.KeyVersions{"k1": 111, "k2": 222}, true)
	bytes := txn.Encode()
	t.Logf("txn:     %s", string(bytes))

	newTxn, err := DecodeTxn(123, bytes)
	assert.NoError(err)
	t.Logf("new_txn: %s", string(newTxn.Encode()))
	assert.Equal(txn.ID, newTxn.ID)
	assert.Equal(txn.TxnType, newTxn.TxnType)
	assert.Equal(txn.TxnState, newTxn.TxnState)
	assert.Equal(txn.GetWrittenKey2LastVersion(), newTxn.GetWrittenKey2LastVersion())
}
