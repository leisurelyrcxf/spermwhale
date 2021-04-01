package txn

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/leisurelyrcxf/spermwhale/types/basic"

	"github.com/leisurelyrcxf/spermwhale/consts"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
)

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
	return ts.CheckSerializability() && ts.CheckReadModifyWriteOnly(key)
}

func testTxnLostUpdateWriteAfterWrite(ctx context.Context, ts *TestCase) (b bool) {
	const (
		initialValue     = 101
		delta            = 6
		writeTimesPerTxn = 6
		key              = "k1"
	)
	if !ts.Truef(ts.GoRoutineNum&1 == 0, "ts.GoRoutineNum&1 != 0") {
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
	time.Sleep(time.Millisecond * 100)

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

func testTxnReadModifyWrite2Keys(ctx context.Context, ts *TestCase) (b bool) {
	const (
		key1InitialValue = 10
		key2InitialValue = 100
		delta            = 6

		key1, key2 = "k1", "k2"
	)
	sc := ts.scs[0]
	if err := sc.SetInt(ctx, key1, key1InitialValue); !ts.NoError(err) {
		return
	}
	if err := sc.SetInt(ctx, key2, key2InitialValue); !ts.NoError(err) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < ts.GoRoutineNum; i++ {
		wg.Add(1)
		go func(goRoutineIdx int) {
			defer wg.Done()

			ts.True(ts.DoTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
				{
					val, err := txn.Get(ctx, key1)
					if err != nil {
						return err
					}
					v1, err := val.Int()
					if !ts.NoError(err) {
						return err
					}
					if err := txn.Set(ctx, key1, types.NewIntValue(v1+delta).V); err != nil {
						return err
					}
				}
				{
					val, err := txn.Get(ctx, key2)
					if err != nil {
						return err
					}
					v2, err := val.Int()
					if !ts.NoError(err) {
						return err
					}
					if err := txn.Set(ctx, key2, types.NewIntValue(v2+delta).V); err != nil {
						return err
					}
				}
				return nil
			}))
		}(i)
	}

	wg.Wait()
	{
		v1, err := sc.GetInt(ctx, key1)
		if !ts.NoError(err) {
			return
		}
		ts.t.Logf("v1: %d", v1)
		if !ts.Equal(ts.GoRoutineNum*delta+key1InitialValue, v1) {
			return
		}
	}
	{
		v2, err := sc.GetInt(ctx, key2)
		if !ts.NoError(err) {
			return
		}
		ts.t.Logf("v2: %d", v2)
		if !ts.Equal(ts.GoRoutineNum*delta+key2InitialValue, v2) {
			return
		}
	}

	return ts.CheckSerializability() &&
		ts.CheckReadModifyWriteOnly(key1, key2)
}

func testTxnReadModifyWrite2KeysMGetMSet(ctx context.Context, ts *TestCase) (b bool) {
	const (
		key1InitialValue = 10
		key2InitialValue = 100

		key1, key2           = "k1", "k2"
		key1Delta, key2Delta = 6, 7
	)
	sc := ts.scs[0]
	if err := sc.MSetInts(ctx, []string{key1, key2}, []int{key1InitialValue, key2InitialValue}); !ts.NoError(err) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < ts.GoRoutineNum; i++ {
		wg.Add(1)
		go func(goRoutineIdx int) {
			defer wg.Done()

			ts.True(ts.DoTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
				values, err := txn.MGet(ctx, []string{key1, key2})
				if err != nil {
					return err
				}
				return txn.MSet(ctx, []string{key1, key2, key2}, [][]byte{types.NewIntValue(values[0].MustInt() + key1Delta).V, types.NewIntValue(values[1].MustInt() + key2Delta).V, types.NewIntValue(values[1].MustInt() + key2Delta).V})
			}))
		}(i)
	}

	wg.Wait()
	vs, err := sc.MGetInts(ctx, []string{key1, key2}, types.TxnTypeDefault)
	if !ts.NoError(err) {
		return
	}
	ts.t.Logf("'%s': %d, '%s': %d", key1, vs[0], key2, vs[1])
	if !ts.Equal(ts.GoRoutineNum*key1Delta+key1InitialValue, vs[0]) ||
		!ts.Equal(ts.GoRoutineNum*key2Delta+key2InitialValue, vs[1]) {
		return
	}
	return ts.CheckSerializability() &&
		ts.CheckReadModifyWriteOnly(key1, key2)
}

func testTxnReadModifyWrite2KeysDeadlock(ctx context.Context, ts *TestCase) (b bool) {
	const (
		key1InitialValue = 10
		key2InitialValue = 100
		delta            = 6

		key1, key2 = "k1", "k2"
	)
	sc := ts.scs[0]
	if err := sc.SetInt(ctx, key1, key1InitialValue); !ts.NoError(err) {
		return
	}
	if err := sc.SetInt(ctx, key2, key2InitialValue); !ts.NoError(err) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < ts.GoRoutineNum; i++ {
		wg.Add(1)

		go func(goRoutineIdx int) {
			defer wg.Done()

			ts.True(ts.DoTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
				if goRoutineIdx&1 == 1 {
					{
						val, err := txn.Get(ctx, key1)
						if err != nil {
							return err
						}
						v1, err := val.Int()
						if !ts.NoError(err) {
							return err
						}
						if err := txn.Set(ctx, key1, types.NewIntValue(v1+delta).V); err != nil {
							return err
						}
					}
					{
						val, err := txn.Get(ctx, key2)
						if err != nil {
							return err
						}
						v2, err := val.Int()
						if !ts.NoError(err) {
							return err
						}
						if err := txn.Set(ctx, key2, types.NewIntValue(v2+delta).V); err != nil {
							return err
						}
					}
				} else {
					{
						val, err := txn.Get(ctx, key2)
						if err != nil {
							return err
						}
						v2, err := val.Int()
						if !ts.NoError(err) {
							return err
						}
						if err := txn.Set(ctx, key2, types.NewIntValue(v2+delta).V); err != nil {
							return err
						}
					}
					{
						val, err := txn.Get(ctx, key1)
						if err != nil {
							return err
						}
						v1, err := val.Int()
						if !ts.NoError(err) {
							return err
						}
						if err := txn.Set(ctx, key1, types.NewIntValue(v1+delta).V); err != nil {
							return err
						}
					}
				}
				return nil
			}))
		}(i)
	}

	wg.Wait()
	{
		v1, err := sc.GetInt(ctx, key1)
		if !ts.NoError(err) {
			return
		}
		ts.t.Logf("v1: %d", v1)
		if !ts.Equal(ts.GoRoutineNum*delta+key1InitialValue, v1) {
			return
		}
	}
	{
		v2, err := sc.GetInt(ctx, key2)
		if !ts.NoError(err) {
			return
		}
		ts.t.Logf("v2: %d", v2)
		if !ts.Equal(ts.GoRoutineNum*delta+key2InitialValue, v2) {
			return
		}
	}

	return ts.CheckSerializability() &&
		ts.CheckReadModifyWriteOnly(key1, key2)
}

func testTxnReadModifyWrite2KeysDeadlockMGetMSet(ctx context.Context, ts *TestCase) (b bool) {
	const (
		key1InitialValue = 10
		key2InitialValue = 100

		key1, key2           = "k1", "k2"
		key1Delta, key2Delta = 6, 7
	)
	sc := ts.scs[0]
	if err := sc.MSetInts(ctx, []string{key1, key2}, []int{key1InitialValue, key2InitialValue}); !ts.NoError(err) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < ts.GoRoutineNum; i++ {
		wg.Add(1)
		go func(goRoutineIdx int) {
			defer wg.Done()

			if goRoutineIdx&1 == 1 {
				ts.True(ts.DoTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
					values, err := txn.MGet(ctx, []string{key1, key2})
					if err != nil {
						return err
					}
					return txn.MSet(ctx, []string{key1, key2}, [][]byte{types.NewIntValue(values[0].MustInt() + key1Delta).V, types.NewIntValue(values[1].MustInt() + key2Delta).V})
				}))
			} else {
				ts.True(ts.DoTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
					values, err := txn.MGet(ctx, []string{key2, key1})
					if err != nil {
						return err
					}
					return txn.MSet(ctx, []string{key2, key1}, [][]byte{types.NewIntValue(values[0].MustInt() + key2Delta).V, types.NewIntValue(values[1].MustInt() + key1Delta).V})
				}))
			}
		}(i)
	}

	wg.Wait()
	vs, err := sc.MGetInts(ctx, []string{key1, key2}, types.TxnTypeDefault)
	if !ts.NoError(err) {
		return
	}
	ts.t.Logf("'%s': %d, '%s': %d", key1, vs[0], key2, vs[1])
	if !ts.Equal(ts.GoRoutineNum*key1Delta+key1InitialValue, vs[0]) ||
		!ts.Equal(ts.GoRoutineNum*key2Delta+key2InitialValue, vs[1]) {
		return
	}
	return ts.CheckSerializability() &&
		ts.CheckReadModifyWriteOnly(key1, key2)
}

func testTxnReadModifyWriteNKeys(ctx context.Context, ts *TestCase) (b bool) {
	const (
		initialValue = 10
		delta        = 6
		N            = 10
	)

	var (
		sc      = ts.scs[0]
		allKeys = make([]string, 0, N)
	)
	for i := 0; i < N; i++ {
		var key = fmt.Sprintf("k%d", i)
		if err := sc.SetInt(ctx, key, initialValue); !ts.NoError(err) {
			return
		}
		allKeys = append(allKeys, key)
	}

	var wg sync.WaitGroup
	for i := 0; i < ts.GoRoutineNum; i++ {
		wg.Add(1)

		go func(goRoutineIdx int) {
			defer wg.Done()

			ts.True(ts.DoTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
				for i := 0; i < N; i++ {
					var key = fmt.Sprintf("k%d", i)
					val, err := txn.Get(ctx, key)
					if err != nil {
						return err
					}
					v, err := val.Int()
					if !ts.NoError(err) {
						return err
					}
					if err := txn.Set(ctx, key, types.NewIntValue(v+delta).V); err != nil {
						return err
					}
				}
				return nil
			}))
		}(i)
	}

	wg.Wait()
	{
		for _, key := range allKeys {
			v, err := sc.GetInt(ctx, key)
			if !ts.NoError(err) {
				return
			}
			ts.t.Logf("%s: %d", key, v)
			if !ts.Equal(ts.GoRoutineNum*delta+initialValue, v) {
				return
			}
		}
	}

	return ts.CheckSerializability() && ts.CheckReadModifyWriteOnly(allKeys...)
}

func testTxnLostUpdateModAdd(ctx context.Context, ts *TestCase) (b bool) {
	const (
		initialValue1 = 101
		initialValue2 = 222
		delta         = 6
	)
	sc := ts.scs[0]
	if err := sc.SetInt(ctx, "k1", initialValue1); !ts.NoError(err) {
		return
	}
	if err := sc.SetInt(ctx, "k22", initialValue2); !ts.NoError(err) {
		return
	}

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
	for i := 0; i < ts.GoRoutineNum; i++ {
		wg.Add(1)
		go func(goRoutineIdx int) {
			defer wg.Done()

			var (
				g1, g2 func(int) int
			)
			if ts.True(ts.DoTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
				{
					val, err := txn.Get(ctx, "k1")
					if err != nil {
						return err
					}
					v1, err := val.Int()
					if !ts.NoError(err) {
						return err
					}
					g1, v1 = selector(goRoutineIdx, v1)
					if err := txn.Set(ctx, "k1", types.NewIntValue(v1).V); err != nil {
						return err
					}
				}

				{
					val, err := txn.Get(ctx, "k22")
					if err != nil {
						return err
					}
					v2, err := val.Int()
					if !ts.NoError(err) {
						return err
					}
					g2, v2 = selector(goRoutineIdx+1, v2)
					if err := txn.Set(ctx, "k22", types.NewIntValue(v2).V); err != nil {
						return err
					}
				}
				return nil
			})) {
				ts.executedTxnsPerGoRoutine[goRoutineIdx][len(ts.executedTxnsPerGoRoutine[goRoutineIdx])-1].AdditionalInfo = [2]func(int) int{g1, g2}
			}
		}(i)
	}

	wg.Wait()
	val1, err := sc.GetInt(ctx, "k1")
	if !ts.NoError(err) {
		return
	}
	val2, err := sc.GetInt(ctx, "k22")
	if !ts.NoError(err) {
		return
	}
	ts.t.Logf("res_v1: %d, res_v2: %d", val1, val2)

	if !ts.CheckSerializability() {
		return false
	}
	if !ts.CheckReadModifyWriteOnly("k1", "k22") {
		return false
	}

	v1, v2 := initialValue1, initialValue2
	for _, txn := range ts.allExecutedTxns {
		g1, g2 := txn.AdditionalInfo.([2]func(int) int)[0], txn.AdditionalInfo.([2]func(int) int)[1]
		v1 = g1(v1)
		v2 = g2(v2)
	}
	ts.t.Logf("replay_res_v1 :%d, replay_res_v2 :%d", v1, v2)
	return ts.Equal(v1, val1) && ts.Equal(v2, val2)
}

func testTxnReadWriteAfterWrite(ctx context.Context, ts *TestCase) (b bool) {
	const (
		initialValue = 101
		delta        = 6
	)
	sc := ts.scs[0]
	if err := sc.SetInt(ctx, "k1", initialValue); !ts.NoError(err) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < ts.GoRoutineNum; i++ {
		wg.Add(1)

		go func(goRoutineIdx int) {
			defer wg.Done()

			ts.True(ts.DoTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
				val, err := txn.Get(ctx, "k1")
				if err != nil {
					return err
				}
				v1, err := val.Int()
				if !ts.NoError(err) {
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
				if !ts.NoError(err) {
					return err
				}
				if !ts.Equalf(v1, v2, "read_version: %d, txn_version: %d", val2.Version, txn.GetId()) {
					return errors.ErrAssertFailed
				}
				return nil
			}))
		}(i)
	}

	wg.Wait()
	val, err := sc.GetInt(ctx, "k1")
	if !ts.NoError(err) {
		return
	}
	ts.t.Logf("k1: %d", val)
	if !ts.Equal(ts.GoRoutineNum*delta+initialValue, val) {
		return
	}

	return ts.CheckSerializability()
}

func testTxnReadWriteAfterWriteMSet(ctx context.Context, ts *TestCase) (b bool) {
	const (
		initialValue = 101
		delta        = 6
	)
	sc := ts.scs[0]
	if err := sc.SetInt(ctx, "k1", initialValue); !ts.NoError(err) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < ts.GoRoutineNum; i++ {
		wg.Add(1)

		go func(goRoutineIdx int) {
			defer wg.Done()

			ts.True(ts.DoTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
				val, err := txn.Get(ctx, "k1")
				if err != nil {
					return err
				}
				v1, err := val.Int()
				if !ts.NoError(err) {
					return err
				}
				v1 += delta

				if err := txn.MSet(ctx, []string{"k1", "k1", "k1"}, [][]byte{types.NewIntValue(v1 - 1).V, types.NewIntValue(v1 - 3).V, types.NewIntValue(v1).V}); err != nil {
					return err
				}
				val2, err := txn.Get(ctx, "k1")
				if err != nil {
					return err
				}
				v2, err := val2.Int()
				if !ts.NoError(err) {
					return err
				}
				if !ts.Equalf(v1, v2, "read_version: %d, txn_version: %d", val2.Version, txn.GetId()) {
					return errors.ErrAssertFailed
				}
				return nil
			}))
		}(i)
	}

	wg.Wait()
	val, err := sc.GetInt(ctx, "k1")
	if !ts.NoError(err) {
		return
	}
	ts.t.Logf("k1: %d", val)
	if !ts.Equal(ts.GoRoutineNum*delta+initialValue, val) {
		return
	}

	return ts.CheckSerializability()
}

func testTxnLostUpdateWithSomeAbortedCommitFailed(ctx context.Context, ts *TestCase, abortFactor int) (b bool) {
	const (
		initialValue = 101
		delta        = 6
	)

	var (
		wg                     sync.WaitGroup
		goodTxns               basic.AtomicUint64
		abortedTxnPerGoRoutine = make([]types.Txn, ts.GoRoutineNum)
		sc                     = ts.scs[0]
	)
	if err := sc.SetInt(ctx, "k1", initialValue); !ts.NoError(err) {
		return
	}
	for i := 0; i < ts.GoRoutineNum; i++ {
		wg.Add(1)

		go func(goRoutineIdx int) {
			defer wg.Done()

			if goRoutineIdx%abortFactor == 0 {
				ts.SetSkipRoundCheck(goRoutineIdx)
				if tx, _, err := sc.DoTransactionRaw(ctx, types.NewTxnOption(ts.TxnType), func(ctx context.Context, txn types.Txn) (error, bool) {
					val, err := txn.Get(ctx, "k1")
					if err != nil {
						return err, true
					}
					v1, err := val.Int()
					if !ts.NoError(err) {
						return err, true
					}
					return txn.Set(ctx, "k1", types.NewIntValue(v1+delta).V), true
				}, func() error {
					return errors.ErrInject
				}, nil); ts.Equal(errors.ErrInject, err) {
					abortedTxnPerGoRoutine[goRoutineIdx] = tx
				}
			} else {
				goodTxns.Add(1)
				ts.True(ts.DoTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
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
			}
		}(i)
	}

	wg.Wait()
	val, err := sc.GetInt(ctx, "k1")
	if !ts.NoError(err) {
		return
	}
	ts.t.Logf("val: %d", val)
	if !ts.Equal(int(goodTxns.Get())*delta+initialValue, val) {
		return
	}
	kv := ts.txnManagers[0].(*TransactionManager).kv
	for _, txn := range abortedTxnPerGoRoutine {
		if txn != nil {
			if _, err := kv.Get(ctx, "k1", types.NewKVCCReadOption(txn.GetId().Version()).WithExactVersion(txn.GetId().Version()).
				WithNotGetMaxReadVersion().WithNotUpdateTimestampCache()); !ts.Equal(consts.ErrCodeKeyOrVersionNotExists, errors.GetErrorCode(err)) {
				return
			}
		}
	}
	return ts.CheckSerializability() && ts.Equal(int(goodTxns.Get()), ts.allExecutedTxns.Len())
}

func testTxnLostUpdateWithSomeAbortedRollbackFailed(ctx context.Context, ts *TestCase, abortFactor int) (b bool) {
	const (
		initialValue = 101
		delta        = 6
	)

	var (
		wg                     sync.WaitGroup
		goodTxns               basic.AtomicUint64
		abortedTxnPerGoRoutine = make([]types.Txn, ts.GoRoutineNum)
		sc                     = ts.scs[0]
	)
	if err := sc.SetInt(ctx, "k1", initialValue); !ts.NoError(err) {
		return
	}
	for i := 0; i < ts.GoRoutineNum; i++ {
		wg.Add(1)

		go func(goRoutineIdx int) {
			defer wg.Done()

			if goRoutineIdx%abortFactor == 0 {
				ts.SetSkipRoundCheck(goRoutineIdx)

				start := time.Now()
				if tx, retryTimes, err := sc.DoTransactionRaw(ctx, types.NewTxnOption(ts.TxnType), func(ctx context.Context, txn types.Txn) (error, bool) {
					val, err := txn.Get(ctx, "k1")
					if err != nil {
						return err, true
					}
					v1, err := val.Int()
					if !ts.NoError(err) {
						return err, true
					}
					return txn.Set(ctx, "k1", types.NewIntValue(v1+delta).V), true
				}, nil, func() error {
					return errors.ErrInject
				}); err == nil {
					ts.CollectExecutedTxnInfo(goRoutineIdx, tx, retryTimes, time.Since(start))
					goodTxns.Add(1)
				} else if ts.Equal(errors.ErrInject, err) {
					abortedTxnPerGoRoutine[goRoutineIdx] = tx
				}
			} else {
				if ts.True(ts.DoTransaction(ctx, goRoutineIdx, sc, func(ctx context.Context, txn types.Txn) error {
					val, err := txn.Get(ctx, "k1")
					if err != nil {
						return err
					}
					v1, err := val.Int()
					if !ts.NoError(err) {
						return err
					}
					return txn.Set(ctx, "k1", types.NewIntValue(v1+delta).V)
				})) {
					goodTxns.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()
	val, err := sc.GetInt(ctx, "k1")
	if !ts.NoError(err) {
		return
	}
	ts.t.Logf("val: %d", val)
	if !ts.Equal(int(goodTxns.Get())*delta+initialValue, val) {
		return
	}
	kv := ts.txnManagers[0].(*TransactionManager).kv
	for _, txn := range abortedTxnPerGoRoutine {
		if txn != nil {
			if _, err := kv.Get(ctx, "k1", types.NewKVCCReadOption(txn.GetId().Version()).WithExactVersion(txn.GetId().Version()).
				WithNotGetMaxReadVersion().WithNotUpdateTimestampCache()); !ts.Equal(consts.ErrCodeKeyOrVersionNotExists, errors.GetErrorCode(err)) {
				return
			}
		}
	}
	ts.t.Logf("good txns: %d", goodTxns.Get())
	return ts.CheckSerializability() && ts.Equal(int(goodTxns.Get()), ts.allExecutedTxns.Len())
}
