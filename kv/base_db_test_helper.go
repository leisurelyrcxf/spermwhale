package kv

import (
	"context"
	"flag"
	"sync"
	"time"

	"github.com/golang/glog"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

const ClearValueMetaBitMaskCommitted = (^consts.ValueMetaBitMaskCommitted) & 0xff

func newValueWithoutWriteIntent(val []byte, version uint64) types.Value {
	return types.Value{
		Meta: types.Meta{
			Version: version,
		},
		V: val,
	}
}

func RunTestCase(t types.T, rounds int, newDB func() (*DB, error), testCase func(t types.T, db *DB) bool) (b bool) {
	testifyassert.NoError(t, flag.Set("logtostderr", "true"))
	testifyassert.NoError(t, flag.Set("v", "10"))

	db, err := newDB()
	if err != nil {
		t.Errorf("%s construct db failed: '%v'", t.Name(), err)
		return
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("%s close db failed: '%v'", t.Name(), err)
			b = false
		}
	}()

	for i := 1; i <= rounds; i++ {
		if !testCase(t, db) {
			t.Errorf("%s failed @round %d", t.Name(), i)
			return
		}
		if i%100 == 1 || i == rounds {
			t.Logf("%s round %d succeeded", t.Name(), i)
		}
	}
	return true
}

func TestDB(t types.T, db *DB) (b bool) {
	for _, dbug := range []bool{true, false} {
		utils.SetCustomizedDebugFlag(dbug)
		if !testKeyStore(t, db) {
			t.Logf("testKeyStore failed, debug: %v", dbug)
			return
		}
		if !testTxnRecordStore(t, db) {
			t.Logf("testTxnRecordStore failed, debug: %v", dbug)
			return
		}
	}
	return true
}

func testKeyStore(t types.T, db *DB) (b bool) {
	assert := testifyassert.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	const (
		key = "kkk"
	)

	var (
		versions = map[uint64]struct {
			V               []byte
			InternalVersion types.TxnInternalVersion
			WriteIntent     bool
		}{
			1: {
				V:               []byte("111"),
				InternalVersion: 254,
				WriteIntent:     true,
			},
			2: {
				V:               []byte("222"),
				InternalVersion: 2,
				WriteIntent:     false,
			},
		}
		clearWriteIntentOfVersion = func(v uint64) {
			obj, ok := versions[v]
			assert.True(ok)
			obj.WriteIntent = false
			versions[v] = obj
		}
		valueOfVersion = func(version uint64) types.Value {
			obj, ok := versions[version]
			assert.True(ok)
			val := types.NewValue(obj.V, version).WithInternalVersion(obj.InternalVersion)
			if !obj.WriteIntent {
				return val.WithCommitted()
			}
			return val
		}
		assertValueOfVersion = func(val types.Value, err error, expVersion uint64) bool {
			expObj, ok := versions[expVersion]
			assert.True(ok)
			return assert.NoError(err) &&
				assert.Equal(expVersion, val.Version) &&
				assert.Equal(expObj.V, val.V) &&
				assert.Equal(expObj.InternalVersion, val.InternalVersion) &&
				assert.Equal(expObj.WriteIntent, val.IsDirty())
		}
	)

	{
		// Prepare test
		for version := range versions {
			if err := db.Set(ctx, key, types.NewValue(nil, version), types.NewKVWriteOption()); !assert.NoError(err) {
				return
			}
			if err := db.RollbackKey(ctx, key, version); !errors.AssertNilOrErr(assert, err, errors.ErrKeyOrVersionNotExist) {
				return
			}
			utils.WithLogLevel(0, func() {
				if val, err := db.Get(ctx, key, types.NewKVReadOptionWithExactVersion(version)); !errors.AssertIsKeyOrVersionNotExistsErr(assert, err) || !assert.True(val.IsEmpty()) {
					return
				}
			})
		}
		utils.WithLogLevel(0, func() {
			if val, err := db.Get(ctx, key, types.NewKVReadOption(types.MaxTxnVersion)); !errors.AssertIsKeyOrVersionNotExistsErr(assert, err) || !assert.True(val.IsEmpty()) {
				return
			}
		})
	}

	if !assert.NoError(db.Set(ctx, key, valueOfVersion(1), types.NewKVWriteOption())) {
		return
	}
	if !assert.NoError(db.Set(ctx, key, valueOfVersion(2), types.NewKVWriteOption())) {
		return
	}
	if val, err := db.Get(ctx, key, types.NewKVReadOption(3)); !assertValueOfVersion(val, err, 2) {
		return
	}
	if val, err := db.Get(ctx, key, types.NewKVReadOption(2)); !assertValueOfVersion(val, err, 2) {
		return
	}
	if val, err := db.Get(ctx, key, types.NewKVReadOption(1)); !assertValueOfVersion(val, err, 1) {
		return
	}
	utils.WithLogLevel(0, func() {
		if _, err := db.Get(ctx, key, types.NewKVReadOption(0)); !errors.AssertIsKeyOrVersionNotExistsErr(assert, err) {
			return
		}
		if _, err := db.Get(ctx, key, types.NewKVReadOptionWithExactVersion(0)); !errors.AssertIsKeyOrVersionNotExistsErr(assert, err) {
			return
		}
		if _, err := db.Get(ctx, key, types.NewKVReadOptionWithExactVersion(3)); !errors.AssertIsKeyOrVersionNotExistsErr(assert, err) {
			return
		}
	})
	if val, err := db.Get(ctx, key, types.NewKVReadOptionWithExactVersion(2)); !assertValueOfVersion(val, err, 2) {
		return
	}
	if val, err := db.Get(ctx, key, types.NewKVReadOptionWithExactVersion(1)); !assertValueOfVersion(val, err, 1) {
		return
	}
	if err := db.ClearWriteIntent(ctx, key, 1); !assert.NoError(err) {
		return
	}
	clearWriteIntentOfVersion(1)
	if val, err := db.Get(ctx, key, types.NewKVReadOptionWithExactVersion(1)); !assertValueOfVersion(val, err, 1) {
		return
	}

	{
		if err := db.updateFlagOfKey(ctx, key, 2, consts.ValueMetaBitMaskHasWriteIntent, func(value types.DBValue) types.DBValue {
			value.VFlag &= ClearValueMetaBitMaskCommitted
			value.VFlag |= consts.ValueMetaBitMaskHasWriteIntent
			return value
		}); !assert.NoError(err) {
			return
		}
		if err := db.RollbackKey(ctx, key, 2); !assert.NoError(err) {
			return
		}
		if val, err := db.Get(ctx, key, types.NewKVReadOption(3)); !assertValueOfVersion(val, err, 1) || !assert.True(val.IsCommitted()) {
			return
		}
		utils.WithLogLevel(0, func() {
			if err := db.updateFlagOfKey(ctx, key, 2, consts.ValueMetaBitMaskHasWriteIntent, func(value types.DBValue) types.DBValue {
				value.VFlag &= ClearValueMetaBitMaskCommitted
				value.VFlag |= consts.ValueMetaBitMaskHasWriteIntent
				return value
			}); !errors.AssertIsKeyOrVersionNotExistsErr(assert, err) {
				return
			}
		})
	}

	{
		if err := db.updateFlagOfKey(ctx, key, 1, consts.ValueMetaBitMaskHasWriteIntent, func(value types.DBValue) types.DBValue {
			value.VFlag &= ClearValueMetaBitMaskCommitted
			value.VFlag |= consts.ValueMetaBitMaskHasWriteIntent
			return value
		}); !assert.NoError(err) {
			return
		}
		if err := db.RollbackKey(ctx, key, 1); !assert.NoError(err) {
			return
		}
		utils.WithLogLevel(0, func() {
			if err := db.updateFlagOfKey(ctx, key, 2, consts.ValueMetaBitMaskHasWriteIntent, func(value types.DBValue) types.DBValue {
				value.VFlag &= ClearValueMetaBitMaskCommitted
				value.VFlag |= consts.ValueMetaBitMaskHasWriteIntent
				return value
			}); !errors.AssertIsKeyOrVersionNotExistsErr(assert, err) {
				return
			}
		})
	}

	if _, err := db.Get(ctx, key, types.NewKVReadOption(3)); !errors.AssertIsKeyOrVersionNotExistsErr(assert, err) {
		return
	}

	return true
}

func testTxnRecordStore(t types.T, db *DB) (b bool) {
	assert := testifyassert.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var (
		versions = map[uint64]struct {
			V []byte
		}{
			1: {
				V: []byte("111"),
			},
			2: {
				V: []byte("222"),
			},
		}
		valueOfVersion = func(version uint64) types.Value {
			obj, ok := versions[version]
			assert.True(ok)
			return types.NewTxnValue(obj.V, version)
		}
		assertValueOfVersion = func(val types.Value, err error, expVersion uint64) bool {
			expObj, ok := versions[expVersion]
			assert.True(ok)
			return assert.NoError(err) &&
				assert.Equal(expVersion, val.Version) &&
				assert.Equal(expObj.V, val.V)
		}
		writeOpt   = types.NewKVWriteOption()
		newReadOpt = func(version uint64) types.KVReadOption {
			return types.NewKVReadOptionWithExactVersion(version).CondTxnRecord(true)
		}
	)

	{
		// Prepare test
		for version := range versions {
			if err := db.Set(ctx, "", types.NewTxnValue(nil, version), writeOpt); !assert.NoError(err) {
				return
			}
			if err := db.RemoveTxnRecord(ctx, version); !errors.AssertNilOrErr(assert, err, errors.ErrKeyOrVersionNotExist) {
				return
			}
			if val, err := db.Get(ctx, "", newReadOpt(version)); !errors.AssertIsKeyOrVersionNotExistsErr(assert, err) || !assert.True(val.IsEmpty()) {
				return
			}
		}
		if val, err := db.Get(ctx, "", newReadOpt(types.MaxTxnVersion)); !errors.AssertIsKeyOrVersionNotExistsErr(assert, err) || !assert.True(val.IsEmpty()) {
			return
		}
	}

	if !assert.NoError(db.Set(ctx, "", valueOfVersion(1), writeOpt)) {
		return
	}
	if !assert.NoError(db.Set(ctx, "", valueOfVersion(2), writeOpt)) {
		return
	}
	if val, err := db.Get(ctx, "", newReadOpt(3)); !errors.AssertIsKeyOrVersionNotExistsErr(assert, err) || !assert.True(val.IsEmpty()) {
		return
	}
	if val, err := db.Get(ctx, "", newReadOpt(2)); !assertValueOfVersion(val, err, 2) {
		return
	}
	if val, err := db.Get(ctx, "", newReadOpt(1)); !assertValueOfVersion(val, err, 1) {
		return
	}
	if _, err := db.Get(ctx, "", newReadOpt(0)); !errors.AssertIsKeyOrVersionNotExistsErr(assert, err) {
		return
	}
	if err := db.RemoveTxnRecord(ctx, 2); !assert.NoError(err) {
		return
	}
	if err := db.RemoveTxnRecord(ctx, 1); !assert.NoError(err) {
		return
	}
	utils.WithLogLevel(0, func() {
		if err := db.RemoveTxnRecord(ctx, 3); !errors.AssertNilOrErr(assert, err, errors.ErrKeyOrVersionNotExist) {
			return
		}
		if val, err := db.Get(ctx, "", newReadOpt(3)); !errors.AssertIsKeyOrVersionNotExistsErr(assert, err) || !assert.True(val.IsEmpty()) {
			return
		}
		if val, err := db.Get(ctx, "", newReadOpt(2)); !errors.AssertIsKeyOrVersionNotExistsErr(assert, err) || !assert.True(val.IsEmpty()) {
			return
		}
		if val, err := db.Get(ctx, "", newReadOpt(1)); !errors.AssertIsKeyOrVersionNotExistsErr(assert, err) || !assert.True(val.IsEmpty()) {
			return
		}
	})
	return true
}

func TestCausalConsistency(t types.T, db *DB) (b bool) {
	assert := types.NewAssertion(t)
	const key = "k1"

	if !assert.NoError(db.Set(context.Background(), key, types.NewValue([]byte("111"), 1), types.NewKVWriteOption())) {
		return
	}
	val, err := db.Get(context.Background(), key, types.NewKVReadOptionWithExactVersion(1))
	if !assert.NoError(err) || !assert.True(val.IsDirty()) {
		return false
	}

	var done = make(chan struct{})
	go func() {
		defer close(done)

		if err := db.ClearWriteIntent(context.Background(), key, 1); !assert.NoError(err) {
			return
		}
	}()

	<-done
	val, err = db.Get(context.Background(), key, types.NewKVReadOptionWithExactVersion(1))
	return assert.NoError(err) && assert.True(val.IsCommitted())
}

const (
	writerNum = 1000
	readerNum = 1000
)

func TestConcurrentClearWriteIntent(t types.T, db *DB) (b bool) {
	assert := types.NewAssertion(t)
	const key = "k1"

	if !assert.NoError(db.Set(context.Background(), key, types.NewValue([]byte("111"), 1), types.NewKVWriteOption())) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < writerNum; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := db.ClearWriteIntent(context.Background(), key, 1); !assert.NoError(err) {
				return
			}
		}()
	}

	for i := 0; i < readerNum; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if _, err := db.Get(context.Background(), key, types.NewKVReadOptionWithExactVersion(1)); !assert.NoError(err) {
				return
			}
		}()
	}

	wg.Wait()
	val, err := db.Get(context.Background(), key, types.NewKVReadOptionWithExactVersion(1))
	if !assert.NoError(err) || !assert.True(val.IsCommitted()) {
		return
	}
	return true
}

func TestConcurrentRemoveVersion(t types.T, db *DB) (b bool) {
	assert := types.NewAssertion(t)
	const key = "key"

	if !assert.NoError(db.Set(context.Background(), key, types.NewValue([]byte("111"), 1), types.NewKVWriteOption())) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < writerNum; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := db.RollbackKey(context.Background(), key, 1); !assert.NoError(err) {
				return
			}
		}()
	}

	for i := 0; i < readerNum; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if _, err := db.Get(context.Background(), key, types.NewKVReadOption(2)); !errors.AssertNilOrErr(&assert.Assertions, err, errors.ErrKeyOrVersionNotExist) {
				return
			}
		}()
	}

	wg.Wait()
	if _, err := db.Get(context.Background(), key, types.NewKVReadOptionWithExactVersion(1)); !errors.AssertIsKeyOrVersionNotExistsErr(&assert.Assertions, err) {
		return
	}
	return true
}

func TestConcurrentClearWriteIntentRemoveVersion(t types.T, db *DB) (b bool) {
	assert := types.NewAssertion(t)
	test = true
	const key = "key"

	if !assert.NoError(db.Set(context.Background(), key, types.NewValue([]byte("111"), 1), types.NewKVWriteOption())) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < writerNum*2; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			if i%2 == 0 {
				if err := db.ClearWriteIntent(context.Background(), key, 1); !errors.AssertNilOrErr(&assert.Assertions, err, errors.ErrKeyOrVersionNotExist) {
					return
				}
			} else {
				if err := db.RollbackKey(context.Background(), key, 1); !errors.AssertNilOrErr(&assert.Assertions, err, errors.ErrCantRemoveCommittedValue) {
					return
				}
			}
		}(i)
	}

	wg.Wait()
	val, err := db.Get(context.Background(), key, types.NewKVReadOptionWithExactVersion(1))
	if err == nil {
		return assert.Equal(uint64(1), val.Version) && assert.True(val.IsCommitted())
	}
	return errors.AssertIsErr(&assert.Assertions, err, errors.ErrKeyOrVersionNotExist)
}

func (db *DB) updateFlagOfKey(ctx context.Context, key string, version uint64, newFlag uint8, modifyFlag func(types.DBValue) types.DBValue) error {
	return db.updateFlagOfKeyRaw(ctx, key, version, newFlag, modifyFlag, func(err error) error {
		if glog.V(1) {
			glog.Errorf("failed to modify flag for version %d of key %s: version not exist", version, key)
		}
		return err
	})
}

func (db *DB) ClearWriteIntent(ctx context.Context, key string, version uint64) error {
	return db.UpdateMeta(ctx, key, version, consts.KVKVCCUpdateMetaOptBitMaskClearWriteIntent)
}
