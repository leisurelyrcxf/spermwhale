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
				return val.WithNoWriteIntent()
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
				assert.Equal(expObj.WriteIntent, val.HasWriteIntent())
		}
	)

	{
		// Prepare test
		for version := range versions {
			if err := db.Set(ctx, key, types.NewValue(nil, version), types.NewKVWriteOption()); !assert.NoError(err) {
				return
			}
			if err := db.Set(ctx, key, types.NewValue(nil, version).WithNoWriteIntent(), types.NewKVWriteOption().WithRemoveVersion()); !errors.AssertNilOrErr(assert, err, errors.ErrKeyOrVersionNotExist) {
				return
			}
			utils.WithLogLevel(0, func() {
				if val, err := db.Get(ctx, key, types.NewKVReadOption(version).WithExactVersion()); !errors.AssertIsKeyOrVersionNotExistsErr(assert, err) || !assert.True(val.IsEmpty()) {
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
		if _, err := db.Get(ctx, key, types.NewKVReadOption(0).WithExactVersion()); !errors.AssertIsKeyOrVersionNotExistsErr(assert, err) {
			return
		}
		if _, err := db.Get(ctx, key, types.NewKVReadOption(3).WithExactVersion()); !errors.AssertIsKeyOrVersionNotExistsErr(assert, err) {
			return
		}
	})
	if val, err := db.Get(ctx, key, types.NewKVReadOption(2).WithExactVersion()); !assertValueOfVersion(val, err, 2) {
		return
	}
	if val, err := db.Get(ctx, key, types.NewKVReadOption(1).WithExactVersion()); !assertValueOfVersion(val, err, 1) {
		return
	}
	if err := db.Set(ctx, key, types.NewValue(nil, 1).WithNoWriteIntent(), types.NewKVWriteOption().WithClearWriteIntent()); !assert.NoError(err) {
		return
	}
	clearWriteIntentOfVersion(1)
	if val, err := db.Get(ctx, key, types.NewKVReadOption(1).WithExactVersion()); !assertValueOfVersion(val, err, 1) {
		return
	}

	{
		if err := db.updateFlagOfKey(ctx, key, 2, consts.ValueMetaBitMaskHasWriteIntent, func(value Value) Value {
			value.Flag |= consts.ValueMetaBitMaskHasWriteIntent
			return value
		}); !assert.NoError(err) {
			return
		}
		if err := db.Set(ctx, key, types.NewValue(nil, 2).WithNoWriteIntent(), types.NewKVWriteOption().WithRemoveVersion()); !assert.NoError(err) {
			return
		}
		if val, err := db.Get(ctx, key, types.NewKVReadOption(3)); !assertValueOfVersion(val, err, 1) || !assert.False(val.HasWriteIntent()) {
			return
		}
		utils.WithLogLevel(0, func() {
			if err := db.updateFlagOfKey(ctx, key, 2, consts.ValueMetaBitMaskHasWriteIntent, func(value Value) Value {
				value.Flag |= consts.ValueMetaBitMaskHasWriteIntent
				return value
			}); !errors.AssertIsKeyOrVersionNotExistsErr(assert, err) {
				return
			}
		})
	}

	{
		if err := db.updateFlagOfKey(ctx, key, 1, consts.ValueMetaBitMaskHasWriteIntent, func(value Value) Value {
			value.Flag |= consts.ValueMetaBitMaskHasWriteIntent
			return value
		}); !assert.NoError(err) {
			return
		}
		if err := db.Set(ctx, key, types.NewValue(nil, 1).WithNoWriteIntent(), types.NewKVWriteOption().WithRemoveVersion()); !assert.NoError(err) {
			return
		}
		utils.WithLogLevel(0, func() {
			if err := db.updateFlagOfKey(ctx, key, 2, consts.ValueMetaBitMaskHasWriteIntent, func(value Value) Value {
				value.Flag |= consts.ValueMetaBitMaskHasWriteIntent
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
			return types.NewValue(obj.V, version)
		}
		assertValueOfVersion = func(val types.Value, err error, expVersion uint64) bool {
			expObj, ok := versions[expVersion]
			assert.True(ok)
			return assert.NoError(err) &&
				assert.Equal(expVersion, val.Version) &&
				assert.Equal(expObj.V, val.V)
		}
		writeOpt   = types.NewKVWriteOption().WithTxnRecord()
		newReadOpt = func(version uint64) types.KVReadOption {
			return types.NewKVReadOption(version).WithTxnRecord().WithExactVersion()
		}
	)

	{
		// Prepare test
		for version := range versions {
			if err := db.Set(ctx, "", types.NewValue(nil, version), writeOpt); !assert.NoError(err) {
				return
			}
			if err := db.Set(ctx, "", types.NewValue(nil, version).WithNoWriteIntent(), writeOpt.WithRemoveVersion()); !errors.AssertNilOrErr(assert, err, errors.ErrKeyOrVersionNotExist) {
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
	if err := db.Set(ctx, "", types.NewValue(nil, 2).WithNoWriteIntent(), writeOpt.WithRemoveVersion()); !assert.NoError(err) {
		return
	}
	if err := db.Set(ctx, "", types.NewValue(nil, 1).WithNoWriteIntent(), writeOpt.WithRemoveVersion()); !assert.NoError(err) {
		return
	}
	utils.WithLogLevel(0, func() {
		if err := db.Set(ctx, "", types.NewValue(nil, 3).WithNoWriteIntent(), writeOpt.WithRemoveVersion()); !errors.AssertNilOrErr(assert, err, errors.ErrKeyOrVersionNotExist) {
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

func TestConcurrentInsert(t types.T, db *DB) (b bool) {
	assert := types.NewAssertion(t)
	const key = "key_update"

	if !assert.NoError(db.Set(context.Background(), key, types.NewValue([]byte("111"), 1), types.NewKVWriteOption())) {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := db.Set(context.Background(), key, types.NewValue(nil, 1).WithNoWriteIntent(),
				types.NewKVWriteOption()); !errors.AssertNilOrErr(assert, err, errors.ErrVersionAlreadyExists) {
				return
			}
		}()
	}

	wg.Wait()
	val, err := db.Get(context.Background(), key, types.NewKVReadOption(1).WithExactVersion())
	if !assert.NoError(err) || !assert.True(!val.HasWriteIntent()) {
		return
	}
	return true
}

func TestCausalConsistency(t types.T, db *DB) (b bool) {
	assert := types.NewAssertion(t)
	const key = "k1"

	if !assert.NoError(db.Set(context.Background(), key, types.NewValue([]byte("111"), 1), types.NewKVWriteOption())) {
		return
	}
	val, err := db.Get(context.Background(), key, types.NewKVReadOption(1).WithExactVersion())
	if !assert.NoError(err) || !assert.True(val.HasWriteIntent()) {
		return false
	}

	var done = make(chan struct{})
	go func() {
		defer close(done)

		if err := db.Set(context.Background(), key, types.NewValue(nil, 1).WithNoWriteIntent(), types.NewKVWriteOption().WithClearWriteIntent()); !assert.NoError(err) {
			return
		}
	}()

	<-done
	val, err = db.Get(context.Background(), key, types.NewKVReadOption(1).WithExactVersion())
	return assert.NoError(err) && assert.True(!val.HasWriteIntent())
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

			if err := db.Set(context.Background(), key, types.NewValue(nil, 1).WithNoWriteIntent(), types.NewKVWriteOption().WithClearWriteIntent()); !assert.NoError(err) {
				return
			}
		}()
	}

	for i := 0; i < readerNum; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if _, err := db.Get(context.Background(), key, types.NewKVReadOption(1).WithExactVersion()); !assert.NoError(err) {
				return
			}
		}()
	}

	wg.Wait()
	val, err := db.Get(context.Background(), key, types.NewKVReadOption(1).WithExactVersion())
	if !assert.NoError(err) || !assert.False(val.HasWriteIntent()) {
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

			if err := db.Set(context.Background(), key, types.NewValue(nil, 1).WithNoWriteIntent(), types.NewKVWriteOption().WithRemoveVersion()); !assert.NoError(err) {
				return
			}
		}()
	}

	for i := 0; i < readerNum; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if _, err := db.Get(context.Background(), key, types.NewKVReadOption(2)); !errors.AssertNilOrErr(assert, err, errors.ErrKeyOrVersionNotExist) {
				return
			}
		}()
	}

	wg.Wait()
	if _, err := db.Get(context.Background(), key, types.NewKVReadOption(1).WithExactVersion()); !errors.AssertIsKeyOrVersionNotExistsErr(assert, err) {
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
				if err := db.Set(context.Background(), key, types.NewValue(nil, 1).WithNoWriteIntent(), types.NewKVWriteOption().WithClearWriteIntent()); !errors.AssertNilOrErr(assert, err, errors.ErrKeyOrVersionNotExist) {
					return
				}
			} else {
				if err := db.Set(context.Background(), key, types.NewValue(nil, 1).WithNoWriteIntent(), types.NewKVWriteOption().WithRemoveVersion()); !errors.AssertNilOrErr(assert, err, errors.ErrCantRemoveCommittedValue) {
					return
				}
			}
		}(i)
	}

	wg.Wait()
	val, err := db.Get(context.Background(), key, types.NewKVReadOption(1).WithExactVersion())
	if err == nil {
		return assert.Equal(uint64(1), val.Version) && assert.False(val.HasWriteIntent())
	}
	return errors.AssertIsErr(assert, err, errors.ErrKeyOrVersionNotExist)
}

func (db *DB) updateFlagOfKey(ctx context.Context, key string, version uint64, newFlag uint8, modifyFlag func(Value) Value) error {
	return db.updateFlagOfKeyRaw(ctx, key, version, newFlag, modifyFlag, func(err error) error {
		if glog.V(1) {
			glog.Errorf("failed to modify flag for version %d of key %s: version not exist", version, key)
		}
		return err
	})
}
