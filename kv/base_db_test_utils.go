package kv

import (
	"context"
	"time"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/leisurelyrcxf/spermwhale/types"
)

func TestDB(t types.T, db *DB) (b bool) {
	assert := testifyassert.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	const (
		key = "kkk"
	)

	var (
		versions = map[uint64]struct {
			V           []byte
			WriteIntent bool
		}{
			1: {
				V:           []byte("111"),
				WriteIntent: true,
			},
			2: {
				V:           []byte("222"),
				WriteIntent: false,
			},
		}
		valueOfVersion = func(version uint64) types.Value {
			obj, ok := versions[version]
			assert.True(ok)
			val := types.NewValue(obj.V, version)
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
				assert.Equal(expObj.WriteIntent, val.HasWriteIntent())
		}
	)

	{
		// Prepare test
		for version := range versions {
			if err := db.Upsert(ctx, key, types.NewValue(nil, version)); !assert.NoError(err) {
				return
			}
			if err := db.Set(ctx, key, types.NewValue(nil, version).WithNoWriteIntent(), types.NewKVWriteOption().WithRemoveVersion()); !errors.AssertNilOrErr(assert, err, errors.ErrVersionNotExists) {
				return
			}
			if val, err := db.Get(ctx, key, types.NewKVReadOption(version).WithExactVersion()); !errors.AssertIsVersionNotExistsErr(assert, err) || !assert.True(val.IsEmpty()) {
				return
			}
		}
		if val, err := db.Get(ctx, key, types.NewKVReadOption(types.MaxTxnVersion)); !errors.AssertIsVersionNotExistsErr(assert, err) || !assert.True(val.IsEmpty()) {
			return
		}
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
	if _, err := db.Get(ctx, key, types.NewKVReadOption(0)); !errors.AssertIsVersionNotExistsErr(assert, err) {
		return
	}
	if _, err := db.Get(ctx, key, types.NewKVReadOption(3).WithExactVersion()); !errors.AssertIsVersionNotExistsErr(assert, err) {
		return
	}
	if val, err := db.Get(ctx, key, types.NewKVReadOption(2).WithExactVersion()); !assertValueOfVersion(val, err, 2) {
		return
	}
	if val, err := db.Get(ctx, key, types.NewKVReadOption(1).WithExactVersion()); !assertValueOfVersion(val, err, 1) {
		return
	}
	if _, err := db.Get(ctx, key, types.NewKVReadOption(0).WithExactVersion()); !errors.AssertIsVersionNotExistsErr(assert, err) {
		return
	}
	if err := db.Set(ctx, key, types.NewValue(nil, 1).WithNoWriteIntent(), types.NewKVWriteOption().WithClearWriteIntent()); !assert.NoError(err) {
		return
	}
	versions[1] = struct {
		V           []byte
		WriteIntent bool
	}{V: versions[1].V, WriteIntent: false}
	if val, err := db.Get(ctx, key, types.NewKVReadOption(1).WithExactVersion()); !assertValueOfVersion(val, err, 1) {
		return
	}

	{
		if err := db.updateFlag(ctx, key, 2, consts.ValueMetaBitMaskHasWriteIntent, func(value Value) Value {
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
		if err := db.updateFlag(ctx, key, 2, consts.ValueMetaBitMaskHasWriteIntent, func(value Value) Value {
			value.Flag |= consts.ValueMetaBitMaskHasWriteIntent
			return value
		}); !errors.AssertIsVersionNotExistsErr(assert, err) {
			return
		}
	}

	{
		if err := db.updateFlag(ctx, key, 1, consts.ValueMetaBitMaskHasWriteIntent, func(value Value) Value {
			value.Flag |= consts.ValueMetaBitMaskHasWriteIntent
			return value
		}); !assert.NoError(err) {
			return
		}
		if err := db.Set(ctx, key, types.NewValue(nil, 1).WithNoWriteIntent(), types.NewKVWriteOption().WithRemoveVersion()); !assert.NoError(err) {
			return
		}
		if err := db.updateFlag(ctx, key, 2, consts.ValueMetaBitMaskHasWriteIntent, func(value Value) Value {
			value.Flag |= consts.ValueMetaBitMaskHasWriteIntent
			return value
		}); !errors.AssertIsVersionNotExistsErr(assert, err) {
			return
		}
	}

	if _, err := db.Get(ctx, key, types.NewKVReadOption(3)); !errors.AssertIsVersionNotExistsErr(assert, err) {
		return
	}

	return true
}
