package memory

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/consts"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
)

type VersionedValues struct {
	concurrency.ConcurrentTreeMap
}

func NewVersionedValues() *VersionedValues {
	return &VersionedValues{
		ConcurrentTreeMap: *concurrency.NewConcurrentTreeMap(func(a, b interface{}) int {
			av, bv := a.(uint64), b.(uint64)
			if av > bv {
				return -1
			}
			if av == bv {
				return 0
			}
			return 1
		}),
	}
}

func (vvs *VersionedValues) Get(version uint64) (types.Value, error) {
	val, ok := vvs.ConcurrentTreeMap.Get(version)
	if !ok {
		return types.EmptyValue, errors.Annotatef(errors.ErrVersionNotExists, "version: %d", version)
	}
	return val.(types.Value), nil
}

func (vvs *VersionedValues) Put(val types.Value) {
	vvs.ConcurrentTreeMap.Put(val.Version, val)
}

func (vvs *VersionedValues) Max() (types.Value, error) {
	// Key is revered sorted, thus min is actually max version..
	key, dbVal := vvs.ConcurrentTreeMap.Min()
	if key == nil {
		return types.EmptyValue, errors.ErrVersionNotExists
	}
	return dbVal.(types.Value), nil
}

func (vvs *VersionedValues) Min() (types.Value, error) {
	// Key is revered sorted, thus max is the min version.
	key, dbVal := vvs.ConcurrentTreeMap.Max()
	if key == nil {
		return types.EmptyValue, errors.ErrVersionNotExists
	}
	return dbVal.(types.Value), nil
}

func (vvs *VersionedValues) FindMaxBelow(upperVersion uint64) (types.Value, error) {
	version, dbVal := vvs.Find(func(key interface{}, value interface{}) bool {
		return key.(uint64) <= upperVersion
	})
	if version == nil {
		return types.EmptyValue, errors.Annotatef(errors.ErrVersionNotExists, "upperVersion: %d", upperVersion)
	}
	return dbVal.(types.Value), nil
}

type DB struct {
	values concurrency.ConcurrentMap
}

func NewDB() *DB {
	db := &DB{}
	db.values.Initialize(256)
	return db
}

func (db *DB) Get(_ context.Context, key string, opt types.ReadOption) (types.Value, error) {
	vvs, err := db.getVersionedValues(key)
	if err != nil {
		return types.EmptyValue, err
	}
	if opt.IsGetExactVersion() {
		return vvs.Get(opt.Version)
	}
	return vvs.FindMaxBelow(opt.Version)
}

func (db *DB) Set(_ context.Context, key string, val types.Value, opt types.WriteOption) error {
	if opt.IsClearWriteIntent() {
		vvs, err := db.getVersionedValues(key)
		if err != nil {
			return errors.Annotatef(err, "key: %s", key)
		}
		vv, err := vvs.Get(val.Version)
		if err != nil {
			return errors.Annotatef(err, "key: %s", key)
		}
		vv.Meta.ClearWriteIntent()
		vvs.Put(vv)
		return nil
	}
	if opt.IsRemoveVersion() {
		if val.HasWriteIntent() {
			return errors.Annotatef(errors.ErrNotSupported, "soft remove")
		}
		vvs, err := db.getVersionedValues(key)
		if err != nil {
			return errors.Annotatef(err, "key: %s", key)
		}
		prevVal, err := vvs.Get(val.Version)
		if err != nil {
			assert.Must(errors.GetErrorCode(err) == consts.ErrCodeVersionNotExists)
			return nil
		}
		if !prevVal.HasWriteIntent() {
			assert.Must(false)
			return errors.ErrCantRemoveCommittedValue
		}
		vvs.Remove(val.Version)
		return nil
	}
	db.values.GetLazy(key, func() interface{} {
		return NewVersionedValues()
	}).(*VersionedValues).Put(val)
	return nil
}

func (db *DB) Close() error {
	db.values.Clear()
	return nil
}

func (db *DB) getVersionedValues(key string) (*VersionedValues, error) {
	val, ok := db.values.Get(key)
	if !ok {
		return nil, errors.ErrKeyNotExist
	}
	return val.(*VersionedValues), nil
}

func (db *DB) MustRemoveVersion(key string, version uint64) {
	vvs, err := db.getVersionedValues(key)
	assert.MustNoError(err)
	_, err = vvs.Get(version)
	assert.MustNoError(err)
	vvs.Remove(version)
}

func (db *DB) MustClearVersions(key string) {
	vvs, err := db.getVersionedValues(key)
	assert.MustNoError(err)
	vvs.Clear()
}

//func (db *DB) Snapshot() map[string]float64 {
//	m := make(map[string]float64)
//	if !db.mvccEnabled {
//		db.values.ForEachStrict(func(k string, vv interface{}) {
//			m[k] = vv.(Value).Value
//		})
//	} else {
//		db.values.ForEachStrict(func(k string, vvs interface{}) {
//			dbValue, err := vvs.(VersionedValues).Max()
//			if err != nil {
//				m[k] = math.NaN()
//				return
//			}
//			m[k] = dbValue.Value
//		})
//	}
//	return m
//}
