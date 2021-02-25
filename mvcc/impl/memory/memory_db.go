package memory

import (
	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/data_struct"
	"github.com/leisurelyrcxf/spermwhale/mvcc"
	"github.com/leisurelyrcxf/spermwhale/types"
)

type VersionedValues struct {
	data_struct.ConcurrentTreeMap
}

func NewVersionedValues() *VersionedValues {
	return &VersionedValues{
		ConcurrentTreeMap: *data_struct.NewConcurrentTreeMap(func(a, b interface{}) int {
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
	if ok {
		return val.(types.Value), nil
	}
	return types.Value{}, mvcc.ErrVersionTooStale
}

func (vvs *VersionedValues) Put(dbValue string, version uint64, writeIntent bool) {
	vvs.ConcurrentTreeMap.Put(version, types.NewValue(dbValue, version, writeIntent))
}

func (vvs *VersionedValues) Max() (types.Value, error) {
	// Key is revered sorted, thus min is actually max version..
	key, dbVal := vvs.ConcurrentTreeMap.Min()
	if key == nil {
		return types.Value{}, mvcc.ErrVersionTooStale
	}
	return dbVal.(types.Value), nil
}

func (vvs *VersionedValues) Min() (types.Value, error) {
	// Key is revered sorted, thus max is the min version.
	key, dbVal := vvs.ConcurrentTreeMap.Max()
	if key == nil {
		return types.Value{}, mvcc.ErrVersionTooStale
	}
	return dbVal.(types.Value), nil
}

func (vvs *VersionedValues) FindMaxBelow(upperVersion uint64) (types.Value, error) {
	version, dbVal := vvs.Find(func(key interface{}, value interface{}) bool {
		return key.(uint64) <= upperVersion
	})
	if version == nil {
		return types.Value{}, mvcc.ErrVersionTooStale
	}
	return dbVal.(types.Value), nil
}

type DB struct {
	values data_struct.ConcurrentMap
}

func NewDB() *DB {
	return &DB{
		values: data_struct.NewConcurrentMap(1024),
	}
}

func (db *DB) Get(key string, upperVersion uint64) (types.Value, error) {
	vvs, err := db.getValues(key)
	if err != nil {
		return types.Value{}, err
	}
	return vvs.FindMaxBelow(upperVersion)
}

func (db *DB) Set(key string, val string, version uint64, writeIntent bool) {
	db.values.GetLazy(key, func() interface{} {
		return NewVersionedValues()
	}).(*VersionedValues).Put(val, version, writeIntent)
}

func (db *DB) getValues(key string) (*VersionedValues, error) {
	val, ok := db.values.Get(key)
	if !ok {
		return nil, mvcc.ErrKeyNotExist
	}
	return val.(*VersionedValues), nil
}

func (db *DB) MustRemoveVersion(key string, version uint64) {
	vvs, err := db.getValues(key)
	assert.MustNoError(err)
	_, err = vvs.Get(version)
	assert.MustNoError(err)
	vvs.Remove(version)
}

func (db *DB) MustClearVersions(key string) {
	vvs, err := db.getValues(key)
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
