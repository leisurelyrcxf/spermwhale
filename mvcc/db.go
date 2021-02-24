package mvcc

import (
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/data_struct"
	"github.com/leisurelyrcxf/spermwhale/types"
)

var (
	KeyNotExist     = fmt.Errorf("key not exist")
	VersionNotExist = fmt.Errorf("version not exist")
)

type VersionedValues struct {
	data_struct.ConcurrentTreeMap
}

func NewDBVersionedValues() *VersionedValues {
	return &VersionedValues{
		ConcurrentTreeMap: *data_struct.NewConcurrentTreeMap(func(a, b interface{}) int {
			return -int(a.(uint64) - b.(uint64))
		}),
	}
}

func (vvs *VersionedValues) Get(version uint64) (string, error) {
	val, ok := vvs.ConcurrentTreeMap.Get(version)
	if ok {
		return val.(string), nil
	}
	return "", VersionNotExist
}

func (vvs *VersionedValues) Put(dbValue string, version uint64) {
	vvs.ConcurrentTreeMap.Put(version, dbValue)
}

func (vvs *VersionedValues) Max() (types.VersionedValue, error) {
	// Key is revered sorted, thus min is actually max version..
	key, dbVal := vvs.ConcurrentTreeMap.Min()
	if key == nil {
		return types.VersionedValue{}, VersionNotExist
	}
	return types.NewVersionedValue(dbVal.(string), key.(uint64)), nil
}

func (vvs *VersionedValues) Min() (types.VersionedValue, error) {
	// Key is revered sorted, thus max is the min version.
	key, dbVal := vvs.ConcurrentTreeMap.Max()
	if key == nil {
		return types.VersionedValue{}, VersionNotExist
	}
	return types.NewVersionedValue(dbVal.(string), key.(uint64)), nil
}

func (vvs *VersionedValues) FindMaxBelow(upperVersion uint64) (types.VersionedValue, error) {
	version, dbVal := vvs.Find(func(key interface{}, value interface{}) bool {
		return key.(uint64) <= upperVersion
	})
	if version == nil {
		return types.VersionedValue{}, VersionNotExist
	}
	return types.NewVersionedValue(dbVal.(string), version.(uint64)), nil
}

type DB struct {
	values data_struct.ConcurrentMap
}

func NewDB() *DB {
	return &DB{
		values: data_struct.NewConcurrentMap(1024),
	}
}

func (db *DB) Get(key string, upperVersion uint64) (types.VersionedValue, error) {
	vvs, err := db.getDBVersionedValues(key)
	if err != nil {
		return types.VersionedValue{}, err
	}
	return vvs.FindMaxBelow(upperVersion)
}

func (db *DB) Set(key string, val string, version uint64) {
	db.values.GetLazy(key, func() interface{} {
		return NewDBVersionedValues()
	}).(*VersionedValues).Put(val, version)

	//var vvs VersionedValues
	//vvsObj, ok := db.values.Get(key)
	//if !ok {
	//	vvs = NewDBVersionedValues()
	//	db.values.Set(key, vvs)
	//} else {
	//	vvs = vvsObj.(VersionedValues)
	//}
	//vvs.Put(writtenTxn.GetTimestamp(), NewValue(val, writtenTxn))
}

func (db *DB) getDBVersionedValues(key string) (VersionedValues, error) {
	val, ok := db.values.Get(key)
	if !ok {
		return VersionedValues{}, KeyNotExist
	}
	return val.(VersionedValues), nil
}

func (db *DB) MustRemoveVersion(key string, version uint64) {
	vvs, err := db.getDBVersionedValues(key)
	assert.MustNoError(err)
	_, err = vvs.Get(version)
	assert.MustNoError(err)
	vvs.Remove(version)
}

func (db *DB) MustClearVersions(key string) {
	vvs, err := db.getDBVersionedValues(key)
	assert.MustNoError(err)
	vvs.Clear()
}

//func (db *DB) Snapshot() map[string]float64 {
//	m := make(map[string]float64)
//	if !db.mvccEnabled {
//		db.values.ForEachStrict(func(k string, vv interface{}) {
//			m[k] = vv.(VersionedValue).Value
//		})
//	} else {
//		db.values.ForEachStrict(func(k string, vvs interface{}) {
//			dbVersionedValue, err := vvs.(VersionedValues).Max()
//			if err != nil {
//				m[k] = math.NaN()
//				return
//			}
//			m[k] = dbVersionedValue.Value
//		})
//	}
//	return m
//}
