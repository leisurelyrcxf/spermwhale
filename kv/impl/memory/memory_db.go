package memory

import (
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/kv"
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

func (vvs *VersionedValues) Insert(val types.Value) error {
	if !vvs.ConcurrentTreeMap.Insert(val.Version, val) {
		return errors.ErrVersionAlreadyExists
	}
	return nil
}

func (vvs *VersionedValues) Put(val types.Value) error {
	vvs.ConcurrentTreeMap.Put(val.Version, val)
	return nil
}

func (vvs *VersionedValues) UpdateFlag(version uint64, modifyFlag func(types.Value) types.Value) error {
	if !vvs.ConcurrentTreeMap.Update(version, func(old interface{}) (new interface{}, modified bool) {
		oldVal := old.(types.Value)
		newVal := modifyFlag(oldVal)
		return newVal, newVal.Flag != oldVal.Flag
	}) {
		return errors.ErrVersionNotExists
	}
	return nil
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

func (vvs *VersionedValues) RemoveIf(version uint64, pred func(prev types.Value) error) error {
	var err error
	vvs.ConcurrentTreeMap.RemoveIf(version, func(prev interface{}) bool {
		if predErr := pred(prev.(types.Value)); predErr != nil {
			err = predErr
			return false
		}
		return true
	})
	return err
}

func (vvs *VersionedValues) Remove(version uint64) error {
	vvs.ConcurrentTreeMap.Remove(version)
	return nil
}

func NewMemoryDB() *kv.DB {
	allKeyValues := &concurrency.ConcurrentMap{}
	allKeyValues.Initialize(256)
	return kv.NewDB(kv.VersionedValuesFactory{
		Get: func(key string) (vvs kv.VersionedValues, err error) {
			val, ok := allKeyValues.Get(key)
			if !ok {
				return nil, errors.ErrKeyNotExist
			}
			return val.(*VersionedValues), nil
		},
		GetLazy: func(key string) kv.VersionedValues {
			return allKeyValues.GetLazy(key, func() interface{} {
				return NewVersionedValues()
			}).(*VersionedValues)
		},
	}, func() error {
		allKeyValues.Clear()
		return nil
	})
}
