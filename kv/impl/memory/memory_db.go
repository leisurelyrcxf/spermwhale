package memory

import (
	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/kv"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
)

type VersionedValues struct {
	concurrency.ConcurrentMap
}

func NewVersionedValues() *VersionedValues {
	vvs := &VersionedValues{}
	vvs.Initialize(256)
	return vvs
}

func (vvs *VersionedValues) get(key string) (*KeyVersionedValues, error) {
	val, ok := vvs.ConcurrentMap.Get(key)
	if !ok {
		return nil, errors.Annotatef(errors.ErrKeyNotExist, "key: '%s'", key)
	}
	return val.(*KeyVersionedValues), nil
}

func (vvs *VersionedValues) lazyGet(key string) *KeyVersionedValues {
	return vvs.GetLazy(key, func() interface{} {
		return newKeyVersionedValues()
	}).(*KeyVersionedValues)
}

func (vvs *VersionedValues) Get(key string, version uint64) (types.Value, error) {
	kvvs, err := vvs.get(key)
	if err != nil {
		return types.EmptyValue, err
	}
	return kvvs.Get(version)
}

func (vvs *VersionedValues) Upsert(key string, val types.Value) error {
	return vvs.lazyGet(key).Upsert(val)
}

func (vvs *VersionedValues) UpdateFlag(key string, version uint64, modifyFlag func(val types.Value) types.Value, onNotExists func(err error) error) error {
	kvvs, err := vvs.get(key)
	if err != nil {
		assert.Must(errors.IsNotExistsErr(err))
		return onNotExists(err)
	}
	return kvvs.UpdateFlag(version, modifyFlag, onNotExists)
}

func (vvs *VersionedValues) FindMaxBelow(key string, upperVersion uint64) (types.Value, error) {
	kvvs, err := vvs.get(key)
	if err != nil {
		return types.EmptyValue, err
	}
	return kvvs.FindMaxBelow(upperVersion)
}

func (vvs *VersionedValues) Remove(key string, version uint64) error {
	kvvs, err := vvs.get(key)
	if err != nil {
		return err
	}
	return kvvs.Remove(version)
}

func (vvs *VersionedValues) RemoveIf(key string, version uint64, pred func(prev types.Value) error) error {
	kvvs, err := vvs.get(key)
	if err != nil {
		return err
	}
	return kvvs.RemoveIf(version, pred)
}

func (vvs *VersionedValues) Close() error {
	vvs.Clear()
	return nil
}

type KeyVersionedValues concurrency.ConcurrentTreeMap

func newKeyVersionedValues() *KeyVersionedValues {
	return (*KeyVersionedValues)(concurrency.NewConcurrentTreeMap(func(a, b interface{}) int {
		av, bv := a.(uint64), b.(uint64)
		if av > bv {
			return -1
		}
		if av == bv {
			return 0
		}
		return 1
	}))
}

func (kvvs *KeyVersionedValues) Get(version uint64) (types.Value, error) {
	val, ok := (*concurrency.ConcurrentTreeMap)(kvvs).Get(version)
	if !ok {
		return types.EmptyValue, errors.Annotatef(errors.ErrVersionNotExists, "version: %d", version)
	}
	return val.(types.Value), nil
}

func (kvvs *KeyVersionedValues) Insert(val types.Value) error {
	if !(*concurrency.ConcurrentTreeMap)(kvvs).Insert(val.Version, val) {
		return errors.ErrVersionAlreadyExists
	}
	return nil
}

func (kvvs *KeyVersionedValues) Upsert(val types.Value) error {
	(*concurrency.ConcurrentTreeMap)(kvvs).Put(val.Version, val)
	return nil
}

func (kvvs *KeyVersionedValues) UpdateFlag(version uint64, modifyFlag func(types.Value) types.Value, onNotExists func(error) error) error {
	if !(*concurrency.ConcurrentTreeMap)(kvvs).Update(version, func(old interface{}) (new interface{}, modified bool) {
		oldVal := old.(types.Value)
		newVal := modifyFlag(oldVal)
		return newVal, newVal.Flag != oldVal.Flag
	}) {
		return onNotExists(errors.ErrVersionNotExists)
	}
	return nil
}

func (kvvs *KeyVersionedValues) Max() (types.Value, error) {
	// Key is revered sorted, thus min is actually max version..
	// Key is revered sorted, thus max is the min version.
	key, dbVal := (*concurrency.ConcurrentTreeMap)(kvvs).Min()
	if key == nil {
		return types.EmptyValue, errors.ErrVersionNotExists
	}
	return dbVal.(types.Value), nil
}

func (kvvs *KeyVersionedValues) Min() (types.Value, error) {
	// Key is revered sorted, thus max is the min version.
	key, dbVal := (*concurrency.ConcurrentTreeMap)(kvvs).Max()
	if key == nil {
		return types.EmptyValue, errors.ErrVersionNotExists
	}
	return dbVal.(types.Value), nil
}

func (kvvs *KeyVersionedValues) FindMaxBelow(upperVersion uint64) (types.Value, error) {
	version, dbVal := (*concurrency.ConcurrentTreeMap)(kvvs).Find(func(key interface{}, value interface{}) bool {
		return key.(uint64) <= upperVersion
	})
	if version == nil {
		return types.EmptyValue, errors.Annotatef(errors.ErrVersionNotExists, "upperVersion: %d", upperVersion)
	}
	return dbVal.(types.Value), nil
}

func (kvvs *KeyVersionedValues) RemoveIf(version uint64, pred func(prev types.Value) error) error {
	var err error
	(*concurrency.ConcurrentTreeMap)(kvvs).RemoveIf(version, func(prev interface{}) bool {
		if predErr := pred(prev.(types.Value)); predErr != nil {
			err = predErr
			return false
		}
		return true
	})
	return err
}

func (kvvs *KeyVersionedValues) Remove(version uint64) error {
	(*concurrency.ConcurrentTreeMap)(kvvs).Remove(version)
	return nil
}

func NewMemoryDB() *kv.DB {
	return kv.NewDB(NewVersionedValues())
}
