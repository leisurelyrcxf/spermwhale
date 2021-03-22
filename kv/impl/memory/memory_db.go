package memory

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/kv"
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

func (vvs *VersionedValues) Get(_ context.Context, key string, version uint64) (kv.Value, error) {
	kvvs, err := vvs.get(key)
	if err != nil {
		return kv.EmptyValue, err
	}
	return kvvs.Get(version)
}

func (vvs *VersionedValues) Upsert(_ context.Context, key string, version uint64, val kv.Value) error {
	return vvs.lazyGet(key).Upsert(version, val)
}

func (vvs *VersionedValues) UpdateFlag(ctx context.Context, key string, version uint64, newFlag uint8) error {
	return errors.ErrNotSupported
}

func (vvs *VersionedValues) ReadModifyWrite(_ context.Context, key string, version uint64, modifyFlag func(val kv.Value) kv.Value, onNotExists func(err error) error) error {
	kvvs, err := vvs.get(key)
	if err != nil {
		assert.Must(errors.IsNotExistsErr(err))
		return onNotExists(err)
	}
	return kvvs.ReadModifyWrite(version, modifyFlag, onNotExists)
}

func (vvs *VersionedValues) FindMaxBelow(_ context.Context, key string, upperVersion uint64) (val kv.Value, version uint64, err error) {
	kvvs, err := vvs.get(key)
	if err != nil {
		return kv.EmptyValue, 0, err
	}
	return kvvs.FindMaxBelow(upperVersion)
}

func (vvs *VersionedValues) Remove(_ context.Context, key string, version uint64) error {
	kvvs, err := vvs.get(key)
	if err != nil {
		return err
	}
	return kvvs.Remove(version)
}

func (vvs *VersionedValues) RemoveIf(_ context.Context, key string, version uint64, pred func(prev kv.Value) error) error {
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

func (kvvs *KeyVersionedValues) Get(version uint64) (kv.Value, error) {
	val, ok := (*concurrency.ConcurrentTreeMap)(kvvs).Get(version)
	if !ok {
		return kv.EmptyValue, errors.Annotatef(errors.ErrVersionNotExists, "version: %d", version)
	}
	return val.(kv.Value), nil
}

func (kvvs *KeyVersionedValues) Insert(version uint64, val kv.Value) error {
	if !(*concurrency.ConcurrentTreeMap)(kvvs).Insert(version, val) {
		return errors.ErrVersionAlreadyExists
	}
	return nil
}

func (kvvs *KeyVersionedValues) Upsert(version uint64, val kv.Value) error {
	(*concurrency.ConcurrentTreeMap)(kvvs).Put(version, val)
	return nil
}

func (kvvs *KeyVersionedValues) ReadModifyWrite(version uint64, modifyFlag func(kv.Value) kv.Value, onNotExists func(error) error) error {
	if !(*concurrency.ConcurrentTreeMap)(kvvs).Update(version, func(old interface{}) (new interface{}, modified bool) {
		oldVal := old.(kv.Value)
		newVal := modifyFlag(oldVal)
		return newVal, newVal.Flag != oldVal.Flag
	}) {
		return onNotExists(errors.ErrVersionNotExists)
	}
	return nil
}

func (kvvs *KeyVersionedValues) Max() (kv.Value, error) {
	// Key is revered sorted, thus min is actually max version..
	// Key is revered sorted, thus max is the min version.
	key, dbVal := (*concurrency.ConcurrentTreeMap)(kvvs).Min()
	if key == nil {
		return kv.EmptyValue, errors.ErrVersionNotExists
	}
	return dbVal.(kv.Value), nil
}

func (kvvs *KeyVersionedValues) Min() (kv.Value, error) {
	// Key is revered sorted, thus max is the min version.
	key, dbVal := (*concurrency.ConcurrentTreeMap)(kvvs).Max()
	if key == nil {
		return kv.EmptyValue, errors.ErrVersionNotExists
	}
	return dbVal.(kv.Value), nil
}

func (kvvs *KeyVersionedValues) FindMaxBelow(upperVersion uint64) (val kv.Value, version uint64, err error) {
	key, dbVal := (*concurrency.ConcurrentTreeMap)(kvvs).Find(func(key interface{}, value interface{}) bool {
		return key.(uint64) <= upperVersion
	})
	if key == nil {
		return kv.EmptyValue, 0, errors.Annotatef(errors.ErrVersionNotExists, "upperVersion: %d", upperVersion)
	}
	return dbVal.(kv.Value), key.(uint64), nil
}

func (kvvs *KeyVersionedValues) RemoveIf(version uint64, pred func(prev kv.Value) error) error {
	var err error
	(*concurrency.ConcurrentTreeMap)(kvvs).RemoveIf(version, func(prev interface{}) bool {
		if predErr := pred(prev.(kv.Value)); predErr != nil {
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
