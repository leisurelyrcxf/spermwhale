package memory

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/kv"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/types/concurrency"
)

type TxnRecordStore struct {
	txns concurrency.ConcurrentTxnMap
}

func NewTxnRecordStore() *TxnRecordStore {
	ts := &TxnRecordStore{}
	ts.txns.Initialize(256) // TODO tune these values
	return ts
}

func (ts *TxnRecordStore) GetTxnRecord(_ context.Context, version uint64) (kv.Value, error) {
	val, ok := ts.txns.Get(types.TxnId(version))
	if !ok {
		return kv.EmptyValue, errors.Annotatef(errors.ErrKeyOrVersionNotExist, "txn: %d", version)
	}
	return val.(kv.Value), nil
}

func (ts *TxnRecordStore) UpsertTxnRecord(_ context.Context, version uint64, val kv.Value) error {
	ts.txns.Set(types.TxnId(version), val)
	return nil
}

func (ts *TxnRecordStore) RemoveTxnRecord(_ context.Context, version uint64) error {
	ts.txns.Del(types.TxnId(version))
	return nil
}

func (ts *TxnRecordStore) Close() error {
	ts.txns.Close()
	return nil
}

type VersionedValues struct {
	keys concurrency.ConcurrentMap
}

func NewVersionedValues() *VersionedValues {
	vvs := &VersionedValues{}
	vvs.keys.Initialize(256) // TODO tune these values
	return vvs
}

func (vvs *VersionedValues) Get(_ context.Context, key string, version uint64) (kv.Value, error) {
	kvvs, err := vvs.getKey(key)
	if err != nil {
		return kv.EmptyValue, err
	}
	return kvvs.Get(version)
}

func (vvs *VersionedValues) Upsert(_ context.Context, key string, version uint64, val kv.Value) error {
	return vvs.keys.GetLazy(key, func() interface{} {
		return newKeyVersionedValues()
	}).(*KeyVersionedValues).Upsert(version, val)
}

func (vvs *VersionedValues) UpdateFlag(context.Context, string, uint64, uint8) error {
	return errors.ErrNotSupported
}

func (vvs *VersionedValues) ReadModifyWriteKey(_ context.Context, key string, version uint64, modifyFlag func(val kv.Value) kv.Value, onNotExists func(err error) error) error {
	kvvs, err := vvs.getKey(key)
	if err != nil {
		assert.Must(errors.IsNotExistsErr(err))
		return onNotExists(err)
	}
	return kvvs.ReadModifyWrite(version, modifyFlag, onNotExists)
}

func (vvs *VersionedValues) Floor(_ context.Context, key string, upperVersion uint64) (val kv.Value, version uint64, err error) {
	kvvs, err := vvs.getKey(key)
	if err != nil {
		return kv.EmptyValue, 0, err
	}
	return kvvs.Floor(upperVersion)
}

func (vvs *VersionedValues) Remove(_ context.Context, key string, version uint64) error {
	kvvs, err := vvs.getKey(key)
	if err != nil {
		return err
	}
	return kvvs.Remove(version)
}

func (vvs *VersionedValues) RemoveIf(_ context.Context, key string, version uint64, pred func(prev kv.Value) error) error {
	kvvs, err := vvs.getKey(key)
	if err != nil {
		return err
	}
	return kvvs.RemoveIf(version, pred)
}

func (vvs *VersionedValues) Close() error {
	vvs.keys.Clear()
	return nil
}

func (vvs *VersionedValues) getKey(key string) (*KeyVersionedValues, error) {
	val, ok := vvs.keys.Get(key)
	if !ok {
		return nil, errors.Annotatef(errors.ErrKeyOrVersionNotExist, "key: '%s'", key)
	}
	return val.(*KeyVersionedValues), nil
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
		return kv.EmptyValue, errors.Annotatef(errors.ErrKeyOrVersionNotExist, "version: %d", version)
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
		return onNotExists(errors.ErrKeyOrVersionNotExist)
	}
	return nil
}

func (kvvs *KeyVersionedValues) Max() (kv.Value, error) {
	// Key is revered sorted, thus min is actually max version..
	// Key is revered sorted, thus max is the min version.
	key, dbVal := (*concurrency.ConcurrentTreeMap)(kvvs).Min()
	if key == nil {
		return kv.EmptyValue, errors.ErrKeyOrVersionNotExist
	}
	return dbVal.(kv.Value), nil
}

func (kvvs *KeyVersionedValues) Min() (kv.Value, error) {
	// Key is revered sorted, thus max is the min version.
	key, dbVal := (*concurrency.ConcurrentTreeMap)(kvvs).Max()
	if key == nil {
		return kv.EmptyValue, errors.ErrKeyOrVersionNotExist
	}
	return dbVal.(kv.Value), nil
}

func (kvvs *KeyVersionedValues) Floor(upperVersion uint64) (val kv.Value, version uint64, err error) {
	key, dbVal := (*concurrency.ConcurrentTreeMap)(kvvs).Find(func(key interface{}, value interface{}) bool {
		return key.(uint64) <= upperVersion
	})

	//key, dbVal := (*concurrency.ConcurrentTreeMap)(kvvs).Ceiling(upperVersion)
	if key == nil {
		return kv.EmptyValue, 0, errors.Annotatef(errors.ErrKeyOrVersionNotExist, "upperVersion: %d", upperVersion)
	}
	assert.Must(key.(uint64) != 0)
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
	return kv.NewDB(NewVersionedValues(), NewTxnRecordStore())
}
