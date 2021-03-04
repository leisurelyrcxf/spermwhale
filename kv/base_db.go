package kv

import (
	"context"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/build_opt"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
)

var (
	Testing = false
)

type VersionedValues interface {
	Get(version uint64) (types.Value, error)
	Put(val types.Value) error
	UpdateFlag(version uint64, modifyFlag func(types.Value) types.Value) error
	Max() (types.Value, error)
	Min() (types.Value, error)
	FindMaxBelow(upperVersion uint64) (types.Value, error)
	Remove(version uint64) error
	RemoveIf(version uint64, pred func(prev types.Value) error) error
}

type VersionedValuesFactory struct {
	Get     func(key string) (VersionedValues, error)
	GetLazy func(key string) VersionedValues // if key not exists, create a new one
}

type DB struct {
	factory VersionedValuesFactory
	onClose func() error
}

func NewDB(getVersionedValues VersionedValuesFactory, onClose func() error) *DB {
	return &DB{
		factory: getVersionedValues,
		onClose: onClose,
	}
}

func (db *DB) Get(_ context.Context, key string, opt types.KVReadOption) (types.Value, error) {
	vvs, err := db.factory.Get(key)
	if err != nil {
		return types.EmptyValue, err
	}
	if opt.ExactVersion {
		return vvs.Get(opt.Version)
	}
	return vvs.FindMaxBelow(opt.Version)
}

func (db *DB) Set(ctx context.Context, key string, val types.Value, opt types.KVWriteOption) error {
	if opt.IsClearWriteIntent() {
		if build_opt.Debug {
			return db.updateFlagRaw(key, val.Version, func(value types.Value) types.Value {
				return value.WithNoWriteIntent()
			}, func(key string, version uint64) {
				if !Testing {
					glog.Fatalf("want to clear write intent for version %d of key %, but the version doesn't exist", version, key)
				}
			})
		}
		vvs, err := db.factory.Get(key)
		if err != nil {
			return errors.Annotatef(err, "key: %s", key)
		}
		oldVal, err := vvs.Get(val.Version)
		if err != nil {
			return errors.Annotatef(err, "key: %s", key)
		}
		return vvs.Put(oldVal.WithNoWriteIntent())
	}
	if opt.IsRemoveVersion() {
		if val.HasWriteIntent() {
			return errors.Annotatef(errors.ErrNotSupported, "soft remove")
		}
		vvs, err := db.factory.Get(key)
		if err != nil {
			return errors.Annotatef(err, "key: %s", key)
		}
		// TODO can remove the check in the future if stable enough
		if build_opt.Debug {
			return vvs.RemoveIf(val.Version, func(prev types.Value) error {
				if !prev.HasWriteIntent() {
					if !Testing {
						glog.Fatalf("want to remove key %s of version %d which doesn't have write intent", key, val.Version)
					}
					return errors.ErrCantRemoveCommittedValue
				}
				return nil
			})
		}
		return vvs.Remove(val.Version)

	}
	return db.Put(ctx, key, val)
}

func (db *DB) updateFlag(key string, version uint64, modifyFlag func(types.Value) types.Value) error {
	return db.updateFlagRaw(key, version, modifyFlag, func(key string, version uint64) {
		glog.Errorf("failed to modify flag for version %d of key %s: version not exist", version, key)
	})
}

func (db *DB) updateFlagRaw(key string, version uint64, modifyFlag func(types.Value) types.Value, onVersionNotExists func(key string, version uint64)) error {
	vvs, err := db.factory.Get(key)
	if err != nil {
		return errors.Annotatef(err, "key: %s", key)
	}
	if err := vvs.UpdateFlag(version, modifyFlag); err != nil {
		if errors.IsNotExistsErr(err) {
			onVersionNotExists(key, version)
		}
		return errors.Annotatef(err, "key: %s", key)
	}
	return nil
}

func (db *DB) Put(_ context.Context, key string, val types.Value) error {
	if err := db.factory.GetLazy(key).Put(val); err != nil {
		return errors.Annotatef(err, "key: %s", key)
	}
	return nil
}

func (db *DB) Close() error {
	return db.onClose()
}
