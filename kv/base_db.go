package kv

import (
	"context"

	"github.com/leisurelyrcxf/spermwhale/assert"

	"github.com/leisurelyrcxf/spermwhale/utils"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
)

var (
	Testing = false
)

type VersionedValues interface {
	Get(key string, version uint64) (types.Value, error)
	Upsert(key string, val types.Value) error
	UpdateFlag(key string, version uint64, modifyFlag func(val types.Value) types.Value, onNotExists func(err error) error) error
	FindMaxBelow(key string, upperVersion uint64) (types.Value, error)
	Remove(key string, version uint64) error
	RemoveIf(key string, version uint64, pred func(prev types.Value) error) error
	Close() error
	//MaxVersion(key string) (types.Value, error)
	//MinVersion(key string) (types.Value, error)
}

type DB struct {
	vvs VersionedValues
}

func NewDB(getVersionedValues VersionedValues) *DB {
	return &DB{vvs: getVersionedValues}
}

func (db *DB) Get(_ context.Context, key string, opt types.KVReadOption) (types.Value, error) {
	if opt.ExactVersion {
		return db.vvs.Get(key, opt.Version)
	}
	return db.vvs.FindMaxBelow(key, opt.Version)
}

func (db *DB) Set(ctx context.Context, key string, val types.Value, opt types.KVWriteOption) error {
	if opt.IsClearWriteIntent() {
		if utils.IsDebug() {
			return db.updateFlagRaw(key, val.Version, func(value types.Value) types.Value {
				return value.WithNoWriteIntent()
			}, func(err error) error {
				if !Testing {
					glog.Fatalf("want to clear write intent for version %d of key %s, but the version doesn't exist", val.Version, key)
				}
				return err
			})
		}
		oldVal, err := db.vvs.Get(key, val.Version)
		if err != nil {
			if errors.IsNotExistsErr(err) && !Testing {
				glog.Fatalf("want to clear write intent for version %d of key %s, but the version doesn't exist", val.Version, key)
			}
			return errors.Annotatef(err, "key: %s", key)
		}
		if !oldVal.HasWriteIntent() {
			return nil
		}
		return db.vvs.Upsert(key, oldVal.WithNoWriteIntent())
	}
	if opt.IsRemoveVersion() {
		assert.Must(!val.HasWriteIntent())
		// TODO can remove the check in the future if stable enough
		if utils.IsDebug() {
			return db.vvs.RemoveIf(key, val.Version, func(prev types.Value) error {
				if !prev.HasWriteIntent() {
					if !Testing {
						glog.Fatalf("want to remove key %s of version %d which doesn't have write intent", key, val.Version)
					}
					return errors.ErrCantRemoveCommittedValue
				}
				return nil
			})
		}
		return db.vvs.Remove(key, val.Version)
	}
	return db.Upsert(ctx, key, val)
}

func (db *DB) updateFlag(key string, version uint64, modifyFlag func(types.Value) types.Value) error {
	return db.updateFlagRaw(key, version, modifyFlag, func(err error) error {
		glog.Errorf("failed to modify flag for version %d of key %s: version not exist", version, key)
		return err
	})
}

func (db *DB) updateFlagRaw(key string, version uint64, modifyFlag func(types.Value) types.Value, onNotExists func(err error) error) error {
	if err := db.vvs.UpdateFlag(key, version, modifyFlag, onNotExists); err != nil {
		return errors.Annotatef(err, "key: %s", key)
	}
	return nil
}

func (db *DB) Upsert(_ context.Context, key string, val types.Value) error {
	if err := db.vvs.Upsert(key, val); err != nil {
		return errors.Annotatef(err, "key: %s", key)
	}
	return nil
}

func (db *DB) Close() error {
	return db.vvs.Close()
}
