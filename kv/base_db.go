package kv

import (
	"context"
	"encoding/json"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/types"
	"github.com/leisurelyrcxf/spermwhale/utils"
)

var (
	Testing = false
)

type Meta struct {
	Flag            uint8                    `json:"F"`
	InternalVersion types.TxnInternalVersion `json:"I"`
}

func (m Meta) HasWriteIntent() bool {
	return m.Flag&consts.ValueMetaBitMaskHasWriteIntent == consts.ValueMetaBitMaskHasWriteIntent
}

type Value struct {
	Meta

	V []byte `json:"V"`
}

var EmptyValue = Value{}

func NewValue(v types.Value) Value {
	return Value{
		Meta: Meta{
			InternalVersion: v.InternalVersion,
			Flag:            v.Meta.Flag,
		},
		V: v.V,
	}
}

func (v Value) WithNoWriteIntent() Value {
	v.Meta.Flag &= consts.ValueMetaBitMaskClearWriteIntent
	return v
}

func (v Value) Encode() []byte {
	b, err := json.Marshal(v)
	if err != nil {
		glog.Fatalf("encode to json failed: '%v'", err)
	}
	return b
}

func (v *Value) Decode(data []byte) error {
	return json.Unmarshal(data, v)
}

func (v Value) WithVersion(version uint64) types.Value {
	return types.Value{
		Meta: types.Meta{
			Version:         version,
			InternalVersion: v.InternalVersion,
			Flag:            v.Meta.Flag,
		},
		V: v.V,
	}
}

type VersionedValues interface {
	Get(ctx context.Context, key string, version uint64) (Value, error)
	Upsert(ctx context.Context, key string, version uint64, val Value) error
	UpdateFlag(ctx context.Context, key string, version uint64, newFlag uint8) error
	FindMaxBelow(ctx context.Context, key string, upperVersion uint64) (Value, uint64, error)
	Remove(ctx context.Context, key string, version uint64) error
	RemoveIf(ctx context.Context, key string, version uint64, pred func(prev Value) error) error
	Close() error
	//MaxVersion(key string) (Value, error)
	//MinVersion(key string) (Value, error)
}

type VersionedValuesEx interface {
	ReadModifyWrite(ctx context.Context, key string, version uint64, modifyFlag func(val Value) Value, onNotExists func(err error) error) error
}

type DB struct {
	vvs VersionedValues
}

func NewDB(getVersionedValues VersionedValues) *DB {
	return &DB{vvs: getVersionedValues}
}

func (db *DB) Get(ctx context.Context, key string, opt types.KVReadOption) (types.Value, error) {
	var (
		val     Value
		version = opt.Version
		err     error
	)
	if opt.ExactVersion {
		val, err = db.vvs.Get(ctx, key, version)
	} else {
		val, version, err = db.vvs.FindMaxBelow(ctx, key, version)
	}
	if err != nil {
		return types.EmptyValue, err
	}
	return val.WithVersion(version), nil
}

func (db *DB) Set(ctx context.Context, key string, val types.Value, opt types.KVWriteOption) error {
	if opt.IsClearWriteIntent() {
		if utils.IsDebug() {
			return db.updateFlagRaw(ctx, key, val.Version, 0 /* TODO if there are multi bits, this is dangerous */, func(value Value) Value {
				return value.WithNoWriteIntent()
			}, func(err error) error {
				if !Testing {
					glog.Fatalf("want to clear write intent for version %d of key %s, but the version doesn't exist", val.Version, key)
				}
				return err
			})
		}
		oldVal, err := db.vvs.Get(ctx, key, val.Version)
		if err != nil {
			if errors.IsNotExistsErr(err) && !Testing {
				glog.Fatalf("want to clear write intent for version %d of key %s, but the version doesn't exist", val.Version, key)
			}
			return errors.Annotatef(err, "key: %s", key)
		}
		if !oldVal.HasWriteIntent() {
			return nil
		}
		return db.vvs.Upsert(ctx, key, val.Version, oldVal.WithNoWriteIntent())
	}
	if opt.IsRemoveVersion() {
		assert.Must(!val.HasWriteIntent())
		// TODO can remove the check in the future if stable enough
		if utils.IsDebug() {
			return db.vvs.RemoveIf(ctx, key, val.Version, func(prev Value) error {
				if !prev.HasWriteIntent() {
					if !Testing {
						glog.Fatalf("want to remove key %s of version %d which doesn't have write intent", key, val.Version)
					}
					return errors.ErrCantRemoveCommittedValue
				}
				return nil
			})
		}
		return db.vvs.Remove(ctx, key, val.Version)
	}
	return db.Upsert(ctx, key, val)
}

func (db *DB) updateFlag(ctx context.Context, key string, version uint64, newFlag uint8, modifyFlag func(Value) Value) error {
	return db.updateFlagRaw(ctx, key, version, newFlag, modifyFlag, func(err error) error {
		glog.Errorf("failed to modify flag for version %d of key %s: version not exist", version, key)
		return err
	})
}

func (db *DB) updateFlagRaw(ctx context.Context, key string, version uint64, newFlag uint8, modifyFlag func(Value) Value, onNotExists func(err error) error) error {
	var err error
	if vvsEx, ok := db.vvs.(VersionedValuesEx); ok {
		err = vvsEx.ReadModifyWrite(ctx, key, version, modifyFlag, onNotExists)
	} else {
		err = db.vvs.UpdateFlag(ctx, key, version, newFlag)
	}
	if err != nil {
		return errors.Annotatef(err, "key: %s", key)
	}
	return nil
}

func (db *DB) Upsert(ctx context.Context, key string, val types.Value) error {
	if err := db.vvs.Upsert(ctx, key, val.Version, NewValue(val)); err != nil {
		return errors.Annotatef(err, "key: %s", key)
	}
	return nil
}

func (db *DB) Close() error {
	return db.vvs.Close()
}
