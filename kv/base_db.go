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
	test = false
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

type KeyStore interface {
	GetKey(ctx context.Context, key string, version uint64) (Value, error)
	UpsertKey(ctx context.Context, key string, version uint64, val Value) error
	RemoveKey(ctx context.Context, key string, version uint64) error
	RemoveKeyIf(ctx context.Context, key string, version uint64, pred func(prev Value) error) error
	UpdateFlagOfKey(ctx context.Context, key string, version uint64, newFlag uint8) error
	FindMaxBelowOfKey(ctx context.Context, key string, upperVersion uint64) (Value, uint64, error)
	Close() error
}

type KeyStoreEx interface {
	ReadModifyWriteKey(ctx context.Context, key string, version uint64, modifyFlag func(val Value) Value, onNotExists func(err error) error) error
}

type TxnRecordStore interface {
	GetTxnRecord(ctx context.Context, version uint64) (Value, error)
	UpsertTxnRecord(ctx context.Context, version uint64, val Value) error
	RemoveTxnRecord(ctx context.Context, version uint64) error
	Close() error
}

type DB struct {
	vvs KeyStore
	ts  TxnRecordStore
}

func NewDB(getVersionedValues KeyStore, txnRecordStore TxnRecordStore) *DB {
	return &DB{vvs: getVersionedValues, ts: txnRecordStore}
}

func (db *DB) Get(ctx context.Context, key string, opt types.KVReadOption) (types.Value, error) {
	var (
		isTxnRecord = opt.IsTxnRecord()
		val         Value
		version     = opt.Version
		err         error
	)
	assert.Must((isTxnRecord && key == "" && opt.IsGetExactVersion()) || (!isTxnRecord && key != ""))
	if opt.IsGetExactVersion() {
		if isTxnRecord {
			val, err = db.ts.GetTxnRecord(ctx, version)
		} else {
			val, err = db.vvs.GetKey(ctx, key, version)
		}
	} else {
		assert.Must(!isTxnRecord)
		val, version, err = db.vvs.FindMaxBelowOfKey(ctx, key, version)
	}
	if err != nil {
		return types.EmptyValue, err
	}
	return val.WithVersion(version), nil
}

func (db *DB) Set(ctx context.Context, key string, val types.Value, opt types.KVWriteOption) error {
	var (
		isTxnRecord = opt.IsTxnRecord()
	)
	assert.Must((isTxnRecord && key == "") || (!isTxnRecord && key != ""))
	if opt.IsClearWriteIntent() {
		assert.Must(!isTxnRecord)
		if utils.IsDebug() {
			return db.updateFlagOfKeyRaw(ctx, key, val.Version, 0 /* TODO if there are multi bits, this is dangerous */, func(value Value) Value {
				return value.WithNoWriteIntent()
			}, func(err error) error {
				if !test {
					glog.Fatalf("want to clear write intent for version %d of key %s, but the version doesn't exist", val.Version, key)
				}
				return err
			})
		}
		oldVal, err := db.vvs.GetKey(ctx, key, val.Version)
		if err != nil {
			if errors.IsNotExistsErr(err) && !test {
				glog.Fatalf("want to clear write intent for version %d of key %s, but the version doesn't exist", val.Version, key)
			}
			return errors.Annotatef(err, "key: %s", key)
		}
		if !oldVal.HasWriteIntent() {
			return nil
		}
		return db.vvs.UpsertKey(ctx, key, val.Version, oldVal.WithNoWriteIntent())
	}
	if opt.IsRemoveVersion() {
		assert.Must(!val.IsDirty())
		// TODO can remove the check in the future if stable enough
		if !isTxnRecord && utils.IsDebug() {
			return db.vvs.RemoveKeyIf(ctx, key, val.Version, func(prev Value) error {
				if !prev.HasWriteIntent() {
					if !test {
						glog.Fatalf("want to remove key %s of version %d which doesn't have write intent", key, val.Version)
					}
					return errors.ErrCantRemoveCommittedValue
				}
				return nil
			})
		}
		if isTxnRecord {
			return db.ts.RemoveTxnRecord(ctx, val.Version)
		} else {
			return db.vvs.RemoveKey(ctx, key, val.Version)
		}
	}
	if isTxnRecord {
		return errors.Annotatef(db.ts.UpsertTxnRecord(ctx, val.Version, NewValue(val)), "txn-record: %d", val.Version)
	}
	return errors.Annotatef(db.vvs.UpsertKey(ctx, key, val.Version, NewValue(val)), "key: '%s'", key)
}

func (db *DB) Close() error {
	return errors.Wrap(db.vvs.Close(), db.ts.Close())
}

func (db *DB) updateFlagOfKeyRaw(ctx context.Context, key string, version uint64, newFlag uint8, modifyFlag func(Value) Value, onNotExists func(err error) error) error {
	var err error
	if vvsEx, ok := db.vvs.(KeyStoreEx); ok {
		err = vvsEx.ReadModifyWriteKey(ctx, key, version, modifyFlag, onNotExists)
	} else {
		err = db.vvs.UpdateFlagOfKey(ctx, key, version, newFlag)
	}
	if err != nil {
		return errors.Annotatef(err, "key: %s", key)
	}
	return nil
}
