package kv

import (
	"context"

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

type KeyStore interface {
	Get(ctx context.Context, key string, version uint64) (types.DBValue, error) // TODO add metaOnly
	Upsert(ctx context.Context, key string, version uint64, val types.DBValue) error
	Remove(ctx context.Context, key string, version uint64) error
	RemoveIf(ctx context.Context, key string, version uint64, pred func(prev types.DBValue) error) error
	UpdateFlag(ctx context.Context, key string, version uint64, newFlag uint8) error
	Floor(ctx context.Context, key string, upperVersion uint64) (types.DBValue, uint64, error)
	Close() error
}

type KeyStoreEx interface {
	ReadModifyWriteKey(ctx context.Context, key string, version uint64, modifyFlag func(val types.DBValue) types.DBValue, onNotExists func(err error) error) error
}

type TxnRecordStore interface {
	GetTxnRecord(ctx context.Context, version uint64) (types.DBValue, error)
	UpsertTxnRecord(ctx context.Context, version uint64, val types.DBValue) error
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
		val         types.DBValue
		version     = opt.Version
		err         error
	)
	assert.Must((isTxnRecord && key == "" && opt.IsReadExactVersion()) || (!isTxnRecord && key != ""))
	if opt.IsReadExactVersion() {
		if isTxnRecord {
			val, err = db.ts.GetTxnRecord(ctx, version)
		} else {
			val, err = db.vvs.Get(ctx, key, version)
		}
	} else {
		assert.Must(!isTxnRecord)
		val, version, err = db.vvs.Floor(ctx, key, version)
	}
	if err != nil {
		return types.EmptyValue, err
	}
	return val.WithVersion(version), nil
}

func (db *DB) Set(ctx context.Context, key string, val types.Value, _ types.KVWriteOption) error {
	var (
		isTxnRecord = val.IsTxnRecord()
	)
	assert.Must((isTxnRecord && key == "") || (!isTxnRecord && key != ""))
	if isTxnRecord {
		return errors.Annotatef(db.ts.UpsertTxnRecord(ctx, val.Version, val.ToDB()), "txn-record: %d", val.Version)
	}
	return errors.Annotatef(db.vvs.Upsert(ctx, key, val.Version, val.ToDB()), "key: '%s'", key)
}

func (db *DB) UpdateMeta(ctx context.Context, key string, version uint64, opt types.KVUpdateMetaOption) error {
	if !opt.IsClearWriteIntent() {
		return errors.ErrNotSupported
	}
	if utils.IsDebug() {
		return db.updateFlagOfKeyRaw(ctx, key, version, consts.ValueMetaBitMaskCommitted /* TODO if there are multi bits, this is dangerous */, func(value types.DBValue) types.DBValue {
			return value.WithCommitted()
		}, func(err error) error {
			if !test {
				_, _ = key, version
				glog.Fatalf("want to clear write intent for version %d of key %s, but the version doesn't exist", version, key)
			}
			return err
		})
	}
	oldVal, err := db.vvs.Get(ctx, key, version)
	if err != nil {
		if errors.IsNotExistsErr(err) && !test {
			glog.Fatalf("want to clear write intent for version %d of key %s, but the version doesn't exist", version, key)
		}
		return errors.Annotatef(err, "key: %s", key)
	}
	if oldVal.IsCommitted() {
		return nil
	}
	return db.vvs.Upsert(ctx, key, version, oldVal.WithCommitted())
}

func (db *DB) RollbackKey(ctx context.Context, key string, version uint64) error {
	assert.Must(key != "")

	// TODO can remove the check in the future if stable enough
	if utils.IsDebug() {
		return db.vvs.RemoveIf(ctx, key, version, func(prev types.DBValue) error {
			if prev.IsCommitted() {
				if !test {
					glog.Fatalf("want to remove key %s of version %d which doesn't have write intent", key, version)
				}
				return errors.ErrCantRemoveCommittedValue
			}
			return nil
		})
	}
	return db.vvs.Remove(ctx, key, version)
}

func (db *DB) RemoveTxnRecord(ctx context.Context, version uint64) error {
	return db.ts.RemoveTxnRecord(ctx, version)
}

func (db *DB) Close() error {
	return errors.Wrap(db.vvs.Close(), db.ts.Close())
}

func (db *DB) updateFlagOfKeyRaw(ctx context.Context, key string, version uint64, newFlag uint8, modifyFlag func(types.DBValue) types.DBValue, onNotExists func(err error) error) error {
	var err error
	if vvsEx, ok := db.vvs.(KeyStoreEx); ok {
		err = vvsEx.ReadModifyWriteKey(ctx, key, version, modifyFlag, onNotExists)
	} else {
		err = db.vvs.UpdateFlag(ctx, key, version, newFlag)
	}
	if err != nil {
		return errors.Annotatef(err, "key: %s", key)
	}
	return nil
}
