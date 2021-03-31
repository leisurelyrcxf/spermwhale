package mongodb

import (
	"context"
	"fmt"

	"github.com/golang/glog"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/kv"
)

const (
	txnRecordCollection = "txn_records"
)

type MTxnStore struct {
	cli *mongo.Client
}

func newMongoTxnStore(cli *mongo.Client) MTxnStore {
	return MTxnStore{cli: cli}
}

func pkEqualOfTxnRecord(version uint64) bson.D {
	return bson.D{
		{Key: attrId, Value: bson.D{
			{Key: "$eq", Value: version}},
		}}
}

func encodeTxnRecord(val kv.Value) bson.D {
	assert.Must(val.InternalVersion == 0)
	return bson.D{
		{Key: attrFlag, Value: val.Flag},
		{Key: attrValue, Value: val.V},
	}
}

func (m MTxnStore) GetTxnRecord(ctx context.Context, version uint64) (value kv.Value, err error) {
	res := m.cli.Database(defaultDatabase).Collection(txnRecordCollection).FindOne(ctx, pkEqualOfTxnRecord(version))
	if err := res.Err(); err != nil {
		return kv.EmptyValue, errors.CASError2(err, mongo.ErrNoDocuments, errors.ErrKeyOrVersionNotExist)
	}
	raw, err := res.DecodeBytes()
	if err != nil {
		return kv.EmptyValue, err
	}
	elements, err := raw.Elements()
	if err != nil {
		return kv.EmptyValue, err
	}

	var (
		gotAttrs int
	)
	for _, ele := range elements {
		switch attr, val := ele.Key(), ele.Value(); attr {
		case attrId:
			v, ok := val.Int64OK()
			assert.Must(ok)
			assert.Must(uint64(v) == version)
			gotAttrs++
		case attrFlag:
			v, ok := val.Int32OK()
			assert.Must(ok)
			value.Flag = uint8(v)
			gotAttrs++
		case attrValue:
			switch val.Type {
			case bsontype.Null:
				value.V = ([]byte)(nil)
				gotAttrs++
			case bsontype.Binary:
				_, bytes, ok := val.BinaryOK()
				if !ok {
					panic(!ok) // TODO remove the asserts
				}
				value.V = bytes
				gotAttrs++
			default:
				panic(fmt.Sprintf("unknown type: '%v'", val.Type))
			}
		default:
			assert.Must(false)
		}
	}

	if gotAttrs != 3 {
		return kv.EmptyValue, fmt.Errorf("expect got 2 fields but got only %d", gotAttrs) // TODO change error
	}
	return value, nil
}

func (m MTxnStore) UpsertTxnRecord(ctx context.Context, version uint64, val kv.Value) error {
	updateResult, err := m.cli.Database(defaultDatabase).Collection(txnRecordCollection).ReplaceOne(ctx, pkEqualOfTxnRecord(version),
		encodeTxnRecord(val), options.Replace().SetUpsert(true))
	if err != nil {
		glog.Errorf("[MongoVVS][UpsertTxnRecord] txn-%d upsert txn record failed: '%v'", version, err)
		return err
	}
	glog.V(80).Infof("[MongoVVS][UpsertTxnRecord] txn-%d upsert txn record succeeded, upserted record id: '%v'", version, updateResult.UpsertedID)
	return nil
}

func (m MTxnStore) RemoveTxnRecord(ctx context.Context, version uint64) error {
	deleteResult, err := m.cli.Database(defaultDatabase).Collection(txnRecordCollection).DeleteOne(ctx, pkEqualOfTxnRecord(version))
	if err != nil {
		glog.Errorf("[MongoVVS][RemoveTxnRecord] txn-%d remove txn record failed: '%v'", version, err)
		return err
	}
	glog.V(80).Infof("[MongoVVS][RemoveTxnRecord] txn-%d remove txn record succeeded, removed record count: %d", version, deleteResult.DeletedCount)
	return nil
}

func (m MTxnStore) Close() error {
	// let MongoVVS close this
	return nil
}
