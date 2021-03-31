package mongodb

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/kv"
	"github.com/leisurelyrcxf/spermwhale/types"
)

const (
	keyCollection = "keys"

	keyAttrIdKey           = "key"
	keyAttrIdVersion       = "version"
	keyAttrInternalVersion = "internal_version"
)

type MongoVVS struct {
	cli *mongo.Client
}

func newMongoVVS(cli *mongo.Client) MongoVVS {
	return MongoVVS{cli: cli}
}

func pkEqualOfKey(key string, version uint64) bson.D {
	return bson.D{{Key: attrId, Value: bson.D{
		{Key: "$eq", Value: bson.D{
			{Key: keyAttrIdKey, Value: key},
			{Key: keyAttrIdVersion, Value: version}},
		}},
	}}
}

func encodeValueOfKey(val kv.Value) bson.D {
	return bson.D{
		{Key: attrFlag, Value: val.Flag},
		{Key: keyAttrInternalVersion, Value: val.InternalVersion},
		{Key: attrValue, Value: val.V},
	}
}

func (m MongoVVS) GetKey(ctx context.Context, key string, version uint64) (kv.Value, error) {
	gotKey, val, _, err := m.getOne(m.cli.Database(defaultDatabase).Collection(keyCollection).FindOne(ctx, pkEqualOfKey(key, version)))
	if err != nil {
		return kv.EmptyValue, err
	}
	assert.Must(gotKey == key)
	return val, nil
}

func (m MongoVVS) FindMaxBelowOfKey(ctx context.Context, key string, upperVersion uint64) (kv.Value, uint64, error) {
	gotKey, value, version, err := m.getOne(m.cli.Database(defaultDatabase).Collection(keyCollection).FindOne(ctx, bson.D{
		{Key: attrId, Value: bson.D{
			{Key: "$lte", Value: bson.D{
				{Key: keyAttrIdKey, Value: key},
				{Key: keyAttrIdVersion, Value: upperVersion}},
			}},
		}}, options.FindOne().SetSort(bson.D{{Key: attrId, Value: -1}})))
	if err != nil {
		return kv.EmptyValue, 0, err
	}
	if gotKey != key {
		assert.Must(gotKey < key)
		return kv.EmptyValue, 0, errors.ErrKeyOrVersionNotExist
	}
	return value, version, nil
}

func (m MongoVVS) UpsertKey(ctx context.Context, key string, version uint64, val kv.Value) error {
	single, err := m.cli.Database(defaultDatabase).Collection(keyCollection).ReplaceOne(ctx, pkEqualOfKey(key, version),
		encodeValueOfKey(val), options.Replace().SetUpsert(true))
	if err != nil {
		glog.Errorf("[MongoVVS][UpsertKey] txn-%d upsert key '%s' failed: '%v'", version, key, err)
		return err
	}
	glog.V(80).Infof("[MongoVVS][UpsertKey] txn-%d upsert key '%s' succeeded, inserted id: '%v", version, key, single.UpsertedID)
	return nil
}

func (m MongoVVS) UpdateFlagOfKey(ctx context.Context, key string, version uint64, newFlag uint8) error {
	return errors.CASError2(m.cli.Database(defaultDatabase).Collection(keyCollection).FindOneAndUpdate(ctx, pkEqualOfKey(key, version),
		bson.D{{Key: "$set", Value: bson.D{
			{Key: attrFlag, Value: newFlag}},
		}}).Err(), mongo.ErrNoDocuments, errors.ErrKeyOrVersionNotExist)
}

func (m MongoVVS) RemoveKey(ctx context.Context, key string, version uint64) error {
	deleteResult, err := m.cli.Database(defaultDatabase).Collection(keyCollection).DeleteOne(ctx, pkEqualOfKey(key, version))
	if err != nil {
		glog.Errorf("[MongoVVS][RemoveKey] txn-%d remove key '%s' failed: '%v'", version, key, err)
		return err
	}
	glog.V(80).Infof("[MongoVVS][RemoveKey] txn-%d remove key '%s' succeeded, deleted count: %d", version, key, deleteResult.DeletedCount)
	return nil
}

func (m MongoVVS) RemoveKeyIf(ctx context.Context, key string, version uint64, pred func(prev kv.Value) error) error {
	val, err := m.GetKey(ctx, key, version)
	if err != nil {
		return err
	}
	if err := pred(val); err != nil {
		return err
	}
	return m.RemoveKey(ctx, key, version)
}

func (m MongoVVS) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	return m.cli.Disconnect(ctx)
}

func (m MongoVVS) getOne(res *mongo.SingleResult) (gotKey string, value kv.Value, version uint64, _ error) {
	if err := res.Err(); err != nil {
		return "", kv.EmptyValue, 0, errors.CASError2(err, mongo.ErrNoDocuments, errors.ErrKeyOrVersionNotExist)
	}
	raw, err := res.DecodeBytes()
	if err != nil {
		return "", kv.EmptyValue, 0, err
	}
	elements, err := raw.Elements()
	if err != nil {
		return "", kv.EmptyValue, 0, err
	}

	var (
		gotAttrs int
	)
	for _, ele := range elements {
		switch attr, val := ele.Key(), ele.Value(); attr {
		case attrId:
			idDoc, ok := ele.Value().DocumentOK()
			assert.Must(ok)
			idElements, err := idDoc.Elements()
			if err != nil {
				return "", kv.EmptyValue, 0, err
			}
			for _, idEle := range idElements {
				switch idAttr, idVal := idEle.Key(), idEle.Value(); idAttr {
				case keyAttrIdKey:
					idKey, ok := idVal.StringValueOK()
					assert.Must(ok)
					gotKey = idKey
					gotAttrs++
				case keyAttrIdVersion:
					txnId, ok := idVal.Int64OK()
					assert.Must(ok)
					version = uint64(txnId)
					gotAttrs++
				}
			}
		case attrFlag:
			v, ok := val.Int32OK()
			assert.Must(ok)
			value.Flag = uint8(v)
			gotAttrs++
		case keyAttrInternalVersion:
			iVersion, ok := val.Int32OK()
			assert.Must(ok)
			value.InternalVersion = types.TxnInternalVersion(iVersion)
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

	if gotAttrs != 5 {
		return "", kv.EmptyValue, 0, fmt.Errorf("expect 5 fields but got '%v'", gotAttrs)
	}
	return gotKey, value, version, nil
}

//func encode(key string, version uint64, val kv.Value) bson.D {
//	return bson.D{
//		{attrId, bson.D{
//		{keyAttrIdKey, key},
//		{keyAttrIdVersion, version},
//	}},
//		{attrFlag, val.Flag},
//		{keyAttrInternalVersion, val.InternalVersion},
//		{attrValue, val.V},
//	}
//}
