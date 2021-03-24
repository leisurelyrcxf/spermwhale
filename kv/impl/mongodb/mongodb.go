package mongodb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/kv"
	"github.com/leisurelyrcxf/spermwhale/types"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	defaultDatabase     = "spermwhale_db"
	collectionKeys      = "keys"
	collectionTxnRecord = "txn_records"

	attrId        = "_id"
	attrIdKey     = "key"
	attrIdVersion = "version"

	attrFlag            = "flag"
	attrInternalVersion = "internal_version"
	attrValue           = "V"
)

type MongoVVS struct {
	cli *mongo.Client
}

func newMongoVVS(cli *mongo.Client) MongoVVS {
	return MongoVVS{cli: cli}
}

func pkEqual(key string, version uint64) bson.D {
	return bson.D{{"_id", bson.D{
		{Key: "$eq", Value: bson.D{
			{attrIdKey, key},
			{attrIdVersion, version}},
		}},
	}}
}

//func encode(key string, version uint64, val kv.Value) bson.D {
//	return bson.D{
//		{"_id", bson.D{
//		{attrIdKey, key},
//		{attrIdVersion, version},
//	}},
//		{attrFlag, val.Flag},
//		{attrInternalVersion, val.InternalVersion},
//		{attrValue, val.V},
//	}
//}

func encodeValue(val kv.Value) bson.D {
	return bson.D{
		{attrFlag, val.Flag},
		{attrInternalVersion, val.InternalVersion},
		{attrValue, val.V},
	}
}

func getCollection(key string) string {
	if strings.HasPrefix(key, "txn-") {
		return collectionTxnRecord
	}
	return collectionKeys
}

func (m MongoVVS) GetTxnRecord(ctx context.Context, version uint64) (kv.Value, error) {
	return m.GetKey(ctx, types.TxnId(version).String(), version)
}

func (m MongoVVS) UpsertTxnRecord(ctx context.Context, version uint64, val kv.Value) error {
	return m.UpsertKey(ctx, types.TxnId(version).String(), version, val)
}

func (m MongoVVS) RemoveTxnRecord(ctx context.Context, version uint64) error {
	return m.RemoveKey(ctx, types.TxnId(version).String(), version)
}

func (m MongoVVS) GetKey(ctx context.Context, key string, version uint64) (kv.Value, error) {
	gotKey, val, _, err := m.getOne(key, m.cli.Database(defaultDatabase).Collection(getCollection(key)).FindOne(ctx, pkEqual(key, version)))
	if err != nil {
		return kv.EmptyValue, err
	}
	assert.Must(gotKey == key)
	return val, nil
}

func (m MongoVVS) FindMaxBelowOfKey(ctx context.Context, key string, upperVersion uint64) (kv.Value, uint64, error) {
	gotKey, value, version, err := m.getOne(key, m.cli.Database(defaultDatabase).Collection(getCollection(key)).FindOne(ctx, bson.D{
		{attrId, bson.D{
			{"$lte", bson.D{
				{attrIdKey, key},
				{attrIdVersion, upperVersion}},
			}},
		}}, options.FindOne().SetSort(bson.D{{attrId, -1}})))
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
	collection := m.cli.Database(defaultDatabase).Collection(getCollection(key))
	single, err := collection.ReplaceOne(ctx, pkEqual(key, version),
		encodeValue(val), options.Replace().SetUpsert(true))
	if err != nil {
		glog.Errorf("[MongoVVS][UpsertKey] txn-%d upsert key '%s' failed: '%v'", version, key, err)
		return err
	}
	glog.V(80).Infof("inserted one document with id: '%v'", single.UpsertedID)
	return nil
}

func (m MongoVVS) UpdateFlagOfKey(ctx context.Context, key string, version uint64, newFlag uint8) error {
	return errors.CASError2(m.cli.Database(defaultDatabase).Collection(getCollection(key)).FindOneAndUpdate(ctx, pkEqual(key, version),
		bson.D{{"$set", bson.D{{attrFlag, newFlag}}}}).Err(), mongo.ErrNoDocuments, errors.ErrKeyOrVersionNotExist)
}

func (m MongoVVS) RemoveKey(ctx context.Context, key string, version uint64) error {
	_, err := m.cli.Database(defaultDatabase).Collection(getCollection(key)).DeleteOne(ctx, pkEqual(key, version))
	return err
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

func (m MongoVVS) getOne(key string, res *mongo.SingleResult) (gotKey string, value kv.Value, version uint64, _ error) {
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
				case attrIdKey:
					idKey, ok := idVal.StringValueOK()
					assert.Must(ok)
					gotKey = idKey
					gotAttrs++
				case attrIdVersion:
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
		case attrInternalVersion:
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

func NewDB(addr string, credential *options.Credential) (*kv.DB, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opt := options.Client().SetMaxPoolSize(400).SetMinPoolSize(50)
	if credential != nil {
		opt.SetAuth(*credential)
	}
	client, err := mongo.Connect(ctx, opt.ApplyURI(fmt.Sprintf("mongodb://%s", addr)))
	if err != nil {
		return nil, err
	}
	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, err
	}
	return kv.NewDB(newMongoVVS(client), newMongoVVS(client)), nil
}
