package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/bsontype"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/assert"
	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/kv"
	"github.com/leisurelyrcxf/spermwhale/types"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	defaultDatabase = "spermwhale_db"

	attrId              = "_id"
	attrFlag            = "flag"
	attrInternalVersion = "internal_version"
	attrValue           = "V"
)

var allAttrs = []string{attrId, attrFlag, attrInternalVersion, attrValue}

type MongoVVS struct {
	cli *mongo.Client
}

func newMongoVVS(cli *mongo.Client) MongoVVS {
	return MongoVVS{cli: cli}
}

func (m MongoVVS) encode(version uint64, val kv.Value) bson.D {
	return bson.D{
		{"_id", version},
		{attrFlag, val.Flag},
		{attrInternalVersion, val.InternalVersion},
		{attrValue, val.V},
	}
}

func (m MongoVVS) encodeValue(val kv.Value) bson.D {
	return bson.D{
		{attrFlag, val.Flag},
		{attrInternalVersion, val.InternalVersion},
		{attrValue, val.V},
	}
}

func (m MongoVVS) Get(ctx context.Context, key string, version uint64) (kv.Value, error) {
	res := m.cli.Database(defaultDatabase).Collection(key).FindOne(ctx, bson.D{
		{"_id", bson.D{
			{"$eq", version},
		}},
	})
	val, _, err := m.getOne(res)
	return val, err
}

func (m MongoVVS) Upsert(ctx context.Context, key string, version uint64, val kv.Value) error {
	collection := m.cli.Database(defaultDatabase).Collection(key)
	single, err := collection.UpdateOne(ctx, bson.D{
		{"_id", bson.D{
			{"$eq", version},
		}},
	}, bson.D{
		{"$set", m.encodeValue(val)},
	}, options.Update().SetUpsert(true))
	if err != nil {
		return err
	}
	glog.Infof("inserted one document with id: '%v'", single.UpsertedID)
	return nil
}

func (m MongoVVS) UpdateFlag(ctx context.Context, key string, version uint64, newFlag uint8) error {
	return errors.CASError2(m.cli.Database(defaultDatabase).Collection(key).FindOneAndUpdate(
		ctx, bson.D{
			{"_id", bson.D{
				{"$eq", version},
			}},
		},
		bson.D{{"$set", bson.D{{attrFlag, newFlag}}}}).Err(), mongo.ErrNoDocuments, errors.ErrVersionNotExists)
}

func (m MongoVVS) FindMaxBelow(ctx context.Context, key string, upperVersion uint64) (kv.Value, uint64, error) {
	return m.getOne(m.cli.Database(defaultDatabase).Collection(key).FindOne(ctx, bson.D{
		{"_id", bson.D{
			{"$lte", upperVersion},
		}},
	}, options.FindOne().SetSort(bson.D{{"_id", -1}})))
}

func (m MongoVVS) Remove(ctx context.Context, key string, version uint64) error {
	_, err := m.cli.Database(defaultDatabase).Collection(key).DeleteOne(ctx, bson.D{
		{"_id", bson.D{
			{"$eq", version},
		}},
	})
	return err
}

func (m MongoVVS) RemoveIf(ctx context.Context, key string, version uint64, pred func(prev kv.Value) error) error {
	val, err := m.Get(ctx, key, version)
	if err != nil {
		return err
	}
	if err := pred(val); err != nil {
		return err
	}
	return m.Remove(ctx, key, version)
}

func (m MongoVVS) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	return m.cli.Disconnect(ctx)
}

func (m MongoVVS) getOne(res *mongo.SingleResult) (kv.Value, uint64, error) {
	if err := res.Err(); err != nil {
		return kv.EmptyValue, 0, errors.CASError2(err, mongo.ErrNoDocuments, errors.ErrVersionNotExists)
	}
	raw, err := res.DecodeBytes()
	if err != nil {
		return kv.EmptyValue, 0, err
	}

	fields := make(map[string]interface{})
	for _, attr := range allAttrs {
		val, err := raw.LookupErr(attr)
		if err != nil {
			return kv.EmptyValue, 0, err
		}
		switch attr {
		case attrId:
			id, ok := val.Int64OK()
			assert.Must(ok)
			fields[attr] = uint64(id)
		case attrFlag:
			flag, ok := val.Int32OK()
			assert.Must(ok)
			fields[attr] = uint8(flag)
		case attrInternalVersion:
			iVersion, ok := val.Int32OK()
			assert.Must(ok)
			fields[attr] = types.TxnInternalVersion(iVersion)
		case attrValue:
			switch val.Type {
			case bsontype.Null:
				fields[attr] = ([]byte)(nil)
			case bsontype.Binary:
				_, bytes, ok := val.BinaryOK()
				if !ok {
					panic(!ok) // TODO remove the asserts
				}
				fields[attr] = bytes
			default:
				panic(fmt.Sprintf("unknown type: '%v'", val.Type))
			}
		default:
			assert.Must(false)
		}
	}
	if len(fields) != 4 {
		return kv.EmptyValue, 0, fmt.Errorf("expect 4 fields, got %v", fields)
	}
	glog.Errorf("doc: %v", fields)
	return kv.Value{
		Meta: kv.Meta{
			Flag:            fields[attrFlag].(uint8),
			InternalVersion: fields[attrInternalVersion].(types.TxnInternalVersion),
		},
		V: fields[attrValue].([]byte),
	}, fields[attrId].(uint64), nil
}

func NewDB(addr string, credential *options.Credential) (*kv.DB, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli := options.Client()
	if credential != nil {
		cli.SetAuth(*credential)
	}
	client, err := mongo.Connect(ctx, cli.ApplyURI(fmt.Sprintf("mongodb://%s", addr)))
	if err != nil {
		return nil, err
	}
	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, err
	}
	return kv.NewDB(newMongoVVS(client)), nil
}
