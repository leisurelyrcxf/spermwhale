package mongodb

import (
	"context"
	"fmt"
	"time"

	"github.com/leisurelyrcxf/spermwhale/kv"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	defaultDatabase = "spermwhale_db"

	attrId    = "_id"
	attrFlag  = "flag"
	attrValue = "V"
)

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
	return kv.NewDB(newMongoVVS(client), newMongoTxnStore(client)), nil
}
