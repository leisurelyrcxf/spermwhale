package common

import (
	"flag"
	"time"

	"github.com/leisurelyrcxf/spermwhale/types"

	"github.com/leisurelyrcxf/spermwhale/consts"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/models/client"

	"github.com/leisurelyrcxf/spermwhale/models"
)

var (
	CoordinatorType     *string
	CoordinatorAddrList *string
	CoordinatorAuth     *string
)

func RegisterCoordinatorFlags() {
	CoordinatorType = flag.String("coordinator", "etcd", "client type")
	CoordinatorAddrList = flag.String("coordinator-addr-list", "127.0.0.1:2379", "coordinator addr list")
	CoordinatorAuth = flag.String("coordinator-auth", "", "coordinator auth")
}

func NewClient() client.Client {
	cli, err := client.NewClient(*CoordinatorType, *CoordinatorAddrList, *CoordinatorAuth, time.Minute)
	if err != nil {
		glog.Fatalf("create %s client failed: '%v', addr: %v", *CoordinatorType, err, *CoordinatorAddrList)
	}
	return cli
}

var (
	ClusterName *string
)

func RegisterStoreFlags() {
	ClusterName = flag.String("cluster-name", "spermwhale", "cluster name")
}

func NewStore() *models.Store {
	cli := NewClient()
	return models.NewStore(cli, *ClusterName)
}

var (
	TxnConfigStaleWriteThreshold *time.Duration
	TxnConfigMaxClockDrift       *time.Duration
)

func RegisterTxnConfigFlags() {
	TxnConfigStaleWriteThreshold = flag.Duration("txn-stale-write-threshold", consts.DefaultTooStaleWriteThreshold, "stale write threshold")
	TxnConfigMaxClockDrift = flag.Duration("max-clock-drift", consts.DefaultMaxClockDrift, "stale write threshold")
}

func NewTxnConfig() types.TxnConfig {
	return types.TxnConfig{
		StaleWriteThreshold: *TxnConfigStaleWriteThreshold,
		MaxClockDrift:       *TxnConfigMaxClockDrift,
	}
}
