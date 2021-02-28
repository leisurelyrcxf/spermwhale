package cmd

import (
	"flag"
	"time"

	"github.com/leisurelyrcxf/spermwhale/types"

	"github.com/leisurelyrcxf/spermwhale/consts"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/topo/client"

	"github.com/leisurelyrcxf/spermwhale/topo"
)

var (
	FlagPort *int
)

func RegisterPortFlags(defaultPort int) {
	FlagPort = flag.Int("port", defaultPort, "port")
}

var (
	flagCoordinatorType     *string
	flagCoordinatorAddrList *string
	flagCoordinatorAuth     *string
)

func registerCoordinatorFlags() {
	flagCoordinatorType = flag.String("coordinator", "etcd", "client type")
	flagCoordinatorAddrList = flag.String("coordinator-addr-list", "127.0.0.1:2379", "coordinator addr list")
	flagCoordinatorAuth = flag.String("coordinator-auth", "", "coordinator auth")
}

func newClient() client.Client {
	cli, err := client.NewClient(*flagCoordinatorType, *flagCoordinatorAddrList, *flagCoordinatorAuth, time.Minute)
	if err != nil {
		glog.Fatalf("create %s client failed: '%v', addr: %v", *flagCoordinatorType, err, *flagCoordinatorAddrList)
	}
	return cli
}

var (
	FlagClusterName *string
)

func RegisterStoreFlags() {
	FlagClusterName = flag.String("cluster-name", "spermwhale", "cluster name")
	registerCoordinatorFlags()
}

func NewStore() *topo.Store {
	cli := newClient()
	return topo.NewStore(cli, *FlagClusterName)
}

var (
	flagTxnConfigStaleWriteThreshold *time.Duration
	flagTxnConfigMaxClockDrift       *time.Duration
)

func RegisterTxnConfigFlags() {
	flagTxnConfigStaleWriteThreshold = flag.Duration("txn-stale-write-threshold", consts.DefaultTooStaleWriteThreshold, "stale write threshold")
	flagTxnConfigMaxClockDrift = flag.Duration("max-clock-drift", consts.DefaultMaxClockDrift, "stale write threshold")
}

func NewTxnConfig() types.TxnConfig {
	return types.TxnConfig{
		StaleWriteThreshold: *flagTxnConfigStaleWriteThreshold,
		MaxClockDrift:       *flagTxnConfigMaxClockDrift,
	}
}
