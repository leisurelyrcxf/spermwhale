package cmd

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/leisurelyrcxf/spermwhale/utils"

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
	flagCoordinatorType *string
	flagCoordinatorAddr *string
	flagCoordinatorAuth *string
)

func registerCoordinatorFlags() {
	flagCoordinatorType = flag.String("coordinator", "etcd",
		"coordinator type, must be one of the following: "+client.SupportedCoordinatorsDesc())
	flagCoordinatorAddr = flag.String("coordinator-addr", "127.0.0.1:2379",
		"coordinator addr list, could be form 127.0.0.1:2379,127.0.0.1:12379,,127.0.0.1:22379")
	flagCoordinatorAuth = flag.String("coordinator-auth", "", "coordinator auth")
}

func newClient() client.Client {
	cli, err := client.NewClient(*flagCoordinatorType, *flagCoordinatorAddr, *flagCoordinatorAuth, time.Minute)
	if err != nil {
		glog.Fatalf("create %s client failed: '%v', addr: %v", *flagCoordinatorType, err, *flagCoordinatorAddr)
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
	flagVersion *bool
)

func RegisterVersionFlags() {
	flagVersion = flag.Bool("version", false, "show version")
}

func CheckVersionFlag() {
	if *flagVersion {
		fmt.Println(utils.Version())
		os.Exit(0)
	}
}

func ParseFlags() {
	RegisterVersionFlags()
	flag.Parse()
	CheckVersionFlag()
}
