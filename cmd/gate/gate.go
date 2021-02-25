package main

import (
	"flag"

	"github.com/leisurelyrcxf/spermwhale/consts"

	"github.com/leisurelyrcxf/spermwhale/gate"
	"github.com/leisurelyrcxf/spermwhale/txn"

	"github.com/golang/glog"
	"github.com/leisurelyrcxf/spermwhale/cmd/common"
)

func main() {
	port := flag.Int("port", 9999, "port")
	workerNum := flag.Int("txn-worker-num", consts.DefaultTxnManagerWorkerNumber, "txn manager worker number")
	common.RegisterStoreFlags()
	common.RegisterTxnConfigFlags()
	flag.Parse()

	store := common.NewStore()
	gAte, err := gate.NewGate(store)
	if err != nil {
		glog.Fatalf("can't create gate: %v", err)
	}
	cfg := common.NewTxnConfig()
	server := txn.NewServer(gAte, cfg, *workerNum, *port)
	if err := server.Start(); err != nil {
		glog.Fatalf("failed to start: %v", err)
	}
	<-server.Done
}
