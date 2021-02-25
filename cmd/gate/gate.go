package main

import (
	"flag"

	"github.com/leisurelyrcxf/spermwhale/kv"

	"github.com/leisurelyrcxf/spermwhale/consts"

	"github.com/leisurelyrcxf/spermwhale/gate"
	"github.com/leisurelyrcxf/spermwhale/txn"

	"github.com/golang/glog"
	"github.com/leisurelyrcxf/spermwhale/cmd/common"
)

func main() {
	txnPort := flag.Int("port-txn", 9999, "txn port")
	kvPort := flag.Int("port-kv", 10001, "kv port ")
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
	txnServer := txn.NewServer(gAte, cfg, *workerNum, *txnPort)
	if err := txnServer.Start(); err != nil {
		glog.Fatalf("failed to start txn server: %v", err)
	}
	kvServer := kv.NewServer(*kvPort, gAte, true)
	if err := kvServer.Start(); err != nil {
		glog.Fatalf("failed to start kv server: %v", err)
	}
	select {
	case <-txnServer.Done:
		return
	case <-kvServer.Done:
		return
	}
}
