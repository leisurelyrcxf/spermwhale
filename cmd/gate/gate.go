package main

import (
	"flag"

	"github.com/leisurelyrcxf/spermwhale/types"

	"github.com/leisurelyrcxf/spermwhale/cmd"

	"github.com/leisurelyrcxf/spermwhale/kv"

	"github.com/leisurelyrcxf/spermwhale/consts"

	"github.com/leisurelyrcxf/spermwhale/gate"
	"github.com/leisurelyrcxf/spermwhale/txn"

	"github.com/golang/glog"
)

func main() {
	flagTxnPort := flag.Int("port-txn", consts.DefaultTxnServerPort, "txn port")
	flagKVPort := flag.Int("port-kv", consts.DefaultKVServerPort, "kv port ")
	flagClearWorkerNum := flag.Int("clear-worker-num", consts.DefaultTxnManagerClearWorkerNumber, "txn manager worker number")
	flagIOWorkerNum := flag.Int("io-worker-num", consts.DefaultTxnManagerIOWorkerNumber, "txn manager worker number")
	flagWoundUncommittedTxnThreshold := flag.Duration("wound-uncommitted-txn-threshold", consts.DefaultWoundUncommittedTxnThreshold,
		"transaction older than this may be wounded by another transaction")
	cmd.RegisterStoreFlags()
	cmd.ParseFlags()

	store := cmd.NewStore()
	gAte, err := gate.NewGate(store)
	if err != nil {
		glog.Fatalf("can't create gate: %v", err)
	}
	cfg := types.NewTxnManagerConfig(*flagWoundUncommittedTxnThreshold)
	if err := cfg.Validate(); err != nil {
		glog.Fatalf("invalid config: %v", err)
	}
	txnServer, err := txn.NewServer(*flagTxnPort, gAte, cfg, *flagClearWorkerNum, *flagIOWorkerNum, store)
	if err != nil {
		glog.Fatalf("failed to new txn server: %v", err)
	}
	if err := txnServer.Start(); err != nil {
		glog.Fatalf("failed to start txn server: %v", err)
	}
	kvServer := kv.NewServer(*flagKVPort, gate.NewReadOnlyKV(gAte))
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
