package main

import (
	"flag"

	"github.com/leisurelyrcxf/spermwhale/tablet"

	"github.com/leisurelyrcxf/spermwhale/cmd/common"

	"github.com/golang/glog"
)

func main() {
	port := flag.Int("port", 9999, "port")
	gid := flag.Int("gid", -1, "gid")
	common.RegisterStoreFlags()
	common.RegisterTxnConfigFlags()
	flag.Parse()

	if *gid == -1 {
		glog.Fatalf("must provide gid")
	}

	store := common.NewStore()
	cfg := common.NewTxnConfig()
	server := tablet.NewServer(*port, cfg, *gid, store)
	if err := server.Start(); err != nil {
		glog.Fatalf("failed to start: %v", err)
	}
	<-server.Done
}
