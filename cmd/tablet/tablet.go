package main

import (
	"flag"

	"github.com/leisurelyrcxf/spermwhale/cmd"

	"github.com/leisurelyrcxf/spermwhale/tablet"

	"github.com/golang/glog"
)

func main() {
	cmd.RegisterPortFlags(20000)
	flagGid := flag.Int("gid", -1, "gid")
	cmd.RegisterStoreFlags()
	cmd.RegisterTxnConfigFlags()
	flag.Parse()

	if *flagGid == -1 {
		glog.Fatalf("must provide gid")
	}

	store := cmd.NewStore()
	cfg := cmd.NewTxnConfig()
	server := tablet.NewServer(*cmd.FlagPort, cfg, *flagGid, store)
	if err := server.Start(); err != nil {
		glog.Fatalf("failed to start tablet server: %v", err)
	}
	<-server.Done
}
