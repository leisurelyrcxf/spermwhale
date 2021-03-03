package main

import (
	"flag"

	"github.com/leisurelyrcxf/spermwhale/consts"

	"github.com/leisurelyrcxf/spermwhale/cmd"

	"github.com/leisurelyrcxf/spermwhale/kvcc"

	"github.com/golang/glog"
)

func main() {
	cmd.RegisterPortFlags(consts.DefaultTabletServerPort)
	flagGid := flag.Int("gid", -1, "gid, range [0, groupNum)")
	flagTestMode := flag.Bool("test", false, "test mode, won't sleep at start")
	cmd.RegisterStoreFlags()
	cmd.RegisterTxnConfigFlags()
	flag.Parse()

	if *flagGid == -1 {
		glog.Fatalf("must provide gid")
	}

	store := cmd.NewStore()
	cfg := cmd.NewTxnConfig()
	var server *kvcc.Server
	if *flagTestMode {
		server = kvcc.NewServerForTesting(*cmd.FlagPort, cfg, *flagGid, store)
	} else {
		server = kvcc.NewServer(*cmd.FlagPort, cfg, *flagGid, store)
	}
	if err := server.Start(); err != nil {
		glog.Fatalf("failed to start kvcc server: %v", err)
	}
	<-server.Done
}
