package main

import (
	"flag"

	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"

	"github.com/leisurelyrcxf/spermwhale/oracle/impl/logical"

	"github.com/leisurelyrcxf/spermwhale/oracle"

	"github.com/leisurelyrcxf/spermwhale/cmd"

	"github.com/leisurelyrcxf/spermwhale/oracle/impl"

	"github.com/golang/glog"
)

func main() {
	cmd.RegisterPortFlags(9999)
	flagAllocInAdvance := flag.Uint64("alloc-in-advance", 1000, "pre-allocate size")
	flagLogical := flag.Bool("logical", false, "logical oracle")

	cmd.RegisterStoreFlags()
	flag.Parse()
	logicalOracle := *flagLogical

	var o oracle.Oracle
	if logicalOracle {
		var err error
		if o, err = logical.NewOracle(*flagAllocInAdvance, cmd.NewStore()); err != nil {
			glog.Fatalf("failed to create oracle: %v", err)
		}
	} else {
		o = physical.NewOracle()
	}
	server := impl.NewServer(*cmd.FlagPort, o)
	if err := server.Start(); err != nil {
		glog.Fatalf("failed to start: %v", err)
	}
	<-server.Done
}
