package main

import (
	"flag"

	"github.com/leisurelyrcxf/spermwhale/consts"

	"github.com/leisurelyrcxf/spermwhale/oracle/impl/physical"

	"github.com/leisurelyrcxf/spermwhale/oracle/impl/logical"

	"github.com/leisurelyrcxf/spermwhale/oracle"

	"github.com/leisurelyrcxf/spermwhale/cmd"

	"github.com/leisurelyrcxf/spermwhale/oracle/impl"

	"github.com/golang/glog"
)

func main() {
	cmd.RegisterPortFlags(consts.DefaultOracleServerPort)
	flagAllocInAdvance := flag.Uint64("alloc-in-advance", 1000, "pre-allocate size, only used when --logical is set")
	flagLogical := flag.Bool("logical", false, "logical oracle, do not use this because this is not compatible with txn framework!")
	flagLoosedPrecisionOracle := flag.Bool("loose", false, "loosed precision physical oracle, used for redis backend only")

	cmd.RegisterStoreFlags()
	flag.Parse()
	logicalOracle := *flagLogical

	store := cmd.NewStore()
	var o oracle.Oracle
	if logicalOracle {
		var err error
		if o, err = logical.NewOracle(*flagAllocInAdvance, store); err != nil {
			glog.Fatalf("failed to create oracle: %v", err)
		}
	} else {
		if *flagLoosedPrecisionOracle {
			o = physical.NewLoosedPrecisionOracle()
		} else {
			o = physical.NewOracle()
		}
	}
	server := impl.NewServer(*cmd.FlagPort, o, store)
	if err := server.Start(); err != nil {
		glog.Fatalf("failed to start: %v", err)
	}
	<-server.Done
}
