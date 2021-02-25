package main

import (
	"flag"

	"github.com/leisurelyrcxf/spermwhale/cmd/common"

	"github.com/leisurelyrcxf/spermwhale/oracle/impl/logical"

	"github.com/leisurelyrcxf/spermwhale/oracle/impl"

	"github.com/golang/glog"
)

func main() {
	port := flag.Int("port", 9999, "port")
	allocInAdvance := flag.Uint64("alloc-in-advance", 1000, "pre-allocate size")

	common.RegisterStoreFlags()
	flag.Parse()

	client := common.NewClient()
	oracle, err := logical.NewOracle(*allocInAdvance, client)
	if err != nil {
		glog.Fatalf("failed to create oracle: %v", err)
	}
	server := impl.NewServer(*port, oracle)
	if err := server.Start(); err != nil {
		glog.Fatalf("failed to start: %v", err)
	}
	<-server.Done
}
