package main

import (
	"flag"
	"time"

	client2 "github.com/leisurelyrcxf/spermwhale/models/client"

	"github.com/leisurelyrcxf/spermwhale/oracle/impl"

	"github.com/golang/glog"
)

func main() {
	port := flag.Int("port", 9999, "port")
	allocInAdvance := flag.Uint64("alloc-in-advance", 1000, "pre-allocate size")
	coordinator := flag.String("coordinator", "fs", "client type")
	coordinatorAddrList := flag.String("coordinator-addr-list", "/tmp", "coordinator addr list")
	coordinatorAuth := flag.String("coordinator-auth", "", "coordinator auth")
	flag.Parse()

	client, err := client2.NewClient(*coordinator, *coordinatorAddrList, *coordinatorAuth, time.Minute)
	if err != nil {
		glog.Fatalf("failed to create client: %v", err)
	}
	server := impl.NewServer(*port, *allocInAdvance, client)
	if err := server.Start(); err != nil {
		glog.Fatalf("failed to start: %v", err)
	}
	<-server.Done
}
