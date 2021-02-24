package main

import (
	"flag"

	"github.com/golang/glog"
	"github.com/leisurelyrcxf/spermwhale/tablet"
)

func main() {
	port := flag.Int("port", 9999, "port")
	flag.Parse()

	server := tablet.NewServer(*port)
	if err := server.Start(); err != nil {
		glog.Fatalf("failed to start: %v", err)
	}
	<-server.Done
}
