package main

import (
	"flag"
	"fmt"
	"net"

	"github.com/golang/glog"
	"github.com/leisurelyrcxf/spermwhale/proto/tabletpb"
	"github.com/leisurelyrcxf/spermwhale/tablet"
	"google.golang.org/grpc"
)

func main() {
	port := flag.Int("port", 9999, "port")
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		glog.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	tabletpb.RegisterKVServer(grpcServer, tablet.NewKV())
	if err := grpcServer.Serve(lis); err != nil {
		glog.Fatalf("serve failed: %v", err)
	}
}
