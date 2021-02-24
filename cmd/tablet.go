package main

import (
    "flag"
    "fmt"
    "github.com/golang/glog"
    "github.com/leisurelyrcxf/spermwhale/proto/kvpb"
    "google.golang.org/grpc"
    "net"
    "github.com/leisurelyrcxf/spermwhale/tablet"
)

func main() {
    port := flag.Int("port", 9999, "port")
    flag.Parse()
    lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
    if err != nil {
        glog.Fatalf("failed to listen: %v", err)
    }

    grpcServer := grpc.NewServer()
    kvpb.RegisterKVServer(grpcServer, &tablet.KV{})
    if err := grpcServer.Serve(lis); err != nil {
        glog.Fatalf("serve failed: %v", err)
    }
}