package kv

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/golang/glog"

	"google.golang.org/grpc"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/proto/kvpb"
	"github.com/leisurelyrcxf/spermwhale/types"
)

type Stub struct {
	kv types.KV
}

func (stub *Stub) Get(ctx context.Context, req *kvpb.KVGetRequest) (*kvpb.KVGetResponse, error) {
	opt := types.NewKVReadOptionFromPB(req.Opt)
	vv, err := stub.kv.Get(ctx, req.Key, opt)
	//noinspection ALL
	return &kvpb.KVGetResponse{
		V:   vv.ToPB(),
		Err: errors.ToPBError(err),
	}, nil
}

func (stub *Stub) Set(ctx context.Context, req *kvpb.KVSetRequest) (*kvpb.KVSetResponse, error) {
	if err := req.Validate(); err != nil {
		return &kvpb.KVSetResponse{Err: errors.ToPBError(err)}, nil
	}
	err := stub.kv.Set(ctx, req.Key, types.NewValueFromPB(req.Value), types.NewKVWriteOptionFromPB(req.Opt))
	return &kvpb.KVSetResponse{Err: errors.ToPBError(err)}, nil
}

type Server struct {
	Port int

	grpcServer *grpc.Server
	stub       *Stub

	Done chan struct{}
}

// outerService indicate this is outer service
func NewServer(port int, kv types.KV) Server {
	grpcServer := grpc.NewServer()
	stub := &Stub{kv: kv}

	kvpb.RegisterKVServer(grpcServer, stub)
	return Server{
		Port:       port,
		grpcServer: grpcServer,
		stub:       stub,

		Done: make(chan struct{}),
	}
}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		glog.Errorf("failed to listen: %v", err)
		return err
	}

	go func() {
		defer close(s.Done)

		if err := s.grpcServer.Serve(lis); err != nil {
			glog.Errorf("kv server 0.0.0.0:%d serve failed: %v", s.Port, err)
		} else {
			glog.V(6).Infof("kv server 0.0.0.0:%d terminated successfully", s.Port)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	return nil
}

func (s *Server) Close() error {
	s.grpcServer.Stop()
	<-s.Done
	return s.stub.kv.Close()
}
