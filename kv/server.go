package kv

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/leisurelyrcxf/spermwhale/errors"

	"github.com/leisurelyrcxf/spermwhale/types"

	"github.com/golang/glog"
	"github.com/leisurelyrcxf/spermwhale/proto/tabletpb"
	"google.golang.org/grpc"
)

type Stub struct {
	kv           types.KV
	outerService bool
}

func (stub *Stub) Get(ctx context.Context, req *tabletpb.GetRequest) (*tabletpb.GetResponse, error) {
	opt := types.NewReadOptionFromPB(req.Opt)
	if stub.outerService {
		opt.SetNotUpdateTimestampCache()
	}
	vv, err := stub.kv.Get(ctx, req.Key, opt)
	if err != nil {
		return &tabletpb.GetResponse{
			Err: errors.ToPBError(err),
		}, nil
	}
	return &tabletpb.GetResponse{
		V: vv.ToPB(),
	}, nil
}

func (stub *Stub) Set(ctx context.Context, req *tabletpb.SetRequest) (*tabletpb.SetResponse, error) {
	if stub.outerService {
		return &tabletpb.SetResponse{Err: errors.ToPBError(errors.Annotatef(errors.ErrNotSupported, "outer kv service is readonly"))}, nil
	}
	if err := req.Validate(); err != nil {
		return &tabletpb.SetResponse{Err: errors.ToPBError(err)}, nil
	}
	err := stub.kv.Set(ctx, req.Key, types.NewValueFromPB(req.Value), types.NewWriteOptionFromPB(req.Opt))
	return &tabletpb.SetResponse{Err: errors.ToPBError(err)}, nil
}

type Server struct {
	Port int

	grpcServer  *grpc.Server
	beforeStart func() error

	Done chan struct{}
}

// outerService indicate this is outer service
func NewServer(port int, kv types.KV, outerService bool) Server {
	grpcServer := grpc.NewServer()
	tabletpb.RegisterKVServer(grpcServer, &Stub{kv: kv, outerService: outerService})

	return Server{
		Port:       port,
		grpcServer: grpcServer,

		Done: make(chan struct{}),
	}
}

func (s *Server) SetBeforeStart(beforeStart func() error) *Server {
	s.beforeStart = beforeStart
	return s
}

func (s *Server) Start() error {
	if s.beforeStart != nil {
		if err := s.beforeStart(); err != nil {
			return err
		}
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		glog.Errorf("failed to listen: %v", err)
		return err
	}

	go func() {
		defer close(s.Done)

		if err := s.grpcServer.Serve(lis); err != nil {
			glog.Errorf("tablet serve failed: %v", err)
		} else {
			glog.Infof("tablet server terminated successfully")
		}
	}()

	time.Sleep(100 * time.Millisecond)
	return nil
}

func (s *Server) Stop() {
	s.grpcServer.Stop()
	<-s.Done
}
