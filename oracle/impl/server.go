package impl

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/leisurelyrcxf/spermwhale/topo"

	"github.com/leisurelyrcxf/spermwhale/consts"
	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"

	"github.com/leisurelyrcxf/spermwhale/oracle"

	"github.com/leisurelyrcxf/spermwhale/proto/oraclepb"

	"github.com/golang/glog"
	"google.golang.org/grpc"
)

type Stub struct {
	oraclepb.UnimplementedOracleServer

	delegate oracle.Oracle
}

func (o *Stub) Fetch(ctx context.Context, _ *oraclepb.FetchRequest) (*oraclepb.FetchResponse, error) {
	ts, err := o.delegate.FetchTimestamp(ctx)
	if err != nil {
		return &oraclepb.FetchResponse{
			Err: &commonpb.Error{
				Code: consts.ErrCodeUnknown,
				Msg:  err.Error(),
			},
		}, nil
	}
	return &oraclepb.FetchResponse{Ts: ts}, nil
}

type Server struct {
	grpcServer *grpc.Server
	stub       *Stub
	store      *topo.Store

	port int
	Done chan struct{}
}

func NewServer(port int, oracle oracle.Oracle, store *topo.Store) *Server {
	s := &Server{
		grpcServer: grpc.NewServer(),
		stub: &Stub{
			delegate: oracle,
		},
		store: store,
		port:  port,
		Done:  make(chan struct{}),
	}
	oraclepb.RegisterOracleServer(s.grpcServer, s.stub)
	return s
}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		glog.Errorf("failed to listen: %v", err)
		return err
	}

	if err := s.online(); err != nil {
		glog.Errorf("online oracle server failed: %v", err)
		return err
	}

	go func() {
		defer close(s.Done)

		if err := s.grpcServer.Serve(lis); err != nil {
			glog.Errorf("oracle serve failed: %v", err)
		} else {
			glog.Infof("oracle server terminated successfully")
		}
	}()

	time.Sleep(100 * time.Millisecond)
	return nil
}

func (s *Server) Stop() {
	s.grpcServer.Stop()
	<-s.Done
}

func (s *Server) online() error {
	localIP, err := s.store.GetLocalIP()
	if err != nil {
		return err
	}
	return s.store.UpdateOracle(
		&topo.Oracle{
			ServerAddr: fmt.Sprintf("%s:%d", localIP, s.port),
		},
	)
}
