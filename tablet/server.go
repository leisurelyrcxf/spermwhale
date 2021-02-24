package tablet

import (
	"fmt"
	"net"
	"time"

	"github.com/golang/glog"
	"github.com/leisurelyrcxf/spermwhale/mvcc/impl/memory"
	"github.com/leisurelyrcxf/spermwhale/proto/tabletpb"
	"google.golang.org/grpc"
)

type Server struct {
	grpcServer *grpc.Server

	port int
	Done chan struct{}
}

func NewServer(port int) *Server {
	grpcServer := grpc.NewServer()
	db := memory.NewDB()
	tabletpb.RegisterKVServer(grpcServer, NewKV(db))

	return &Server{
		grpcServer: grpcServer,

		port: port,
		Done: make(chan struct{}),
	}
}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
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
