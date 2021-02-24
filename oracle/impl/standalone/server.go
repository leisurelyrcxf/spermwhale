package standalone

import (
	"fmt"
	"net"
	"time"

	"github.com/leisurelyrcxf/spermwhale/models"

	"github.com/leisurelyrcxf/spermwhale/proto/oraclepb"

	"github.com/golang/glog"
	"google.golang.org/grpc"
)

type Server struct {
	grpcServer *grpc.Server

	allocInAdvance uint64
	client         models.Client
	port           int
	Done           chan struct{}
}

func NewServer(port int, allocInAdvance uint64, client models.Client) *Server {
	return &Server{
		grpcServer: grpc.NewServer(),

		allocInAdvance: allocInAdvance,
		client:         client,
		port:           port,
		Done:           make(chan struct{}),
	}
}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		glog.Errorf("failed to listen: %v", err)
		return err
	}

	oracle, err := NewOracle(s.allocInAdvance, s.client)
	if err != nil {
		return err
	}

	oraclepb.RegisterOracleServer(s.grpcServer, oracle)

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
