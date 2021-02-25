package tablet

import (
	"context"
	"fmt"
	"net"
	"time"

	fsclient "github.com/leisurelyrcxf/spermwhale/models/client/fs"

	"github.com/leisurelyrcxf/spermwhale/utils/network"

	"github.com/leisurelyrcxf/spermwhale/models"

	"github.com/leisurelyrcxf/spermwhale/proto/commonpb"

	"github.com/golang/glog"
	"github.com/leisurelyrcxf/spermwhale/mvcc/impl/memory"
	"github.com/leisurelyrcxf/spermwhale/proto/tabletpb"
	"google.golang.org/grpc"
)

type Stub struct {
	tabletpb.UnimplementedKVServer

	kvcc *KVCCPhysical
}

func (kv *Stub) Get(ctx context.Context, req *tabletpb.GetRequest) (*tabletpb.GetResponse, error) {
	vv, err := kv.kvcc.Get(ctx, req.Key, req.Version)
	if err != nil {
		return &tabletpb.GetResponse{
			Err: &commonpb.Error{
				Code: -1,
				Msg:  err.Error(),
			},
		}, nil
	}
	return &tabletpb.GetResponse{
		V: &commonpb.Value{
			Meta: &commonpb.ValueMeta{
				WriteIntent: vv.WriteIntent,
				Version:     vv.Version,
			},
			Val: vv.V,
		},
	}, nil
}

func (kv *Stub) Set(ctx context.Context, req *tabletpb.SetRequest) (*tabletpb.SetResponse, error) {
	err := kv.kvcc.Set(ctx, req.Key, req.Value.Value(), req.Opt.WriteOption())
	return &tabletpb.SetResponse{Err: commonpb.ToPBError(err)}, nil
}

type Server struct {
	gid   int
	store *models.Store

	grpcServer *grpc.Server
	port       int
	Done       chan struct{}
}

func NewServer(staleWriteThr, maxClockDrift time.Duration, gid int, port int, store *models.Store) *Server {
	grpcServer := grpc.NewServer()
	db := memory.NewDB()
	tabletpb.RegisterKVServer(grpcServer, &Stub{
		kvcc: NewKVCC(db, staleWriteThr, maxClockDrift),
	})

	return &Server{
		gid:   gid,
		store: store,

		grpcServer: grpcServer,
		port:       port,
		Done:       make(chan struct{}),
	}
}

func (s *Server) Start() error {
	if err := s.online(); err != nil {
		return err
	}

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

func (s *Server) online() error {
	localAddr, err := network.GetLocalAddr(s.targetAddr())
	if err != nil {
		return err
	}
	return s.store.UpdateGroup(
		&models.Group{
			Id:         s.gid,
			ServerAddr: localAddr,
		},
	)
}

func (s *Server) targetAddr() string {
	if _, isFsClient := s.store.Client().(*fsclient.Client); isFsClient {
		return "8.8.8.8:53"
	}
	return s.store.Client().AddrList()
}

func (s *Server) Stop() {
	s.grpcServer.Stop()
	<-s.Done
}
