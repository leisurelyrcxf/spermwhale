package kvcc

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/leisurelyrcxf/spermwhale/assert"

	"github.com/golang/glog"

	"google.golang.org/grpc"

	"github.com/leisurelyrcxf/spermwhale/errors"
	"github.com/leisurelyrcxf/spermwhale/proto/kvccpb"
	"github.com/leisurelyrcxf/spermwhale/topo"
	"github.com/leisurelyrcxf/spermwhale/types"
)

type Stub struct {
	kvcc types.KVCC
}

func (stub *Stub) Get(ctx context.Context, req *kvccpb.KVCCGetRequest) (*kvccpb.KVCCGetResponse, error) {
	opt := types.NewKVCCReadOptionFromPB(req.Opt)
	vv, err := stub.kvcc.Get(ctx, req.Key, opt)
	assert.Must(err != nil || !vv.IsEmpty())
	//noinspection ALL
	return &kvccpb.KVCCGetResponse{
		V:   vv.ToPB(),
		Err: errors.ToPBError(err),
	}, nil
}

func (stub *Stub) Set(ctx context.Context, req *kvccpb.KVCCSetRequest) (*kvccpb.KVCCSetResponse, error) {
	if err := req.Validate(); err != nil {
		return &kvccpb.KVCCSetResponse{Err: errors.ToPBError(err)}, nil
	}
	err := stub.kvcc.Set(ctx, req.Key, types.NewValueFromPB(req.Value), types.NewKVCCWriteOptionFromPB(req.Opt))
	return &kvccpb.KVCCSetResponse{Err: errors.ToPBError(err)}, nil
}

type Server struct {
	Port int

	grpcServer *grpc.Server
	stub       *Stub

	gid   int
	store *topo.Store

	Done chan struct{}
}

// readOnly indicate this is outer service
func NewServer(port int, db types.KV, cfg types.TxnConfig, gid int, store *topo.Store) *Server {
	return newServer(port, db, cfg, gid, store, false)
}

func NewServerForTesting(port int, db types.KV, cfg types.TxnConfig, gid int, store *topo.Store) *Server {
	return newServer(port, db, cfg, gid, store, true)
}

func newServer(port int, db types.KV, cfg types.TxnConfig, gid int, store *topo.Store, testing bool) *Server {
	grpcServer := grpc.NewServer()
	kvcc := newKVCC(db, cfg, testing)
	stub := &Stub{kvcc: kvcc}

	kvccpb.RegisterKVCCServer(grpcServer, stub)
	return &Server{
		Port: port,

		grpcServer: grpcServer,
		stub:       stub,

		gid:   gid,
		store: store,

		Done: make(chan struct{}),
	}
}

func (s *Server) Start() error {
	if err := s.online(); err != nil {
		return err
	}

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
			glog.Infof("kv server 0.0.0.0:%d terminated successfully", s.Port)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	return nil
}

func (s *Server) Close() (err error) {
	storeErr := s.store.Close()
	s.grpcServer.Stop()
	<-s.Done
	kvErr := s.stub.kvcc.Close()
	return errors.Wrap(storeErr, kvErr)
}

func (s *Server) online() error {
	localIP, err := s.store.GetLocalIP()
	if err != nil {
		return err
	}
	return s.store.UpdateGroup(
		&topo.Group{
			Id:         s.gid,
			ServerAddr: fmt.Sprintf("%s:%d", localIP, s.Port),
		},
	)
}
